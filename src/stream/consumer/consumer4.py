from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, dayofweek, from_json, hour, month,
    round as spark_round, to_timestamp, when, window,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

KAFKA_BROKERS = "kafka1:19092,kafka2:19093"
JDBC_URL = "jdbc:postgresql://pg:5432/streaming"
JDBC_DRIVER = "org.postgresql.Driver"

aq_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("aqi", IntegerType()),
    StructField("aqi_category", StringType()),
    StructField("dominant_pollutant", StringType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("co", DoubleType()),
])

wx_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("wind_direction_deg", IntegerType()),
    StructField("pressure_hpa", DoubleType()),
    StructField("cloud_cover_pct", IntegerType()),
    StructField("precipitation_mm", DoubleType()),
])

spark = (
    SparkSession.builder
    .appName("SQ4 - Ventilation Coefficient")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", "air_quality_stream,weather_stream")
    .option("startingOffsets", "earliest")
    .load()
    .select(
        col("topic"),
        col("value").cast("string").alias("value_str"),
    )
)

baselines = (
    spark.table("hourly_baselines")
    .filter(col("pollutant") == "PM25")
    .select(
        col("state_name"),
        col("hour").alias("bl_hour"),
        col("month").alias("bl_month"),
        col("is_weekend").alias("bl_is_weekend"),
        col("baseline_avg"),
    )
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    aq_raw = batch_df.filter(col("topic") == "air_quality_stream")
    wx_raw = batch_df.filter(col("topic") == "weather_stream")

    aq = (
        aq_raw
        .select(from_json(col("value_str"), aq_schema).alias("d"))
        .select("d.*")
        .withColumn("timestamp", to_timestamp("timestamp"))
    )

    wx = (
        wx_raw
        .select(from_json(col("value_str"), wx_schema).alias("d"))
        .select("d.*")
        .withColumn("timestamp", to_timestamp("timestamp"))
    )

    if aq.rdd.isEmpty() or wx.rdd.isEmpty():
        return

    aq_agg = (
        aq
        .withColumn("hr", hour("timestamp"))
        .withColumn("mo", month("timestamp"))
        .withColumn("is_wknd", dayofweek("timestamp").isin([1, 7]))
        .groupBy(window("timestamp", "1 hour"), "city", "state", "hr", "mo", "is_wknd")
        .agg(avg("pm25").alias("avg_pm25"))
    )

    wx_agg = (
        wx
        .groupBy(window("timestamp", "1 hour"), "city", "state")
        .agg(
            avg("wind_speed_kmh").alias("avg_wind"),
            avg("cloud_cover_pct").alias("avg_cloud"),
        )
    )

    joined = (
        aq_agg.alias("a").join(
            wx_agg.alias("w"),
            (col("a.city") == col("w.city"))
            & (col("a.state") == col("w.state"))
            & (col("a.window") == col("w.window")),
            "inner",
        )
        .select(
            col("a.window").alias("window"),
            col("a.city").alias("city"),
            col("a.state").alias("state"),
            col("a.hr").alias("hr"),
            col("a.mo").alias("mo"),
            col("a.is_wknd").alias("is_wknd"),
            col("a.avg_pm25").alias("avg_pm25"),
            col("w.avg_wind").alias("avg_wind"),
            col("w.avg_cloud").alias("avg_cloud"),
        )
    )

    result = (
        joined.join(
            baselines,
            (col("state") == baselines.state_name)
            & (col("hr") == baselines.bl_hour)
            & (col("mo") == baselines.bl_month)
            & (col("is_wknd") == baselines.bl_is_weekend),
            "left",
        )
        .withColumn(
            "ventilation_score",
            spark_round(col("avg_wind") * (1.0 + col("avg_cloud") / 100.0), 2),
        )
        .withColumn("above_baseline", col("avg_pm25") > col("baseline_avg"))
        .withColumn(
            "status",
            when((col("ventilation_score") > 30) & ~col("above_baseline"), "self_cleaning")
            .when((col("ventilation_score") > 30) & col("above_baseline"), "dispersing")
            .when((col("ventilation_score") < 15) & col("above_baseline"), "accumulating")
            .when((col("ventilation_score") < 15) & ~col("above_baseline"), "stable_low")
            .otherwise("neutral"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("state"),
            spark_round("avg_pm25", 2).alias("avg_pm25"),
            spark_round("baseline_avg", 2).alias("baseline_avg"),
            spark_round("avg_wind", 1).alias("avg_wind_kmh"),
            spark_round("avg_cloud", 1).alias("avg_cloud_pct"),
            col("ventilation_score"),
            col("status"),
        )
    )

    result.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq4_ventilation") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print("saved to db!")


query = (
    raw.writeStream
    .outputMode("append")
    .trigger(once=True)
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq4")
    .start()
)

query.awaitTermination()
