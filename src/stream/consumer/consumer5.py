from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs, avg, col, dayofweek, dense_rank, from_json, hour, month,
    round as spark_round, to_timestamp, window,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)
from pyspark.sql.window import Window

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

spark = (
    SparkSession.builder
    .appName("SQ5 - State Ranking")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

aq = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", "air_quality_stream")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), aq_schema).alias("d"))
    .select("d.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withWatermark("timestamp", "2 hours")
)

baselines_raw = (
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

hist_rank_win = Window.partitionBy("bl_hour", "bl_month", "bl_is_weekend").orderBy(col("baseline_avg").desc())
baselines = baselines_raw.withColumn("historical_rank", dense_rank().over(hist_rank_win))

agg = (
    aq
    .withColumn("hr", hour("timestamp"))
    .withColumn("mo", month("timestamp"))
    .withColumn("is_wknd", dayofweek("timestamp").isin([1, 7]))
    .groupBy(window("timestamp", "1 hour"), col("state"), col("hr"), col("mo"), col("is_wknd"))
    .agg(avg("pm25").alias("current_avg"))
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    rank_win = Window.partitionBy("window_start").orderBy(col("current_avg").desc())

    ranked = (
        batch_df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .join(
            baselines.select("state_name", "bl_hour", "bl_month", "bl_is_weekend", "historical_rank"),
            (batch_df.state == baselines.state_name)
            & (batch_df.hr == baselines.bl_hour)
            & (batch_df.mo == baselines.bl_month)
            & (batch_df.is_wknd == baselines.bl_is_weekend),
            "left",
        )
        .withColumn("current_rank", dense_rank().over(rank_win))
        .withColumn("is_unusual", spark_abs(col("current_rank") - col("historical_rank")) > 10)
        .select(
            "window_start",
            "window_end",
            "state",
            spark_round(col("current_avg"), 2).alias("current_avg"),
            "current_rank",
            col("historical_rank"),
            "is_unusual",
        )
    )

    ranked.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq5_state_ranking") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print("saved to db!")


query = (
    agg.writeStream
    .outputMode("update")
    .trigger(once=True)
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq5")
    .start()
)

query.awaitTermination()
