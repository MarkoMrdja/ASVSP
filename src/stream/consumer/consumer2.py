from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, from_json, greatest, lag, round as spark_round,
    to_timestamp, when, window,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)
from pyspark.sql.window import Window as SparkWindow

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
    .appName("SQ2 - Dominant Pollutant Trend")
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

windowed = (
    aq
    .groupBy(window("timestamp", "3 hours", "1 hour"), col("state"))
    .agg(
        avg("pm25").alias("avg_pm25"),
        avg("pm10").alias("avg_pm10"),
        avg("o3").alias("avg_o3"),
        avg("no2").alias("avg_no2"),
        avg("so2").alias("avg_so2"),
        avg("co").alias("avg_co"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("state"),
        spark_round("avg_pm25", 2).alias("avg_pm25"),
        spark_round("avg_pm10", 2).alias("avg_pm10"),
        spark_round("avg_o3", 2).alias("avg_o3"),
        spark_round("avg_no2", 2).alias("avg_no2"),
        spark_round("avg_so2", 2).alias("avg_so2"),
        spark_round("avg_co", 2).alias("avg_co"),
    )
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Identify dominant pollutant: the one with the highest avg concentration
    # across all 6 pollutants in this window
    with_dominant = (
        batch_df
        .withColumn(
            "max_conc",
            greatest("avg_pm25", "avg_pm10", "avg_o3", "avg_no2", "avg_so2", "avg_co"),
        )
        .withColumn(
            "dominant_pollutant",
            when(col("max_conc") == col("avg_pm25"), "pm25")
            .when(col("max_conc") == col("avg_pm10"), "pm10")
            .when(col("max_conc") == col("avg_o3"), "o3")
            .when(col("max_conc") == col("avg_no2"), "no2")
            .when(col("max_conc") == col("avg_so2"), "so2")
            .otherwise("co"),
        )
    )

    lag_win = SparkWindow.partitionBy("state").orderBy("window_start")

    with_trend = (
        with_dominant
        .withColumn("prev_max_conc", lag("max_conc", 1).over(lag_win))
        .withColumn(
            "trend",
            when(col("prev_max_conc").isNull(), "stable")
            .when(
                (col("max_conc") - col("prev_max_conc")) / col("prev_max_conc") > 0.10,
                "rising",
            )
            .when(
                (col("prev_max_conc") - col("max_conc")) / col("prev_max_conc") > 0.10,
                "falling",
            )
            .otherwise("stable"),
        )
        .select(
            "window_start",
            "window_end",
            "state",
            "dominant_pollutant",
            spark_round("max_conc", 2).alias("dominant_conc"),
            "trend",
            "avg_pm25", "avg_pm10", "avg_o3", "avg_no2", "avg_so2", "avg_co",
        )
    )

    with_trend.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq2_dominant_pollutant") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print("saved to db!")


query = (
    windowed.writeStream
    .outputMode("update")
    .trigger(once=True)
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq2")
    .start()
)

query.awaitTermination()
