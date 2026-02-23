"""
SQ3 — Per 1-hour window: top 5 cities with highest avg PM2.5.
Stateless: groupBy window + city + state, avg pm25, rank, keep top 5.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, dense_rank, from_json, round as spark_round, to_timestamp, window,
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
    .appName("SQ3 - Top 5 Worst Cities PM2.5")
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
    .groupBy(window("timestamp", "1 hour"), col("city"), col("state"))
    .agg(avg("pm25").alias("avg_pm25"))
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    rank_win = Window.partitionBy("window_start").orderBy(col("avg_pm25").desc())

    top5 = (
        batch_df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("rank", dense_rank().over(rank_win))
        .filter(col("rank") <= 5)
        .select(
            "window_start",
            "window_end",
            "city",
            "state",
            spark_round(col("avg_pm25"), 2).alias("avg_pm25"),
            "rank",
        )
    )

    top5.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq3_worst_cities") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print("saved to db!")


query = (
    windowed.writeStream
    .outputMode("update")
    .trigger(once=True)
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq3")
    .start()
)

query.awaitTermination()
