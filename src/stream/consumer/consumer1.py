from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, dayofweek, from_json, hour, month,
    round as spark_round, to_timestamp, window,
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

spark = (
    SparkSession.builder
    .appName("SQ1 - PM2.5 vs Baseline")
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
    .cache()
)
baselines.count()  # materialise cache before stream starts

aq_state = (
    aq
    .withColumn("hr", hour("timestamp"))
    .withColumn("mo", month("timestamp"))
    .withColumn("is_wknd", dayofweek("timestamp").isin([1, 7]))
    .groupBy(window("timestamp", "1 hour"), col("state"), col("hr"), col("mo"), col("is_wknd"))
    .agg(avg("pm25").alias("avg_pm25"))
)

result = (
    aq_state.join(
        baselines,
        (aq_state.state == baselines.state_name)
        & (aq_state.hr == baselines.bl_hour)
        & (aq_state.mo == baselines.bl_month)
        & (aq_state.is_wknd == baselines.bl_is_weekend),
        "left",
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("state"),
        spark_round(col("avg_pm25"), 2).alias("avg_pm25"),
        spark_round(col("baseline_avg"), 2).alias("baseline_avg"),
        spark_round(col("avg_pm25") / col("baseline_avg") * 100, 1).alias("pct_of_baseline"),
    )
)


def save_to_postgres(df, batch_id):
    if df.rdd.isEmpty():
        return
    df.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq1_vs_baseline") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print("saved to db!")


query = (
    result.writeStream
    .outputMode("update")
    .trigger(once=True)
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq1")
    .start()
)

query.awaitTermination()
