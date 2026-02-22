import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

WAREHOUSE = "/hive/warehouse"

conf = SparkConf().setAppName("Daily Aggregation").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", WAREHOUSE)
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

hourly_df = spark.table("hourly_measurements")

daily_df = hourly_df.groupBy("state_name", "pollutant", "date_local").agg(
    F.avg("measurement").alias("daily_avg"),
    F.max("measurement").alias("daily_max"),
    F.count("measurement").cast("int").alias("measurement_count")
)

daily_df = daily_df.withColumn("day_of_week", F.dayofweek("date_local"))
daily_df = daily_df.withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
daily_df = daily_df.withColumn("year", F.year("date_local"))
daily_df = daily_df.withColumn("month", F.month("date_local"))

OUTPUT_PATH = f"{HDFS_NAMENODE}{WAREHOUSE}/daily_state_measurements"

daily_df.write.mode("overwrite").partitionBy("pollutant").parquet(OUTPUT_PATH)

# Register/refresh external table in Hive metastore
spark.sql("DROP TABLE IF EXISTS daily_state_measurements")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS daily_state_measurements (
        state_name STRING,
        date_local DATE,
        daily_avg DOUBLE,
        daily_max DOUBLE,
        measurement_count INT,
        is_weekend BOOLEAN,
        day_of_week INT,
        year INT,
        month INT
    )
    PARTITIONED BY (pollutant STRING)
    STORED AS PARQUET
    LOCATION '{OUTPUT_PATH}'
""")

spark.sql("MSCK REPAIR TABLE daily_state_measurements")

print("Daily state measurements aggregated successfully!")
