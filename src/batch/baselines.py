import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

WAREHOUSE = "/hive/warehouse"

conf = SparkConf().setAppName("Baselines Calculation").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", WAREHOUSE)
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

OUTPUT_PATH = f"{HDFS_NAMENODE}{WAREHOUSE}/hourly_baselines"

hourly_df = spark.table("hourly_measurements")

hourly_df = hourly_df.withColumn("month", F.month("date_local"))
hourly_df = hourly_df.withColumn("day_of_week", F.dayofweek("date_local"))
hourly_df = hourly_df.withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))

baselines_df = hourly_df.groupBy(
    "state_name", "pollutant", "month", "hour_local", "is_weekend"
).agg(
    F.avg("measurement").alias("baseline_avg"),
    F.stddev("measurement").alias("baseline_stddev")
)

baselines_df = baselines_df.withColumnRenamed("hour_local", "hour")

baselines_df.write.mode("overwrite").partitionBy("pollutant").parquet(OUTPUT_PATH)

spark.sql("DROP TABLE IF EXISTS hourly_baselines")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS hourly_baselines (
        state_name STRING,
        month INT,
        hour INT,
        is_weekend BOOLEAN,
        baseline_avg DOUBLE,
        baseline_stddev DOUBLE
    )
    PARTITIONED BY (pollutant STRING)
    STORED AS PARQUET
    LOCATION '{OUTPUT_PATH}'
""")

spark.sql("MSCK REPAIR TABLE hourly_baselines")

print("Hourly baselines calculated successfully!")
