import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

WAREHOUSE = "/hive/warehouse"

conf = SparkConf().setAppName("Monthly Aggregation").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", WAREHOUSE)
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

daily_df = spark.table("daily_state_measurements")

# Flag based on NAAQS thresholds
daily_df = daily_df.withColumn(
    "exceeds_naaqs",
    F.when((F.col("pollutant") == "PM25") & (F.col("daily_avg") > 35.0), True)
     .when((F.col("pollutant") == "PM10") & (F.col("daily_avg") > 150.0), True)
     .when((F.col("pollutant") == "O3") & (F.col("daily_avg") > 0.070), True)
     .when((F.col("pollutant") == "NO2") & (F.col("daily_avg") > 0.100), True)
     .when((F.col("pollutant") == "SO2") & (F.col("daily_avg") > 0.075), True)
     .when((F.col("pollutant") == "CO") & (F.col("daily_avg") > 9.0), True)
     .otherwise(False)
)

monthly_df = daily_df.groupBy("state_name", "pollutant", "year", "month").agg(
    F.avg("daily_avg").alias("monthly_avg"),
    F.max("daily_max").alias("monthly_max"),
    F.count("date_local").cast("int").alias("days_with_data"),
    F.sum(F.when(F.col("exceeds_naaqs"), 1).otherwise(0)).cast("int").alias("exceedance_days")
)

mom_window = Window.partitionBy("state_name", "pollutant").orderBy("year", "month")

monthly_df = monthly_df.withColumn(
    "prev_month_avg", F.lag("monthly_avg").over(mom_window)
)
monthly_df = monthly_df.withColumn(
    "mom_pct_change",
    F.round((F.col("monthly_avg") - F.col("prev_month_avg")) / F.col("prev_month_avg") * 100, 2)
)

yoy_month_window = Window.partitionBy("state_name", "pollutant", "month").orderBy("year")

monthly_df = monthly_df.withColumn( "same_month_prev_year_avg", F.lag("monthly_avg").over(yoy_month_window)
                                   )
monthly_df = monthly_df.withColumn( "yoy_month_change", F.round(F.col("monthly_avg") - F.col("same_month_prev_year_avg"), 4))

OUTPUT_PATH = f"{HDFS_NAMENODE}{WAREHOUSE}/monthly_state_measurements"

monthly_df.write.mode("overwrite").partitionBy("pollutant").parquet(OUTPUT_PATH)

# Register/refresh external table in Hive metastore
spark.sql("DROP TABLE IF EXISTS monthly_state_measurements")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS monthly_state_measurements (
        state_name STRING,
        year INT,
        month INT,
        monthly_avg DOUBLE,
        monthly_max DOUBLE,
        days_with_data INT,
        exceedance_days INT,
        prev_month_avg DOUBLE,
        mom_pct_change DOUBLE,
        same_month_prev_year_avg DOUBLE,
        yoy_month_change DOUBLE
    )
    PARTITIONED BY (pollutant STRING)
    STORED AS PARQUET
    LOCATION '{OUTPUT_PATH}'
""")

spark.sql("MSCK REPAIR TABLE monthly_state_measurements")

print("Monthly state measurements aggregated successfully!")
