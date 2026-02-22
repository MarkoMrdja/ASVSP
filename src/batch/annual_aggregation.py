import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

WAREHOUSE = "/hive/warehouse"

conf = SparkConf().setAppName("Annual Aggregation").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", WAREHOUSE)
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

monthly_df = spark.table("monthly_state_measurements")

annual_df = monthly_df.groupBy("state_name", "pollutant", "year").agg(
    F.avg("monthly_avg").alias("annual_avg"),
    F.max("monthly_max").alias("annual_max"),
    F.sum("exceedance_days").cast("int").alias("total_exceedance_days")
)

yoy_window = Window.partitionBy("state_name", "pollutant").orderBy("year")

annual_df = annual_df.withColumn( "prev_year_avg", F.lag("annual_avg").over(yoy_window))

annual_df = annual_df.withColumn("yoy_avg_change", F.round(F.col("annual_avg") - F.col("prev_year_avg"), 4))

annual_df = annual_df.withColumn(
    "yoy_pct_change",
    F.round((F.col("annual_avg") - F.col("prev_year_avg")) / F.col("prev_year_avg") * 100, 2)
)

cumulative_window = Window.partitionBy("state_name", "pollutant") \
    .orderBy("year") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

annual_df = annual_df.withColumn(
    "cumulative_exceedance_days",
    F.sum("total_exceedance_days").over(cumulative_window).cast("int")
)

OUTPUT_PATH = f"{HDFS_NAMENODE}{WAREHOUSE}/annual_state_measurements"

annual_df.write.mode("overwrite").partitionBy("pollutant").parquet(OUTPUT_PATH)

# Register/refresh external table in Hive metastore
spark.sql("DROP TABLE IF EXISTS annual_state_measurements")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS annual_state_measurements (
        state_name STRING,
        year INT,
        annual_avg DOUBLE,
        annual_max DOUBLE,
        total_exceedance_days INT,
        prev_year_avg DOUBLE,
        yoy_avg_change DOUBLE,
        yoy_pct_change DOUBLE,
        cumulative_exceedance_days INT
    )
    PARTITIONED BY (pollutant STRING)
    STORED AS PARQUET
    LOCATION '{OUTPUT_PATH}'
""")

spark.sql("MSCK REPAIR TABLE annual_state_measurements")

print("Annual state measurements aggregated successfully!")
