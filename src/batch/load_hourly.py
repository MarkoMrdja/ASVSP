import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

WAREHOUSE = "/hive/warehouse"

conf = SparkConf().setAppName("Load Hourly Measurements").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", WAREHOUSE)
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

OUTPUT_PATH = f"{HDFS_NAMENODE}{WAREHOUSE}/hourly_measurements"

pollutants = ["O3", "PM25", "PM10", "NO2", "SO2", "CO"]

for i, pollutant_name in enumerate(pollutants):
    print(f"  Processing {pollutant_name}...")

    csv_path = f"{HDFS_NAMENODE}/data/raw/{pollutant_name}/"
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_path)

    # Normalize column names
    for col_name in df.columns:
        normalized = col_name.replace(" ", "_")
        if normalized != col_name:
            df = df.withColumnRenamed(col_name, normalized)

    df = df.withColumn(
        "site_id",
        (F.col("State_Code").cast("int") * 10000000 +
         F.col("County_Code").cast("int") * 10000 +
         F.col("Site_Num").cast("int")).cast("long")
    )

    df = df.withColumn("hour_local", F.substring(F.col("Time_Local"), 1, 2).cast("int"))

    # Adjust NO2 and SO2 units
    if pollutant_name in ("NO2", "SO2"):
        raw_measurement = F.col("Sample_Measurement").cast("double") / 1000.0
    else:
        raw_measurement = F.col("Sample_Measurement").cast("double")

    df = df.select(
        F.col("site_id"),
        F.col("State_Code").cast("int").alias("state_code"),
        F.col("County_Code").cast("int").alias("county_code"),
        F.col("Site_Num").cast("int").alias("site_num"),
        F.col("State_Name").alias("state_name"),
        F.lit(pollutant_name).alias("pollutant"),
        F.to_date(F.col("Date_Local"), "yyyy-MM-dd").alias("date_local"),
        F.col("hour_local"),
        raw_measurement.alias("measurement"),
        F.when(
            F.col("Units_of_Measure") == "Parts per billion",
            F.lit("Parts per million")
        ).otherwise(F.col("Units_of_Measure")).alias("units")
    )

    write_mode = "overwrite" if i == 0 else "append"
    df.write.mode(write_mode).partitionBy("pollutant").parquet(OUTPUT_PATH)

# Register external table in Hive metastore
spark.sql("DROP TABLE IF EXISTS hourly_measurements")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS hourly_measurements (
        site_id BIGINT,
        state_code INT,
        county_code INT,
        site_num INT,
        state_name STRING,
        date_local DATE,
        hour_local INT,
        measurement DOUBLE,
        units STRING
    )
    PARTITIONED BY (pollutant STRING)
    STORED AS PARQUET
    LOCATION '{OUTPUT_PATH}'
""")

spark.sql("MSCK REPAIR TABLE hourly_measurements")

print("Hourly measurements loaded successfully!")
