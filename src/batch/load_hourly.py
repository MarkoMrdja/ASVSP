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
years = list(range(2017, 2026))

first_write = True
for pollutant_name in pollutants:
    for year in years:
        csv_path = f"{HDFS_NAMENODE}/data/raw/{pollutant_name}/hourly_{pollutant_name}_{year}.csv"
        print(f"  Processing {pollutant_name} {year}...")

        df = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_path)

        # Normalize column names
        for col_name in df.columns:
            normalized = col_name.replace(" ", "_")
            if normalized != col_name:
                df = df.withColumnRenamed(col_name, normalized)

        df = df.withColumn("hour_local", F.substring(F.col("Time_Local"), 1, 2).cast("int"))

        if pollutant_name in ("NO2", "SO2"):
            raw_measurement = F.col("Sample_Measurement").cast("double") / 1000.0
        else:
            raw_measurement = F.col("Sample_Measurement").cast("double")

        # Fix truncated state names from older EPA CSV files (9-char column limit)
        df = df.withColumn(
            "State_Name",
            F.when(F.col("State_Name") == "Californi",  F.lit("California"))
             .when(F.col("State_Name") == "Connectic",  F.lit("Connecticut"))
             .when(F.col("State_Name") == "Country O",  F.lit("Country Of Mexico"))
             .when(F.col("State_Name") == "District",   F.lit("District Of Columbia"))
             .when(F.col("State_Name") == "Massachus",  F.lit("Massachusetts"))
             .when(F.col("State_Name") == "Mississip",  F.lit("Mississippi"))
             .when(F.col("State_Name") == "New Hamps",  F.lit("New Hampshire"))
             .when(F.col("State_Name") == "New Jerse",  F.lit("New Jersey"))
             .when(F.col("State_Name") == "New Mexic",  F.lit("New Mexico"))
             .when(F.col("State_Name") == "North Car",  F.lit("North Carolina"))
             .when(F.col("State_Name") == "North Dak",  F.lit("North Dakota"))
             .when(F.col("State_Name") == "Pennsylva",  F.lit("Pennsylvania"))
             .when(F.col("State_Name") == "Puerto Ri",  F.lit("Puerto Rico"))
             .when(F.col("State_Name") == "Rhode Isl",  F.lit("Rhode Island"))
             .when(F.col("State_Name") == "South Car",  F.lit("South Carolina"))
             .when(F.col("State_Name") == "South Dak",  F.lit("South Dakota"))
             .when(F.col("State_Name") == "Washingto",  F.lit("Washington"))
             .when(F.col("State_Name") == "West Virg",  F.lit("West Virginia"))
             .otherwise(F.col("State_Name"))
        )

        df = df.select(
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

        write_mode = "overwrite" if first_write else "append"
        first_write = False
        df.write.mode(write_mode).partitionBy("pollutant").parquet(OUTPUT_PATH)

# Register external table in Hive metastore
spark.sql("DROP TABLE IF EXISTS hourly_measurements")

spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS hourly_measurements (
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
