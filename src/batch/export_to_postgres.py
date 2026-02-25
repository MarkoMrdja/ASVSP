import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("Batch Export to PostgreSQL").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

JDBC_URL = "jdbc:postgresql://pg:5432/streaming"
JDBC_DRIVER = "org.postgresql.Driver"

EXPORTS = [
    ("/queries/q01_state_ranking_yoy.sql",     "batch_q01_state_ranking"),
    ("/queries/q03_cumulative_exceedances.sql", "batch_q03_exceedances"),
    ("/queries/q08_covid_impact.sql",           "batch_q08_covid_impact"),
    ("/queries/q09_weekend_effect.sql",         "batch_q09_weekend_effect"),
]

for sql_file, table_name in EXPORTS:
    print(f"Running {sql_file} -> {table_name} ...")
    with open(sql_file) as f:
        query = f.read().strip()
    df = spark.sql(query)
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    print(f"  Done: {table_name}")

spark.stop()
