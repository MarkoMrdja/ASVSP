from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 2g"
    " --executor-memory 3g"
    " --executor-cores 2"
    " --num-executors 2"
    " --conf spark.sql.shuffle.partitions=8"
    " --conf spark.default.parallelism=8"
    " --conf spark.memory.fraction=0.6"
    " --conf spark.memory.storageFraction=0.3"
    " --conf spark.sql.parquet.writeLegacyFormat=true"
    " --conf spark.sql.adaptive.enabled=true"
    " --conf spark.sql.adaptive.coalescePartitions.enabled=true"
)

SPARK_SUBMIT_PG = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 1g"
    " --executor-memory 2g"
    " --executor-cores 2"
    " --num-executors 2"
    " --conf spark.sql.shuffle.partitions=8"
    " --conf spark.sql.adaptive.enabled=true"
    " --jars /stream/consumer/postgresql-42.5.1.jar"
)

with DAG(
    dag_id="asvsp_batch_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["asvsp", "batch"],
) as dag:

    spark_load_hourly = BashOperator(
        task_id="spark_load_hourly",
        bash_command=f"{SPARK_SUBMIT} /batch/load_hourly.py",
    )

    spark_daily_aggregation = BashOperator(
        task_id="spark_daily_aggregation",
        bash_command=f"{SPARK_SUBMIT} /batch/daily_aggregation.py",
    )

    spark_monthly_aggregation = BashOperator(
        task_id="spark_monthly_aggregation",
        bash_command=f"{SPARK_SUBMIT} /batch/monthly_aggregation.py",
    )

    spark_annual_aggregation = BashOperator(
        task_id="spark_annual_aggregation",
        bash_command=f"{SPARK_SUBMIT} /batch/annual_aggregation.py",
    )

    spark_baselines = BashOperator(
        task_id="spark_baselines",
        bash_command=f"{SPARK_SUBMIT} /batch/baselines.py",
    )

    spark_export_postgres = BashOperator(
        task_id="spark_export_to_postgres",
        bash_command=f"{SPARK_SUBMIT_PG} /batch/export_to_postgres.py",
    )

    (
        spark_load_hourly
        >> spark_daily_aggregation
        >> spark_monthly_aggregation
        >> spark_annual_aggregation
        >> spark_baselines
        >> spark_export_postgres
    )
