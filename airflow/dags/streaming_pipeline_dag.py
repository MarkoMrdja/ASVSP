from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT_STREAMING = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 1g"
    " --executor-memory 2g"
    " --executor-cores 1"
    " --num-executors 1"
    " --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
    " --jars /stream/consumer/postgresql-42.5.1.jar"
)

KAFKA_TOPICS = "air_quality_stream weather_stream"

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="asvsp_streaming_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["asvsp", "streaming"],
    default_args=DEFAULT_ARGS,
) as dag:

    clear_kafka_topics = BashOperator(
        task_id="clear_kafka_topics",
        bash_command=(
            "for topic in air_quality_stream weather_stream; do "
            "docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --delete --topic $topic 2>/dev/null || true; "
            "done && sleep 3 && "
            "docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --topic air_quality_stream --partitions 1 --replication-factor 1 && "
            "docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --topic weather_stream --partitions 1 --replication-factor 1 && "
            "docker exec spark-master rm -rf /tmp/ckpt && "
            "docker exec pg psql -U postgres -d streaming -c "
            "'TRUNCATE TABLE sq1_vs_baseline, sq2_dominant_pollutant, sq3_worst_cities, sq4_ventilation, sq5_state_ranking;'"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="docker start -a kafka_producer",
        execution_timeout=timedelta(hours=3),
    )

    streaming_consumer1 = BashOperator(
        task_id="streaming_consumer1",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/consumer1.py",
        execution_timeout=timedelta(hours=3),
    )

    streaming_consumer2 = BashOperator(
        task_id="streaming_consumer2",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/consumer2.py",
        execution_timeout=timedelta(hours=3),
    )

    streaming_consumer3 = BashOperator(
        task_id="streaming_consumer3",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/consumer3.py",
        execution_timeout=timedelta(hours=3),
    )

    streaming_consumer4 = BashOperator(
        task_id="streaming_consumer4",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/consumer4.py",
        execution_timeout=timedelta(hours=3),
    )

    streaming_consumer5 = BashOperator(
        task_id="streaming_consumer5",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/consumer5.py",
        execution_timeout=timedelta(hours=3),
    )

    (
        clear_kafka_topics
        >> run_producer
        >> streaming_consumer1
        >> streaming_consumer2
        >> streaming_consumer3
        >> streaming_consumer4
        >> streaming_consumer5
    )
