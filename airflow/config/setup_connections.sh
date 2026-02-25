#!/bin/bash
# Register Airflow connections for the ASVSP project

airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    || echo "spark_default already exists"

airflow connections add postgres_streaming \
    --conn-type postgres \
    --conn-host pg \
    --conn-port 5432 \
    --conn-schema streaming \
    --conn-login postgres \
    --conn-password postgres \
    || echo "postgres_streaming already exists"
