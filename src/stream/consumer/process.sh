#!/bin/bash
set -e

JAR=/stream/consumer/postgresql-42.5.1.jar
SUBMIT="./spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
  --jars $JAR"

echo "=== SQ1: PM2.5 vs baseline ==="
$SUBMIT /stream/consumer/consumer1.py

echo "=== SQ2: Dominant pollutant trend (sliding window) ==="
$SUBMIT /stream/consumer/consumer2.py

echo "=== SQ3: Top 5 worst cities ==="
$SUBMIT /stream/consumer/consumer3.py

echo "=== SQ4: Ventilation coefficient ==="
$SUBMIT /stream/consumer/consumer4.py

echo "=== SQ5: State ranking anomaly ==="
$SUBMIT /stream/consumer/consumer5.py

echo "=== All streaming queries done ==="
