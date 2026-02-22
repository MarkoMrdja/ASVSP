#!/bin/bash

SUBMIT="/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1500m \
  --executor-cores 2 \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.parquet.writeLegacyFormat=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true"

echo "Starting batch processing pipeline..."

echo "Step 1: Loading hourly measurements..."
$SUBMIT /batch/load_hourly.py

echo "Step 2: Daily aggregation..."
$SUBMIT /batch/daily_aggregation.py

echo "Step 3: Monthly aggregation..."
$SUBMIT /batch/monthly_aggregation.py

echo "Step 4: Annual aggregation..."
$SUBMIT /batch/annual_aggregation.py

echo "Step 5: Calculating baselines..."
$SUBMIT /batch/baselines.py

echo "Batch processing complete!"
