#!/bin/bash
# Place this in src/image_processor/run_test.sh

echo "Starting image processor test (processing 10 batches)..."

# Run the image processor with parameters for test
python main.py --batches 5 --save-frequency 2 --batch-size 20 --hdfs-dir /hdfs/raw/vehicle_detections

echo "Image processor test completed! Check HDFS for results."