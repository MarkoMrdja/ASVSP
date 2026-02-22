#!/bin/bash
# Upload EPA CSV files from local data/raw/ to HDFS /data/raw/
#
# Usage: bash src/scripts/load_to_hdfs.sh [LOCAL_DATA_DIR]
#
# Requires: namenode container running with HDFS available

LOCAL_DATA_DIR="${1:-./data/raw}"
HDFS_DATA_DIR="/data/raw"
NAMENODE_CONTAINER="namenode"

POLLUTANTS=("O3" "PM25" "PM10" "NO2" "SO2" "CO")

echo "Creating HDFS directory structure..."
for pollutant in "${POLLUTANTS[@]}"; do
    docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p "${HDFS_DATA_DIR}/${pollutant}"
done

echo "Uploading CSV files to HDFS..."
for pollutant in "${POLLUTANTS[@]}"; do
    local_dir="${LOCAL_DATA_DIR}/${pollutant}"

    if [ ! -d "$local_dir" ]; then
        echo "  Skipping ${pollutant}: local directory ${local_dir} not found"
        continue
    fi

    for csv_file in "${local_dir}"/hourly_*.csv; do
        if [ ! -f "$csv_file" ]; then
            echo "  Skipping ${pollutant}: no CSV files found (run extract_csv.py first)"
            continue
        fi

        filename=$(basename "$csv_file")
        hdfs_path="${HDFS_DATA_DIR}/${pollutant}/${filename}"

        echo "  Uploading ${pollutant}/${filename}..."
        docker cp "$csv_file" "${NAMENODE_CONTAINER}:/tmp/${filename}"
        docker exec "$NAMENODE_CONTAINER" hdfs dfs -put -f "/tmp/${filename}" "$hdfs_path"
        docker exec "$NAMENODE_CONTAINER" rm "/tmp/${filename}"
        rm "$csv_file"
        echo "  Deleted local ${csv_file}"
    done
done

echo ""
echo "HDFS upload complete. Verifying..."
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls -R "$HDFS_DATA_DIR" 2>/dev/null | head -40
