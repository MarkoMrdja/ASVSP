#!/bin/bash
# Upload EPA Parquet files from local data/raw/ to HDFS /data/raw/
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

echo "Uploading Parquet files to HDFS..."
for pollutant in "${POLLUTANTS[@]}"; do
    local_dir="${LOCAL_DATA_DIR}/${pollutant}"

    if [ ! -d "$local_dir" ]; then
        echo "  Skipping ${pollutant}: local directory ${local_dir} not found"
        continue
    fi

    # Find all year=YYYY subdirectories
    for year_dir in "${local_dir}"/year=*/; do
        if [ ! -d "$year_dir" ]; then
            continue
        fi

        year_name=$(basename "$year_dir")
        hdfs_year_dir="${HDFS_DATA_DIR}/${pollutant}/${year_name}"

        echo "  Uploading ${pollutant}/${year_name}..."
        docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p "$hdfs_year_dir"

        # Copy each parquet file
        for parquet_file in "${year_dir}"*.parquet; do
            if [ ! -f "$parquet_file" ]; then
                continue
            fi
            filename=$(basename "$parquet_file")
            docker cp "$parquet_file" "${NAMENODE_CONTAINER}:/tmp/${pollutant}_${year_name}_${filename}"
            docker exec "$NAMENODE_CONTAINER" hdfs dfs -put -f \
                "/tmp/${pollutant}_${year_name}_${filename}" \
                "${hdfs_year_dir}/${filename}"
            docker exec "$NAMENODE_CONTAINER" rm "/tmp/${pollutant}_${year_name}_${filename}"
        done
    done
done

echo ""
echo "HDFS upload complete. Verifying..."
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls -R "$HDFS_DATA_DIR" 2>/dev/null | head -40
