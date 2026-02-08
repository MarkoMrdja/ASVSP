import os
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.parquet as pq
from data_loader import RetryConfig, retry_with_backoff
import logging

logger = logging.getLogger(__name__)


class DetectionWriter:
    """Handles Parquet writing and HDFS upload for vehicle detections.

    Separates storage concerns from detection logic. Writes a day's
    detections to a single Parquet file with optimal row groups and
    uploads to HDFS with time-based partitioning.
    """

    # Optimized schema for vehicle detections
    # Uses YOLO-style center coordinates (x, y) and dimensions (width, height)
    DETECTION_SCHEMA = pa.schema([
        ('detection_id', pa.binary(16)),
        ('border', pa.string()),
        ('direction', pa.string()),
        ('timestamp', pa.timestamp('ns')),
        ('vehicle_type', pa.string()),
        ('confidence', pa.float32()),
        ('center_x', pa.float32()),
        ('center_y', pa.float32()),
        ('width', pa.float32()),
        ('height', pa.float32()),
        ('image_filename', pa.string())
    ])

    def __init__(
        self,
        hdfs_namenode: str = "http://namenode:9870",
        retry_config: Optional[RetryConfig] = None
    ):
        self.hdfs_client = InsecureClient(hdfs_namenode, user='root')
        self.retry_config = retry_config or RetryConfig()

        # Max 1M detections per row group (~128 MB after compression)
        self.row_group_size: int = 1_000_000

    def write_day(
        self,
        processing_date: date,
        detections: List[Dict[str, Any]]
    ) -> Tuple[str, int]:
        """
        Write a day's detections to Parquet and upload to HDFS.

        Args:
            processing_date: Date being written
            detections: List of detection dicts (must be pre-sorted)

        Returns:
            Tuple of (hdfs_path, file_size_bytes)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = f"/tmp/vehicle_detections_{processing_date.strftime('%Y%m%d')}_{timestamp}.parquet"
        hdfs_path = (
            f"/hdfs/raw/vehicle_detections/"
            f"year={processing_date.year}/"
            f"month={processing_date.month:02d}/"
            f"day={processing_date.day:02d}/"
            f"vehicle_detections.parquet"
        )

        # Ensure HDFS directory exists
        hdfs_dir = (
            f"/hdfs/raw/vehicle_detections/"
            f"year={processing_date.year}/"
            f"month={processing_date.month:02d}/"
            f"day={processing_date.day:02d}"
        )
        try:
            self.hdfs_client.status(hdfs_dir)
        except:
            self.hdfs_client.makedirs(hdfs_dir)

        try:
            # Write Parquet with optimal row groups
            self._write_parquet_with_row_groups(local_path, detections)

            # Upload to HDFS with retry
            self._upload_to_hdfs_with_retry(hdfs_path, local_path)

            file_size_bytes = os.path.getsize(local_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            logger.info(
                f"Saved {len(detections)} detections ({file_size_mb:.2f} MB) "
                f"for {processing_date} to HDFS: {hdfs_path}"
            )

            return hdfs_path, file_size_bytes
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    def _write_parquet_with_row_groups(
        self,
        path: str,
        detections: List[Dict[str, Any]]
    ):
        """
        Write detections to Parquet file with optimal row groups.

        Strategy:
        - Chunk detections into row groups of max 1M each
        - Each write_table() call creates one row group

        Parquet settings:
        - Compression: ZSTD level 3 (balance speed vs compression)
        - Dictionary encoding: Enabled (excellent for border/direction)
        - Statistics: Enabled (Hive/Spark query optimization)
        - Version: 2.6 (latest stable)
        """
        writer = pq.ParquetWriter(
            path,
            self.DETECTION_SCHEMA,
            compression='zstd',
            compression_level=3,
            use_dictionary=True,
            write_statistics=True,
            version='2.6'
        )

        try:
            row_group_count = 0
            for i in range(0, len(detections), self.row_group_size):
                chunk = detections[i:i + self.row_group_size]
                df = pd.DataFrame(chunk)
                table = pa.Table.from_pandas(df, schema=self.DETECTION_SCHEMA)
                writer.write_table(table)

                row_group_count += 1
                chunk_size_mb = table.nbytes / (1024 * 1024)
                logger.debug(
                    f"Wrote row group {row_group_count}: "
                    f"{len(chunk)} detections, ~{chunk_size_mb:.2f} MB"
                )

            logger.info(
                f"Wrote {len(detections)} detections in {row_group_count} row groups "
                f"(~{len(detections) / row_group_count:.0f} detections per group)"
            )
        finally:
            writer.close()

    @retry_with_backoff()
    def _upload_to_hdfs_with_retry(self, hdfs_path: str, local_path: str):
        """Upload file to HDFS with retry logic."""
        self.hdfs_client.upload(hdfs_path, local_path)
