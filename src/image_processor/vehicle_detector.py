import os
import numpy as np
import pandas as pd
import uuid
import torch
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.parquet as pq
from ultralytics import YOLO
from data_loader import ImageLoader, ProcessingStatus, HDFSStatus, RetryConfig, retry_with_backoff
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VehicleDetector:
    """Class to detect vehicles in images with time-based HDFS partitioning"""

    # Vehicle classes in COCO dataset (used by YOLO models)
    # Only detecting cars and motorcycles
    VEHICLE_CLASSES = {
        2: 'car',
        3: 'motorcycle'
    }

    def __init__(
        self,
        model_path: str = "/app/model/yolo11n.pt",
        hdfs_namenode: str = "http://namenode:9870",
        max_file_size_mb: int = 256,
        retry_config: Optional[RetryConfig] = None
    ):
        """
        Initialize vehicle detector with YOLO model

        Args:
            model_path: Path to YOLO model weights
            hdfs_namenode: HDFS namenode address
            max_file_size_mb: Maximum size for parquet files in MB before finalizing
            retry_config: Configuration for retry behavior
        """
        # Ensure model exists (download if needed)
        self._ensure_model(model_path)

        # Detect best available device
        self.device = self._detect_device()

        # Load the YOLO model
        self.model = YOLO(model_path)
        self.model.to(self.device)

        # Set confidence threshold
        self.model.conf = 0.25
        self.model.classes = list(self.VEHICLE_CLASSES.keys())

        # HDFS setup
        self.hdfs_namenode = hdfs_namenode
        self.hdfs_client = InsecureClient(hdfs_namenode, user='root')

        # Retry configuration
        self.retry_config = retry_config or RetryConfig()

        # Cache for accumulating detections by day (time-based partitioning)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024

        # Track per-day writer (instead of per-border)
        self.current_day: Optional[date] = None
        self.day_writer: Optional[pq.ParquetWriter] = None
        self.day_writer_path: Optional[Tuple[str, str]] = None  # (local_path, hdfs_path)
        self.day_detection_count: int = 0
        self.day_file_size: int = 0

        # Buffer for batching detections before writing (improves parquet structure)
        self.detection_buffer: List[Dict[str, Any]] = []
        self.buffer_size_threshold: int = 1000  # Write every 1000 detections as one row group

        print(f"Loaded YOLO model on {self.device}")
        print(f"Connected to HDFS at {hdfs_namenode}")

        # Define optimized schema for vehicle detections
        # Uses YOLO-style center coordinates (x, y) and dimensions (width, height)
        self.detection_schema = pa.schema([
            ('detection_id', pa.binary(16)),
            ('border', pa.string()),
            ('direction', pa.string()),
            ('timestamp', pa.timestamp('ns')),
            ('vehicle_type', pa.string()),
            ('confidence', pa.float32()),
            # Center point coordinates
            ('center_x', pa.float32()),
            ('center_y', pa.float32()),
            # Bounding box dimensions
            ('width', pa.float32()),
            ('height', pa.float32()),
            ('image_filename', pa.string())
        ])

    def _detect_device(self) -> str:
        """Detect the best available device for inference"""
        if torch.backends.mps.is_available():
            logger.info("MPS (Apple Silicon) detected, using GPU acceleration")
            return "mps"
        elif torch.cuda.is_available():
            logger.info("CUDA detected, using NVIDIA GPU acceleration")
            return "cuda"
        else:
            logger.info("No GPU detected, using CPU")
            return "cpu"

    def _ensure_model(self, model_path: str):
        """Ensure model file exists, download if needed"""
        if os.path.exists(model_path):
            return

        model_dir = os.path.dirname(model_path)
        if model_dir and not os.path.exists(model_dir):
            os.makedirs(model_dir, exist_ok=True)

        model_name = os.path.basename(model_path)
        logger.info(f"Model not found at {model_path}, downloading {model_name}...")

        # YOLO will auto-download if we just instantiate with the model name
        # The model will be saved to the ultralytics cache, we copy it to desired location
        try:
            temp_model = YOLO(model_name)
            # The model is now cached, copy to our path
            import shutil
            cached_path = temp_model.ckpt_path if hasattr(temp_model, 'ckpt_path') else None
            if cached_path and os.path.exists(cached_path):
                shutil.copy(cached_path, model_path)
                logger.info(f"Model downloaded and saved to {model_path}")
            else:
                logger.warning(f"Could not locate cached model, using default ultralytics cache")
        except Exception as e:
            logger.warning(f"Could not auto-download model: {e}. Proceeding with ultralytics default.")

    def _get_writer_properties(self):
        """Get optimized writer properties for parquet files"""
        return {
            'compression': 'zstd',
            'use_dictionary': True,
            'write_statistics': True,
            'version': '2.6'
        }

    def _get_detection_day(self, detection: Dict[str, Any]) -> date:
        """Extract the day from a detection's timestamp"""
        timestamp = detection['timestamp']
        if isinstance(timestamp, datetime):
            return timestamp.date()
        return timestamp.date()

    def _initialize_writer(self, detection_day: date, hdfs_base_dir: str) -> Tuple[str, str, pq.ParquetWriter]:
        """Initialize a streaming parquet writer for a specific day

        Args:
            detection_day: The day for this writer
            hdfs_base_dir: Base HDFS directory

        Returns:
            Tuple of (local_path, hdfs_path, writer)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        year = detection_day.year
        month = detection_day.month
        day = detection_day.day

        local_path = f"/tmp/vehicle_detections_{year}{month:02d}{day:02d}_{timestamp}.parquet"
        hdfs_path = f"{hdfs_base_dir}/year={year}/month={month:02d}/day={day:02d}/vehicle_detections_{year}{month:02d}{day:02d}_{timestamp}.parquet"

        try:
            # Ensure HDFS directory exists
            hdfs_dir = f"{hdfs_base_dir}/year={year}/month={month:02d}/day={day:02d}"
            try:
                self.hdfs_client.status(hdfs_dir)
            except:
                self.hdfs_client.makedirs(hdfs_dir)
                logger.info(f"Created HDFS directory: {hdfs_dir}")
        except Exception as e:
            logger.error(f"Error creating HDFS directory: {e}")
            raise

        writer_props = self._get_writer_properties()
        writer = pq.ParquetWriter(
            local_path,
            self.detection_schema,
            compression=writer_props['compression'],
            use_dictionary=writer_props['use_dictionary'],
            write_statistics=writer_props['write_statistics'],
            version=writer_props['version']
        )

        return local_path, hdfs_path, writer

    def detect_vehicles(self, images: List[np.ndarray],
                        paths: List[str]) -> List[Dict[str, Any]]:
        """
        Detect vehicles in a batch of images and extract information.
        Creates one entry per detection (rather than per image).

        Args:
            images: List of images as numpy arrays
            paths: List of image paths corresponding to the images

        Returns:
            List of dictionaries with detection information
        """
        # Run inference on batch
        results = self.model(images, verbose=False)

        detections = []

        for i, result in enumerate(results):
            image_path = paths[i]

            # Extract metadata from path
            path_parts = Path(image_path).parts
            border = path_parts[0]
            direction = path_parts[1]
            year = int(path_parts[2])
            month = int(path_parts[3])
            day = int(path_parts[4])

            filename = Path(image_path).name
            hour, minute, second_ext = filename.split('-')
            second = second_ext.split('.')[0]

            timestamp = datetime(year, month, day,
                               int(hour), int(minute), int(second))

            boxes = result.boxes
            for j in range(len(boxes)):
                # Extract center coordinates and dimensions directly from YOLO (xywh format)
                center_x, center_y, width, height = boxes.xywh[j].tolist()
                conf = float(boxes.conf[j])
                cls = int(boxes.cls[j])

                if cls in self.VEHICLE_CLASSES:
                    vehicle_type = self.VEHICLE_CLASSES[cls]

                    detection = {
                        'detection_id': uuid.uuid4().bytes,
                        'border': border,
                        'direction': direction,
                        'timestamp': timestamp,
                        'vehicle_type': vehicle_type,
                        'confidence': np.float32(conf),
                        'center_x': np.float32(center_x),
                        'center_y': np.float32(center_y),
                        'width': np.float32(width),
                        'height': np.float32(height),
                        'image_filename': filename
                    }

                    detections.append(detection)

                    # Add to buffer instead of writing immediately
                    self._buffer_detection(detection)

        return detections

    def _buffer_detection(self, detection: Dict[str, Any]):
        """Add detection to buffer and flush when threshold is reached

        Args:
            detection: Detection dictionary to buffer
        """
        detection_day = self._get_detection_day(detection)

        # Check for day boundary - flush buffer and finalize current day if needed
        if self.current_day is not None and detection_day != self.current_day:
            logger.info(f"Day boundary detected: {self.current_day} -> {detection_day}")
            self._flush_buffer()
            self.finalize_current_day()

        # Initialize writer for new day if needed
        if self.day_writer is None:
            try:
                local_path, hdfs_path, writer = self._initialize_writer(
                    detection_day, "/hdfs/raw/vehicle_detections")
                self.day_writer = writer
                self.day_writer_path = (local_path, hdfs_path)
                self.current_day = detection_day
                self.day_detection_count = 0
                self.day_file_size = 0
                logger.info(f"Initialized writer for day {detection_day}")
            except Exception as e:
                logger.error(f"Error initializing writer for day {detection_day}: {e}")
                return

        # Add detection to buffer
        self.detection_buffer.append(detection)

        # Flush buffer if threshold reached
        if len(self.detection_buffer) >= self.buffer_size_threshold:
            self._flush_buffer()

    def _flush_buffer(self):
        """Flush buffered detections to parquet file as a single row group"""
        if not self.detection_buffer or self.day_writer is None:
            return

        try:
            # Write all buffered detections as one row group
            df = pd.DataFrame(self.detection_buffer)
            batch = pa.Table.from_pandas(df, schema=self.detection_schema)
            self.day_writer.write_table(batch)

            self.day_detection_count += len(self.detection_buffer)
            self.day_file_size += batch.nbytes

            logger.debug(f"Flushed {len(self.detection_buffer)} detections to parquet (total: {self.day_detection_count})")

            # Clear buffer
            self.detection_buffer = []

            # Check if file size threshold reached
            if self.day_file_size >= self.max_file_size_bytes:
                logger.info(f"File size threshold reached for day {self.current_day} ({self.day_file_size / (1024*1024):.2f} MB), finalizing...")
                self.finalize_current_day()

        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
            # Don't clear buffer on error, keep for retry
            raise

    @retry_with_backoff()
    def _upload_to_hdfs_with_retry(self, hdfs_path: str, local_path: str):
        """Upload file to HDFS with retry logic

        Args:
            hdfs_path: Destination path in HDFS
            local_path: Source local file path
        """
        self.hdfs_client.upload(hdfs_path, local_path)

    def finalize_current_day(self, image_loader: Optional[ImageLoader] = None) -> Optional[str]:
        """
        Finalize the parquet file for the current day, upload to HDFS, and update status

        Args:
            image_loader: Optional ImageLoader to update HDFS status

        Returns:
            Path to saved parquet file in HDFS if successful, None otherwise
        """
        if self.day_writer is None:
            logger.debug("No active writer to finalize")
            return None

        try:
            # Flush any remaining buffered detections before closing
            if self.detection_buffer:
                logger.info(f"Flushing {len(self.detection_buffer)} remaining detections before finalization")
                self._flush_buffer()

            self.day_writer.close()

            local_path, hdfs_path = self.day_writer_path

            # Upload to HDFS with retry
            self._upload_to_hdfs_with_retry(hdfs_path, local_path)

            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            logger.info(
                f"Saved {self.day_detection_count} detections ({file_size_mb:.2f} MB) "
                f"for day {self.current_day} to HDFS: {hdfs_path}"
            )

            if image_loader:
                self._update_all_hdfs_status(image_loader, hdfs_path)

            os.remove(local_path)

            # Reset state
            saved_path = hdfs_path
            self.day_writer = None
            self.day_writer_path = None
            self.current_day = None
            self.day_detection_count = 0
            self.day_file_size = 0
            self.detection_buffer = []

            return saved_path
        except Exception as e:
            logger.error(f"Error finalizing day {self.current_day}: {e}")
            return None

    def _update_all_hdfs_status(self, image_loader: ImageLoader, hdfs_path: str):
        """Update HDFS status for all pending batches

        Args:
            image_loader: ImageLoader instance
            hdfs_path: Path to the HDFS file where data was saved
        """
        pending_batches = image_loader.get_pending_hdfs_batches()
        total_updated = 0

        for border, paths in pending_batches.items():
            for last_image_path in paths:
                image_loader.set_batch_hdfs_status(last_image_path, HDFSStatus.SAVED, hdfs_path)
                total_updated += 1

        if total_updated > 0:
            logger.info(f"Updated HDFS status for {total_updated} batches")

    def finalize_all(self, image_loader: Optional[ImageLoader] = None) -> Dict[str, str]:
        """
        Finalize all parquet files, upload to HDFS, and update status

        Args:
            image_loader: Optional ImageLoader to update HDFS status

        Returns:
            Dictionary mapping day strings to HDFS paths
        """
        saved_paths = {}
        if self.current_day is not None:
            hdfs_path = self.finalize_current_day(image_loader)
            if hdfs_path:
                saved_paths[str(self.current_day)] = hdfs_path
        return saved_paths

    def get_detection_count(self, border: Optional[str] = None) -> int:
        """
        Get count of detections for the current day

        Args:
            border: Ignored (kept for API compatibility)

        Returns:
            Count of detections in current day
        """
        return self.day_detection_count

    # Legacy method for backward compatibility
    def finalize_border(self, border: str, image_loader: Optional[ImageLoader] = None) -> Optional[str]:
        """
        Legacy method - now delegates to finalize_current_day

        Args:
            border: Border name (ignored in new implementation)
            image_loader: Optional ImageLoader to update HDFS status

        Returns:
            Path to saved parquet file in HDFS if successful, None otherwise
        """
        return self.finalize_current_day(image_loader)

    def update_batch_hdfs_status(self, image_loader: ImageLoader, border: str, hdfs_path: str) -> int:
        """
        Update HDFS status for all batches for a border
        Legacy method kept for backward compatibility

        Args:
            image_loader: ImageLoader instance
            border: Border name (ignored in new implementation)
            hdfs_path: Path to the HDFS file where data was saved

        Returns:
            Number of batches updated
        """
        pending_batches = image_loader.get_pending_hdfs_batches()
        if border not in pending_batches:
            return 0

        count = 0
        for last_image_path in pending_batches[border]:
            image_loader.set_batch_hdfs_status(last_image_path, HDFSStatus.SAVED, hdfs_path)
            count += 1

        return count
