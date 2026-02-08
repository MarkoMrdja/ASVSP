import os
import numpy as np
import uuid
import torch
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any, Optional
from ultralytics import YOLO
from data_loader import RetryConfig
from detection_writer import DetectionWriter
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VehicleDetector:
    """Detects vehicles in images using YOLO and delegates storage to DetectionWriter."""

    # Vehicle classes in COCO dataset (used by YOLO models)
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
        Initialize vehicle detector with YOLO model.

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

        # Detection writer for Parquet/HDFS
        self.writer = DetectionWriter(
            hdfs_namenode=hdfs_namenode,
            retry_config=retry_config or RetryConfig()
        )

        # Day-level accumulation
        self.current_day: Optional[date] = None
        self.day_detections: List[Dict[str, Any]] = []

        print(f"Loaded YOLO model on {self.device}")
        print(f"Connected to HDFS at {hdfs_namenode}")

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

        try:
            temp_model = YOLO(model_name)
            import shutil
            cached_path = temp_model.ckpt_path if hasattr(temp_model, 'ckpt_path') else None
            if cached_path and os.path.exists(cached_path):
                shutil.copy(cached_path, model_path)
                logger.info(f"Model downloaded and saved to {model_path}")
            else:
                logger.warning(f"Could not locate cached model, using default ultralytics cache")
        except Exception as e:
            logger.warning(f"Could not auto-download model: {e}. Proceeding with ultralytics default.")

    def detect_vehicles(self, images: List[np.ndarray],
                        paths: List[str]) -> List[Dict[str, Any]]:
        """
        Detect vehicles in batch and accumulate in day buffer.

        IMPORTANT: This method does NOT write to disk.
        All detections accumulated in memory until finalize_day() is called.

        Args:
            images: List of images as numpy arrays
            paths: List of image paths corresponding to the images

        Returns:
            List of detections for this batch

        Raises:
            ValueError: If day boundary detected (processing must be atomic)
        """
        results = self.model(images, verbose=False)
        detections = []

        for i, result in enumerate(results):
            # Extract metadata from path
            path_parts = Path(paths[i]).parts
            border = path_parts[0]
            direction = path_parts[1]
            year, month, day = int(path_parts[2]), int(path_parts[3]), int(path_parts[4])

            filename = Path(paths[i]).name
            hour, minute, second_ext = filename.split('-')
            second = second_ext.split('.')[0]
            timestamp = datetime(year, month, day, int(hour), int(minute), int(second))
            detection_day = timestamp.date()

            # Day boundary check (fail-fast)
            if self.current_day is None:
                self.current_day = detection_day
            elif self.current_day != detection_day:
                raise ValueError(
                    f"Day boundary violation! Expected {self.current_day}, got {detection_day}. "
                    f"Day processing must be atomic - finalize current day first."
                )

            # Extract detections from YOLO results
            boxes = result.boxes
            for j in range(len(boxes)):
                center_x, center_y, width, height = boxes.xywh[j].tolist()
                conf = float(boxes.conf[j])
                cls = int(boxes.cls[j])

                if cls in self.VEHICLE_CLASSES:
                    detection = {
                        'detection_id': uuid.uuid4().bytes,
                        'border': border,
                        'direction': direction,
                        'timestamp': timestamp,
                        'vehicle_type': self.VEHICLE_CLASSES[cls],
                        'confidence': np.float32(conf),
                        'center_x': np.float32(center_x),
                        'center_y': np.float32(center_y),
                        'width': np.float32(width),
                        'height': np.float32(height),
                        'image_filename': filename
                    }

                    detections.append(detection)
                    self.day_detections.append(detection)  # Accumulate

        return detections

    def finalize_day(
        self,
        processing_date: date,
        image_loader
    ) -> Optional[str]:
        """
        Finalize a day's detections: write Parquet, upload to HDFS, update database.

        Transaction boundaries are clear: succeeds completely or fails completely.

        Args:
            processing_date: Date being finalized
            image_loader: ImageLoader instance for database updates

        Returns:
            HDFS path if successful, None otherwise

        Raises:
            ValueError: If processing_date doesn't match current_day
        """
        if not self.day_detections:
            logger.warning(f"No detections to finalize for {processing_date}")
            return None

        if self.current_day != processing_date:
            raise ValueError(f"Day mismatch: finalizing {processing_date} but current day is {self.current_day}")

        try:
            # Sort by timestamp for better compression
            self.day_detections.sort(key=lambda d: d['timestamp'])

            # Write Parquet and upload to HDFS
            hdfs_path, file_size_bytes = self.writer.write_day(processing_date, self.day_detections)

            # Update database
            image_loader.update_day_status(
                processing_date, status='COMPLETED',
                hdfs_path=hdfs_path,
                file_size_bytes=file_size_bytes,
                total_detections=len(self.day_detections)
            )

            self._reset_day_buffer()
            return hdfs_path

        except Exception as e:
            logger.error(f"Error finalizing day {processing_date}: {e}")
            raise

    def rollback_day(self):
        """
        Rollback current day processing by clearing buffer.
        Called when day processing fails and will be retried.
        """
        if self.current_day:
            logger.warning(
                f"Rolling back day {self.current_day} "
                f"({len(self.day_detections)} detections discarded)"
            )
        self._reset_day_buffer()

    def _reset_day_buffer(self):
        """Clear day buffer"""
        self.current_day = None
        self.day_detections = []
