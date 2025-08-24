import os
import numpy as np
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.parquet as pq
from ultralytics import YOLO
from data_loader import ImageLoader, ProcessingStatus, HDFSStatus

class VehicleDetector:
    """Class to detect vehicles in images"""

    # Vehicle classes in COCO dataset (used by YOLO models)
    VEHICLE_CLASSES = {
        2: 'car', 
        3: 'motorcycle', 
        5: 'bus', 
        7: 'truck'
    }

    def __init__(self, model_path: str = "/app/model/yolo11l.pt", 
                 hdfs_namenode: str = "http://namenode:9870",
                 max_file_size_mb: int = 256):
        """
        Initialize vehicle detector with YOLO model
        
        Args:
            model_path: Path to YOLO model weights
            hdfs_namenode: HDFS namenode address
            max_file_size_mb: Maximum size for parquet files in MB before finalizing
        """
        # Load the YOLO model
        self.model = YOLO(model_path)
        
        # Set confidence threshold
        self.model.conf = 0.25
        self.model.classes = list(self.VEHICLE_CLASSES.keys())
        
        # HDFS setup
        self.hdfs_namenode = hdfs_namenode
        self.hdfs_client = InsecureClient(hdfs_namenode, user='root')
        
        # Cache for accumulating detections by border
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024

        # Track streaming writers, file paths, and detection counts
        self.streaming_writers = {}  # border -> writer
        self.writer_paths = {}       # border -> (local_path, hdfs_path)
        self.detection_counts = {}   # border -> count
        self.file_sizes = {}         # border -> approximate size in bytes
        
        device = "GPU" if self.model.device.type == "cuda" else "CPU"
        print(f"Loaded YOLO model on {device}")
        print(f"Connected to HDFS at {hdfs_namenode}")

         # Define optimized schema for vehicle detections
        self.detection_schema = pa.schema([
            ('detection_id', pa.binary(16)),
            ('border', pa.string()),
            ('direction', pa.string()),
            ('timestamp', pa.timestamp('ns')),
            ('vehicle_type', pa.string()),
            ('confidence', pa.float32()),
            ('bbox_x1', pa.float32()),
            ('bbox_y1', pa.float32()),
            ('bbox_x2', pa.float32()),
            ('bbox_y2', pa.float32()),
            ('image_filename', pa.string())
        ])

    def _get_writer_properties(self):
        """Get optimized writer properties for parquet files"""
        return {
            'compression': 'zstd',
            'use_dictionary': True,
            'write_statistics': True,
            'version': '2.6'
        }
    
    def _initialize_writer(self, border: str, hdfs_dir: str) -> Tuple[str, str, pq.ParquetWriter]:
        """Initialize a streaming parquet writer per border
        
        Args:
            border: Border name
            hdfs_dir: Base HDFS directory
            
        Returns:
            Tuple of (local_path, hdfs_path, writer)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = f"/tmp/vehicle_detections_{border}_{timestamp}.parquet"
        hdfs_path = f"{hdfs_dir}/{border}/vehicle_detections_{border}_{timestamp}.parquet"
        
        try:
            border_hdfs_dir = f"{hdfs_dir}/{border}"
            try:
                self.hdfs_client.status(border_hdfs_dir)
            except:
                self.hdfs_client.makedirs(border_hdfs_dir)
        except Exception as e:
            print(f"Error creating HDFS directory: {e}")
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
                x1, y1, x2, y2 = boxes.xyxy[j].tolist()
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
                        'bbox_x1': np.float32(x1),
                        'bbox_y1': np.float32(y1),
                        'bbox_x2': np.float32(x2),
                        'bbox_y2': np.float32(y2),
                        'image_filename': filename
                    }
                    
                    detections.append(detection)
                    
                    # Stream directly to parquet
                    self._stream_detection(detection)
            
        return detections

    def _stream_detection(self, detection: Dict[str, Any]):
        """Stream a single detection directly to parquet
        
        Args:
            detection: Detection dictionary to stream
        """
        border = detection['border']
        
        if border not in self.detection_counts:
            self.detection_counts[border] = 0
        self.detection_counts[border] += 1
        
        df = pd.DataFrame([detection])
        batch = pa.Table.from_pandas(df, schema=self.detection_schema)
        
        # If no writer exists for this border, create one
        if border not in self.streaming_writers:
            try:
                local_path, hdfs_path, writer = self._initialize_writer(
                    border, "/hdfs/raw/vehicle_detections")
                self.streaming_writers[border] = writer
                self.writer_paths[border] = (local_path, hdfs_path)
                self.file_sizes[border] = 0
            except Exception as e:
                print(f"Error initializing writer for border {border}: {e}")
                return
        
        # Write the batch to parquet file
        self.streaming_writers[border].write_table(batch)
        
        self.file_sizes[border] += batch.nbytes
        
        if self.file_sizes[border] >= self.max_file_size_bytes:
            print(f"File size threshold reached for border {border}, finalizing...")
            self.finalize_border(border)

    def finalize_border(self, border: str, image_loader: Optional[ImageLoader] = None) -> Optional[str]:
        """
        Finalize the parquet file for a border, upload to HDFS, and update status
        
        Args:
            border: Border name
            image_loader: Optional ImageLoader to update HDFS status
            
        Returns:
            Path to saved parquet file in HDFS if successful, None otherwise
        """
        if border not in self.streaming_writers:
            print(f"No writer for border {border}")
            return None
            
        try:
            writer = self.streaming_writers[border]
            writer.close()
            
            local_path, hdfs_path = self.writer_paths[border]
            
            # Upload to HDFS
            self.hdfs_client.upload(hdfs_path, local_path)
            detection_count = self.detection_counts.get(border, 0)
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"Saved {detection_count} detections ({file_size_mb:.2f} MB) for border {border} to HDFS: {hdfs_path}")
            
            if image_loader:
                self.update_batch_hdfs_status(image_loader, border, hdfs_path)
                
            os.remove(local_path)
            
            del self.streaming_writers[border]
            del self.writer_paths[border]
            del self.file_sizes[border]
            self.detection_counts[border] = 0
            
            local_path, hdfs_path, writer = self._initialize_writer(
                border, "/hdfs/raw/vehicle_detections")
            self.streaming_writers[border] = writer
            self.writer_paths[border] = (local_path, hdfs_path)
            self.file_sizes[border] = 0
            
            return hdfs_path
        except Exception as e:
            print(f"Error finalizing border {border}: {e}")
            return None
    
    def finalize_all(self, image_loader: Optional[ImageLoader] = None) -> Dict[str, str]:
        """
        Finalize all parquet files, upload to HDFS, and update status
        
        Args:
            image_loader: Optional ImageLoader to update HDFS status
            
        Returns:
            Dictionary mapping border names to HDFS paths
        """
        saved_paths = {}
        for border in list(self.streaming_writers.keys()):
            hdfs_path = self.finalize_border(border, image_loader)
            if hdfs_path:
                saved_paths[border] = hdfs_path
        return saved_paths
    
    def get_detection_count(self, border: Optional[str] = None) -> int:
        """
        Get count of detections for a border or all borders
        
        Args:
            border: Optional border name to get count for. If None, returns total count.
            
        Returns:
            Count of detections
        """
        if border:
            return self.detection_counts.get(border, 0)
        else:
            return sum(self.detection_counts.values())
    
    def update_batch_hdfs_status(self, image_loader: ImageLoader, border: str, hdfs_path: str) -> int:
        """
        Update HDFS status for all batches for a border
        
        Args:
            image_loader: ImageLoader instance
            border: Border name
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