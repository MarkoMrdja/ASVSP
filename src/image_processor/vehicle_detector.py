import os
import numpy as np
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from hdfs import InsecureClient
from ultralytics import YOLO  # Import YOLO from ultralytics
from data_loader import ImageLoader, ProcessingStatus, HDFSStatus

class VehicleDetector:
    """Class to detect vehicles in images using YOLOv11 model"""

    # Vehicle classes in COCO dataset (used by YOLO models)
    VEHICLE_CLASSES = {
        2: 'car', 
        3: 'motorcycle', 
        5: 'bus', 
        7: 'truck'
    }

    def __init__(self, model_path: str = "/app/model/yolo11l.pt", 
                 hdfs_namenode: str = "http://namenode:9870"):
        """
        Initialize vehicle detector with YOLOv11 model
        
        Args:
            model_path: Path to YOLOv11 model weights
            hdfs_namenode: HDFS namenode address
        """
        # Load the YOLO model using ultralytics API
        self.model = YOLO(model_path)
        
        # Set confidence threshold
        self.model.conf = 0.25
        # Only detect vehicle classes
        self.model.classes = list(self.VEHICLE_CLASSES.keys())
        
        # HDFS setup
        self.hdfs_namenode = hdfs_namenode
        self.hdfs_client = InsecureClient(hdfs_namenode, user='root')
        
        # Cache for accumulating detections by border
        self.cached_detections = {}
        
        device = "GPU" if self.model.device.type == "cuda" else "CPU"
        print(f"Loaded YOLO model on {device}")
        print(f"Connected to HDFS at {hdfs_namenode}")

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
        
        # Process results
        detections = []
        
        for i, result in enumerate(results):
            # Get image path
            image_path = paths[i]
            
            # Extract metadata from path
            path_parts = Path(image_path).parts
            border = path_parts[0]
            direction = path_parts[1]
            year = int(path_parts[2])
            month = int(path_parts[3])
            day = int(path_parts[4])
            
            # Extract time from filename (assuming format: HH-MM-SS.jpg)
            filename = Path(image_path).name
            hour, minute, second_ext = filename.split('-')
            second = second_ext.split('.')[0]
            
            timestamp = datetime(year, month, day, 
                               int(hour), int(minute), int(second))
            
            # Process detections (boxes)
            boxes = result.boxes
            for j in range(len(boxes)):
                x1, y1, x2, y2 = boxes.xyxy[j].tolist()
                conf = float(boxes.conf[j])
                cls = int(boxes.cls[j])
                
                if cls in self.VEHICLE_CLASSES:
                    vehicle_type = self.VEHICLE_CLASSES[cls]
                    
                    # Create detection entry
                    detection = {
                        'detection_id': str(uuid.uuid4()),
                        'border': border,
                        'direction': direction,
                        'timestamp': timestamp,
                        'year': year,
                        'month': month,
                        'day': day,
                        'hour': int(hour),
                        'minute': int(minute),
                        'second': int(second),
                        'vehicle_type': vehicle_type,
                        'confidence': conf,
                        'x1': float(x1),
                        'y1': float(y1),
                        'x2': float(x2),
                        'y2': float(y2),
                        'width': float(x2 - x1),
                        'height': float(y2 - y1),
                        'image_path': image_path
                    }
                    
                    detections.append(detection)
                    
                    # Add to border-specific cache
                    if border not in self.cached_detections:
                        self.cached_detections[border] = []
                    self.cached_detections[border].append(detection)
            
        return detections

    def save_detections_for_border(self, border: str, 
                              hdfs_dir: str = "/hdfs/raw/vehicle_detections") -> Optional[str]:
        """
        Save accumulated detection results for a specific border to parquet file in HDFS
        
        Args:
            border: Border name to save detections for
            hdfs_dir: Base HDFS directory to save parquet files
            
        Returns:
            Path to saved parquet file in HDFS if successful, None otherwise
        """
        if border not in self.cached_detections or not self.cached_detections[border]:
            print(f"No cached detections for border {border}")
            return None
            
        # Create DataFrame from detections
        df = pd.DataFrame(self.cached_detections[border])
        
        # Generate output filename based on border and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = f"/tmp/vehicle_detections_{border}_{timestamp}.parquet"
        hdfs_path = f"{hdfs_dir}/{border}/vehicle_detections_{border}_{timestamp}.parquet"
        
        # Save to local temporary file first
        df.to_parquet(local_path, index=False)
        
        # Ensure HDFS directory exists
        try:
            border_hdfs_dir = f"{hdfs_dir}/{border}"
            # Check if directory exists by trying to get its status
            try:
                self.hdfs_client.status(border_hdfs_dir)
            except:
                # If status check fails, directory doesn't exist, so create it
                self.hdfs_client.makedirs(border_hdfs_dir)
        except Exception as e:
            print(f"Error creating HDFS directory: {e}")
            return None
        
        # Upload to HDFS
        try:
            self.hdfs_client.upload(hdfs_path, local_path)
            detection_count = len(self.cached_detections[border])
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"Saved {detection_count} detections ({file_size_mb:.2f} MB) for border {border} to HDFS: {hdfs_path}")
            
            # Remove temporary local file
            os.remove(local_path)
            
            # Clear the cache for this border
            self.cached_detections[border] = []
            
            return hdfs_path
        except Exception as e:
            print(f"Error saving to HDFS: {e}")
            return None
    
    def save_all_cached_detections(self, hdfs_dir: str = "/hdfs/raw/vehicle_detections") -> Dict[str, str]:
        """
        Save all cached detections for all borders to HDFS
        
        Args:
            hdfs_dir: Base HDFS directory to save parquet files
            
        Returns:
            Dictionary mapping border names to HDFS paths
        """
        saved_paths = {}
        for border in list(self.cached_detections.keys()):
            if self.cached_detections[border]:
                hdfs_path = self.save_detections_for_border(border, hdfs_dir)
                if hdfs_path:
                    saved_paths[border] = hdfs_path
        return saved_paths
    
    def get_cached_detection_count(self, border: Optional[str] = None) -> int:
        """
        Get count of cached detections for a border or all borders
        
        Args:
            border: Optional border name to get count for. If None, returns total count.
            
        Returns:
            Count of cached detections
        """
        if border:
            return len(self.cached_detections.get(border, []))
        else:
            return sum(len(detections) for detections in self.cached_detections.values())
    
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