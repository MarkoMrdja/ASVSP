from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from azure.storage.blob import BlobServiceClient
from pathlib import Path
import sqlite3
import logging
from dotenv import load_dotenv
from enum import Enum
import numpy as np
import cv2
import os

class ProcessingStatus(Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class ImageBatchMetadata:
    """Metadata about a processed image batch"""
    last_image_path: str
    batch_size: int
    status: ProcessingStatus
    start_time: datetime
    end_time: Optional[datetime]
    
    @property
    def path_parts(self) -> Tuple[str, str, int, int, int]:
        """Returns (border, direction, year, month, day) from path"""
        parts = Path(self.last_image_path).parts
        return (
            parts[0],  # border
            parts[1],  # direction
            int(parts[2]),  # year
            int(parts[3]),  # month
            int(parts[4])   # day
        )

class ImageLoader:
    def __init__(
        self,
        container_name: str = "not-processed-imgs",
        batch_size: int = 120
    ):
        load_dotenv()

        connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        self.container_client = BlobServiceClient.from_connection_string(
            connection_string
        ).get_container_client(container_name)
        self.db_path = "src/batch/storage/image_processing.db"
        self.batch_size = batch_size
        
        self._init_db()
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _init_db(self):
        """Initialize SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS batch_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_image_path TEXT NOT NULL,
                    batch_size INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP
                )
            ''')

    def _get_last_processed_batch(self) -> Optional[ImageBatchMetadata]:
        """Get the last processed batch from database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM batch_tracking 
                ORDER BY start_time DESC LIMIT 1
            ''')
            row = cursor.fetchone()
            
            if row:
                return ImageBatchMetadata(
                    last_image_path=row['last_image_path'],
                    batch_size=row['batch_size'],
                    status=ProcessingStatus(row['status']),
                    start_time=datetime.fromisoformat(row['start_time']),
                    end_time=datetime.fromisoformat(row['end_time']) 
                        if row['end_time'] else None
                )
        return None

    def _record_batch_start(self, last_image_path: str) -> int:
        """Record the start of batch processing"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                INSERT INTO batch_tracking 
                (last_image_path, batch_size, status, start_time)
                VALUES (?, ?, ?, ?)
            ''', (
                last_image_path,
                self.batch_size,
                ProcessingStatus.IN_PROGRESS.value,
                datetime.now().isoformat()
            ))
            return cursor.lastrowid

    def _list_blobs_after(self, prefix: str, start_after: Optional[str] = None) -> List[str]:
        """List blobs after a certain path"""
        blobs = []
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            if start_after and blob.name <= start_after:
                continue
            blobs.append(blob.name)
            if len(blobs) >= self.batch_size:
                break
        return blobs

    def _get_next_path(self, border: str, direction: str, 
                       year: int, month: int, day: int) -> Optional[str]:
        """Get next valid path to check for images"""
        today = datetime.now().date()
        current = datetime(year, month, day).date()
        
        # Try next day
        next_day = current + timedelta(days=1)
        if next_day < today:
            prefix = f"{border}/{direction}/{next_day.year}/{next_day.month}/{next_day.day}/"
            if any(self.container_client.list_blobs(name_starts_with=prefix)):
                return prefix
        
        # Try next month
        if month < 12:
            prefix = f"{border}/{direction}/{year}/{month+1}/"
            if any(self.container_client.list_blobs(name_starts_with=prefix)):
                return prefix
        
        # Try next year
        prefix = f"{border}/{direction}/{year+1}/"
        if any(self.container_client.list_blobs(name_starts_with=prefix)):
            return prefix
        
        # Try next border
        borders = sorted([
            Path(b.name).parts[0] 
            for b in self.container_client.list_blobs(name_starts_with="")
        ])
        try:
            idx = borders.index(border)
            if idx < len(borders) - 1:
                return f"{borders[idx+1]}/"
        except ValueError:
            pass
        
        return None

    def get_next_batch(self) -> Optional[List[str]]:
        """Get next batch of images to process"""
        today = datetime.now().date()
        last_batch = self._get_last_processed_batch()
        
        if last_batch:
            # Start from where we left off
            border, direction, year, month, day = last_batch.path_parts
            current_prefix = f"{border}/{direction}/{year}/{month}/{day}/"
            
            # Get remaining images in current day
            images = self._list_blobs_after(current_prefix, last_batch.last_image_path)
            
            # If no images in current day, try next valid path
            if not images:
                next_prefix = self._get_next_path(border, direction, year, month, day)
                if next_prefix:
                    images = self._list_blobs_after(next_prefix)
        else:
            # Start from the beginning
            for blob in self.container_client.list_blobs(name_starts_with=""):
                border = Path(blob.name).parts[0]
                current_prefix = f"{border}/"
                images = self._list_blobs_after(current_prefix)
                if images:
                    break
        
        # Filter out today's images
        if images:
            images = [
                img for img in images
                if self._get_image_date(img) < today
            ]
            
            if images:
                # Record batch start
                self._record_batch_start(images[-1])
                return images
                
        return None

    def _get_image_date(self, image_path: str) -> datetime.date:
        """Extract date from image path"""
        parts = Path(image_path).parts
        return datetime(
            int(parts[2]),  # year
            int(parts[3]),  # month
            int(parts[4])   # day
        ).date()

    def set_batch_status(self, last_image_path: str, status: ProcessingStatus):
        """Set status of batch in database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE batch_tracking
                SET status = ?, end_time = ?
                WHERE last_image_path = ? AND status = ?
            ''', (
                status.value,
                datetime.now().isoformat(),
                last_image_path,
                ProcessingStatus.IN_PROGRESS.value
            ))

    def _transform_for_yolo(self, image_data: bytes) -> np.ndarray:
        """Convert image bytes to numpy array for YOLO"""
        nparr = np.frombuffer(image_data, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    def _save_locally(self, image_data: bytes, blob_path: str) -> str:
        """Save image to local storage"""
        temp_dir = "storage/temp"
        local_path = os.path.join(temp_dir, blob_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'wb') as f:
            f.write(image_data)
            
        return local_path

    def download_images(self, blob_paths: List[str], save_first: bool = True) -> List[np.ndarray]:
        """
        Download and process images from blob storage.
        
        Args:
            blob_paths: List of blob paths to download
            save_first: Whether to save first image locally for verification
            
        Returns:
            List of numpy arrays ready for YOLO
        """
        images = []
        
        for i, blob_path in enumerate(blob_paths):
            try:
                # Get blob client and download
                blob_client = self.container_client.get_blob_client(blob_path)
                image_data = blob_client.download_blob().readall()
                
                # Save first image if requested
                # if save_first and i == 0:
                #    self._save_locally(image_data, blob_path)
                
                # Transform for YOLO
                image = self._transform_for_yolo(image_data)
                images.append(image)
                

                # Mark batch as completed TEMPORARYLY
                self.set_batch_status(blob_paths[-1], ProcessingStatus.COMPLETED)



            except Exception as e:
                self.logger.error(f"Error downloading {blob_path}: {e}")
                raise
                
        return images
            