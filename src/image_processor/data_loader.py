from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, Tuple, Dict, Callable
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2 import pool
import logging
from dotenv import load_dotenv
import numpy as np
import cv2
import os
import time
import functools
import json
from collections import defaultdict
from hdfs import InsecureClient

# =============================================================================
# CONSTANTS - Known borders and directions for Serbian border crossings
# =============================================================================

KNOWN_BORDERS = [
    "BATROVCI", "DJALA", "GRADINA", "HORGOS",
    "KELEBIJA", "KOTROMAN", "RACA", "SID",
    "SPILJANI", "VRSKA-CUKA"
]

KNOWN_DIRECTIONS = ["I", "U"]  # I=izlaz/exit, U=ulaz/entrance

# =============================================================================
# RETRY CONFIGURATION
# =============================================================================

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    base_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt using exponential backoff"""
        delay = self.base_delay * (self.exponential_base ** attempt)
        return min(delay, self.max_delay)


def retry_with_backoff(config: Optional[RetryConfig] = None):
    """Decorator for retrying functions with exponential backoff"""
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < config.max_retries:
                        delay = config.get_delay(attempt)
                        logging.warning(
                            f"Attempt {attempt + 1}/{config.max_retries + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logging.error(f"All {config.max_retries + 1} attempts failed for {func.__name__}: {e}")
            raise last_exception
        return wrapper
    return decorator


# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    """Manages PostgreSQL connections with connection pooling"""

    _instance = None
    _pool = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        min_connections: int = 1,
        max_connections: int = 10
    ):
        if self._pool is not None:
            return

        self.host = host or os.environ.get("POSTGRES_HOST", "hive-metastore-postgresql")
        self.port = port or int(os.environ.get("POSTGRES_PORT", "5432"))
        self.database = database or os.environ.get("POSTGRES_DB", "hive")
        self.user = user or os.environ.get("POSTGRES_USER", "hive")
        self.password = password or os.environ.get("POSTGRES_PASSWORD", "hive")

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            min_connections,
            max_connections,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

        self._init_schema()
        logging.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")

    def _init_schema(self):
        """Initialize database schema with new day-level atomic writes architecture"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Create schema
                cur.execute("CREATE SCHEMA IF NOT EXISTS image_processor")

                # Create day_processing table (primary transaction table)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS image_processor.day_processing (
                        -- Identity
                        id SERIAL PRIMARY KEY,
                        processing_date DATE NOT NULL UNIQUE,

                        -- Transaction status
                        status VARCHAR(30) NOT NULL,
                        retry_count INTEGER NOT NULL DEFAULT 0,

                        -- Progress tracking (updated as day processes)
                        borders_completed JSONB NOT NULL DEFAULT '[]',
                        total_batches INTEGER DEFAULT 0,
                        total_images INTEGER DEFAULT 0,
                        total_detections INTEGER DEFAULT 0,

                        -- Discovery metadata
                        available_borders JSONB NOT NULL DEFAULT '[]',

                        -- HDFS results (set on completion)
                        hdfs_path TEXT,
                        file_size_bytes BIGINT,

                        -- Error handling
                        error_message TEXT,
                        last_error_time TIMESTAMPTZ,

                        -- Timestamps
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                        CONSTRAINT chk_status CHECK (status IN (
                            'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PERMANENTLY_FAILED'
                        ))
                    )
                """)

                # Create indexes for day_processing
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_day_processing_date
                    ON image_processor.day_processing(processing_date)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_day_processing_status
                    ON image_processor.day_processing(status)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_day_processing_date_status
                    ON image_processor.day_processing(processing_date, status)
                """)

                # Create batch_log table (audit trail + within-day resume)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS image_processor.batch_log (
                        id SERIAL PRIMARY KEY,
                        processing_date DATE NOT NULL,
                        border VARCHAR(100) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        batch_number INTEGER NOT NULL,

                        -- Batch metrics
                        image_count INTEGER NOT NULL,
                        detection_count INTEGER NOT NULL,
                        last_image_path TEXT NOT NULL,

                        -- Timing
                        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        completed_at TIMESTAMPTZ,
                        duration_seconds NUMERIC(10, 2),

                        -- Foreign key to day
                        day_processing_id INTEGER REFERENCES image_processor.day_processing(id) ON DELETE CASCADE,

                        CONSTRAINT unique_batch UNIQUE(processing_date, border, direction, batch_number)
                    )
                """)

                # Create indexes for batch_log
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_batch_log_date
                    ON image_processor.batch_log(processing_date)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_batch_log_day_id
                    ON image_processor.batch_log(day_processing_id)
                """)

                # Create trigger function for auto-updating updated_at
                cur.execute("""
                    CREATE OR REPLACE FUNCTION image_processor.update_updated_at_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = NOW();
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)

                # Create trigger on day_processing
                cur.execute("""
                    DROP TRIGGER IF EXISTS update_day_processing_updated_at
                    ON image_processor.day_processing
                """)
                cur.execute("""
                    CREATE TRIGGER update_day_processing_updated_at
                        BEFORE UPDATE ON image_processor.day_processing
                        FOR EACH ROW
                        EXECUTE PROCEDURE image_processor.update_updated_at_column()
                """)

                conn.commit()

    def get_connection(self):
        """Get a connection from the pool as a context manager"""
        return _ConnectionContext(self._pool)

    def close(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            DatabaseManager._pool = None
            DatabaseManager._instance = None
            logging.info("Closed all database connections")


class _ConnectionContext:
    """Context manager for database connections"""

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    def __enter__(self):
        self._conn = self._pool.getconn()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            if exc_type is not None:
                self._conn.rollback()
            self._pool.putconn(self._conn)
        return False


# =============================================================================
# IMAGE LOADER
# =============================================================================

class ImageLoader:
    def __init__(
        self,
        container_name: str = "not-processed-imgs",
        batch_size: int = 20,
        db_manager: Optional[DatabaseManager] = None,
        max_workers: int = 5,
        retry_config: Optional[RetryConfig] = None,
        hdfs_namenode: str = "http://namenode:9870"
    ):
        load_dotenv()

        connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        self.container_client = BlobServiceClient.from_connection_string(
            connection_string
        ).get_container_client(container_name)

        self.db_manager = db_manager or DatabaseManager()
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.retry_config = retry_config or RetryConfig()

        # Initialize HDFS client for image caching
        self.hdfs_client = InsecureClient(hdfs_namenode, user="root")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

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

    # =============================================================================
    # DAY-LEVEL DISCOVERY METHODS (93% API Call Reduction)
    # =============================================================================

    def discover_days_in_month(
        self,
        border: str,
        direction: str,
        year: int,
        month: int
    ) -> List[int]:
        """
        Discover all days with data for border/direction/month using walk_blobs.

        Uses Azure's delimiter='/' feature to list "virtual directories" efficiently.

        Args:
            border: Border name
            direction: Direction (I or U)
            year: Year
            month: Month (1-12)

        Returns:
            List of day numbers (1-31) with data

        API calls: 1 per border/direction/month
        """
        prefix = f"{border}/{direction}/{year}/{month}/"
        days = []

        try:
            for item in self.container_client.walk_blobs(
                name_starts_with=prefix,
                delimiter='/'
            ):
                if hasattr(item, 'name'):  # BlobPrefix (virtual directory)
                    day_str = item.name.rstrip('/').split('/')[-1]
                    try:
                        day = int(day_str)
                        if 1 <= day <= 31:
                            days.append(day)
                    except ValueError:
                        continue
        except Exception as e:
            self.logger.warning(f"Error discovering days for {prefix}: {e}")

        return sorted(days)

    def _discover_month_parallel(
        self,
        year: int,
        month: int
    ) -> Dict[int, List[Tuple[str, str]]]:
        """
        Discover all days in a month across all borders in parallel.

        Args:
            year: Year
            month: Month (1-12)

        Returns:
            Dict mapping day number to list of (border, direction) tuples
            Example: {15: [("HORGOS", "I"), ("HORGOS", "U")], 16: [...]}

        API calls: 20 (10 borders x 2 directions)
        """
        day_data = defaultdict(list)

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {}

            for border in KNOWN_BORDERS:
                for direction in KNOWN_DIRECTIONS:
                    future = executor.submit(
                        self.discover_days_in_month,
                        border, direction, year, month
                    )
                    futures[future] = (border, direction)

            for future in as_completed(futures):
                border, direction = futures[future]
                try:
                    days = future.result()
                    for day in days:
                        day_data[day].append((border, direction))
                except Exception as e:
                    self.logger.warning(f"Error discovering {border}/{direction} for {year}-{month}: {e}")

        return day_data

    def discover_next_days_batch(
        self,
        start_date: date,
        max_days: int = 30
    ) -> List[Tuple[date, List[Tuple[str, str]]]]:
        """
        Discover next batch of days with data, starting from start_date.

        Processes month by month using walk_blobs() for efficiency.

        Args:
            start_date: Date to start searching from (exclusive)
            max_days: Maximum days to discover

        Returns:
            List of (date, [(border, direction), ...]) tuples

        API calls: ~20 per month (10 borders x 2 directions)
        """
        results = []
        current = start_date
        today = datetime.now().date()

        while len(results) < max_days and current < today:
            year, month = current.year, current.month

            self.logger.debug(f"Discovering days in {year}-{month:02d}...")

            # Discover all days in this month (20 API calls)
            month_data = self._discover_month_parallel(year, month)

            # Filter to dates >= current and < today
            for day, borders in sorted(month_data.items()):
                try:
                    check_date = date(year, month, day)
                except ValueError:
                    continue

                if check_date >= current and check_date < today:
                    results.append((check_date, borders))
                    if len(results) >= max_days:
                        break

            # Move to next month
            if month == 12:
                current = date(year + 1, 1, 1)
            else:
                current = date(year, month + 1, 1)

        return results

    def _get_last_processed_date(self) -> Optional[date]:
        """Get the last processed date from database"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT processing_date
                    FROM image_processor.day_processing
                    ORDER BY processing_date DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                return row[0] if row else None

    # =============================================================================
    # DAY-LEVEL TRANSACTION MANAGEMENT
    # =============================================================================

    def get_next_day(self) -> Optional[date]:
        """
        Get next day to process with intelligent prioritization.

        Priority order:
        1. FAILED days (retry before discovering new days)
        2. PENDING days (from previous discovery batch)
        3. Discover new days (if no pending/failed)

        Returns:
            Next date to process or None if exhausted
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                # Priority 1: Check for failed days (retry)
                cur.execute("""
                    SELECT processing_date, retry_count
                    FROM image_processor.day_processing
                    WHERE status = 'FAILED' AND retry_count < 3
                    ORDER BY processing_date
                    LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    self.logger.info(f"Found failed day for retry: {row[0]} (attempt {row[1] + 1}/3)")
                    return row[0]

                # Priority 2: Check for pending days
                cur.execute("""
                    SELECT processing_date
                    FROM image_processor.day_processing
                    WHERE status = 'PENDING'
                    ORDER BY processing_date
                    LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    self.logger.info(f"Found pending day: {row[0]}")
                    return row[0]

        # Priority 3: Discover new days
        return self._discover_and_initialize_next_day()

    def _discover_and_initialize_next_day(self) -> Optional[date]:
        """
        Discover next batch of days and initialize them in database.
        """
        last_date = self._get_last_processed_date()

        if last_date is None:
            last_date = date(2020, 1, 1)
            self.logger.info(f"Cold start - searching from {last_date}")

        self.logger.info("Discovering next batch of days...")
        days_batch = self.discover_next_days_batch(start_date=last_date, max_days=30)

        if not days_batch:
            self.logger.info("No more days discovered")
            return None

        self.logger.info(f"Discovered {len(days_batch)} days with data")

        # Initialize all discovered days in database
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                for processing_date, borders in days_batch:
                    borders_json = json.dumps([f"{b}/{d}" for b, d in borders])
                    cur.execute("""
                        INSERT INTO image_processor.day_processing
                        (processing_date, status, available_borders)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (processing_date) DO NOTHING
                    """, (processing_date, 'PENDING', borders_json))
                conn.commit()

        return days_batch[0][0]

    def begin_day_transaction(self, processing_date: date):
        """Mark day as IN_PROGRESS to begin transaction."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE image_processor.day_processing
                    SET status = 'IN_PROGRESS', started_at = NOW()
                    WHERE processing_date = %s
                """, (processing_date,))
                conn.commit()
        self.logger.info(f"BEGIN TRANSACTION: Day {processing_date}")

    def get_borders_for_day(self, processing_date: date) -> List[Tuple[str, str]]:
        """Get list of (border, direction) tuples for a day."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT available_borders
                    FROM image_processor.day_processing
                    WHERE processing_date = %s
                """, (processing_date,))
                row = cur.fetchone()

                if not row or not row[0]:
                    return []

                # Parse JSONB: ["HORGOS/I", "HORGOS/U"] -> [("HORGOS", "I"), ("HORGOS", "U")]
                borders = []
                for item in row[0]:
                    parts = item.split('/')
                    if len(parts) == 2:
                        borders.append((parts[0], parts[1]))
                return borders

    def get_next_batch_for_day(
        self, processing_date: date, border: str, direction: str
    ) -> Optional[List[str]]:
        """
        Get next batch for day/border/direction.
        Uses batch_log for within-day resume.
        """
        # Get last processed image from batch_log
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT last_image_path
                    FROM image_processor.batch_log
                    WHERE processing_date = %s AND border = %s AND direction = %s
                    ORDER BY batch_number DESC
                    LIMIT 1
                """, (processing_date, border, direction))
                row = cur.fetchone()
                last_image_path = row[0] if row else None

        prefix = f"{border}/{direction}/{processing_date.year}/{processing_date.month}/{processing_date.day}/"
        images = self._list_blobs_after(prefix, last_image_path)
        return images if images else None

    def log_batch(
        self, processing_date: date, border: str, direction: str,
        batch_number: int, image_count: int, detection_count: int, last_image_path: str
    ):
        """Log batch to audit trail for debugging and within-day resume."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM image_processor.day_processing
                    WHERE processing_date = %s
                """, (processing_date,))
                day_id = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO image_processor.batch_log
                    (processing_date, border, direction, batch_number,
                     image_count, detection_count, last_image_path,
                     day_processing_id, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (processing_date, border, direction, batch_number)
                    DO UPDATE SET
                        image_count = EXCLUDED.image_count,
                        detection_count = EXCLUDED.detection_count,
                        last_image_path = EXCLUDED.last_image_path,
                        completed_at = NOW()
                """, (processing_date, border, direction, batch_number,
                      image_count, detection_count, last_image_path, day_id))
                conn.commit()

    def mark_border_complete(self, processing_date: date, border: str, direction: str):
        """Mark border/direction complete for day (updates JSONB array)."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                border_key = f"{border}/{direction}"
                cur.execute("""
                    UPDATE image_processor.day_processing
                    SET borders_completed = borders_completed || %s::jsonb
                    WHERE processing_date = %s
                """, (json.dumps([border_key]), processing_date))
                conn.commit()

    def update_day_status(
        self, processing_date: date, status: str,
        hdfs_path: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        total_detections: Optional[int] = None
    ):
        """Update day status with completion results."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                # Aggregate batch metrics
                cur.execute("""
                    SELECT COUNT(*), COALESCE(SUM(image_count), 0)
                    FROM image_processor.batch_log
                    WHERE processing_date = %s
                """, (processing_date,))
                batch_count, image_count = cur.fetchone()

                cur.execute("""
                    UPDATE image_processor.day_processing
                    SET status = %s, hdfs_path = %s, file_size_bytes = %s,
                        total_detections = %s, total_batches = %s, total_images = %s,
                        completed_at = NOW()
                    WHERE processing_date = %s
                """, (status, hdfs_path, file_size_bytes, total_detections,
                      batch_count, image_count, processing_date))
                conn.commit()
        self.logger.info(f"COMMIT TRANSACTION: Day {processing_date} marked {status}")

    def mark_day_failed(self, processing_date: date, error_message: str):
        """Mark day as FAILED (will be retried)."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE image_processor.day_processing
                    SET status = 'FAILED', error_message = %s, last_error_time = NOW()
                    WHERE processing_date = %s
                """, (error_message, processing_date))
                conn.commit()
        self.logger.warning(f"ROLLBACK TRANSACTION: Day {processing_date} marked FAILED")

    def mark_day_permanently_failed(self, processing_date: date, error_message: str):
        """Mark day as PERMANENTLY_FAILED (will be skipped)."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE image_processor.day_processing
                    SET status = 'PERMANENTLY_FAILED', error_message = %s, last_error_time = NOW()
                    WHERE processing_date = %s
                """, (error_message, processing_date))
                conn.commit()
        self.logger.error(f"Day {processing_date} marked PERMANENTLY_FAILED")

    def get_day_retry_count(self, processing_date: date) -> int:
        """Get retry count for a day."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT retry_count FROM image_processor.day_processing
                    WHERE processing_date = %s
                """, (processing_date,))
                row = cur.fetchone()
                return row[0] if row else 0

    def increment_day_retry(self, processing_date: date):
        """Increment retry count and reset status to PENDING for retry."""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE image_processor.day_processing
                    SET retry_count = retry_count + 1, status = 'PENDING', started_at = NULL
                    WHERE processing_date = %s
                """, (processing_date,))
                conn.commit()

    # =============================================================================
    # HDFS IMAGE CACHING (For Retry Resilience)
    # =============================================================================

    def download_images_with_hdfs_cache(
        self,
        batch: List[str],
        processing_date: date
    ) -> Tuple[List[np.ndarray], List[str], List[Tuple[str, str]]]:
        """
        Download images with HDFS caching for retry resilience.

        Strategy:
        1. Check HDFS cache first: /hdfs/temp/images/{processing_date}/{blob_path}
        2. If cached, read from HDFS (fast)
        3. If not cached, download from Azure and save to HDFS
        4. Return images for processing

        Args:
            batch: List of blob paths
            processing_date: Date being processed (for cache directory)

        Returns:
            Tuple of (images, successful_paths, failed_downloads)
        """
        images = []
        successful_paths = []
        failed = []

        hdfs_cache_base = f"/hdfs/temp/images/{processing_date.strftime('%Y-%m-%d')}"

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            for blob_path in batch:
                future = executor.submit(
                    self._download_or_load_cached,
                    blob_path,
                    hdfs_cache_base
                )
                futures[future] = blob_path

            for future in as_completed(futures):
                path = futures[future]
                try:
                    image = future.result()
                    images.append(image)
                    successful_paths.append(path)
                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"Error loading {path}: {error_msg}")
                    failed.append((path, error_msg))

        # Sort by original order
        path_order = {p: i for i, p in enumerate(batch)}
        sorted_pairs = sorted(
            zip(successful_paths, images),
            key=lambda x: path_order.get(x[0], float('inf'))
        )

        if sorted_pairs:
            successful_paths, images = zip(*sorted_pairs)
            return list(images), list(successful_paths), failed

        return [], [], failed

    @retry_with_backoff()
    def _download_or_load_cached(
        self,
        blob_path: str,
        hdfs_cache_base: str
    ) -> np.ndarray:
        """
        Load image from HDFS cache or download from Azure if not cached.

        Args:
            blob_path: Azure blob path (e.g., "HORGOS/I/2024/12/15/14-30-45.jpg")
            hdfs_cache_base: HDFS cache directory (e.g., "/hdfs/temp/images/2024-12-15")

        Returns:
            Image as numpy array
        """
        hdfs_path = f"{hdfs_cache_base}/{blob_path}"

        # Try HDFS cache first
        try:
            with self.hdfs_client.read(hdfs_path) as reader:
                image_data = reader.read()
                image = self._transform_for_yolo(image_data)
                self.logger.debug(f"Loaded from HDFS cache: {blob_path}")
                return image
        except Exception:
            # Not in cache, download from Azure
            pass

        # Download from Azure
        blob_client = self.container_client.get_blob_client(blob_path)
        image_data = blob_client.download_blob().readall()
        image = self._transform_for_yolo(image_data)

        # Save to HDFS cache for future retries
        try:
            hdfs_dir = os.path.dirname(hdfs_path)
            self.hdfs_client.makedirs(hdfs_dir, permission=755)
            with self.hdfs_client.write(hdfs_path, overwrite=True) as writer:
                writer.write(image_data)
            self.logger.debug(f"Cached to HDFS: {blob_path}")
        except Exception as e:
            # Cache write failure is non-fatal (retry will re-download)
            self.logger.warning(f"Failed to cache {blob_path} to HDFS: {e}")

        return image

    def cleanup_day_cache(self, processing_date: date):
        """
        Delete cached images for a day after completion or permanent failure.

        Args:
            processing_date: Date to clean up
        """
        hdfs_cache_dir = f"/hdfs/temp/images/{processing_date.strftime('%Y-%m-%d')}"

        try:
            self.hdfs_client.delete(hdfs_cache_dir, recursive=True)
            self.logger.info(f"Cleaned up HDFS image cache for {processing_date}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup cache for {processing_date}: {e}")

    def _transform_for_yolo(self, image_data: bytes) -> np.ndarray:
        """Convert image bytes to numpy array for YOLO"""
        nparr = np.frombuffer(image_data, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)
