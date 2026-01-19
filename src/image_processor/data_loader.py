from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple, Dict, Callable
from azure.storage.blob import BlobServiceClient
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2 import pool
import itertools
import logging
from dotenv import load_dotenv
from enum import Enum
import numpy as np
import cv2
import os
import time
import functools

# =============================================================================
# CONSTANTS - Known borders and directions for Serbian border crossings
# =============================================================================

KNOWN_BORDERS = [
    "BATROVCI", "DJALA", "GOSTUN", "GRADINA", "HORGOS", "JABUKA",
    "KELEBIJA", "KOTROMAN", "MZVORNIK", "PRESEVO", "RACA", "SID",
    "SPILJANI", "TRBUSNICA", "VATIN", "VRSKA-CUKA"
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
# ENUMS AND DATA CLASSES
# =============================================================================

class ProcessingStatus(Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class HDFSStatus(Enum):
    """Status of batch with respect to HDFS saving"""
    PENDING = "PENDING"
    SAVED = "SAVED"
    FAILED = "FAILED"


class DayProgressStatus(Enum):
    """Status of border/direction processing for a specific day"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    SKIPPED = "SKIPPED"  # No data available for this border/direction/day


@dataclass
class ImageBatchMetadata:
    """Metadata about a processed image batch"""
    last_image_path: str
    batch_size: int
    status: ProcessingStatus
    hdfs_status: HDFSStatus
    start_time: datetime
    end_time: Optional[datetime]
    border: Optional[str] = None
    hdfs_path: Optional[str] = None
    retry_count: int = 0
    error_message: Optional[str] = None

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
        self.database = database or os.environ.get("POSTGRES_DB", "metastore")
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
        """Initialize database schema with migration support"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS image_processor")

                # Create batch_tracking table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS image_processor.batch_tracking (
                        id SERIAL PRIMARY KEY,
                        last_image_path TEXT NOT NULL,
                        batch_size INTEGER NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS',
                        hdfs_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                        start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        end_time TIMESTAMPTZ,
                        border VARCHAR(100),
                        processing_date DATE,
                        hdfs_path TEXT,
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        error_message TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                        CONSTRAINT chk_status CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
                        CONSTRAINT chk_hdfs_status CHECK (hdfs_status IN ('PENDING', 'SAVED', 'FAILED'))
                    )
                """)

                # Create indexes
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_batch_status
                    ON image_processor.batch_tracking(status)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_batch_hdfs_status
                    ON image_processor.batch_tracking(hdfs_status)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_batch_start_time
                    ON image_processor.batch_tracking(start_time DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_batch_processing_date
                    ON image_processor.batch_tracking(processing_date)
                """)

                # Create day_progress table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS image_processor.day_progress (
                        id SERIAL PRIMARY KEY,
                        processing_date DATE NOT NULL,
                        border VARCHAR(100) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        last_image_path TEXT,
                        batch_count INTEGER DEFAULT 0,
                        image_count INTEGER DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                        UNIQUE(processing_date, border, direction),
                        CONSTRAINT chk_day_status CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'SKIPPED'))
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_day_progress_date
                    ON image_processor.day_progress(processing_date)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_day_progress_status
                    ON image_processor.day_progress(status)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_day_progress_date_status
                    ON image_processor.day_progress(processing_date, status)
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

                # Create trigger on day_progress (use PROCEDURE for older PostgreSQL versions)
                cur.execute("""
                    DROP TRIGGER IF EXISTS update_day_progress_updated_at
                    ON image_processor.day_progress
                """)
                cur.execute("""
                    CREATE TRIGGER update_day_progress_updated_at
                        BEFORE UPDATE ON image_processor.day_progress
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
        retry_config: Optional[RetryConfig] = None
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

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _get_last_processed_batch(self) -> Optional[ImageBatchMetadata]:
        """Get the last processed batch from database"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT last_image_path, batch_size, status, hdfs_status,
                           start_time, end_time, border, hdfs_path, retry_count, error_message
                    FROM image_processor.batch_tracking
                    ORDER BY start_time DESC LIMIT 1
                """)
                row = cur.fetchone()

                if row:
                    return ImageBatchMetadata(
                        last_image_path=row[0],
                        batch_size=row[1],
                        status=ProcessingStatus(row[2]),
                        hdfs_status=HDFSStatus(row[3]),
                        start_time=row[4],
                        end_time=row[5],
                        border=row[6],
                        hdfs_path=row[7],
                        retry_count=row[8],
                        error_message=row[9]
                    )
        return None

    def _record_batch_start(self, last_image_path: str, border: str, processing_date: Optional[date] = None) -> int:
        """Record the start of batch processing"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO image_processor.batch_tracking
                    (last_image_path, batch_size, status, hdfs_status, start_time, border, processing_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    last_image_path,
                    self.batch_size,
                    ProcessingStatus.IN_PROGRESS.value,
                    HDFSStatus.PENDING.value,
                    datetime.now(),
                    border,
                    processing_date
                ))
                batch_id = cur.fetchone()[0]
                conn.commit()
                return batch_id

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

    def _get_next_path_for_border_direction(
        self,
        border: str,
        direction: str,
        year: int,
        month: int,
        day: int
    ) -> Optional[str]:
        """Get next valid path within a border/direction"""
        today = datetime.now().date()
        current = datetime(year, month, day).date()

        # Try next day
        next_day = current + timedelta(days=1)
        if next_day < today:
            prefix = f"{border}/{direction}/{next_day.year}/{next_day.month}/{next_day.day}/"
            blobs = list(itertools.islice(
                self.container_client.list_blobs(name_starts_with=prefix), 1
            ))
            if blobs:
                return prefix

        # Try next month
        if month < 12:
            prefix = f"{border}/{direction}/{year}/{month+1}/"
            blobs = list(itertools.islice(
                self.container_client.list_blobs(name_starts_with=prefix), 1
            ))
            if blobs:
                return prefix

        # Try next year
        prefix = f"{border}/{direction}/{year+1}/"
        blobs = list(itertools.islice(
            self.container_client.list_blobs(name_starts_with=prefix), 1
        ))
        if blobs:
            return prefix

        return None

    def _get_next_path(
        self,
        border: str,
        direction: str,
        year: int,
        month: int,
        day: int
    ) -> Optional[str]:
        """Get next valid path to check for images using configured borders/directions"""
        # First, try to continue within the same border/direction
        next_in_current = self._get_next_path_for_border_direction(
            border, direction, year, month, day
        )
        if next_in_current:
            return next_in_current

        # Try next direction in the same border
        try:
            dir_idx = KNOWN_DIRECTIONS.index(direction)
            if dir_idx < len(KNOWN_DIRECTIONS) - 1:
                next_direction = KNOWN_DIRECTIONS[dir_idx + 1]
                prefix = f"{border}/{next_direction}/"
                blobs = list(itertools.islice(
                    self.container_client.list_blobs(name_starts_with=prefix), 1
                ))
                if blobs:
                    return prefix
        except ValueError:
            pass

        # Try next border
        try:
            border_idx = KNOWN_BORDERS.index(border)
            for next_border in KNOWN_BORDERS[border_idx + 1:]:
                for next_direction in KNOWN_DIRECTIONS:
                    prefix = f"{next_border}/{next_direction}/"
                    blobs = list(itertools.islice(
                        self.container_client.list_blobs(name_starts_with=prefix), 1
                    ))
                    if blobs:
                        return prefix
        except ValueError:
            pass

        return None

    def get_all_borders(self) -> List[str]:
        """Get all known border names (from configuration, no API call)"""
        return KNOWN_BORDERS.copy()

    def _find_starting_point(self) -> Optional[str]:
        """Find earliest data by probing configured borders (no full listing)"""
        self.logger.info("Finding starting point by probing configured borders...")

        for border in KNOWN_BORDERS:
            for direction in KNOWN_DIRECTIONS:
                # Probe years from 2020 onwards
                for year in range(2020, datetime.now().year + 1):
                    prefix = f"{border}/{direction}/{year}/"
                    # Only fetch 1 blob to check existence
                    blobs = list(itertools.islice(
                        self.container_client.list_blobs(name_starts_with=prefix), 1
                    ))
                    if blobs:
                        # Found data, drill down to find first day
                        self.logger.info(f"Found data at {prefix}, drilling down...")
                        first_path = self._find_first_day(border, direction, year)
                        if first_path:
                            return first_path
        return None

    def _find_first_day(self, border: str, direction: str, year: int) -> Optional[str]:
        """Find first day with data in a year"""
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    # Validate date
                    datetime(year, month, day)
                    prefix = f"{border}/{direction}/{year}/{month}/{day}/"
                    blobs = list(itertools.islice(
                        self.container_client.list_blobs(name_starts_with=prefix), 1
                    ))
                    if blobs:
                        return prefix
                except ValueError:
                    # Invalid date (e.g., Feb 30)
                    continue
        return f"{border}/{direction}/{year}/"

    # =============================================================================
    # DAY-FIRST PROCESSING METHODS
    # =============================================================================

    def _probe_border_for_day(self, border: str, direction: str, target_date: date) -> Optional[str]:
        """
        Check if a border/direction has data for a specific day.

        Args:
            border: Border name
            direction: Direction (I or U)
            target_date: Date to check

        Returns:
            Prefix path if data exists, None otherwise
        """
        prefix = f"{border}/{direction}/{target_date.year}/{target_date.month}/{target_date.day}/"
        blobs = list(itertools.islice(
            self.container_client.list_blobs(name_starts_with=prefix), 1
        ))
        return prefix if blobs else None

    def _probe_all_borders_parallel(self, target_date: date) -> List[Tuple[str, str, str]]:
        """
        Probe all borders in parallel to find which have data for a specific day.

        Args:
            target_date: Date to check

        Returns:
            List of (border, direction, prefix) tuples with data
        """
        results = []
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = {}
            for border in KNOWN_BORDERS:
                for direction in KNOWN_DIRECTIONS:
                    future = executor.submit(self._probe_border_for_day, border, direction, target_date)
                    futures[future] = (border, direction)

            for future in as_completed(futures):
                border, direction = futures[future]
                try:
                    prefix = future.result()
                    if prefix:
                        results.append((border, direction, prefix))
                except Exception as e:
                    self.logger.warning(f"Error probing {border}/{direction} for {target_date}: {e}")

        return results

    def _get_current_processing_day(self) -> Optional[date]:
        """
        Get the current day being processed.

        Returns:
            Date if there's a day in progress/pending, None otherwise
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT processing_date
                    FROM image_processor.day_progress
                    WHERE status IN (%s, %s)
                    ORDER BY processing_date
                    LIMIT 1
                """, (DayProgressStatus.PENDING.value, DayProgressStatus.IN_PROGRESS.value))
                row = cur.fetchone()
                return row[0] if row else None

    def _initialize_day(self, processing_date: date) -> int:
        """
        Initialize day processing by probing all borders in parallel.

        Args:
            processing_date: Date to initialize

        Returns:
            Count of borders with data
        """
        self.logger.info(f"Initializing day {processing_date} - probing all borders in parallel...")

        # Probe all borders in parallel
        borders_with_data = self._probe_all_borders_parallel(processing_date)

        self.logger.info(f"Found {len(borders_with_data)} border/direction combinations with data for {processing_date}")

        # Create day_progress entries
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                borders_with_data_set = {(b, d) for b, d, _ in borders_with_data}

                # Insert entries for borders with data (PENDING)
                for border, direction, prefix in borders_with_data:
                    cur.execute("""
                        INSERT INTO image_processor.day_progress
                        (processing_date, border, direction, status)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (processing_date, border, direction) DO NOTHING
                    """, (processing_date, border, direction, DayProgressStatus.PENDING.value))

                # Insert entries for borders without data (SKIPPED) for audit trail
                for border in KNOWN_BORDERS:
                    for direction in KNOWN_DIRECTIONS:
                        if (border, direction) not in borders_with_data_set:
                            cur.execute("""
                                INSERT INTO image_processor.day_progress
                                (processing_date, border, direction, status)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (processing_date, border, direction) DO NOTHING
                            """, (processing_date, border, direction, DayProgressStatus.SKIPPED.value))

                conn.commit()

        return len(borders_with_data)

    def _get_next_border_for_day(self, processing_date: date) -> Optional[Tuple[str, str]]:
        """
        Get the next border/direction to process for a specific day.

        Args:
            processing_date: Date to get next border for

        Returns:
            (border, direction) tuple or None if all complete
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT border, direction
                    FROM image_processor.day_progress
                    WHERE processing_date = %s AND status IN (%s, %s)
                    ORDER BY border, direction
                    LIMIT 1
                """, (processing_date, DayProgressStatus.PENDING.value, DayProgressStatus.IN_PROGRESS.value))
                row = cur.fetchone()
                return (row[0], row[1]) if row else None

    def _discover_next_day_with_data(self, after_date: date) -> Optional[date]:
        """
        Find the next day with data after a given date by probing in parallel.

        Args:
            after_date: Date to search after

        Returns:
            Next date with data or None
        """
        today = datetime.now().date()
        current_date = after_date + timedelta(days=1)

        # Check up to 30 days ahead at a time for efficiency
        while current_date < today:
            self.logger.info(f"Searching for next day with data starting from {current_date}...")

            # Check multiple days in parallel (up to 30 days)
            dates_to_check = []
            for i in range(30):
                check_date = current_date + timedelta(days=i)
                if check_date >= today:
                    break
                dates_to_check.append(check_date)

            # Probe each date
            for check_date in dates_to_check:
                borders_with_data = self._probe_all_borders_parallel(check_date)
                if borders_with_data:
                    self.logger.info(f"Found next day with data: {check_date}")
                    return check_date

            current_date += timedelta(days=30)

        self.logger.info("No more days with data found")
        return None

    def _update_day_progress(
        self,
        processing_date: date,
        border: str,
        direction: str,
        status: DayProgressStatus,
        last_image_path: Optional[str] = None,
        increment_batch: bool = False,
        image_count: int = 0
    ):
        """
        Update day_progress entry.

        Args:
            processing_date: Date being processed
            border: Border name
            direction: Direction
            status: New status
            last_image_path: Last processed image path
            increment_batch: Whether to increment batch_count
            image_count: Number of images processed (to add to total)
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                if increment_batch:
                    cur.execute("""
                        UPDATE image_processor.day_progress
                        SET status = %s, last_image_path = %s,
                            batch_count = batch_count + 1,
                            image_count = image_count + %s
                        WHERE processing_date = %s AND border = %s AND direction = %s
                    """, (status.value, last_image_path, image_count, processing_date, border, direction))
                elif last_image_path:
                    cur.execute("""
                        UPDATE image_processor.day_progress
                        SET status = %s, last_image_path = %s
                        WHERE processing_date = %s AND border = %s AND direction = %s
                    """, (status.value, last_image_path, processing_date, border, direction))
                else:
                    cur.execute("""
                        UPDATE image_processor.day_progress
                        SET status = %s
                        WHERE processing_date = %s AND border = %s AND direction = %s
                    """, (status.value, processing_date, border, direction))
                conn.commit()

    def _mark_day_complete(self, processing_date: date):
        """
        Mark a day as complete by ensuring all borders are COMPLETED or SKIPPED.

        Args:
            processing_date: Date to mark complete
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                # Check if all borders are done
                cur.execute("""
                    SELECT COUNT(*)
                    FROM image_processor.day_progress
                    WHERE processing_date = %s AND status IN (%s, %s)
                """, (processing_date, DayProgressStatus.PENDING.value, DayProgressStatus.IN_PROGRESS.value))

                pending_count = cur.fetchone()[0]

                if pending_count == 0:
                    self.logger.info(f"Day {processing_date} is complete")
                    return True

                return False

    def get_next_batch(self) -> Optional[List[str]]:
        """
        Get next batch of images to process using day-first strategy.
        Processes ALL borders for Day N before moving to Day N+1.
        """
        today = datetime.now().date()

        # Step 1: Get current processing day
        current_day = self._get_current_processing_day()

        if not current_day:
            # No day in progress - find or initialize next day
            last_batch = self._get_last_processed_batch()

            if last_batch and last_batch.last_image_path:
                # Resume from last processed image's date
                last_date = self._get_image_date(last_batch.last_image_path)
                next_day = self._discover_next_day_with_data(last_date)
            else:
                # Cold start - find first available day
                self.logger.info("Cold start - searching for first day with data...")
                # Start from a reasonable date (e.g., 2020-01-01)
                start_date = date(2020, 1, 1)
                next_day = self._discover_next_day_with_data(start_date - timedelta(days=1))

            if not next_day or next_day >= today:
                self.logger.info("No more days to process")
                return None

            # Initialize the day
            border_count = self._initialize_day(next_day)
            if border_count == 0:
                self.logger.warning(f"No borders with data for {next_day}")
                return None

            current_day = next_day

        # Step 2: Get next border/direction for this day
        border_direction = self._get_next_border_for_day(current_day)

        if not border_direction:
            # All borders for this day are complete
            self._mark_day_complete(current_day)
            self.logger.info(f"Completed all borders for {current_day}, moving to next day")
            # Recursively get next batch from next day
            return self.get_next_batch()

        border, direction = border_direction

        # Step 3: Fetch batch for this border/direction/day
        prefix = f"{border}/{direction}/{current_day.year}/{current_day.month}/{current_day.day}/"

        # Get last image path for this border/direction/day if resuming
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT last_image_path, status
                    FROM image_processor.day_progress
                    WHERE processing_date = %s AND border = %s AND direction = %s
                """, (current_day, border, direction))
                row = cur.fetchone()
                last_image_path = row[0] if row else None
                current_status = row[1] if row else None

        # List blobs after last processed image
        images = self._list_blobs_after(prefix, last_image_path)

        if not images:
            # No more images for this border/direction/day - mark as complete
            self.logger.info(f"Completed {border}/{direction} for {current_day}")
            self._update_day_progress(
                current_day, border, direction,
                DayProgressStatus.COMPLETED
            )
            # Recursively get next batch (next border or next day)
            return self.get_next_batch()

        # Step 4: Update day_progress - mark as IN_PROGRESS
        self._update_day_progress(
            current_day, border, direction,
            DayProgressStatus.IN_PROGRESS,
            last_image_path=images[-1],
            increment_batch=True,
            image_count=len(images)
        )

        # Step 5: Record batch in batch_tracking
        self._record_batch_start(images[-1], border, current_day)

        self.logger.info(f"Fetched {len(images)} images from {border}/{direction} for {current_day}")

        return images

    def _get_image_date(self, image_path: str) -> datetime.date:
        """Extract date from image path"""
        parts = Path(image_path).parts
        return datetime(
            int(parts[2]),  # year
            int(parts[3]),  # month
            int(parts[4])   # day
        ).date()

    def set_batch_status(
        self,
        last_image_path: str,
        status: ProcessingStatus,
        error_message: Optional[str] = None
    ):
        """Set status of batch in database"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                if error_message:
                    cur.execute("""
                        UPDATE image_processor.batch_tracking
                        SET status = %s, end_time = %s, error_message = %s,
                            updated_at = NOW(), retry_count = retry_count + 1
                        WHERE last_image_path = %s AND status = %s
                    """, (
                        status.value,
                        datetime.now(),
                        error_message,
                        last_image_path,
                        ProcessingStatus.IN_PROGRESS.value
                    ))
                else:
                    cur.execute("""
                        UPDATE image_processor.batch_tracking
                        SET status = %s, end_time = %s, updated_at = NOW()
                        WHERE last_image_path = %s AND status = %s
                    """, (
                        status.value,
                        datetime.now(),
                        last_image_path,
                        ProcessingStatus.IN_PROGRESS.value
                    ))
                conn.commit()

    def set_batch_hdfs_status(
        self,
        last_image_path: str,
        hdfs_status: HDFSStatus,
        hdfs_path: Optional[str] = None
    ):
        """Set HDFS status of batch in database"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                if hdfs_path:
                    cur.execute("""
                        UPDATE image_processor.batch_tracking
                        SET hdfs_status = %s, hdfs_path = %s, updated_at = NOW()
                        WHERE last_image_path = %s
                    """, (
                        hdfs_status.value,
                        hdfs_path,
                        last_image_path
                    ))
                else:
                    cur.execute("""
                        UPDATE image_processor.batch_tracking
                        SET hdfs_status = %s, updated_at = NOW()
                        WHERE last_image_path = %s
                    """, (
                        hdfs_status.value,
                        last_image_path
                    ))
                conn.commit()

    def get_pending_hdfs_batches(self) -> Dict[str, List[str]]:
        """
        Get batches that have been processed but not saved to HDFS,
        grouped by border

        Returns:
            Dictionary mapping border name to list of last_image_paths
        """
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT last_image_path, border
                    FROM image_processor.batch_tracking
                    WHERE status = %s AND hdfs_status = %s
                    ORDER BY start_time
                """, (
                    ProcessingStatus.COMPLETED.value,
                    HDFSStatus.PENDING.value
                ))

                pending = {}
                for row in cur:
                    border = row[1]
                    if border not in pending:
                        pending[border] = []
                    pending[border].append(row[0])

                return pending

    def _transform_for_yolo(self, image_data: bytes) -> np.ndarray:
        """Convert image bytes to numpy array for YOLO"""
        nparr = np.frombuffer(image_data, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    @retry_with_backoff()
    def _download_single_image(self, blob_path: str) -> Tuple[str, np.ndarray]:
        """
        Download and process a single image from blob storage with retry.

        Args:
            blob_path: Blob path to download

        Returns:
            Tuple of (blob_path, numpy array)
        """
        blob_client = self.container_client.get_blob_client(blob_path)
        image_data = blob_client.download_blob().readall()
        image = self._transform_for_yolo(image_data)
        return blob_path, image

    def download_images_parallel(
        self,
        blob_paths: List[str]
    ) -> Tuple[List[np.ndarray], List[str], List[Tuple[str, str]]]:
        """
        Download and process images from blob storage in parallel.

        Args:
            blob_paths: List of blob paths to download

        Returns:
            Tuple of:
                - List of numpy arrays (successfully downloaded images)
                - List of blob paths (corresponding to successful downloads)
                - List of (blob_path, error_message) for failed downloads
        """
        images = []
        successful_paths = []
        failed = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_path = {
                executor.submit(self._download_single_image, path): path
                for path in blob_paths
            }

            for future in as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    _, image = future.result()
                    images.append(image)
                    successful_paths.append(path)
                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"Error downloading {path}: {error_msg}")
                    failed.append((path, error_msg))

        # Sort by original order
        path_order = {p: i for i, p in enumerate(blob_paths)}
        sorted_pairs = sorted(
            zip(successful_paths, images),
            key=lambda x: path_order.get(x[0], float('inf'))
        )

        if sorted_pairs:
            successful_paths, images = zip(*sorted_pairs)
            return list(images), list(successful_paths), failed

        return [], [], failed

    def download_images(self, blob_paths: List[str]) -> List[np.ndarray]:
        """
        Download and process images from blob storage.
        Legacy method for backward compatibility - uses parallel download internally.

        Args:
            blob_paths: List of blob paths to download

        Returns:
            List of numpy arrays ready for YOLO
        """
        images, successful_paths, failed = self.download_images_parallel(blob_paths)

        if failed:
            self.logger.warning(f"{len(failed)} images failed to download")
            if len(images) == 0:
                raise Exception(f"All {len(blob_paths)} downloads failed. First error: {failed[0][1]}")

        return images
