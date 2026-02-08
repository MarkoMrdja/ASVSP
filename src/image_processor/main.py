import os
import argparse
import logging
import time
from datetime import date
from data_loader import ImageLoader, DatabaseManager
from vehicle_detector import VehicleDetector


def setup_logging():
    """Setup logging configuration"""
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/vehicle_detection.log')
        ]
    )
    return logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Vehicle detection on border crossing images')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Number of images to process in each batch')
    parser.add_argument('--model-path', type=str, default='/app/model/yolo11n.pt',
                        help='Path to YOLO model weights (default: yolo11n.pt)')
    parser.add_argument('--hdfs-dir', type=str, default='/hdfs/raw/vehicle_detections',
                        help='HDFS directory to save detection results')
    parser.add_argument('--hdfs-namenode', type=str, default='http://namenode:9870',
                        help='HDFS namenode address')
    parser.add_argument('--max-file-size-mb', type=int, default=256,
                        help='Maximum size for parquet files in MB before finalizing')
    parser.add_argument('--max-days', type=int, default=-1,
                        help='Maximum number of days to process (-1 for unlimited)')
    parser.add_argument('--max-workers', type=int, default=5,
                        help='Number of parallel workers for image downloads')

    parser.add_argument('--test-mode', action='store_true',
                        help='Run in test mode (process 2 days)')

    args = parser.parse_args()

    if args.test_mode:
        args.max_days = 2

    return args


class ProcessingStats:
    """Track processing metrics"""
    def __init__(self):
        self.days_completed = 0
        self.days_failed = 0
        self.total_batches = 0
        self.total_images = 0
        self.total_detections = 0
        self.start_time = time.time()

    def add_day_success(self, processing_date, batches, images, detections):
        self.days_completed += 1
        self.total_batches += batches
        self.total_images += images
        self.total_detections += detections

    def add_day_failure(self, processing_date):
        self.days_failed += 1


def process_single_day(
    processing_date: date,
    loader: ImageLoader,
    detector: VehicleDetector,
    stats: ProcessingStats,
    logger: logging.Logger
):
    """
    Process a single day atomically with clear transaction boundaries.

    Transaction flow:
    1. BEGIN: Mark day IN_PROGRESS
    2. PROCESS: All borders, all batches (detections accumulate in memory)
    3. COMMIT: Write Parquet, upload HDFS, mark COMPLETED
    4. ROLLBACK: On any error, clear buffer, mark FAILED
    """
    logger.info("=" * 70)
    logger.info(f"PROCESSING DAY: {processing_date}")
    logger.info("=" * 70)

    # BEGIN TRANSACTION
    loader.begin_day_transaction(processing_date)

    day_start = time.time()
    day_batches = 0
    day_images = 0

    try:
        # Get all borders for this day
        borders = loader.get_borders_for_day(processing_date)
        logger.info(f"Found {len(borders)} border/direction combinations")

        # Process each border/direction
        for border, direction in borders:
            logger.info(f"  Processing {border}/{direction}...")
            border_batches = 0
            border_images = 0

            # Process all batches for this border/direction
            while True:
                batch = loader.get_next_batch_for_day(processing_date, border, direction)

                if not batch:
                    logger.info(f"    ✓ Completed {border}/{direction}: {border_batches} batches, {border_images} images")
                    loader.mark_border_complete(processing_date, border, direction)
                    break

                # Download images with HDFS caching for retry resilience
                images, paths, failed = loader.download_images_with_hdfs_cache(batch, processing_date)

                if failed:
                    logger.warning(f"    {len(failed)} images failed to download")

                if not images:
                    logger.error(f"    No images downloaded for batch, skipping")
                    continue

                # Detect vehicles (accumulates in detector.day_detections)
                detections = detector.detect_vehicles(images, paths)

                # Log batch to audit trail
                loader.log_batch(
                    processing_date, border, direction,
                    batch_number=day_batches + 1,
                    image_count=len(images),
                    detection_count=len(detections),
                    last_image_path=paths[-1]
                )

                day_batches += 1
                day_images += len(images)
                border_batches += 1
                border_images += len(images)

                logger.info(
                    f"    Batch {day_batches}: {len(images)} images, {len(detections)} detections "
                    f"(day total: {len(detector.day_detections)})"
                )

        # COMMIT TRANSACTION
        day_detections = len(detector.day_detections)
        logger.info(f"All borders complete. Finalizing day {processing_date}...")
        hdfs_path = detector.finalize_day(processing_date, loader)

        if not hdfs_path:
            raise Exception("Failed to finalize day - no HDFS path returned")

        day_duration = time.time() - day_start

        logger.info("=" * 70)
        logger.info(f"✓ DAY {processing_date} COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"  Duration: {day_duration:.2f}s")
        logger.info(f"  Batches: {day_batches}")
        logger.info(f"  Images: {day_images}")
        logger.info(f"  Detections: {day_detections}")
        logger.info(f"  HDFS: {hdfs_path}")
        logger.info("=" * 70)

        stats.add_day_success(processing_date, day_batches, day_images, day_detections)

        # Cleanup HDFS image cache after successful completion
        loader.cleanup_day_cache(processing_date)

    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"✗ DAY {processing_date} FAILED")
        logger.error("=" * 70)
        logger.error(f"  Error: {e}")
        logger.error("=" * 70)

        detector.rollback_day()
        loader.mark_day_failed(processing_date, str(e))
        raise


def handle_day_failure(
    processing_date: date,
    loader: ImageLoader,
    detector: VehicleDetector,
    error_message: str,
    logger: logging.Logger
):
    """
    Handle day processing failure with exponential backoff retry.

    Retry strategy:
    - Attempt 1 (initial): Immediate (transient errors)
    - Attempt 2: Wait 60s (temporary service issues)
    - Attempt 3: Wait 300s (5 minutes, serious issues)
    - After 3 failures: Mark PERMANENTLY_FAILED, skip to next day
    """
    retry_count = loader.get_day_retry_count(processing_date)

    if retry_count < 3:
        delays = [0, 60, 300]  # 0s, 1m, 5m
        delay = delays[retry_count]

        logger.warning("=" * 70)
        logger.warning(f"DAY {processing_date} RETRY STRATEGY")
        logger.warning("=" * 70)
        logger.warning(f"  Attempt: {retry_count + 1}/3")
        logger.warning(f"  Delay: {delay}s")
        logger.warning(f"  Action: Retry entire day")
        logger.warning("=" * 70)

        if delay > 0:
            logger.info(f"Waiting {delay}s before retry...")
            time.sleep(delay)

        loader.increment_day_retry(processing_date)
        logger.info(f"Retry count incremented to {retry_count + 1}. Will retry on next iteration.")
        logger.info(f"HDFS image cache preserved for retry")

    else:
        # Permanent failure - SKIP AND CONTINUE
        logger.error("=" * 70)
        logger.error(f"DAY {processing_date} PERMANENTLY FAILED")
        logger.error("=" * 70)
        logger.error(f"  Retries exhausted: 3/3")
        logger.error(f"  Last error: {error_message}")
        logger.error(f"  Action: Marking PERMANENTLY_FAILED and SKIPPING to next day")
        logger.error("=" * 70)
        logger.error(
            f"NOTE: Day {processing_date} can be manually retried later by "
            f"resetting status to PENDING in database."
        )

        loader.mark_day_permanently_failed(processing_date, error_message)

        # Cleanup HDFS cache for permanent failure
        loader.cleanup_day_cache(processing_date)


def log_processing_summary(stats: ProcessingStats, logger: logging.Logger):
    """Log final summary"""
    duration = time.time() - stats.start_time
    logger.info("=" * 70)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Duration: {duration / 60:.1f} minutes")
    logger.info(f"Days completed: {stats.days_completed}")
    logger.info(f"Days failed: {stats.days_failed}")
    logger.info(f"Total batches: {stats.total_batches}")
    logger.info(f"Total images: {stats.total_images}")
    logger.info(f"Total detections: {stats.total_detections}")
    if stats.days_completed > 0:
        logger.info(f"Avg detections/day: {stats.total_detections / stats.days_completed:.0f}")
    logger.info("=" * 70)


def process_days(args, logger, db_manager: DatabaseManager):
    """
    Process images using day-first atomic strategy.

    Each day is a transaction:
    - All-or-nothing (no partial days in HDFS)
    - Automatic retry on failure (up to 3 times)
    - Skip to next day after permanent failure
    """
    loader = ImageLoader(
        batch_size=args.batch_size,
        db_manager=db_manager,
        max_workers=args.max_workers,
        hdfs_namenode=args.hdfs_namenode
    )
    detector = VehicleDetector(
        model_path=args.model_path,
        hdfs_namenode=args.hdfs_namenode,
        max_file_size_mb=args.max_file_size_mb
    )

    stats = ProcessingStats()

    try:
        while True:
            # Check if we've reached the day limit
            if args.max_days > 0 and stats.days_completed >= args.max_days:
                logger.info(f"Reached maximum days limit ({args.max_days}). Stopping.")
                break

            # Get next day (prioritizes: failed > pending > discovery)
            next_day = loader.get_next_day()

            if not next_day:
                logger.info("No more days to process")
                break

            # Process entire day atomically
            try:
                process_single_day(next_day, loader, detector, stats, logger)
            except Exception as e:
                logger.error(f"Error processing day {next_day}: {e}", exc_info=True)
                handle_day_failure(next_day, loader, detector, str(e), logger)

    finally:
        log_processing_summary(stats, logger)


if __name__ == "__main__":
    logger = setup_logging()
    args = parse_args()

    logger.info(f"Starting vehicle detection service:")
    logger.info(f"  Batch size: {args.batch_size}")
    logger.info(f"  Max days: {args.max_days if args.max_days > 0 else 'unlimited'}")
    logger.info(f"  Model path: {args.model_path}")
    logger.info(f"  HDFS namenode: {args.hdfs_namenode}")
    logger.info(f"  Max file size: {args.max_file_size_mb} MB")
    logger.info(f"  Max workers: {args.max_workers}")

    # Initialize database manager
    db_manager = DatabaseManager()

    try:
        process_days(args, logger, db_manager)
    finally:
        db_manager.close()
        logger.info("Database connections closed")
