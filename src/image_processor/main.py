import os
import argparse
import logging
import time
from pathlib import Path

from data_loader import ImageLoader, ProcessingStatus, HDFSStatus, DatabaseManager
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
    parser.add_argument('--batches', type=int, default=3,
                        help='Number of batches to process (-1 for unlimited)')
    parser.add_argument('--max-workers', type=int, default=5,
                        help='Number of parallel workers for image downloads')

    parser.add_argument('--test-mode', action='store_true',
                        help='Run in test mode (equivalent to --batches 2)')

    args = parser.parse_args()

    if args.test_mode:
        args.batches = 2

    return args


def process_images(args, logger, db_manager: DatabaseManager):
    """Process images from Azure blob storage using YOLO vehicle detection"""
    loader = ImageLoader(
        batch_size=args.batch_size,
        db_manager=db_manager,
        max_workers=args.max_workers
    )
    detector = VehicleDetector(
        model_path=args.model_path,
        hdfs_namenode=args.hdfs_namenode,
        max_file_size_mb=args.max_file_size_mb
    )

    batch_count = 0
    total_detections = 0
    current_borders = set()
    border_batch_counts = {}

    logger.info("Beginning image processing...")

    try:
        while True:
            if args.batches > 0 and batch_count >= args.batches:
                logger.info(f"Processed requested {args.batches} batches. Exiting.")
                break

            batch = loader.get_next_batch()

            if not batch:
                logger.info("No more batches to process. Exiting.")
                break

            batch_count += 1

            border = Path(batch[0]).parts[0]
            current_borders.add(border)

            if border not in border_batch_counts:
                border_batch_counts[border] = 0
            border_batch_counts[border] += 1

            logger.info(f"Processing batch {batch_count} for border {border}: {len(batch)} images")

            try:
                batch_start = time.time()

                # Use parallel downloads
                images, successful_paths, failed = loader.download_images_parallel(batch)

                if failed:
                    logger.warning(f"{len(failed)} images failed to download in batch {batch_count}")

                if not images:
                    logger.error(f"No images downloaded in batch {batch_count}, skipping")
                    loader.set_batch_status(
                        batch[-1],
                        ProcessingStatus.FAILED,
                        error_message="All downloads failed"
                    )
                    continue

                # Process successfully downloaded images
                detections = detector.detect_vehicles(images, successful_paths)

                loader.set_batch_status(batch[-1], ProcessingStatus.COMPLETED)

                batch_end = time.time()

                total_detections += len(detections)
                logger.info(f"Batch {batch_count} completed in {batch_end - batch_start:.2f} seconds")
                logger.info(f"Downloaded {len(images)}/{len(batch)} images, detected {len(detections)} vehicles")

                # Log current day detection count
                day_count = detector.get_detection_count()
                logger.info(f"Current day detection count: {day_count}")

            except Exception as e:
                logger.error(f"Error processing batch: {e}", exc_info=True)
                loader.set_batch_status(
                    batch[-1],
                    ProcessingStatus.FAILED,
                    error_message=str(e)
                )

    finally:
        # Ensure we finalize all files even if processing is interrupted
        logger.info("Finalizing all remaining parquet files...")
        saved_paths = detector.finalize_all(loader)

        for day, hdfs_path in saved_paths.items():
            logger.info(f"Finalized parquet file for day {day}: {hdfs_path}")

        logger.info(f"Processing completed:")
        logger.info(f"  Total batches: {batch_count}")
        logger.info(f"  Total detections: {total_detections}")
        logger.info(f"  Borders processed: {', '.join(current_borders)}")
        for border, count in border_batch_counts.items():
            logger.info(f"  Border {border}: {count} batches")


if __name__ == "__main__":
    logger = setup_logging()
    args = parse_args()

    logger.info(f"Starting vehicle detection service:")
    logger.info(f"  Batch size: {args.batch_size}")
    logger.info(f"  Model path: {args.model_path}")
    logger.info(f"  HDFS namenode: {args.hdfs_namenode}")
    logger.info(f"  HDFS directory: {args.hdfs_dir}")
    logger.info(f"  Max file size: {args.max_file_size_mb} MB")
    logger.info(f"  Max workers: {args.max_workers}")

    # Initialize database manager
    db_manager = DatabaseManager()

    try:
        process_images(args, logger, db_manager)
    finally:
        db_manager.close()
        logger.info("Database connections closed")
