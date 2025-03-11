import os
import argparse
import logging
import time
from pathlib import Path

from data_loader import ImageLoader, ProcessingStatus, HDFSStatus
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
    parser.add_argument('--model-path', type=str, default='/app/model/yolo11l.pt',
                        help='Path to YOLO model weights')
    parser.add_argument('--hdfs-dir', type=str, default='/hdfs/raw/vehicle_detections',
                        help='HDFS directory to save detection results')
    parser.add_argument('--hdfs-namenode', type=str, default='hdfs://namenode:9000',
                        help='HDFS namenode address')
    parser.add_argument('--max-parquet-size-mb', type=int, default=256,
                        help='Target size for Parquet files in MB')
    parser.add_argument('--batches', type=int, default=3,
                        help='Number of batches to process (-1 for unlimited)')
    parser.add_argument('--save-frequency', type=int, default=10,
                        help='Number of batches to process before saving to HDFS')
    
    parser.add_argument('--test-mode', action='store_true',
                        help='Run in test mode (equivalent to --batches 10 --save-frequency 2)')
    
    args = parser.parse_args()
    
    if args.test_mode:
        args.batches = 2
        args.save_frequency = 2

    return args

def process_images(args, logger):
    """Process images from Azure blob storage using YOLO vehicle detection"""
    loader = ImageLoader(batch_size=args.batch_size)
    detector = VehicleDetector(model_path=args.model_path, hdfs_namenode=args.hdfs_namenode)
    
    batch_count = 0
    batches_since_save = 0
    total_detections = 0
    current_borders = set()
    border_batch_counts = {}
    
    logger.info("Beginning image processing...")
    
    while True:
        if args.batches > 0 and batch_count >= args.batches:
            logger.info(f"Processed requested {args.batches} batches. Exiting.")
            break
            
        batch = loader.get_next_batch()
        
        if not batch:
            logger.info("No more batches to process. Exiting.")
            break
            
        batch_count += 1
        batches_since_save += 1
        
        # Get border from first image in batch
        border = Path(batch[0]).parts[0]
        current_borders.add(border)
        
        # Track batches per border
        if border not in border_batch_counts:
            border_batch_counts[border] = 0
        border_batch_counts[border] += 1
        
        logger.info(f"Processing batch {batch_count} for border {border}: {len(batch)} images")
        
        try:
            batch_start = time.time()
            
            images = loader.download_images(batch)
            detections = detector.detect_vehicles(images, batch)

            loader.set_batch_status(batch[-1], ProcessingStatus.COMPLETED)
            
            batch_end = time.time()

            total_detections += len(detections)
            logger.info(f"Batch {batch_count} completed in {batch_end - batch_start:.2f} seconds")
            logger.info(f"Detected {len(detections)} vehicles")
            
            logger.info(f"Current cache status:")
            for b in current_borders:
                count = detector.get_cached_detection_count(b)
                logger.info(f"  Border {b}: {count} detections")
            
            if batches_since_save >= args.save_frequency:
                logger.info(f"Saving to HDFS after {batches_since_save} batches...")
                saved_paths = detector.save_all_cached_detections(args.hdfs_dir)
                batches_since_save = 0
                
                for b, hdfs_path in saved_paths.items():
                    updated = detector.update_batch_hdfs_status(loader, b, hdfs_path)
                    logger.info(f"Updated HDFS status for {updated} batches for border {b}")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}", exc_info=True)
            loader.set_batch_status(batch[-1], ProcessingStatus.FAILED)
    
    logger.info("Saving remaining cached detections to HDFS...")
    saved_paths = detector.save_all_cached_detections(args.hdfs_dir)
    
    for border, hdfs_path in saved_paths.items():
        updated = detector.update_batch_hdfs_status(loader, border, hdfs_path)
        logger.info(f"Updated HDFS status for {updated} batches for border {border}")
    
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
    logger.info(f"  Target parquet size: {args.max_parquet_size_mb} MB")
    logger.info(f"  Save frequency: {args.save_frequency} batches")
    
    process_images(args, logger)