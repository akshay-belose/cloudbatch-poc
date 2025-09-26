import argparse
import asyncio
import os
import sys
from loguru import logger
from google.cloud import storage
import pandas as pd
from app.services.video_processor import VideoProcessor

async def read_from_gcs(bucket_name: str, file_name: str) -> pd.DataFrame:
    """
    Read file from Google Cloud Storage bucket
    """
    if not bucket_name or not file_name:
        raise ValueError("Bucket name and file name must not be empty")
    
    # ...existing code...
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Download to temporary file
        temp_file = os.path.join(os.getcwd(), "input.csv") # Assuming CSV format
        blob.download_to_filename(temp_file)
        
        # Read the file into pandas DataFrame
        df = pd.read_csv(temp_file)
        os.remove(temp_file)  # Cleanup
        
        return df
    except Exception as e:
        logger.error(f"Error reading from GCS: {str(e)}")
        raise

async def run_batch(bucket_name: str, file_name: str, concurrency: int) -> int:
    """
    Run the batch processing job:
      - Reads records from GCS file
      - Processes records with specified concurrency
    Returns 0 on success, non-zero on error.
    """
    try:
        # Read input file
        df = await read_from_gcs(bucket_name, file_name)
        total_records = len(df)
        logger.info(f"Found {total_records} records to process")

        processor = VideoProcessor()
        sem = asyncio.Semaphore(concurrency)

        async def worker(record: dict):
            async with sem:
                record_id = record['external_video_id']  # Column name for video ID
                logger.info(f"Processing record {record_id}")
                return await processor.process_record(record_id)

        # Create tasks for each record
        tasks = [asyncio.create_task(worker(record)) 
                for record in df.to_dict('records')]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for errors
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            for e in errors:
                logger.error(f"Task error: {e}")
            return 1

        logger.info(f"Processed {len(results)} records successfully")
        return 0

    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}")
        return 1

def parse_args():
    parser = argparse.ArgumentParser(description="Cloud Batch video processing script")
    parser.add_argument("--bucket", type=str, required=True,
                      help="GCS bucket name containing the input file")
    parser.add_argument("--file", type=str, required=True,
                      help="Input file name in the GCS bucket")
    parser.add_argument("--concurrency", "-c", type=int, 
                      default=int(os.getenv("CONCURRENCY", "5")),
                      help="Maximum concurrent record processors")
    return parser.parse_args()

def main():
    args = parse_args()
    logger.info(f"Starting batch: bucket={args.bucket}, file={args.file}, "
               f"concurrency={args.concurrency}")
    exit_code = asyncio.run(run_batch(args.bucket, args.file, args.concurrency))
    sys.exit(exit_code)

if __name__ == "__main__":
    main()