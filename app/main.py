import argparse
import asyncio
import os
import sys
from loguru import logger
from app.services.video_processor import VideoProcessor

async def run_batch(total_records: int, concurrency: int) -> int:
    """
    Run the batch processing job:
      - Processes records [0..total_records-1]
      - Limits concurrent processing with a semaphore
    Returns 0 on success, non-zero on error.
    """
    processor = VideoProcessor()
    sem = asyncio.Semaphore(concurrency)

    async def worker(record_id: int):
        async with sem:
            logger.info(f"Processing record {record_id}")
            return await processor.process_record(record_id)

    tasks = [asyncio.create_task(worker(i)) for i in range(total_records)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Log errors and return non-zero if any task failed
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        for e in errors:
            logger.error(f"Task error: {e}")
        return 1

    logger.info(f"Processed {len(results)} records successfully")
    return 0

def parse_args():
    parser = argparse.ArgumentParser(description="Cloud Batch compatible video processing script")
    parser.add_argument("--total-records", "-n", type=int, default=int(os.getenv("TOTAL_RECORDS", "100")),
                        help="Total number of records to process")
    parser.add_argument("--concurrency", "-c", type=int, default=int(os.getenv("CONCURRENCY", "5")),
                        help="Maximum concurrent record processors")
    return parser.parse_args()

def main():
    args = parse_args()
    logger.info(f"Starting batch: total_records={args.total_records}, concurrency={args.concurrency}")
    exit_code = asyncio.run(run_batch(args.total_records, args.concurrency))
    sys.exit(exit_code)

if __name__ == "__main__":
    main()