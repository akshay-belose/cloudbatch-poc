from fastapi import FastAPI, HTTPException
from loguru import logger
from app.services.video_processor import VideoProcessor

app = FastAPI(title="Video Processing Service")
video_processor = VideoProcessor()

@app.get("/")
async def root():
    return {"message": "Video Processing Service is running"}

@app.post("/processvideo")
async def process_video():
    try:
        # Process 100 records (this is a placeholder for your actual record fetching logic)
        total_records = 100
        processed_records = []

        for record_id in range(total_records):
            logger.info(f"Processing record {record_id}")
            result = await video_processor.process_record(record_id)
            processed_records.append(result)

        return {
            "status": "success",
            "message": f"Processed {len(processed_records)} records",
            "processed_records": processed_records
        }

    except Exception as e:
        logger.error(f"Error processing videos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)