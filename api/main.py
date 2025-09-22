import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

import uvicorn
import logging
from datetime import datetime, timezone

from routes import health, photos, exports # Import routers
from services.photo_processor import PhotoProcessingManager
from services.mongo import MongoPhotoService
from services.photo_upload import PhotoUploadService
from services.azure_blob_photo import AzureBlobPhotoManager
from services.database import PhotoFilters


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 1. Load base/default configuration first
load_dotenv('.env')

# Global processing manager
processing_manager = PhotoProcessingManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup background processing"""
    logger.info("Starting photo processing pipeline...")
    
    try:
        # Initialize services
        db_name = os.getenv("MONGO_DATABASE_NAME")
        photo_service = MongoPhotoService(db_name=db_name)
        upload_service = PhotoUploadService()
        
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER")
        blob_manager = AzureBlobPhotoManager(connection_string, container_name)
        
        # Register processor
        processing_manager.register_processor(
            "default",
            blob_manager,
            photo_service,
            upload_service
        )
        
        # Start processing in background
        processing_task = asyncio.create_task(processing_manager.start_all_processors())
        
        logger.info("Photo processing pipeline started successfully")
        
        yield  # Server is running
        
        # Cleanup on shutdown
        logger.info("Shutting down photo processing pipeline...")
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            logger.info("Photo processing pipeline stopped")
        
    except Exception as e:
        logger.error(f"Failed to start photo processing pipeline: {e}")
        yield  # Still allow server to start even if processing fails


# Your existing app
app = FastAPI(
    title="Photo Log Map API",
    description="Web API for managing photos uploaded by HWC employees.",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for dashboard integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(photos.router)
app.include_router(exports.router)


# API Routes

@app.get('/') # Index Route
async def root():

    data = {
        "Message": "Connected to HWC Engineering Photo Log Map API",
        "Framework": "FastApi",
        "Version": "1.0.0",
        "Status": "Running",
        "Timestamp": datetime.now(timezone.utc)
    }

    return data

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc)}

@app.get("/debug/processing")
async def debug_processing():
    """Debug endpoint to check processing pipeline status"""
    try:
        processor = processing_manager.get_processor()
        if processor:
            stats = await processor.get_processing_stats()
            return {
                "processing_manager_active": True,
                "processor_stats": stats,
                "timestamp": datetime.now(timezone.utc)
            }
        else:
            return {
                "processing_manager_active": False,
                "error": "No processor found",
                "timestamp": datetime.now(timezone.utc)
            }
    except Exception as e:
        return {
            "processing_manager_active": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }

@app.get("/debug/storage")
async def debug_storage():
    """Debug endpoint to check Azure Blob Storage"""
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER")
        blob_manager = AzureBlobPhotoManager(connection_string, container_name)
        
        # Get storage usage stats
        usage_stats = await blob_manager.get_storage_usage()
        
        # List recent photos in different folders
        recent_photos_july = await blob_manager.list_photos_in_folder("2025", "07")  # July 2025 based on EXIF dates
        recent_photos_august = await blob_manager.list_photos_in_folder("2025", "08")  # August 2025 
        recent_photos_sept = await blob_manager.list_photos_in_folder("2025", "09")  # September 2025 (current month)
        
        return {
            "storage_connected": True,
            "container_name": container_name,
            "usage_stats": usage_stats,
            "recent_photos_july": recent_photos_july[:5],  # Show first 5
            "recent_photos_august": recent_photos_august[:5],  # Show first 5
            "recent_photos_september": recent_photos_sept[:5],  # Show first 5
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        return {
            "storage_connected": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }

@app.post("/debug/test-upload")
async def debug_test_upload():
    """Debug endpoint to test blob upload directly"""
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER")
        blob_manager = AzureBlobPhotoManager(connection_string, container_name)
        
        # Create a simple test file
        test_content = b"This is a test file for debugging blob upload"
        test_filename = "debug_test.txt"
        test_timestamp = datetime.utcnow()
        
        # Try to upload
        upload_result = await blob_manager.upload_photo_with_thumbnail(
            file_content=test_content,
            filename=test_filename,
            timestamp=test_timestamp,
            content_type="text/plain",
            generate_thumbnails=False  # Skip thumbnails for text file
        )
        
        return {
            "upload_success": True,
            "upload_result": upload_result,
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error(f"Debug upload test failed: {e}")
        return {
            "upload_success": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }

@app.post("/debug/test-photo-processing")
async def debug_test_photo_processing():
    """Debug endpoint to test the full photo processing pipeline"""
    try:
        # Get the processor
        processor = processing_manager.get_processor()
        if not processor:
            return {"error": "No processor available"}
        
        # Create test photo data similar to what upload service creates
        test_photo_data = {
            "filename": "debug_photo.jpg",
            "original_filename": "debug_photo.jpg",
            "file_content": b"fake image content for testing",
            "mime_type": "image/jpeg",
            "file_size": 23,
            "timestamp": datetime(2025, 7, 14, 9, 3, 16),  # Use July date like EXIF
            "latitude": 39.026897222222225,
            "longitude": -86.94798333333334,
            "altitude": 161.4421491431218,
            "camera_make": "Apple",
            "camera_model": "iPhone 16 Pro Max",
            "tags": ["debug", "test"],
            "description": "Debug test photo",
            "hash_md5": "debug_test_hash_123"
        }
        
        # Queue for processing
        job_id = await processor.queue_photo_for_processing(test_photo_data)
        
        # Wait a moment for processing
        import asyncio
        await asyncio.sleep(2)
        
        # Check queue status
        stats = await processor.get_processing_stats()
        
        return {
            "test_success": True,
            "job_id": job_id,
            "queue_stats": stats,
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Debug photo processing test failed: {e}")
        return {
            "test_success": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }

@app.get("/debug/recent-uploads")
async def debug_recent_uploads():
    """Debug endpoint to check recent uploads and their processing status"""
    try:
        db_name = os.getenv("MONGO_DATABASE_NAME")
        photo_service = MongoPhotoService(db_name=db_name)
        
        # Get all photos from database
        all_photos = await photo_service.get_photos(PhotoFilters(limit=20))
        
        # Get processing stats
        processor = processing_manager.get_processor()
        stats = await processor.get_processing_stats() if processor else {"error": "No processor"}
        
        return {
            "total_photos_in_db": len(all_photos),
            "photos": [
                {
                    "id": photo.id,
                    "filename": photo.filename,
                    "processing_status": photo.processing_status,
                    "camera_make": photo.camera_make,
                    "latitude": photo.latitude,
                    "longitude": photo.longitude,
                    "created_at": photo.created_at.isoformat() if photo.created_at else None
                }
                for photo in all_photos[:10]
            ],
            "processor_stats": stats,
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Debug recent uploads failed: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }

@app.get("/debug/processing-logs")
async def debug_processing_logs():
    """Debug endpoint to check recent processing activity"""
    try:
        # This is a simple way to check if processing is working
        # In a production system, you'd use proper log aggregation
        
        processor = processing_manager.get_processor()
        if not processor:
            return {"error": "No processor available"}
        
        # Get current queue status
        stats = await processor.get_processing_stats()
        
        # Try to process a simple test to see if the pipeline is working
        test_data = {
            "filename": "pipeline_test.jpg",
            "original_filename": "pipeline_test.jpg", 
            "file_content": b"test content",
            "mime_type": "image/jpeg",
            "file_size": 12,
            "timestamp": datetime(2025, 8, 14, 12, 0, 0),
            "latitude": 41.0,
            "longitude": -85.0,
            "hash_md5": "pipeline_test_hash"
        }
        
        # Queue test job
        job_id = await processor.queue_photo_for_processing(test_data)
        
        # Wait briefly
        import asyncio
        await asyncio.sleep(1)
        
        # Check queue again
        stats_after = await processor.get_processing_stats()
        
        return {
            "processor_available": True,
            "test_job_id": job_id,
            "queue_before": stats["queue_size"],
            "queue_after": stats_after["queue_size"],
            "pipeline_working": stats_after["queue_size"] == 0,
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Debug processing logs failed: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc)
        }

@app.post("/debug/test-real-photo-upload")
async def debug_test_real_photo_upload():
    """Debug endpoint to test blob upload with a real photo file"""
    try:
        # Read a real photo file for testing
        import os
        photo_path = "../../test-photos/Drone/DJI_0586.JPG"
        
        if not os.path.exists(photo_path):
            return {"error": f"Test photo not found: {photo_path}"}
        
        # Read the file
        with open(photo_path, 'rb') as f:
            file_content = f.read()
        
        logger.info(f"Read test photo: {len(file_content)} bytes")
        
        # Test blob upload directly
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER")
        blob_manager = AzureBlobPhotoManager(connection_string, container_name)
        
        test_filename = "debug_real_photo_test.jpg"
        test_timestamp = datetime.utcnow()
        
        # Try to upload the real photo
        upload_result = await blob_manager.upload_photo_with_thumbnail(
            file_content=file_content,
            filename=test_filename,
            timestamp=test_timestamp,
            content_type="image/jpeg",
            generate_thumbnails=True
        )
        
        return {
            "upload_success": True,
            "file_size": len(file_content),
            "upload_result": upload_result,
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Debug real photo upload failed: {e}")
        import traceback
        return {
            "upload_success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now(timezone.utc)
        }

if __name__ == "__main__":
    uvicorn.run(app) # Start the server when the script is run directly