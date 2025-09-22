"""
Photo management routes with integrated upload service and processing pipeline
"""

import logging
import os
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form, Depends
from fastapi.responses import StreamingResponse

from models.photo import Photo, JobStatus
from services.database import PhotoFilters
from services.mongo import MongoPhotoService
from services.photo_upload import PhotoUploadService
from services.azure_blob_photo import AzureBlobPhotoManager
from services.photo_processor import PhotoProcessor


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/photos", tags=["Photos"])


# Dependencies
def get_photo_service() -> MongoPhotoService:
    db_name = os.getenv("MONGO_DATABASE_NAME", "photo_mapper")
    return MongoPhotoService(db_name=db_name)

def get_upload_service() -> PhotoUploadService:
    return PhotoUploadService()

def get_blob_manager() -> AzureBlobPhotoManager:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise HTTPException(status_code=500, detail="Azure Storage connection string not configured")
    
    container_name = os.getenv("AZURE_STORAGE_CONTAINER", "photo-log-map")
    return AzureBlobPhotoManager(connection_string, container_name)

def get_photo_processor() -> PhotoProcessor:
    blob_manager = get_blob_manager()
    database_service = get_photo_service()
    upload_service = get_upload_service()
    return PhotoProcessor(blob_manager, database_service, upload_service)


@router.post("/upload", response_model=dict)
async def upload_photo(
    file: UploadFile = File(...),
    tags: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    uploader_id: Optional[str] = Form(None),
    upload_service: PhotoUploadService = Depends(get_upload_service)
):
    """
    Upload a photo and extract geographic metadata from EXIF data
    """
    try:
        # Read file content
        file_content = await file.read()
        
        # Process upload with new service
        photo_data = await upload_service.process_upload(
            file_content=file_content,
            filename=file.filename,
            content_type=file.content_type,
            tags=tags.split(',') if tags else None,
            description=description,
            uploader_id=uploader_id
        )
        
        # Check for duplicates
        photo_service = get_photo_service()
        existing_photos = await photo_service.get_photos_by_hash(photo_data["hash_md5"])
        if existing_photos:
            return {
                "photo_id": existing_photos[0].id,
                "status": "duplicate",
                "message": "Photo already exists (duplicate detected)",
                "existing_photo_id": existing_photos[0].id
            }
        
        # Queue for async processing using global processing manager
        from main import processing_manager
        processor = processing_manager.get_processor()
        if not processor:
            raise HTTPException(status_code=500, detail="Processing pipeline not available")
        
        job_id = await processor.queue_photo_for_processing(photo_data)
        
        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Photo uploaded and queued for processing",
            "extracted_data": {
                "latitude": photo_data.get('latitude'),
                "longitude": photo_data.get('longitude'),
                "altitude": photo_data.get('altitude'),
                "camera_make": photo_data.get('camera_make'),
                "camera_model": photo_data.get('camera_model'),
                "datetime_taken": photo_data.get('timestamp'),
                "coordinate_source": photo_data.get('coordinate_source')
            }
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to upload photo: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload photo")


@router.get("/processing/{job_id}", response_model=dict)
async def get_processing_status(
    job_id: str,
    photo_processor: PhotoProcessor = Depends(get_photo_processor)
):
    """
    Get processing status for an upload job
    """
    try:
        # Get processing stats (this would need to be enhanced to track specific jobs)
        stats = await photo_processor.get_processing_stats()
        
        # For now, return basic status
        # In a full implementation, we'd track individual jobs
        return {
            "job_id": job_id,
            "status": "processing",
            "message": "Job status tracking not fully implemented yet",
            "queue_size": stats.get("queue_size", 0)
        }
        
    except Exception as e:
        logger.error(f"Failed to get processing status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get processing status")


@router.get("/", response_model=List[Photo])
async def get_photos(
    start_date: Optional[datetime] = Query(None, description="Filter photos from this date"),
    end_date: Optional[datetime] = Query(None, description="Filter photos until this date"),
    tags: Optional[str] = Query(None, description="Comma-separated list of tags to filter by"),
    min_lat: Optional[float] = Query(None, description="Minimum latitude for geographic filtering"),
    max_lat: Optional[float] = Query(None, description="Maximum latitude for geographic filtering"),
    min_lng: Optional[float] = Query(None, description="Minimum longitude for geographic filtering"),
    max_lng: Optional[float] = Query(None, description="Maximum longitude for geographic filtering"),
    uploader_id: Optional[str] = Query(None, description="Filter by uploader ID"),
    limit: Optional[int] = Query(100, description="Maximum number of photos to return"),
    offset: Optional[int] = Query(0, description="Number of photos to skip"),
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Retrieve photos with optional filtering
    """
    try:
        filters = PhotoFilters(
            start_date=start_date,
            end_date=end_date,
            tags=tags.split(',') if tags else None,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lng=min_lng,
            max_lng=max_lng,
            uploader_id=uploader_id,
            limit=limit,
            offset=offset
        )
        
        photos = await photo_service.get_photos(filters)
        return photos
        
    except Exception as e:
        logger.error(f"Failed to get photos: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve photos")


@router.get("/{photo_id}", response_model=Photo)
async def get_photo(
    photo_id: str,
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Get a specific photo by ID
    """
    try:
        photo = await photo_service.get_photo(photo_id)
        if not photo:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        return photo
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve photo")


@router.post("/upload-manual", response_model=dict)
async def upload_photo_manual_coordinates(
    file: UploadFile = File(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    altitude: Optional[float] = Form(None),
    tags: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    uploader_id: Optional[str] = Form(None),
    upload_service: PhotoUploadService = Depends(get_upload_service)
):
    """
    Upload a photo with manually provided coordinates (fallback for photos without GPS EXIF data)
    """
    try:
        # Read file content
        file_content = await file.read()
        
        # Prepare manual coordinates
        manual_coordinates = {
            "latitude": latitude,
            "longitude": longitude,
            "altitude": altitude
        }
        
        # Process upload with manual coordinates
        photo_data = await upload_service.process_upload(
            file_content=file_content,
            filename=file.filename,
            content_type=file.content_type,
            manual_coordinates=manual_coordinates,
            tags=tags.split(',') if tags else None,
            description=description,
            uploader_id=uploader_id
        )
        
        # Check for duplicates
        photo_service = get_photo_service()
        existing_photos = await photo_service.get_photos_by_hash(photo_data["hash_md5"])
        if existing_photos:
            return {
                "photo_id": existing_photos[0].id,
                "status": "duplicate",
                "message": "Photo already exists (duplicate detected)",
                "existing_photo_id": existing_photos[0].id
            }
        
        # Queue for async processing using global processing manager
        from main import processing_manager
        processor = processing_manager.get_processor()
        if not processor:
            raise HTTPException(status_code=500, detail="Processing pipeline not available")
        
        job_id = await processor.queue_photo_for_processing(photo_data)
        
        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Photo uploaded with manual coordinates and queued for processing",
            "coordinates": {
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude,
                "source": "manual"
            }
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to upload photo with manual coordinates: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload photo")


@router.put("/{photo_id}/metadata", response_model=dict)
async def update_photo_metadata(
    photo_id: str,
    tags: Optional[List[str]] = None,
    description: Optional[str] = None,
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Update photo metadata (tags, description)
    """
    try:
        updates = {}
        if tags is not None:
            updates["tags"] = tags
        if description is not None:
            updates["description"] = description
        
        if not updates:
            raise HTTPException(status_code=400, detail="No updates provided")
        
        success = await photo_service.update_photo(photo_id, updates)
        if not success:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        return {"message": "Photo metadata updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update photo")


@router.put("/{photo_id}/coordinates", response_model=dict)
async def update_photo_coordinates(
    photo_id: str,
    latitude: float,
    longitude: float,
    altitude: Optional[float] = None,
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Update photo coordinates (for correcting GPS data or adding coordinates to photos without GPS)
    """
    try:
        updates = {
            "latitude": latitude,
            "longitude": longitude
        }
        if altitude is not None:
            updates["altitude"] = altitude
        
        success = await photo_service.update_photo(photo_id, updates)
        if not success:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        return {
            "message": "Photo coordinates updated successfully",
            "coordinates": {
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update photo coordinates {photo_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update photo coordinates")


@router.delete("/{photo_id}", response_model=dict)
async def delete_photo(
    photo_id: str,
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Delete a photo (admin only - TODO: Add authentication)
    """
    try:
        success = await photo_service.delete_photo(photo_id)
        if not success:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        # TODO: Also delete from Azure Blob Storage
        
        return {"message": "Photo deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete photo")


@router.get("/{photo_id}/thumbnail")
async def get_photo_thumbnail(
    photo_id: str,
    size: str = Query("medium", description="Thumbnail size: small, medium, or large"),
    photo_service: MongoPhotoService = Depends(get_photo_service),
    blob_manager: AzureBlobPhotoManager = Depends(get_blob_manager)
):
    """
    Get photo thumbnail in specified size
    
    - **size**: Thumbnail size (small: 150x150, medium: 300x300, large: 800x600)
    """
    try:
        photo = await photo_service.get_photo(photo_id)
        if not photo:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        # Validate size parameter
        valid_sizes = ["small", "medium", "large"]
        if size not in valid_sizes:
            raise HTTPException(status_code=400, detail=f"Invalid size. Must be one of: {valid_sizes}")
        
        # Generate thumbnail blob path
        thumbnail_blob_path = blob_manager._get_thumbnail_blob_path(
            photo.filename, 
            photo.timestamp, 
            size
        )
        
        # Generate download URL for the specific thumbnail size
        thumbnail_url = blob_manager.generate_download_url(thumbnail_blob_path, expiry_hours=1)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=thumbnail_url)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get thumbnail for photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve thumbnail")


@router.get("/bounds/search", response_model=List[Photo])
async def get_photos_in_bounds(
    min_lat: float = Query(..., description="Minimum latitude"),
    max_lat: float = Query(..., description="Maximum latitude"),
    min_lng: float = Query(..., description="Minimum longitude"),
    max_lng: float = Query(..., description="Maximum longitude"),
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Get photos within geographic bounds
    """
    try:
        photos = await photo_service.get_photos_in_bounds(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lng=min_lng,
            max_lng=max_lng
        )
        
        return photos
        
    except Exception as e:
        logger.error(f"Failed to get photos in bounds: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve photos")


@router.post("/batch/metadata", response_model=dict)
async def update_batch_metadata(
    photo_ids: List[str],
    tags: Optional[List[str]] = None,
    description: Optional[str] = None,
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Update metadata for multiple photos
    """
    try:
        updates = {}
        if tags is not None:
            updates["tags"] = tags
        if description is not None:
            updates["description"] = description
        
        if not updates:
            raise HTTPException(status_code=400, detail="No updates provided")
        
        updated_count = 0
        for photo_id in photo_ids:
            success = await photo_service.update_photo(photo_id, updates)
            if success:
                updated_count += 1
        
        return {
            "message": f"Updated {updated_count} of {len(photo_ids)} photos",
            "updated_count": updated_count,
            "total_count": len(photo_ids)
        }
        
    except Exception as e:
        logger.error(f"Failed to update batch metadata: {e}")
        raise HTTPException(status_code=500, detail="Failed to update photos")


@router.get("/duplicates/{hash_md5}", response_model=List[Photo])
async def get_duplicate_photos(
    hash_md5: str,
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Get photos with the same hash (duplicates)
    """
    try:
        photos = await photo_service.get_photos_by_hash(hash_md5)
        return photos
        
    except Exception as e:
        logger.error(f"Failed to get duplicate photos: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve duplicate photos")


@router.get("/stats/count", response_model=dict)
async def get_photo_count(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    tags: Optional[str] = Query(None),
    uploader_id: Optional[str] = Query(None),
    photo_service: MongoPhotoService = Depends(get_photo_service)
):
    """
    Get count of photos matching filters
    """
    try:
        filters = None
        if any([start_date, end_date, tags, uploader_id]):
            filters = PhotoFilters(
                start_date=start_date,
                end_date=end_date,
                tags=tags.split(',') if tags else None,
                uploader_id=uploader_id
            )
        
        count = await photo_service.count_photos(filters)
        
        return {"count": count}
        
    except Exception as e:
        logger.error(f"Failed to get photo count: {e}")
        raise HTTPException(status_code=500, detail="Failed to get photo count")