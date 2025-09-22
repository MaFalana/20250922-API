"""
Export routes for KML, KMZ, and ZIP generation
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Query
from datetime import datetime

from models.export import (
    ExportRequest, 
    ExportResponse, 
    ExportStatusResponse, 
    DownloadResponse,
    ExportFormat,
    CoordinateSystem
)
from services.export_service import ExportService
from services.database import DatabaseService
from services.azure_blob_photo import AzureBlobPhotoManager
from services.mongo import MongoPhotoService
from db.mongo import AzureMongoManager, AzureBlobManager
import os


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/exports", tags=["exports"])


# Global export service instance
_export_service_instance = None

async def get_export_service() -> ExportService:
    """Get configured export service instance (singleton)"""
    global _export_service_instance
    
    if _export_service_instance is None:
        try:
            # Initialize database service using environment variables
            db_name = os.getenv("MONGO_DATABASE_NAME", "photo_mapper")
            database_service = MongoPhotoService(db_name=db_name)
            
            # Initialize blob manager using environment variables
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            if not connection_string:
                raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment")
            
            container_name = os.getenv("AZURE_STORAGE_CONTAINER", "photo-log-map")
            blob_manager = AzureBlobPhotoManager(
                connection_string=connection_string,
                container_name=container_name
            )
            
            # Create export service
            _export_service_instance = ExportService(
                database_service=database_service,
                blob_manager=blob_manager
            )
            
            # Start processing
            await _export_service_instance.start_processing()
            logger.info("Export service initialized and started")
            
        except Exception as e:
            logger.error(f"Failed to initialize export service: {e}")
            raise HTTPException(status_code=500, detail=f"Export service initialization failed: {str(e)}")
    
    return _export_service_instance


@router.post("/photos", response_model=ExportResponse)
async def create_photo_export(
    request: ExportRequest,
    background_tasks: BackgroundTasks,
    export_service: ExportService = Depends(get_export_service)
):
    """
    Create a new photo export job
    
    - **photo_ids**: List of photo IDs to export
    - **export_type**: Export format (kml, kmz, zip, photos)
    - **coordinate_system**: Target coordinate system (default: WGS84)
    - **include_altitude**: Include altitude data (default: true)
    - **include_photos_in_kmz**: Embed photos in KMZ files (default: true)
    - **include_thumbnails**: Include thumbnail images (default: true)
    """
    try:
        if not request.photo_ids:
            raise HTTPException(status_code=400, detail="No photo IDs provided")
        
        if len(request.photo_ids) > 1000:
            raise HTTPException(status_code=400, detail="Too many photos requested (max 1000)")
        
        # Create export job
        logger.info(f"Creating export job for photos: {request.photo_ids}")
        job = await export_service.create_export_job(
            photo_ids=request.photo_ids,
            export_format=request.export_type,
            coordinate_system=request.coordinate_system,
            include_altitude=request.include_altitude,
            include_photos_in_kmz=request.include_photos_in_kmz,
            include_thumbnails=request.include_thumbnails,
            requester_id=request.requester_id
        )
        logger.info(f"Created export job: {job.id}, status: {job.status}")
        
        # Schedule cleanup task
        background_tasks.add_task(
            _schedule_cleanup,
            export_service,
            job.id,
            delay_hours=25  # Clean up 1 hour after expiration
        )
        
        return ExportResponse(
            job_id=job.id,
            status=job.status,
            message="Export job created successfully. Processing will begin shortly.",
            estimated_completion=job.created_at.replace(
                minute=job.created_at.minute + 5  # Rough estimate
            )
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create export job: {e}")
        raise HTTPException(status_code=500, detail=f"Export job creation failed: {str(e)}")


@router.get("/kml", response_model=ExportResponse)
async def create_kml_export(
    photo_ids: List[str] = Query(..., description="Photo IDs to export"),
    coordinate_system: CoordinateSystem = Query(CoordinateSystem.WGS84, description="Coordinate system"),
    include_altitude: bool = Query(True, description="Include altitude data"),
    requester_id: Optional[str] = Query(None, description="Requester ID"),
    export_service: ExportService = Depends(get_export_service)
):
    """
    Create KML export job (convenience endpoint)
    """
    request = ExportRequest(
        photo_ids=photo_ids,
        export_type=ExportFormat.KML,
        coordinate_system=coordinate_system,
        include_altitude=include_altitude,
        requester_id=requester_id
    )
    
    return await create_photo_export(request, BackgroundTasks(), export_service)


@router.get("/kmz", response_model=ExportResponse)
async def create_kmz_export(
    photo_ids: List[str] = Query(..., description="Photo IDs to export"),
    coordinate_system: CoordinateSystem = Query(CoordinateSystem.WGS84, description="Coordinate system"),
    include_altitude: bool = Query(True, description="Include altitude data"),
    include_photos: bool = Query(True, description="Embed photos in KMZ"),
    include_thumbnails: bool = Query(True, description="Include thumbnails"),
    requester_id: Optional[str] = Query(None, description="Requester ID"),
    export_service: ExportService = Depends(get_export_service)
):
    """
    Create KMZ export job (convenience endpoint)
    """
    request = ExportRequest(
        photo_ids=photo_ids,
        export_type=ExportFormat.KMZ,
        coordinate_system=coordinate_system,
        include_altitude=include_altitude,
        include_photos_in_kmz=include_photos,
        include_thumbnails=include_thumbnails,
        requester_id=requester_id
    )
    
    return await create_photo_export(request, BackgroundTasks(), export_service)


@router.get("/{job_id}/status", response_model=ExportStatusResponse)
async def get_export_status(
    job_id: str,
    export_service: ExportService = Depends(get_export_service)
):
    """
    Get the status of an export job
    
    - **job_id**: Export job ID
    """
    try:
        job = await export_service.get_job_status(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Export job not found")
        
        return ExportStatusResponse(
            job_id=job.id,
            status=job.status,
            progress=job.progress,
            total_photos=job.total_photos,
            processed_photos=job.processed_photos,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            expires_at=job.expires_at,
            download_url=f"/api/exports/{job.id}/download" if job.status == "completed" else None,
            file_size=job.file_size,
            error_message=job.error_message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get export status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get export status")


@router.get("/{job_id}/download")
async def download_export_file(
    job_id: str,
    export_service: ExportService = Depends(get_export_service)
):
    """
    Download the export file directly
    
    - **job_id**: Export job ID
    """
    from fastapi.responses import FileResponse
    
    try:
        job = await export_service.get_job_status(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Export job not found")
        
        if job.status != "completed":
            raise HTTPException(
                status_code=400, 
                detail=f"Export job is not completed (status: {job.status})"
            )
        
        if job.is_expired():
            raise HTTPException(status_code=410, detail="Export has expired")
        
        # Get file path from export service
        file_path = await export_service.get_export_file(job_id)
        if not file_path:
            raise HTTPException(status_code=500, detail="Export file not available")
        
        # Determine content type
        content_type_map = {
            "kml": "application/vnd.google-earth.kml+xml",
            "kmz": "application/vnd.google-earth.kmz",
            "zip": "application/zip",
            "photos": "application/zip"
        }
        content_type = content_type_map.get(job.export_type.value, "application/octet-stream")
        
        filename = job.output_filename or f"export_{job.id}.{job.export_type.value}"
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to download export file: {e}")
        raise HTTPException(status_code=500, detail="Failed to download export file")


@router.delete("/{job_id}")
async def cancel_export_job(
    job_id: str,
    export_service: ExportService = Depends(get_export_service)
):
    """
    Cancel an export job
    
    - **job_id**: Export job ID
    """
    try:
        success = await export_service.cancel_job(job_id)
        
        if not success:
            raise HTTPException(
                status_code=400, 
                detail="Job cannot be cancelled (not found or already completed)"
            )
        
        return {"message": "Export job cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel export job: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel export job")


@router.post("/cleanup")
async def cleanup_expired_exports(
    export_service: ExportService = Depends(get_export_service)
):
    """
    Manually trigger cleanup of expired export jobs (admin endpoint)
    """
    try:
        await export_service.cleanup_expired_jobs()
        return {"message": "Cleanup completed successfully"}
        
    except Exception as e:
        logger.error(f"Failed to cleanup expired exports: {e}")
        raise HTTPException(status_code=500, detail="Cleanup failed")


@router.get("/stats")
async def get_export_statistics(
    export_service: ExportService = Depends(get_export_service)
):
    """
    Get export job statistics (admin endpoint)
    """
    try:
        stats = await export_service.get_job_statistics()
        active_count = await export_service.get_active_jobs_count()
        
        return {
            "statistics": stats,
            "active_jobs": active_count,
            "total_active_jobs_in_dict": len(export_service.active_jobs),
            "active_job_ids": list(export_service.active_jobs.keys()),
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Failed to get export statistics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get statistics")

@router.get("/debug/test")
async def debug_test_export_service(
    export_service: ExportService = Depends(get_export_service)
):
    """
    Debug endpoint to test export service
    """
    try:
        return {
            "service_initialized": export_service is not None,
            "active_jobs_count": len(export_service.active_jobs),
            "active_job_ids": list(export_service.active_jobs.keys()),
            "processing_task_running": export_service._processing_task is not None and not export_service._processing_task.done() if export_service._processing_task else False
        }
    except Exception as e:
        logger.error(f"Debug test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Debug test failed: {str(e)}")


async def _schedule_cleanup(export_service: ExportService, job_id: str, delay_hours: int):
    """Background task to schedule cleanup of export job"""
    import asyncio
    
    try:
        # Wait for the specified delay
        await asyncio.sleep(delay_hours * 3600)
        
        # Clean up the specific job if it's expired
        job = await export_service.get_job_status(job_id)
        if job and job.is_expired():
            # Remove from active jobs and clean up temporary file
            if job.file_path and os.path.exists(job.file_path):
                try:
                    os.remove(job.file_path)
                    logger.info(f"Cleaned up export file: {job.file_path}")
                except Exception as e:
                    logger.error(f"Failed to clean up export file: {e}")
            
            # Remove from active jobs
            if job_id in export_service.active_jobs:
                del export_service.active_jobs[job_id]
                logger.info(f"Cleaned up expired export job {job_id}")
                
    except Exception as e:
        logger.error(f"Failed to schedule cleanup for job {job_id}: {e}")