"""
Export service for generating KML, KMZ, and ZIP files from photo data
"""

import logging
import asyncio
import os
import tempfile
import shutil
import zipfile
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from models.export import ExportJob, ExportFormat, CoordinateSystem
from models.photo import Photo
from services.database import DatabaseService
from services.azure_blob_photo import AzureBlobPhotoManager
from services.kml_generator import KMLGenerator, KMZGenerator


logger = logging.getLogger(__name__)


class ExportService:
    """Service for managing photo export jobs and file generation"""
    
    def __init__(
        self, 
        database_service: DatabaseService,
        blob_manager: AzureBlobPhotoManager,
        max_concurrent_jobs: int = 3
    ):
        self.database_service = database_service
        self.blob_manager = blob_manager
        self.max_concurrent_jobs = max_concurrent_jobs
        self.active_jobs: Dict[str, ExportJob] = {}
        self.job_queue: asyncio.Queue = asyncio.Queue()
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_jobs)
        self._processing_task = None
        
        # Use temporary directory for exports (no persistent storage)
        self.temp_export_dir = tempfile.mkdtemp(prefix="photo_exports_")
        
        # Initialize generators
        self.kml_generator = KMLGenerator(blob_manager)
        self.kmz_generator = KMZGenerator(blob_manager)
    

    
    async def start_processing(self):
        """Start the background job processing task"""
        if self._processing_task is None or self._processing_task.done():
            self._processing_task = asyncio.create_task(self._process_job_queue())
            logger.info("Started export job processing")
    
    async def stop_processing(self):
        """Stop the background job processing task"""
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
            logger.info("Stopped export job processing")
    
    async def create_export_job(
        self,
        photo_ids: List[str],
        export_format: ExportFormat,
        coordinate_system: CoordinateSystem = CoordinateSystem.WGS84,
        include_altitude: bool = True,
        include_photos_in_kmz: bool = True,
        include_thumbnails: bool = True,
        requester_id: Optional[str] = None
    ) -> ExportJob:
        """
        Create a new export job and add it to the processing queue
        
        Args:
            photo_ids: List of photo IDs to export
            export_format: Export format (KML, KMZ, ZIP, etc.)
            coordinate_system: Target coordinate system
            include_altitude: Include altitude data
            include_photos_in_kmz: Embed photos in KMZ files
            include_thumbnails: Include thumbnail images
            requester_id: ID of requesting user
            
        Returns:
            Created ExportJob instance
        """
        try:
            # Validate photo IDs exist
            valid_photo_ids = await self._validate_photo_ids(photo_ids)
            if not valid_photo_ids:
                raise ValueError("No valid photos found for export")
            
            # Create export job
            export_job = ExportJob(
                photo_ids=valid_photo_ids,
                export_type=export_format,
                coordinate_system=coordinate_system,
                include_altitude=include_altitude,
                include_photos_in_kmz=include_photos_in_kmz,
                include_thumbnails=include_thumbnails,
                requester_id=requester_id
            )
            
            # Store job in active jobs
            self.active_jobs[export_job.id] = export_job
            
            # Add to processing queue
            await self.job_queue.put(export_job.id)
            
            logger.info(f"Created export job {export_job.id} for {len(valid_photo_ids)} photos")
            return export_job
            
        except Exception as e:
            logger.error(f"Failed to create export job: {e}")
            raise
    
    async def get_job_status(self, job_id: str) -> Optional[ExportJob]:
        """Get the current status of an export job"""
        return self.active_jobs.get(job_id)
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel an export job if it's still pending or processing"""
        job = self.active_jobs.get(job_id)
        if not job:
            return False
        
        if job.status in ["pending", "queued", "processing"]:
            job.status = "cancelled"
            job.updated_at = datetime.utcnow()
            logger.info(f"Cancelled export job {job_id}")
            return True
        
        return False
    
    async def cleanup_expired_jobs(self):
        """Clean up expired export jobs and their files"""
        expired_jobs = []
        
        for job_id, job in self.active_jobs.items():
            if job.is_expired():
                expired_jobs.append(job_id)
        
        for job_id in expired_jobs:
            job = self.active_jobs[job_id]
            
            # Delete temporary file if exists
            if job.file_path and os.path.exists(job.file_path):
                try:
                    os.remove(job.file_path)
                    logger.info(f"Deleted expired export file: {job.file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete expired export file: {e}")
            
            # Remove from active jobs
            del self.active_jobs[job_id]
            logger.info(f"Cleaned up expired export job {job_id}")
    
    async def _validate_photo_ids(self, photo_ids: List[str]) -> List[str]:
        """Validate that photo IDs exist and return valid ones"""
        valid_ids = []
        
        for photo_id in photo_ids:
            photo = await self.database_service.get_photo(photo_id)
            if photo:
                valid_ids.append(photo_id)
            else:
                logger.warning(f"Photo ID {photo_id} not found, skipping")
        
        return valid_ids
    
    async def _process_job_queue(self):
        """Background task to process export jobs from the queue"""
        logger.info("Export job queue processor started")
        
        try:
            while True:
                try:
                    # Get next job from queue (wait up to 1 second)
                    job_id = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)
                    
                    job = self.active_jobs.get(job_id)
                    if not job:
                        logger.warning(f"Job {job_id} not found in active jobs")
                        continue
                    
                    if job.status == "cancelled":
                        logger.info(f"Skipping cancelled job {job_id}")
                        continue
                    
                    # Process the job
                    await self._process_export_job(job)
                    
                except asyncio.TimeoutError:
                    # No jobs in queue, continue waiting
                    continue
                except asyncio.CancelledError:
                    logger.info("Export job processor cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in job queue processor: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Fatal error in job queue processor: {e}")
    
    async def _process_export_job(self, job: ExportJob):
        """Process a single export job"""
        try:
            logger.info(f"Processing export job {job.id}")
            job.mark_started()
            
            # Get photo data
            photos = await self._get_photos_for_export(job.photo_ids)
            if not photos:
                job.mark_failed("No photos found for export")
                return
            
            # Generate export file based on format
            if job.export_type == ExportFormat.KML:
                await self._generate_kml_export(job, photos)
            elif job.export_type == ExportFormat.KMZ:
                await self._generate_kmz_export(job, photos)
            elif job.export_type == ExportFormat.ZIP:
                await self._generate_zip_export(job, photos)
            elif job.export_type == ExportFormat.PHOTOS_ONLY:
                await self._generate_photos_export(job, photos)
            else:
                job.mark_failed(f"Unsupported export format: {job.export_type}")
                return
            
            logger.info(f"Completed export job {job.id}")
            
        except Exception as e:
            logger.error(f"Failed to process export job {job.id}: {e}")
            job.mark_failed(str(e))
    
    async def _get_photos_for_export(self, photo_ids: List[str]) -> List[Photo]:
        """Get photo objects for export"""
        photos = []
        
        for photo_id in photo_ids:
            photo = await self.database_service.get_photo(photo_id)
            if photo:
                photos.append(photo)
        
        return photos
    
    async def _generate_kml_export(self, job: ExportJob, photos: List[Photo]):
        """Generate KML export"""
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.kml', delete=False) as temp_file:
                # Generate KML content
                kml_content = self.kml_generator.generate_kml(
                    photos=photos,
                    coordinate_system=job.coordinate_system,
                    include_altitude=job.include_altitude,
                    title=f"Photo Export - {job.created_at.strftime('%Y-%m-%d %H:%M')}"
                )
                
                # Write to temporary file
                temp_file.write(kml_content)
                temp_file.flush()
                
                # Keep temporary file for direct download
                file_size = os.path.getsize(temp_file.name)
                job.mark_completed(temp_file.name, file_size)
                
                logger.info(f"Generated KML export for job {job.id}")
                
        except Exception as e:
            logger.error(f"KML export failed for job {job.id}: {e}")
            job.mark_failed(f"KML export failed: {str(e)}")
    
    async def _generate_kmz_export(self, job: ExportJob, photos: List[Photo]):
        """Generate KMZ export with embedded photos"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.kmz', delete=False) as temp_file:
                temp_file.close()  # Close so KMZ generator can write to it
                
                # Generate KMZ file
                await self.kmz_generator.generate_kmz(
                    photos=photos,
                    output_path=temp_file.name,
                    coordinate_system=job.coordinate_system,
                    include_altitude=job.include_altitude,
                    include_photos=job.include_photos_in_kmz,
                    include_thumbnails=job.include_thumbnails,
                    title=f"Photo Export - {job.created_at.strftime('%Y-%m-%d %H:%M')}"
                )
                
                # Keep temporary file for direct download
                file_size = os.path.getsize(temp_file.name)
                job.mark_completed(temp_file.name, file_size)
                
                logger.info(f"Generated KMZ export for job {job.id}")
                
        except Exception as e:
            logger.error(f"KMZ export failed for job {job.id}: {e}")
            job.mark_failed(f"KMZ export failed: {str(e)}")
    
    async def _generate_zip_export(self, job: ExportJob, photos: List[Photo]):
        """Generate ZIP export with photos and KML"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                temp_file.close()  # Close so we can write to it
                
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Generate KML file
                    kml_content = self.kml_generator.generate_kml(
                        photos=photos,
                        coordinate_system=job.coordinate_system,
                        include_altitude=job.include_altitude,
                        title=f"Photo Export - {job.created_at.strftime('%Y-%m-%d %H:%M')}"
                    )
                    
                    kml_path = os.path.join(temp_dir, 'photos.kml')
                    with open(kml_path, 'w', encoding='utf-8') as f:
                        f.write(kml_content)
                    
                    # Create ZIP file
                    with zipfile.ZipFile(temp_file.name, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        # Add KML file
                        zip_file.write(kml_path, 'photos.kml')
                        
                        # Add photos if requested
                        if job.include_photos_in_kmz:  # Reuse this flag for ZIP
                            await self._add_photos_to_zip(zip_file, photos, job)
                
                # Keep temporary file for direct download
                file_size = os.path.getsize(temp_file.name)
                job.mark_completed(temp_file.name, file_size)
                
                logger.info(f"Generated ZIP export for job {job.id}")
                
        except Exception as e:
            logger.error(f"ZIP export failed for job {job.id}: {e}")
            job.mark_failed(f"ZIP export failed: {str(e)}")
    
    async def _generate_photos_export(self, job: ExportJob, photos: List[Photo]):
        """Generate photos-only ZIP export"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                temp_file.close()  # Close so we can write to it
                
                # Create ZIP file with photos only
                with zipfile.ZipFile(temp_file.name, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    await self._add_photos_to_zip(zip_file, photos, job)
                
                # Upload to blob storage
                await self._upload_export_file(job, temp_file.name)
                
                # Clean up temporary file
                os.unlink(temp_file.name)
                
                logger.info(f"Generated photos-only export for job {job.id}")
                
        except Exception as e:
            logger.error(f"Photos export failed for job {job.id}: {e}")
            job.mark_failed(f"Photos export failed: {str(e)}")
    
    async def _add_photos_to_zip(self, zip_file: zipfile.ZipFile, photos: List[Photo], job: ExportJob):
        """Add photos to ZIP file"""
        import requests
        
        for i, photo in enumerate(photos):
            try:
                # Download photo
                response = requests.get(photo.blob_url, stream=True, timeout=30)
                response.raise_for_status()
                
                # Create safe filename
                safe_filename = self._make_safe_filename(photo.original_filename)
                
                # Add to ZIP
                zip_file.writestr(safe_filename, response.content)
                
                # Update progress
                job.update_progress(i + 1)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Added {i + 1}/{len(photos)} photos to ZIP")
                    
            except Exception as e:
                logger.error(f"Failed to add photo {photo.id} to ZIP: {e}")
                continue
    
    def _make_safe_filename(self, filename: str) -> str:
        """Make filename safe for ZIP archive"""
        # Remove or replace unsafe characters
        safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
        safe_filename = ''.join(c if c in safe_chars else '_' for c in filename)
        
        # Ensure it's not empty
        if not safe_filename:
            safe_filename = "photo.jpg"
        
        return safe_filename
    
    async def _upload_export_file(self, job: ExportJob, file_path: str) -> str:
        """Upload export file to blob storage and return download URL"""
        try:
            # Generate blob path within exports directory
            timestamp = datetime.utcnow().strftime("%Y/%m/%d")
            blob_path = f"{self.export_directory}/{timestamp}/{job.output_filename}"
            
            # Upload file to the same container as photos
            with open(file_path, 'rb') as file_data:
                export_blob_client = self.blob_manager.blob_service_client.get_blob_client(
                    container=self.blob_manager.container_client.container_name,
                    blob=blob_path
                )
                
                export_blob_client.upload_blob(
                    data=file_data,
                    overwrite=True,
                    metadata={
                        'job_id': job.id,
                        'export_type': job.export_type.value,
                        'created_at': job.created_at.isoformat(),
                        'expires_at': job.expires_at.isoformat() if job.expires_at else None
                    }
                )
            
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Generate download URL with SAS token
            download_url = self.blob_manager.generate_download_url(
                blob_path=blob_path,
                expiry_hours=24
            )
            
            # Update job with completion info
            job.mark_completed(download_url, file_size, blob_path)
            
            logger.info(f"Uploaded export file for job {job.id}: {blob_path}")
            return download_url
            
        except Exception as e:
            logger.error(f"Failed to upload export file: {e}")
            raise
    
    def get_content_type(self, export_format: ExportFormat) -> str:
        """Get MIME type for export format"""
        content_types = {
            ExportFormat.KML: "application/vnd.google-earth.kml+xml",
            ExportFormat.KMZ: "application/vnd.google-earth.kmz",
            ExportFormat.ZIP: "application/zip",
            ExportFormat.PHOTOS_ONLY: "application/zip"
        }
        return content_types.get(export_format, "application/octet-stream")
    
    async def get_active_jobs_count(self) -> int:
        """Get count of active (non-completed, non-failed) jobs"""
        active_count = 0
        for job in self.active_jobs.values():
            if job.status in ["pending", "queued", "processing"]:
                active_count += 1
        return active_count
    
    async def get_job_statistics(self) -> Dict[str, Any]:
        """Get export job statistics"""
        stats = {
            "total_jobs": len(self.active_jobs),
            "pending": 0,
            "queued": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0
        }
        
        for job in self.active_jobs.values():
            if job.status in stats:
                stats[job.status] += 1
        
        return stats  
  
    async def get_export_file(self, job_id: str) -> Optional[str]:
        """
        Get the file path for a completed export job
        
        Args:
            job_id: Export job ID
            
        Returns:
            File path if job is completed and file exists, None otherwise
        """
        job = self.active_jobs.get(job_id)
        if not job:
            return None
        
        if job.status != JobStatus.COMPLETED:
            return None
        
        if not job.file_path or not os.path.exists(job.file_path):
            return None
        
        return job.file_path
    
    def __del__(self):
        """Cleanup temporary directory on service destruction"""
        try:
            if hasattr(self, 'temp_export_dir') and os.path.exists(self.temp_export_dir):
                shutil.rmtree(self.temp_export_dir)
                logger.info(f"Cleaned up temporary export directory: {self.temp_export_dir}")
        except Exception as e:
            logger.error(f"Failed to cleanup temporary directory: {e}")