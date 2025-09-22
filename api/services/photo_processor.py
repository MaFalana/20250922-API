"""
Photo processing pipeline with async processing, format conversion, and error recovery
"""

import logging
import asyncio
import io
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from PIL import Image
import pillow_heif  # For HEIC support

from models.photo import Photo, JobStatus
from services.photo_upload import PhotoUploadService
from services.azure_blob_photo import AzureBlobPhotoManager
from services.database import DatabaseService


logger = logging.getLogger(__name__)


class ProcessingStatus(str, Enum):
    """Photo processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


class PhotoProcessor:
    """Async photo processing pipeline"""
    
    def __init__(
        self, 
        blob_manager: AzureBlobPhotoManager,
        database_service: DatabaseService,
        upload_service: PhotoUploadService
    ):
        self.blob_manager = blob_manager
        self.database_service = database_service
        self.upload_service = upload_service
        self.processing_queue = asyncio.Queue()
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Register HEIC opener
        pillow_heif.register_heif_opener()
    
    async def start_processing(self):
        """Start the async processing worker"""
        logger.info("Starting photo processing pipeline")
        while True:
            try:
                # Get next processing job
                job = await self.processing_queue.get()
                await self._process_photo_job(job)
                self.processing_queue.task_done()
            except Exception as e:
                logger.error(f"Processing pipeline error: {e}")
                await asyncio.sleep(1)  # Brief pause before continuing
    
    async def queue_photo_for_processing(self, photo_data: Dict[str, Any]) -> str:
        """
        Queue a photo for async processing
        
        Args:
            photo_data: Photo data from upload service
            
        Returns:
            Job ID for tracking
        """
        job_id = f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{photo_data['hash_md5'][:8]}"
        
        job = {
            "job_id": job_id,
            "photo_data": photo_data,
            "status": ProcessingStatus.PENDING,
            "created_at": datetime.utcnow(),
            "retry_count": 0,
            "error_message": None
        }
        
        await self.processing_queue.put(job)
        logger.info(f"Queued photo for processing: {job_id}")
        
        return job_id
    
    async def _process_photo_job(self, job: Dict[str, Any]):
        """Process a single photo job"""
        job_id = job["job_id"]
        photo_data = job["photo_data"]
        
        try:
            logger.info(f"Processing photo job: {job_id} - File: {photo_data['filename']} ({len(photo_data['file_content'])} bytes)")
            
            # Update status to processing
            job["status"] = ProcessingStatus.PROCESSING
            
            # Step 1: Convert image format if needed (HEIC to JPEG)
            logger.info(f"Step 1: Converting image format for {job_id}")
            processed_content = await self._convert_image_format(
                photo_data["file_content"],
                photo_data["mime_type"],
                photo_data["filename"]
            )
            
            # Update photo data with processed content
            if processed_content["converted"]:
                photo_data["file_content"] = processed_content["content"]
                photo_data["mime_type"] = processed_content["mime_type"]
                photo_data["filename"] = processed_content["filename"]
                logger.info(f"Image format converted for {job_id}: {processed_content['mime_type']}")
            else:
                logger.info(f"No format conversion needed for {job_id}")
            
            # Step 2: Validate coordinates
            logger.info(f"Step 2: Validating coordinates for {job_id}")
            if photo_data.get("latitude") and photo_data.get("longitude"):
                await self.upload_service.validate_coordinates(
                    photo_data["latitude"],
                    photo_data["longitude"],
                    photo_data.get("altitude")
                )
                logger.info(f"Coordinates validated for {job_id}: {photo_data['latitude']}, {photo_data['longitude']}")
            
            # Step 3: Upload to blob storage with thumbnails
            logger.info(f"Step 3: Uploading to blob storage: {photo_data['filename']} ({len(photo_data['file_content'])} bytes)")
            
            # Skip thumbnails for very large files to avoid memory issues
            generate_thumbnails = len(photo_data['file_content']) < 10 * 1024 * 1024  # 10MB limit
            if not generate_thumbnails:
                logger.info(f"Skipping thumbnails for large file: {photo_data['filename']} ({len(photo_data['file_content'])} bytes)")
            
            # Upload with timeout for large files
            try:
                logger.info(f"Starting blob upload for {job_id}: {photo_data['filename']} ({len(photo_data['file_content'])} bytes)")
                upload_result = await asyncio.wait_for(
                    self.blob_manager.upload_photo_with_thumbnail(
                        file_content=photo_data["file_content"],
                        filename=photo_data["filename"],
                        timestamp=photo_data["timestamp"],
                        content_type=photo_data["mime_type"],
                        generate_thumbnails=generate_thumbnails
                    ),
                    timeout=120  # 2 minute timeout for large files
                )
                logger.info(f"✅ Blob upload successful for {job_id}: {upload_result['photo_url']}")
                
            except asyncio.TimeoutError:
                logger.error(f"❌ Blob upload timeout for {job_id} after 2 minutes")
                raise Exception("Blob upload timeout - file too large or network issues")
            except Exception as upload_error:
                logger.error(f"❌ Blob upload failed for {job_id}: {upload_error}")
                raise
            
            # Step 4: Create photo record
            logger.info(f"Step 4: Creating database record for {job_id}")
            photo = Photo(
                filename=photo_data["filename"],
                original_filename=photo_data["original_filename"],
                blob_url=upload_result["photo_url"],
                thumbnail_urls=upload_result["thumbnail_urls"],
                thumbnail_url=upload_result["thumbnail_urls"].get("medium"),  # Backward compatibility
                latitude=photo_data["latitude"],
                longitude=photo_data["longitude"],
                altitude=photo_data.get("altitude"),
                timestamp=photo_data["timestamp"],
                file_size=photo_data["file_size"],
                mime_type=photo_data["mime_type"],
                camera_make=photo_data.get("camera_make"),
                camera_model=photo_data.get("camera_model"),
                camera_settings=photo_data.get("camera_settings"),
                tags=photo_data.get("tags", []),
                description=photo_data.get("description"),
                uploader_id=photo_data.get("uploader_id"),
                hash_md5=photo_data["hash_md5"],
                processing_status="completed"
            )
            
            # Step 5: Save to database
            logger.info(f"Step 5: Saving photo to database: {photo.filename}")
            photo_id = await self.database_service.create_photo(photo)
            logger.info(f"Database save successful for {job_id}: {photo_id}")
            
            # Update job status
            job["status"] = ProcessingStatus.COMPLETED
            job["photo_id"] = photo_id
            job["completed_at"] = datetime.utcnow()
            
            logger.info(f"✅ Successfully processed photo job: {job_id} -> {photo_id}")
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"❌ Photo processing failed for job {job_id}: {e}")
            logger.error(f"Full error traceback: {error_details}")
            
            # Handle retry logic
            job["retry_count"] += 1
            job["error_message"] = str(e)
            
            if job["retry_count"] < self.max_retries:
                job["status"] = ProcessingStatus.RETRY
                logger.info(f"Retrying job {job_id} (attempt {job['retry_count']})")
                
                # Wait before retry
                await asyncio.sleep(self.retry_delay * job["retry_count"])
                
                # Re-queue for retry
                await self.processing_queue.put(job)
            else:
                job["status"] = ProcessingStatus.FAILED
                job["failed_at"] = datetime.utcnow()
                logger.error(f"Job {job_id} failed after {self.max_retries} retries")
                
                # Cleanup any partial uploads
                try:
                    await self.blob_manager.delete_photo_and_thumbnails(
                        photo_data["filename"],
                        photo_data["timestamp"]
                    )
                except Exception as cleanup_error:
                    logger.error(f"Cleanup failed for job {job_id}: {cleanup_error}")
    
    async def _convert_image_format(
        self, 
        file_content: bytes, 
        mime_type: str, 
        filename: str
    ) -> Dict[str, Any]:
        """
        Convert image format if needed (HEIC to JPEG) while preserving EXIF
        
        Args:
            file_content: Original file bytes
            mime_type: Original MIME type
            filename: Original filename
            
        Returns:
            Dict with conversion results
        """
        try:
            # Check if conversion is needed
            if mime_type not in ['image/heic', 'image/heif']:
                # For large JPEG files, optimize them to reduce memory usage
                if len(file_content) > 10 * 1024 * 1024:  # 10MB+
                    logger.info(f"Large JPEG detected ({len(file_content)} bytes), optimizing...")
                    return await self._optimize_large_image(file_content, mime_type, filename)
                
                return {
                    "converted": False,
                    "content": file_content,
                    "mime_type": mime_type,
                    "filename": filename
                }
            
            logger.info(f"Converting HEIC image: {filename} ({len(file_content)} bytes)")
            
            # Open HEIC image with memory optimization
            image_io = io.BytesIO(file_content)
            image = Image.open(image_io)
            
            # Log image details
            logger.info(f"Image opened: {image.size} pixels, mode: {image.mode}")
            
            # Preserve EXIF data
            exif_dict = None
            try:
                if hasattr(image, 'getexif'):
                    exif_dict = image.getexif()
                    logger.info(f"EXIF data preserved: {len(exif_dict) if exif_dict else 0} entries")
            except Exception as exif_error:
                logger.warning(f"Could not preserve EXIF data: {exif_error}")
            
            # Convert to RGB if necessary
            if image.mode in ('RGBA', 'LA', 'P'):
                logger.info(f"Converting from {image.mode} to RGB")
                # Create white background for transparency
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                if image.mode in ('RGBA', 'LA'):
                    background.paste(image, mask=image.split()[-1])
                image = background
            elif image.mode != 'RGB':
                logger.info(f"Converting from {image.mode} to RGB")
                image = image.convert('RGB')
            
            # Save as JPEG with memory optimization
            output_io = io.BytesIO()
            save_kwargs = {
                'format': 'JPEG',
                'quality': 85,  # Slightly lower quality for large files
                'optimize': True
            }
            
            # Include EXIF if available
            if exif_dict:
                try:
                    save_kwargs['exif'] = exif_dict
                except Exception as exif_save_error:
                    logger.warning(f"Could not save EXIF data: {exif_save_error}")
            
            image.save(output_io, **save_kwargs)
            output_io.seek(0)
            
            # Generate new filename
            name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
            new_filename = f"{name_without_ext}.jpg"
            
            converted_content = output_io.getvalue()
            
            # Clean up memory
            image.close()
            image_io.close()
            output_io.close()
            
            logger.info(f"Successfully converted HEIC to JPEG: {filename} -> {new_filename} ({len(converted_content)} bytes)")
            
            return {
                "converted": True,
                "content": converted_content,
                "mime_type": "image/jpeg",
                "filename": new_filename,
                "original_size": len(file_content),
                "converted_size": len(converted_content)
            }
            
        except Exception as e:
            logger.error(f"Image format conversion failed: {e}")
            # Return original if conversion fails
            return {
                "converted": False,
                "content": file_content,
                "mime_type": mime_type,
                "filename": filename,
                "error": str(e)
            }
    
    async def _optimize_large_image(
        self, 
        file_content: bytes, 
        mime_type: str, 
        filename: str
    ) -> Dict[str, Any]:
        """
        Optimize large images to reduce memory usage and processing time
        
        Args:
            file_content: Original file bytes
            mime_type: Original MIME type
            filename: Original filename
            
        Returns:
            Dict with optimization results
        """
        try:
            logger.info(f"Optimizing large image: {filename} ({len(file_content)} bytes)")
            
            # Open image
            image_io = io.BytesIO(file_content)
            image = Image.open(image_io)
            
            logger.info(f"Original image: {image.size} pixels, mode: {image.mode}")
            
            # Preserve EXIF data
            exif_dict = None
            try:
                if hasattr(image, 'getexif'):
                    exif_dict = image.getexif()
            except Exception:
                pass
            
            # Resize if image is very large (keep aspect ratio)
            max_dimension = 4000  # Max width or height
            if max(image.size) > max_dimension:
                ratio = max_dimension / max(image.size)
                new_size = tuple(int(dim * ratio) for dim in image.size)
                logger.info(f"Resizing from {image.size} to {new_size}")
                image = image.resize(new_size, Image.Resampling.LANCZOS)
            
            # Convert to RGB if necessary
            if image.mode != 'RGB':
                if image.mode in ('RGBA', 'LA', 'P'):
                    background = Image.new('RGB', image.size, (255, 255, 255))
                    if image.mode == 'P':
                        image = image.convert('RGBA')
                    if image.mode in ('RGBA', 'LA'):
                        background.paste(image, mask=image.split()[-1])
                    image = background
                else:
                    image = image.convert('RGB')
            
            # Save optimized JPEG
            output_io = io.BytesIO()
            save_kwargs = {
                'format': 'JPEG',
                'quality': 80,  # Lower quality for large files
                'optimize': True
            }
            
            # Include EXIF if available
            if exif_dict:
                try:
                    save_kwargs['exif'] = exif_dict
                except Exception:
                    pass
            
            image.save(output_io, **save_kwargs)
            output_io.seek(0)
            
            optimized_content = output_io.getvalue()
            
            # Clean up memory
            image.close()
            image_io.close()
            output_io.close()
            
            logger.info(f"Image optimized: {len(file_content)} -> {len(optimized_content)} bytes ({(1 - len(optimized_content)/len(file_content))*100:.1f}% reduction)")
            
            return {
                "converted": True,
                "content": optimized_content,
                "mime_type": "image/jpeg",
                "filename": filename,
                "original_size": len(file_content),
                "converted_size": len(optimized_content)
            }
            
        except Exception as e:
            logger.error(f"Image optimization failed: {e}")
            return {
                "converted": False,
                "content": file_content,
                "mime_type": mime_type,
                "filename": filename,
                "error": str(e)
            }
    
    async def process_manual_coordinates(
        self, 
        photo_id: str, 
        latitude: float, 
        longitude: float, 
        altitude: Optional[float] = None
    ) -> bool:
        """
        Process manual coordinate assignment for photos without GPS data
        
        Args:
            photo_id: Photo ID to update
            latitude: Manual latitude
            longitude: Manual longitude
            altitude: Optional manual altitude
            
        Returns:
            True if successful
        """
        try:
            # Validate coordinates
            await self.upload_service.validate_coordinates(latitude, longitude, altitude)
            
            # Update photo record
            updates = {
                "latitude": latitude,
                "longitude": longitude,
                "processing_status": "completed"
            }
            
            if altitude is not None:
                updates["altitude"] = altitude
            
            success = await self.database_service.update_photo(photo_id, updates)
            
            if success:
                logger.info(f"Updated manual coordinates for photo {photo_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Manual coordinate processing failed for {photo_id}: {e}")
            return False
    
    async def reprocess_failed_photo(self, photo_id: str) -> bool:
        """
        Reprocess a failed photo
        
        Args:
            photo_id: Photo ID to reprocess
            
        Returns:
            True if reprocessing started successfully
        """
        try:
            # Get photo record
            photo = await self.database_service.get_photo(photo_id)
            if not photo:
                logger.error(f"Photo not found for reprocessing: {photo_id}")
                return False
            
            # Mark as pending for reprocessing
            await self.database_service.update_photo(
                photo_id, 
                {"processing_status": "pending"}
            )
            
            # Note: This would require storing original file data or re-uploading
            # For now, just update status
            logger.info(f"Marked photo {photo_id} for reprocessing")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reprocess photo {photo_id}: {e}")
            return False
    
    async def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get processing pipeline statistics
        
        Returns:
            Dict with processing stats
        """
        try:
            # Get queue size
            queue_size = self.processing_queue.qsize()
            
            # Get photo counts by status
            # Note: This would require extending the database service
            # For now, return basic stats
            
            return {
                "queue_size": queue_size,
                "max_retries": self.max_retries,
                "retry_delay": self.retry_delay,
                "supported_formats": list(self.upload_service.supported_formats.keys()),
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
            return {
                "queue_size": 0,
                "error": str(e),
                "timestamp": datetime.utcnow()
            }
    
    async def cleanup_old_failed_jobs(self, days_old: int = 7) -> int:
        """
        Clean up old failed processing jobs
        
        Args:
            days_old: Remove jobs older than this many days
            
        Returns:
            Number of jobs cleaned up
        """
        try:
            # This would require a job tracking system
            # For now, just log the intent
            logger.info(f"Would clean up failed jobs older than {days_old} days")
            return 0
            
        except Exception as e:
            logger.error(f"Failed to cleanup old jobs: {e}")
            return 0


class PhotoProcessingManager:
    """Manager for photo processing operations"""
    
    def __init__(self):
        self.processors: Dict[str, PhotoProcessor] = {}
        self.default_processor: Optional[PhotoProcessor] = None
    
    def register_processor(
        self, 
        name: str, 
        blob_manager: AzureBlobPhotoManager,
        database_service: DatabaseService,
        upload_service: PhotoUploadService
    ):
        """Register a photo processor"""
        processor = PhotoProcessor(blob_manager, database_service, upload_service)
        self.processors[name] = processor
        
        if self.default_processor is None:
            self.default_processor = processor
        
        logger.info(f"Registered photo processor: {name}")
    
    async def start_all_processors(self):
        """Start all registered processors"""
        tasks = []
        for name, processor in self.processors.items():
            task = asyncio.create_task(processor.start_processing())
            tasks.append(task)
            logger.info(f"Started processor: {name}")
        
        if tasks:
            await asyncio.gather(*tasks)
    
    def get_processor(self, name: Optional[str] = None) -> Optional[PhotoProcessor]:
        """Get a processor by name or default"""
        if name:
            return self.processors.get(name)
        return self.default_processor