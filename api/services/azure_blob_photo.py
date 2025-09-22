"""
Extended Azure Blob Storage manager for photo storage with thumbnail generation
"""

import logging
import io
import os
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from PIL import Image
from azure.storage.blob import BlobServiceClient, BlobClient, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError

from db.mongo import AzureBlobManager


logger = logging.getLogger(__name__)


class AzureBlobPhotoManager(AzureBlobManager):
    """Extended Azure Blob manager for photo storage with organized structure and thumbnails"""
    
    def __init__(self, connection_string: str, container_name: str = "photos"):
        super().__init__(connection_string, container_name)
        self.thumbnail_sizes = {
            'small': (150, 150),
            'medium': (300, 300),
            'large': (800, 600)
        }
        self.thumbnail_quality = 85
        self._ensure_container_exists()
    
    def _ensure_container_exists(self):
        """Ensure the container exists, create if it doesn't"""
        try:
            self.container_client.get_container_properties()
            logger.info(f"Container '{self.container_client.container_name}' exists")
        except ResourceNotFoundError:
            try:
                self.container_client.create_container()
                logger.info(f"Created container '{self.container_client.container_name}'")
            except Exception as e:
                logger.error(f"Failed to create container: {e}")
                raise
    
    def _get_photo_blob_path(self, filename: str, timestamp: datetime) -> str:
        """
        Generate organized blob path for photos: uploads/YYYY/MM/filename
        
        Args:
            filename: Photo filename
            timestamp: Photo timestamp for folder organization
            
        Returns:
            Blob path string
        """
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        return f"uploads/{year}/{month}/{filename}"
    
    def _get_thumbnail_blob_path(self, filename: str, timestamp: datetime, size: str = 'medium') -> str:
        """
        Generate organized blob path for thumbnails: uploads/YYYY/MM/thumbnails/size_filename
        
        Args:
            filename: Original photo filename
            timestamp: Photo timestamp for folder organization
            size: Thumbnail size (small, medium, large)
            
        Returns:
            Thumbnail blob path string
        """
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        name, ext = os.path.splitext(filename)
        return f"uploads/{year}/{month}/thumbnails/{size}_{name}.jpg"
    
    async def upload_photo_with_thumbnail(
        self, 
        file_content: bytes, 
        filename: str, 
        timestamp: datetime,
        content_type: str = "image/jpeg",
        generate_thumbnails: bool = True
    ) -> Dict[str, Any]:
        """
        Upload photo and generate thumbnails
        
        Args:
            file_content: Raw photo bytes
            filename: Photo filename
            timestamp: Photo timestamp for organization
            content_type: MIME type
            generate_thumbnails: Whether to generate thumbnails
            
        Returns:
            Dict with upload results including URLs
            
        Raises:
            Exception: If upload fails
        """
        try:
            # Generate blob paths
            photo_blob_path = self._get_photo_blob_path(filename, timestamp)
            
            # Upload main photo
            photo_blob_client = self.blob_service_client.get_blob_client(
                container=self.container_client.container_name,
                blob=photo_blob_path
            )
            
            photo_blob_client.upload_blob(
                data=file_content,
                content_type=content_type,
                overwrite=True,
                metadata={
                    'original_filename': filename,
                    'upload_timestamp': timestamp.isoformat(),
                    'content_type': content_type
                }
            )
            
            photo_url = photo_blob_client.url
            logger.info(f"Uploaded photo to: {photo_blob_path}")
            
            # Generate and upload thumbnails
            thumbnail_urls = {}
            if generate_thumbnails:
                try:
                    thumbnails = await self._generate_thumbnails(file_content)
                    
                    for size, thumbnail_data in thumbnails.items():
                        thumbnail_blob_path = self._get_thumbnail_blob_path(filename, timestamp, size)
                        
                        thumbnail_blob_client = self.blob_service_client.get_blob_client(
                            container=self.container_client.container_name,
                            blob=thumbnail_blob_path
                        )
                        
                        thumbnail_blob_client.upload_blob(
                            data=thumbnail_data,
                            content_type="image/jpeg",
                            overwrite=True,
                            metadata={
                                'original_filename': filename,
                                'thumbnail_size': size,
                                'upload_timestamp': timestamp.isoformat()
                            }
                        )
                        
                        thumbnail_urls[size] = thumbnail_blob_client.url
                        logger.info(f"Uploaded {size} thumbnail to: {thumbnail_blob_path}")
                        
                except Exception as e:
                    logger.error(f"Thumbnail generation failed: {e}")
                    # Continue without thumbnails rather than failing the upload
            
            return {
                "photo_url": photo_url,
                "photo_blob_path": photo_blob_path,
                "thumbnail_urls": thumbnail_urls,
                "upload_success": True
            }
            
        except Exception as e:
            logger.error(f"Photo upload failed: {e}")
            # Cleanup any partially uploaded files
            await self._cleanup_failed_upload(filename, timestamp)
            raise
    
    async def _generate_thumbnails(self, file_content: bytes) -> Dict[str, bytes]:
        """
        Generate thumbnails in multiple sizes
        
        Args:
            file_content: Original image bytes
            
        Returns:
            Dict mapping size names to thumbnail bytes
        """
        thumbnails = {}
        
        try:
            # Open original image
            original_image = Image.open(io.BytesIO(file_content))
            
            # Convert to RGB if necessary (for PNG with transparency, etc.)
            if original_image.mode in ('RGBA', 'LA', 'P'):
                # Create white background
                background = Image.new('RGB', original_image.size, (255, 255, 255))
                if original_image.mode == 'P':
                    original_image = original_image.convert('RGBA')
                background.paste(original_image, mask=original_image.split()[-1] if original_image.mode in ('RGBA', 'LA') else None)
                original_image = background
            elif original_image.mode != 'RGB':
                original_image = original_image.convert('RGB')
            
            # Generate thumbnails for each size
            for size_name, (width, height) in self.thumbnail_sizes.items():
                try:
                    # Create thumbnail
                    thumbnail = original_image.copy()
                    thumbnail.thumbnail((width, height), Image.Resampling.LANCZOS)
                    
                    # Save to bytes
                    thumbnail_io = io.BytesIO()
                    thumbnail.save(
                        thumbnail_io, 
                        format='JPEG', 
                        quality=self.thumbnail_quality,
                        optimize=True
                    )
                    thumbnail_io.seek(0)
                    
                    thumbnails[size_name] = thumbnail_io.getvalue()
                    
                except Exception as e:
                    logger.error(f"Failed to generate {size_name} thumbnail: {e}")
                    continue
            
            return thumbnails
            
        except Exception as e:
            logger.error(f"Thumbnail generation error: {e}")
            return {}
    
    async def delete_photo_and_thumbnails(self, filename: str, timestamp: datetime) -> bool:
        """
        Delete photo and all associated thumbnails
        
        Args:
            filename: Photo filename
            timestamp: Photo timestamp for path generation
            
        Returns:
            True if deletion successful
        """
        try:
            success = True
            
            # Delete main photo
            photo_blob_path = self._get_photo_blob_path(filename, timestamp)
            try:
                photo_blob_client = self.blob_service_client.get_blob_client(
                    container=self.container_client.container_name,
                    blob=photo_blob_path
                )
                photo_blob_client.delete_blob()
                logger.info(f"Deleted photo: {photo_blob_path}")
            except ResourceNotFoundError:
                logger.warning(f"Photo not found for deletion: {photo_blob_path}")
            except Exception as e:
                logger.error(f"Failed to delete photo {photo_blob_path}: {e}")
                success = False
            
            # Delete thumbnails
            for size in self.thumbnail_sizes.keys():
                thumbnail_blob_path = self._get_thumbnail_blob_path(filename, timestamp, size)
                try:
                    thumbnail_blob_client = self.blob_service_client.get_blob_client(
                        container=self.container_client.container_name,
                        blob=thumbnail_blob_path
                    )
                    thumbnail_blob_client.delete_blob()
                    logger.info(f"Deleted thumbnail: {thumbnail_blob_path}")
                except ResourceNotFoundError:
                    logger.warning(f"Thumbnail not found for deletion: {thumbnail_blob_path}")
                except Exception as e:
                    logger.error(f"Failed to delete thumbnail {thumbnail_blob_path}: {e}")
                    success = False
            
            return success
            
        except Exception as e:
            logger.error(f"Photo deletion error: {e}")
            return False
    
    async def _cleanup_failed_upload(self, filename: str, timestamp: datetime):
        """Clean up any files from a failed upload"""
        try:
            await self.delete_photo_and_thumbnails(filename, timestamp)
            logger.info(f"Cleaned up failed upload for {filename}")
        except Exception as e:
            logger.error(f"Failed to cleanup failed upload: {e}")
    
    def generate_download_url(
        self, 
        blob_path: str, 
        expiry_hours: int = 24,
        permissions: str = "r"
    ) -> str:
        """
        Generate a SAS URL for downloading a blob
        
        Args:
            blob_path: Path to the blob
            expiry_hours: Hours until URL expires
            permissions: SAS permissions (r=read, w=write, etc.)
            
        Returns:
            SAS URL string
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_client.container_name,
                blob=blob_path
            )
            
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=blob_client.account_name,
                container_name=blob_client.container_name,
                blob_name=blob_client.blob_name,
                account_key=self.blob_service_client.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
            )
            
            return f"{blob_client.url}?{sas_token}"
            
        except Exception as e:
            logger.error(f"Failed to generate download URL: {e}")
            return blob_client.url  # Return URL without SAS as fallback
    
    async def get_photo_info(self, blob_path: str) -> Optional[Dict[str, Any]]:
        """
        Get photo blob information and metadata
        
        Args:
            blob_path: Path to the photo blob
            
        Returns:
            Dict with blob info or None if not found
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_client.container_name,
                blob=blob_path
            )
            
            properties = blob_client.get_blob_properties()
            
            return {
                "blob_path": blob_path,
                "url": blob_client.url,
                "size": properties.size,
                "content_type": properties.content_settings.content_type,
                "last_modified": properties.last_modified,
                "metadata": properties.metadata,
                "etag": properties.etag
            }
            
        except ResourceNotFoundError:
            logger.warning(f"Photo blob not found: {blob_path}")
            return None
        except Exception as e:
            logger.error(f"Failed to get photo info: {e}")
            return None
    
    async def list_photos_in_folder(self, year: str, month: str) -> list:
        """
        List all photos in a specific year/month folder
        
        Args:
            year: Year (YYYY)
            month: Month (MM)
            
        Returns:
            List of blob names in the folder
        """
        try:
            folder_prefix = f"uploads/{year}/{month}/"
            
            blob_list = []
            for blob in self.container_client.list_blobs(name_starts_with=folder_prefix):
                # Only include actual photos, not thumbnails
                if not "/thumbnails/" in blob.name:
                    blob_list.append(blob.name)
            
            return blob_list
            
        except Exception as e:
            logger.error(f"Failed to list photos in folder {year}/{month}: {e}")
            return []
    
    async def get_storage_usage(self) -> Dict[str, Any]:
        """
        Get storage usage statistics
        
        Returns:
            Dict with storage usage info
        """
        try:
            total_size = 0
            photo_count = 0
            thumbnail_count = 0
            
            for blob in self.container_client.list_blobs():
                total_size += blob.size
                
                if blob.name.startswith("uploads/") and "/thumbnails/" not in blob.name:
                    photo_count += 1
                elif "/thumbnails/" in blob.name:
                    thumbnail_count += 1
            
            return {
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "photo_count": photo_count,
                "thumbnail_count": thumbnail_count,
                "container_name": self.container_client.container_name
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage usage: {e}")
            return {
                "total_size_bytes": 0,
                "total_size_mb": 0,
                "photo_count": 0,
                "thumbnail_count": 0,
                "error": str(e)
            }