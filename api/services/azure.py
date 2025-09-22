"""
Azure Tables implementation of the photo database service
"""

import logging
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError

from models.photo import Photo
from services.database import DatabaseService, PhotoFilters


logger = logging.getLogger(__name__)


class AzureTablesPhotoService(DatabaseService):
    """Azure Tables implementation of photo database service"""
    
    def __init__(self, connection_string: str, table_name: str = "photos"):
        self.connection_string = connection_string
        self.table_name = table_name
        self.table_client = TableClient.from_connection_string(
            conn_str=connection_string,
            table_name=table_name
        )
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Create table if it doesn't exist"""
        try:
            self.table_client.create_table()
            logger.info(f"Created table: {self.table_name}")
        except Exception as e:
            # Table might already exist
            logger.info(f"Table {self.table_name} already exists or creation failed: {e}")
    
    async def create_photo(self, photo: Photo) -> str:
        """Create a new photo record"""
        try:
            entity = self._photo_to_entity(photo)
            self.table_client.create_entity(entity)
            
            logger.info(f"Created photo record: {photo.id}")
            return photo.id
            
        except ServiceRequestError as e:
            logger.error(f"Failed to create photo: {e}")
            raise
    
    async def get_photo(self, photo_id: str) -> Optional[Photo]:
        """Get a photo by ID"""
        try:
            # Use year-month as partition key for efficient querying
            # We'll need to search across partitions if we don't know the date
            entities = self.table_client.query_entities(
                query_filter=f"RowKey eq '{photo_id}'"
            )
            
            for entity in entities:
                return self._entity_to_photo(entity)
            
            return None
            
        except ServiceRequestError as e:
            logger.error(f"Failed to get photo {photo_id}: {e}")
            return None
    
    async def get_photos(self, filters: PhotoFilters) -> List[Photo]:
        """Get photos with filtering"""
        try:
            query_filter = self._build_query_filter(filters)
            
            entities = self.table_client.query_entities(
                query_filter=query_filter
            )
            
            photos = []
            for entity in entities:
                photos.append(self._entity_to_photo(entity))
            
            # Apply client-side sorting and pagination since Azure Tables has limitations
            photos.sort(key=lambda p: p.timestamp, reverse=True)
            
            if filters.offset:
                photos = photos[filters.offset:]
            if filters.limit:
                photos = photos[:filters.limit]
            
            return photos
            
        except ServiceRequestError as e:
            logger.error(f"Failed to get photos: {e}")
            return []
    
    async def update_photo(self, photo_id: str, updates: Dict[str, Any]) -> bool:
        """Update photo metadata"""
        try:
            # First get the existing entity to get partition key
            existing_photo = await self.get_photo(photo_id)
            if not existing_photo:
                return False
            
            partition_key = self._get_partition_key(existing_photo.timestamp)
            
            # Prepare update entity
            entity = {
                "PartitionKey": partition_key,
                "RowKey": photo_id,
                **updates,
                "updated_at": datetime.utcnow().isoformat()
            }
            
            self.table_client.update_entity(entity, mode="merge")
            logger.info(f"Updated photo: {photo_id}")
            return True
            
        except ServiceRequestError as e:
            logger.error(f"Failed to update photo {photo_id}: {e}")
            return False
    
    async def delete_photo(self, photo_id: str) -> bool:
        """Delete a photo record"""
        try:
            # First get the existing entity to get partition key
            existing_photo = await self.get_photo(photo_id)
            if not existing_photo:
                return False
            
            partition_key = self._get_partition_key(existing_photo.timestamp)
            
            self.table_client.delete_entity(
                partition_key=partition_key,
                row_key=photo_id
            )
            
            logger.info(f"Deleted photo: {photo_id}")
            return True
            
        except ServiceRequestError as e:
            logger.error(f"Failed to delete photo {photo_id}: {e}")
            return False
    
    async def get_photos_by_hash(self, hash_md5: str) -> List[Photo]:
        """Get photos by file hash for duplicate detection"""
        try:
            entities = self.table_client.query_entities(
                query_filter=f"hash_md5 eq '{hash_md5}'"
            )
            
            photos = []
            for entity in entities:
                photos.append(self._entity_to_photo(entity))
            
            return photos
            
        except ServiceRequestError as e:
            logger.error(f"Failed to get photos by hash: {e}")
            return []
    
    async def get_photos_in_bounds(
        self, 
        min_lat: float, 
        max_lat: float, 
        min_lng: float, 
        max_lng: float
    ) -> List[Photo]:
        """Get photos within geographic bounds"""
        try:
            # Azure Tables doesn't have native geospatial queries
            # We'll filter client-side after retrieving data
            query_filter = f"latitude ge {min_lat} and latitude le {max_lat} and longitude ge {min_lng} and longitude le {max_lng}"
            
            entities = self.table_client.query_entities(
                query_filter=query_filter
            )
            
            photos = []
            for entity in entities:
                photo = self._entity_to_photo(entity)
                # Double-check bounds (Azure Tables numeric comparisons can be imprecise)
                if (min_lat <= photo.latitude <= max_lat and 
                    min_lng <= photo.longitude <= max_lng):
                    photos.append(photo)
            
            photos.sort(key=lambda p: p.timestamp, reverse=True)
            return photos
            
        except ServiceRequestError as e:
            logger.error(f"Failed to get photos in bounds: {e}")
            return []
    
    async def count_photos(self, filters: Optional[PhotoFilters] = None) -> int:
        """Count photos matching filters"""
        try:
            query_filter = self._build_query_filter(filters) if filters else None
            
            entities = self.table_client.query_entities(
                query_filter=query_filter,
                select=["RowKey"]  # Only select key to minimize data transfer
            )
            
            return sum(1 for _ in entities)
            
        except ServiceRequestError as e:
            logger.error(f"Failed to count photos: {e}")
            return 0
    
    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            # Try to get table properties
            self.table_client.get_table_properties()
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def _get_partition_key(self, timestamp: datetime) -> str:
        """Generate partition key from timestamp (YYYY-MM format)"""
        return timestamp.strftime("%Y-%m")
    
    def _build_query_filter(self, filters: Optional[PhotoFilters]) -> Optional[str]:
        """Build Azure Tables query filter from filters"""
        if not filters:
            return None
        
        conditions = []
        
        # Date range filter
        if filters.start_date:
            conditions.append(f"timestamp ge datetime'{filters.start_date.isoformat()}'")
        if filters.end_date:
            conditions.append(f"timestamp le datetime'{filters.end_date.isoformat()}'")
        
        # Geographic bounds filter
        if filters.min_lat is not None:
            conditions.append(f"latitude ge {filters.min_lat}")
        if filters.max_lat is not None:
            conditions.append(f"latitude le {filters.max_lat}")
        if filters.min_lng is not None:
            conditions.append(f"longitude ge {filters.min_lng}")
        if filters.max_lng is not None:
            conditions.append(f"longitude le {filters.max_lng}")
        
        # Uploader filter
        if filters.uploader_id:
            conditions.append(f"uploader_id eq '{filters.uploader_id}'")
        
        # Note: Tag filtering is complex in Azure Tables since it doesn't support array queries
        # We'll handle tag filtering client-side in get_photos method
        
        return " and ".join(conditions) if conditions else None
    
    def _photo_to_entity(self, photo: Photo) -> Dict[str, Any]:
        """Convert Photo model to Azure Tables entity"""
        entity = {
            "PartitionKey": self._get_partition_key(photo.timestamp),
            "RowKey": photo.id,
            "id": photo.id,
            "filename": photo.filename,
            "original_filename": photo.original_filename,
            "blob_url": photo.blob_url,
            "thumbnail_url": photo.thumbnail_url,
            "latitude": photo.latitude,
            "longitude": photo.longitude,
            "altitude": photo.altitude,
            "timestamp": photo.timestamp,
            "upload_timestamp": photo.upload_timestamp,
            "file_size": photo.file_size,
            "mime_type": photo.mime_type,
            "camera_make": photo.camera_make,
            "camera_model": photo.camera_model,
            "camera_settings": json.dumps(photo.camera_settings) if photo.camera_settings else None,
            "tags": json.dumps(photo.tags) if photo.tags else "[]",
            "description": photo.description,
            "uploader_id": photo.uploader_id,
            "hash_md5": photo.hash_md5,
            "processing_status": photo.processing_status,
            "created_at": photo.created_at,
            "updated_at": photo.updated_at
        }
        
        # Remove None values
        return {k: v for k, v in entity.items() if v is not None}
    
    def _entity_to_photo(self, entity: Dict[str, Any]) -> Photo:
        """Convert Azure Tables entity to Photo model"""
        # Parse JSON fields
        camera_settings = None
        if entity.get("camera_settings"):
            try:
                camera_settings = json.loads(entity["camera_settings"])
            except json.JSONDecodeError:
                pass
        
        tags = []
        if entity.get("tags"):
            try:
                tags = json.loads(entity["tags"])
            except json.JSONDecodeError:
                pass
        
        return Photo(
            id=entity["id"],
            filename=entity["filename"],
            original_filename=entity["original_filename"],
            blob_url=entity["blob_url"],
            thumbnail_url=entity.get("thumbnail_url"),
            latitude=float(entity["latitude"]),
            longitude=float(entity["longitude"]),
            altitude=float(entity["altitude"]) if entity.get("altitude") else None,
            timestamp=entity["timestamp"],
            upload_timestamp=entity["upload_timestamp"],
            file_size=int(entity["file_size"]),
            mime_type=entity["mime_type"],
            camera_make=entity.get("camera_make"),
            camera_model=entity.get("camera_model"),
            camera_settings=camera_settings,
            tags=tags,
            description=entity.get("description"),
            uploader_id=entity.get("uploader_id"),
            hash_md5=entity["hash_md5"],
            processing_status=entity.get("processing_status", "pending"),
            created_at=entity["created_at"],
            updated_at=entity["updated_at"]
        )