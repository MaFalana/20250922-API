"""
MongoDB implementation of the photo database service
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from pymongo import GEOSPHERE, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from db.mongo import AzureMongoManager
from models.photo import Photo
from services.database import DatabaseService, PhotoFilters


logger = logging.getLogger(__name__)


class MongoPhotoService(DatabaseService):
    """MongoDB implementation of photo database service"""
    
    def __init__(self, db_name: str = "photo_mapper"):
        self.mongo_manager = AzureMongoManager(db_name)
        self.collection_name = "photos"
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create necessary indexes for efficient querying"""
        try:
            collection = self.mongo_manager.db[self.collection_name]
            
            # Geospatial index for coordinate-based queries
            collection.create_index([("location", GEOSPHERE)])
            
            # Compound index for common queries
            collection.create_index([
                ("timestamp", DESCENDING),
                ("uploader_id", ASCENDING)
            ])
            
            # Index for duplicate detection
            collection.create_index("hash_md5")
            
            # Index for tag filtering
            collection.create_index("tags")
            
            logger.info("Database indexes created successfully")
            
        except PyMongoError as e:
            logger.error(f"Failed to create indexes: {e}")
    
    async def create_photo(self, photo: Photo) -> str:
        """Create a new photo record"""
        try:
            # Convert photo to MongoDB document format
            doc = self._photo_to_document(photo)
            
            collection = self.mongo_manager.db[self.collection_name]
            result = collection.insert_one(doc)
            
            logger.info(f"Created photo record: {photo.id}")
            return photo.id
            
        except PyMongoError as e:
            logger.error(f"Failed to create photo: {e}")
            raise
    
    async def get_photo(self, photo_id: str) -> Optional[Photo]:
        """Get a photo by ID"""
        try:
            result = self.mongo_manager.query(self.collection_name, {"id": photo_id})
            
            if result:
                return self._document_to_photo(result)
            return None
            
        except PyMongoError as e:
            logger.error(f"Failed to get photo {photo_id}: {e}")
            return None
    
    async def get_photos(self, filters: PhotoFilters) -> List[Photo]:
        """Get photos with filtering"""
        try:
            query = self._build_query(filters)
            collection = self.mongo_manager.db[self.collection_name]
            
            cursor = collection.find(query)
            
            # Apply sorting
            cursor = cursor.sort("timestamp", DESCENDING)
            
            # Apply pagination
            if filters.offset:
                cursor = cursor.skip(filters.offset)
            if filters.limit:
                cursor = cursor.limit(filters.limit)
            
            photos = []
            for doc in cursor:
                photos.append(self._document_to_photo(doc))
            
            return photos
            
        except PyMongoError as e:
            logger.error(f"Failed to get photos: {e}")
            return []
    
    async def update_photo(self, photo_id: str, updates: Dict[str, Any]) -> bool:
        """Update photo metadata"""
        try:
            # Add updated timestamp
            updates["updated_at"] = datetime.utcnow()
            
            collection = self.mongo_manager.db[self.collection_name]
            result = collection.update_one(
                {"id": photo_id},
                {"$set": updates}
            )
            
            success = result.modified_count > 0
            if success:
                logger.info(f"Updated photo: {photo_id}")
            
            return success
            
        except PyMongoError as e:
            logger.error(f"Failed to update photo {photo_id}: {e}")
            return False
    
    async def delete_photo(self, photo_id: str) -> bool:
        """Delete a photo record"""
        try:
            collection = self.mongo_manager.db[self.collection_name]
            result = collection.delete_one({"id": photo_id})
            
            success = result.deleted_count > 0
            if success:
                logger.info(f"Deleted photo: {photo_id}")
            
            return success
            
        except PyMongoError as e:
            logger.error(f"Failed to delete photo {photo_id}: {e}")
            return False
    
    async def get_photos_by_hash(self, hash_md5: str) -> List[Photo]:
        """Get photos by file hash for duplicate detection"""
        try:
            collection = self.mongo_manager.db[self.collection_name]
            cursor = collection.find({"hash_md5": hash_md5})
            
            photos = []
            for doc in cursor:
                photos.append(self._document_to_photo(doc))
            
            return photos
            
        except PyMongoError as e:
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
            # Use MongoDB geospatial query
            query = {
                "location": {
                    "$geoWithin": {
                        "$box": [
                            [min_lng, min_lat],  # Bottom left
                            [max_lng, max_lat]   # Top right
                        ]
                    }
                }
            }
            
            collection = self.mongo_manager.db[self.collection_name]
            cursor = collection.find(query).sort("timestamp", DESCENDING)
            
            photos = []
            for doc in cursor:
                photos.append(self._document_to_photo(doc))
            
            return photos
            
        except PyMongoError as e:
            logger.error(f"Failed to get photos in bounds: {e}")
            return []
    
    async def count_photos(self, filters: Optional[PhotoFilters] = None) -> int:
        """Count photos matching filters"""
        try:
            query = self._build_query(filters) if filters else {}
            collection = self.mongo_manager.db[self.collection_name]
            
            return collection.count_documents(query)
            
        except PyMongoError as e:
            logger.error(f"Failed to count photos: {e}")
            return 0
    
    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            # Simple ping to check connection
            self.mongo_manager.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def _build_query(self, filters: Optional[PhotoFilters]) -> Dict[str, Any]:
        """Build MongoDB query from filters"""
        if not filters:
            return {}
        
        query = {}
        
        # Date range filter
        if filters.start_date or filters.end_date:
            date_filter = {}
            if filters.start_date:
                date_filter["$gte"] = filters.start_date
            if filters.end_date:
                date_filter["$lte"] = filters.end_date
            query["timestamp"] = date_filter
        
        # Tag filter
        if filters.tags:
            query["tags"] = {"$in": filters.tags}
        
        # Geographic bounds filter
        if all([filters.min_lat, filters.max_lat, filters.min_lng, filters.max_lng]):
            query["location"] = {
                "$geoWithin": {
                    "$box": [
                        [filters.min_lng, filters.min_lat],
                        [filters.max_lng, filters.max_lat]
                    ]
                }
            }
        
        # Uploader filter
        if filters.uploader_id:
            query["uploader_id"] = filters.uploader_id
        
        return query
    
    def _photo_to_document(self, photo: Photo) -> Dict[str, Any]:
        """Convert Photo model to MongoDB document"""
        doc = photo.dict()
        
        # Create GeoJSON point for geospatial indexing
        doc["location"] = {
            "type": "Point",
            "coordinates": [photo.longitude, photo.latitude]
        }
        
        return doc
    
    def _document_to_photo(self, doc: Dict[str, Any]) -> Photo:
        """Convert MongoDB document to Photo model"""
        # Remove MongoDB-specific fields
        doc.pop("_id", None)
        doc.pop("location", None)  # Remove GeoJSON, we have lat/lng
        
        return Photo(**doc)