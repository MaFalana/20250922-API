"""
Database service abstraction layer for photo management
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime

from models.photo import Photo


class PhotoFilters:
    """Filter criteria for photo queries"""
    def __init__(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        tags: Optional[List[str]] = None,
        min_lat: Optional[float] = None,
        max_lat: Optional[float] = None,
        min_lng: Optional[float] = None,
        max_lng: Optional[float] = None,
        uploader_id: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.tags = tags or []
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_lng = min_lng
        self.max_lng = max_lng
        self.uploader_id = uploader_id
        self.limit = limit
        self.offset = offset


class DatabaseService(ABC):
    """Abstract base class for photo database operations"""
    
    @abstractmethod
    async def create_photo(self, photo: Photo) -> str:
        """Create a new photo record and return the photo ID"""
        pass
    
    @abstractmethod
    async def get_photo(self, photo_id: str) -> Optional[Photo]:
        """Get a photo by ID"""
        pass
    
    @abstractmethod
    async def get_photos(self, filters: PhotoFilters) -> List[Photo]:
        """Get photos with optional filtering"""
        pass
    
    @abstractmethod
    async def update_photo(self, photo_id: str, updates: Dict[str, Any]) -> bool:
        """Update photo metadata. Returns True if successful"""
        pass
    
    @abstractmethod
    async def delete_photo(self, photo_id: str) -> bool:
        """Delete a photo record. Returns True if successful"""
        pass
    
    @abstractmethod
    async def get_photos_by_hash(self, hash_md5: str) -> List[Photo]:
        """Get photos by file hash for duplicate detection"""
        pass
    
    @abstractmethod
    async def get_photos_in_bounds(
        self, 
        min_lat: float, 
        max_lat: float, 
        min_lng: float, 
        max_lng: float
    ) -> List[Photo]:
        """Get photos within geographic bounds"""
        pass
    
    @abstractmethod
    async def count_photos(self, filters: Optional[PhotoFilters] = None) -> int:
        """Count photos matching filters"""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check database connectivity"""
        pass