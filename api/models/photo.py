from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum
import uuid


class JobStatus(str, Enum):
    """Job status enumeration"""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Photo(BaseModel):
    """Photo model with geographic and EXIF metadata"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    filename: str
    original_filename: str
    blob_url: str
    thumbnail_urls: Optional[Dict[str, str]] = None  # URLs for small, medium, large thumbnails
    thumbnail_url: Optional[str] = None  # Backward compatibility - medium thumbnail
    
    # Geographic data
    latitude: float
    longitude: float
    altitude: Optional[float] = None  # Preserved from EXIF when available
    
    # Metadata
    timestamp: datetime
    upload_timestamp: datetime = Field(default_factory=datetime.utcnow)
    file_size: int
    mime_type: str
    
    # EXIF data
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    camera_settings: Optional[Dict[str, Any]] = None
    
    # User data
    tags: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    uploader_id: Optional[str] = None
    
    # System fields
    hash_md5: str
    processing_status: str = "pending"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "filename": "survey_point_001.jpg",
                "original_filename": "IMG_001.jpg",
                "blob_url": "https://storage.blob.core.windows.net/photos/survey_point_001.jpg",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "altitude": 10.5,
                "timestamp": "2024-01-01T12:00:00Z",
                "file_size": 2048576,
                "mime_type": "image/jpeg",
                "camera_make": "Canon",
                "camera_model": "EOS R5",
                "tags": ["survey", "site-1"],
                "hash_md5": "d41d8cd98f00b204e9800998ecf8427e",
                "processing_status": "completed"
            }
        }


class UploadResponse(BaseModel):
    """Response model for file upload"""
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Initial job status")
    message: str = Field(..., description="Status message")
    files_uploaded: int = Field(..., description="Number of files uploaded")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "queued",
                "message": "Files uploaded successfully. Processing will begin shortly.",
                "files_uploaded": 2
            }
        }


class JobStatusResponse(BaseModel):
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Current job status")
    created_at: datetime = Field(..., description="Job creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")
    progress: Optional[float] = Field(None, description="Progress percentage (0-100)")
    input_files: List[str] = Field(..., description="Input file names")
    output_files: List[str] = Field(default_factory=list, description="Output file names")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "completed",
                "created_at": "2024-01-01T12:00:00Z",
                "updated_at": "2024-01-01T12:05:00Z",
                "completed_at": "2024-01-01T12:05:00Z",
                "progress": 100.0,
                "input_files": ["terrain.las"],
                "output_files": ["terrain.dxf", "terrain.csv"]
            }
        }


class DownloadResponse(BaseModel):
    """Response model for file download"""
    job_id: str = Field(..., description="Job identifier")
    download_urls: Dict[str, str] = Field(..., description="Map of filename to download URL")
    expires_at: datetime = Field(..., description="URL expiration time")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "download_urls": {
                    "terrain.dxf": "https://storage.blob.core.windows.net/...",
                    "terrain.csv": "https://storage.blob.core.windows.net/..."
                },
                "expires_at": "2024-01-01T13:00:00Z"
            }
        }