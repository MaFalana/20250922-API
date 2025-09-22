"""
Export job models for KML, KMZ, and ZIP generation
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from enum import Enum
import uuid

from models.photo import JobStatus


class CoordinateSystem(str, Enum):
    """Coordinate systems for photo mapping"""
    WGS84 = "WGS84"  # GPS coordinates - universal compatibility


class ExportFormat(str, Enum):
    """Export format types"""
    KML = "kml"
    KMZ = "kmz"
    ZIP = "zip"
    PHOTOS_ONLY = "photos"


class ExportJob(BaseModel):
    """Export job model with civil engineering specific fields"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    export_type: ExportFormat
    photo_ids: List[str]
    status: JobStatus = JobStatus.PENDING
    
    # Export options
    coordinate_system: CoordinateSystem = CoordinateSystem.WGS84  # Default and recommended for photo mapping
    include_altitude: bool = True
    include_photos_in_kmz: bool = True  # Embed photos in KMZ for Google Earth
    include_thumbnails: bool = True
    
    # File management (temporary files, not persistent storage)
    output_filename: Optional[str] = None
    file_path: Optional[str] = None  # Local temporary file path
    file_size: Optional[int] = None
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    
    # Progress and error tracking
    progress: float = 0.0  # 0-100
    total_photos: int = 0
    processed_photos: int = 0
    error_message: Optional[str] = None
    
    # Metadata
    requester_id: Optional[str] = None
    export_metadata: Optional[Dict[str, Any]] = None
    
    def __init__(self, **data):
        super().__init__(**data)
        # Set total photos count
        if self.photo_ids:
            self.total_photos = len(self.photo_ids)
        # Set default expiration (24 hours from creation)
        if not self.expires_at:
            self.expires_at = self.created_at + timedelta(hours=24)
        # Set default filename
        if not self.output_filename:
            timestamp = self.created_at.strftime("%Y%m%d_%H%M%S")
            self.output_filename = f"export_{timestamp}.{self.export_type.value}"
    
    def update_progress(self, processed_count: int):
        """Update progress based on processed photo count"""
        self.processed_photos = processed_count
        if self.total_photos > 0:
            self.progress = (processed_count / self.total_photos) * 100
        self.updated_at = datetime.utcnow()
    
    def mark_started(self):
        """Mark job as started"""
        self.status = JobStatus.PROCESSING
        self.started_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def mark_completed(self, file_path: str, file_size: int):
        """Mark job as completed with file info"""
        self.status = JobStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.file_path = file_path
        self.file_size = file_size
        self.progress = 100.0
    
    def mark_failed(self, error_message: str):
        """Mark job as failed with error message"""
        self.status = JobStatus.FAILED
        self.error_message = error_message
        self.updated_at = datetime.utcnow()
    
    def is_expired(self) -> bool:
        """Check if export has expired"""
        return self.expires_at and datetime.utcnow() > self.expires_at
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "export_type": "kmz",
                "photo_ids": ["photo1", "photo2", "photo3"],
                "coordinate_system": "WGS84",
                "include_altitude": True,
                "include_photos_in_kmz": True,
                "status": "pending",
                "total_photos": 3,
                "processed_photos": 0,
                "progress": 0.0
            }
        }


class ExportRequest(BaseModel):
    """Request model for creating export jobs"""
    photo_ids: List[str] = Field(..., description="List of photo IDs to export")
    export_type: ExportFormat = Field(..., description="Export format")
    coordinate_system: CoordinateSystem = Field(CoordinateSystem.WGS84, description="Coordinate system")
    include_altitude: bool = Field(True, description="Include altitude data")
    include_photos_in_kmz: bool = Field(True, description="Embed photos in KMZ files")
    include_thumbnails: bool = Field(True, description="Include thumbnail images")
    requester_id: Optional[str] = Field(None, description="ID of requesting user")
    
    class Config:
        json_schema_extra = {
            "example": {
                "photo_ids": ["photo1", "photo2", "photo3"],
                "export_type": "kmz",
                "coordinate_system": "WGS84",
                "include_altitude": True,
                "include_photos_in_kmz": True,
                "include_thumbnails": True
            }
        }


class ExportResponse(BaseModel):
    """Response model for export job creation"""
    job_id: str = Field(..., description="Export job ID")
    status: JobStatus = Field(..., description="Initial job status")
    message: str = Field(..., description="Status message")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "queued",
                "message": "Export job created successfully. Processing will begin shortly.",
                "estimated_completion": "2024-01-01T12:05:00Z"
            }
        }


class ExportStatusResponse(BaseModel):
    """Response model for export job status"""
    job_id: str = Field(..., description="Export job ID")
    status: JobStatus = Field(..., description="Current job status")
    progress: float = Field(..., description="Progress percentage (0-100)")
    total_photos: int = Field(..., description="Total photos to process")
    processed_photos: int = Field(..., description="Photos processed so far")
    created_at: datetime = Field(..., description="Job creation time")
    updated_at: datetime = Field(..., description="Last update time")
    started_at: Optional[datetime] = Field(None, description="Processing start time")
    completed_at: Optional[datetime] = Field(None, description="Completion time")
    expires_at: Optional[datetime] = Field(None, description="Download expiration time")
    download_url: Optional[str] = Field(None, description="Download URL when ready")
    file_size: Optional[int] = Field(None, description="File size in bytes")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "processing",
                "progress": 65.0,
                "total_photos": 10,
                "processed_photos": 6,
                "created_at": "2024-01-01T12:00:00Z",
                "updated_at": "2024-01-01T12:03:00Z",
                "started_at": "2024-01-01T12:01:00Z"
            }
        }


class DownloadResponse(BaseModel):
    """Response model for export download"""
    job_id: str = Field(..., description="Export job ID")
    download_url: str = Field(..., description="Download URL")
    filename: str = Field(..., description="Export filename")
    file_size: int = Field(..., description="File size in bytes")
    expires_at: datetime = Field(..., description="URL expiration time")
    content_type: str = Field(..., description="MIME type")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "download_url": "https://storage.blob.core.windows.net/exports/...",
                "filename": "export_20240101_120000.kmz",
                "file_size": 2048576,
                "expires_at": "2024-01-02T12:00:00Z",
                "content_type": "application/vnd.google-earth.kmz"
            }
        }