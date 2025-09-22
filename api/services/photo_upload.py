"""
Photo upload service with EXIF data extraction and file processing
"""

import logging
import hashlib
import io
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
import exifread

from models.photo import Photo


logger = logging.getLogger(__name__)


class PhotoUploadService:
    """Service for handling photo uploads with EXIF processing"""
    
    def __init__(self):
        self.supported_formats = {
            'image/jpeg': ['.jpg', '.jpeg'],
            'image/png': ['.png'],
            'image/tiff': ['.tiff', '.tif'],
            'image/heic': ['.heic', '.heif'],
            'application/octet-stream': ['.heic', '.heif']  # HEIC files often detected as octet-stream
        }
        self.max_file_size = 50 * 1024 * 1024  # 50MB
    
    async def validate_file(self, file_content: bytes, filename: str, content_type: str) -> Dict[str, Any]:
        """
        Validate uploaded file for type, size, and format
        
        Args:
            file_content: Raw file bytes
            filename: Original filename
            content_type: MIME type
            
        Returns:
            Dict with validation results
            
        Raises:
            ValueError: If file validation fails
        """
        try:
            # Check file size
            if len(file_content) > self.max_file_size:
                raise ValueError(f"File size {len(file_content)} bytes exceeds maximum {self.max_file_size} bytes")
            
            # Check content type
            if content_type not in self.supported_formats:
                raise ValueError(f"Unsupported file type: {content_type}")
            
            # Verify file extension matches content type
            file_ext = filename.lower().split('.')[-1] if '.' in filename else ''
            expected_extensions = self.supported_formats[content_type]
            if not any(file_ext == ext.lstrip('.') for ext in expected_extensions):
                logger.warning(f"File extension .{file_ext} doesn't match content type {content_type}")
            
            # Try to open image to verify it's valid
            try:
                image = Image.open(io.BytesIO(file_content))
                image.verify()  # Verify image integrity
                
                # Get basic image info
                image = Image.open(io.BytesIO(file_content))  # Reopen after verify
                width, height = image.size
                format_name = image.format
                
            except Exception as e:
                raise ValueError(f"Invalid or corrupted image file: {str(e)}")
            
            return {
                "valid": True,
                "file_size": len(file_content),
                "dimensions": {"width": width, "height": height},
                "format": format_name,
                "content_type": content_type
            }
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"File validation error: {e}")
            raise ValueError(f"File validation failed: {str(e)}")
    
    async def calculate_file_hash(self, file_content: bytes) -> str:
        """
        Calculate MD5 hash of file content for duplicate detection
        
        Args:
            file_content: Raw file bytes
            
        Returns:
            MD5 hash as hex string
        """
        return hashlib.md5(file_content).hexdigest()
    
    async def extract_exif_data(self, file_content: bytes) -> Dict[str, Any]:
        """
        Extract EXIF data from image file content using both PIL and exifread
        
        Args:
            file_content: Raw file bytes
            
        Returns:
            Dict containing extracted EXIF data
        """
        try:
            exif_data = {}
            
            # Try PIL first (works well for JPEG)
            pil_data = await self._extract_exif_pil(file_content)
            exif_data.update(pil_data)
            
            # Try exifread as fallback/supplement (better for some formats)
            if not exif_data.get('latitude') or not exif_data.get('longitude'):
                exifread_data = await self._extract_exif_exifread(file_content)
                # Only use exifread GPS data if PIL didn't find it
                if not exif_data.get('latitude') and exifread_data.get('latitude'):
                    exif_data.update(exifread_data)
            
            return exif_data
            
        except Exception as e:
            logger.error(f"EXIF extraction error: {e}")
            return {}
    
    async def _extract_exif_pil(self, file_content: bytes) -> Dict[str, Any]:
        """Extract EXIF data using PIL"""
        try:
            image = Image.open(io.BytesIO(file_content))
            
            # Get EXIF data
            exif_dict = {}
            if hasattr(image, '_getexif') and image._getexif():
                exif_data = image._getexif()
                for tag_id, value in exif_data.items():
                    tag = TAGS.get(tag_id, tag_id)
                    exif_dict[tag] = value
            
            # Extract GPS coordinates
            gps_data = {}
            if 'GPSInfo' in exif_dict:
                gps_info = exif_dict['GPSInfo']
                for key in gps_info.keys():
                    name = GPSTAGS.get(key, key)
                    gps_data[name] = gps_info[key]
            
            # Convert GPS coordinates to decimal degrees
            latitude, longitude, altitude = self._parse_gps_coordinates(gps_data)
            
            # Extract camera information
            camera_make = exif_dict.get('Make', '').strip() or None
            camera_model = exif_dict.get('Model', '').strip() or None
            
            # Extract camera settings
            camera_settings = self._extract_camera_settings(exif_dict)
            
            # Extract datetime taken
            datetime_taken = self._extract_datetime(exif_dict)
            
            return {
                'latitude': latitude,
                'longitude': longitude,
                'altitude': altitude,
                'camera_make': camera_make,
                'camera_model': camera_model,
                'camera_settings': camera_settings,
                'datetime_taken': datetime_taken
            }
            
        except Exception as e:
            logger.error(f"PIL EXIF extraction error: {e}")
            return {}
    
    async def _extract_exif_exifread(self, file_content: bytes) -> Dict[str, Any]:
        """Extract EXIF data using exifread library (better for some formats)"""
        try:
            # Reset file pointer
            file_obj = io.BytesIO(file_content)
            
            # Extract EXIF tags
            tags = exifread.process_file(file_obj, details=False)
            
            # Extract GPS coordinates
            latitude = None
            longitude = None
            altitude = None
            
            if 'GPS GPSLatitude' in tags and 'GPS GPSLatitudeRef' in tags:
                lat_deg = self._convert_to_degrees(tags['GPS GPSLatitude'])
                if lat_deg and tags['GPS GPSLatitudeRef'].values == 'S':
                    lat_deg = -lat_deg
                latitude = lat_deg
            
            if 'GPS GPSLongitude' in tags and 'GPS GPSLongitudeRef' in tags:
                lon_deg = self._convert_to_degrees(tags['GPS GPSLongitude'])
                if lon_deg and tags['GPS GPSLongitudeRef'].values == 'W':
                    lon_deg = -lon_deg
                longitude = lon_deg
            
            if 'GPS GPSAltitude' in tags:
                altitude_val = tags['GPS GPSAltitude'].values[0]
                if hasattr(altitude_val, 'num') and hasattr(altitude_val, 'den'):
                    altitude = float(altitude_val.num) / float(altitude_val.den)
                    if 'GPS GPSAltitudeRef' in tags and tags['GPS GPSAltitudeRef'].values[0] == 1:
                        altitude = -altitude
            
            # Extract camera info
            camera_make = str(tags.get('Image Make', '')).strip() or None
            camera_model = str(tags.get('Image Model', '')).strip() or None
            
            # Extract datetime
            datetime_taken = None
            if 'EXIF DateTimeOriginal' in tags:
                try:
                    datetime_taken = datetime.strptime(str(tags['EXIF DateTimeOriginal']), '%Y:%m:%d %H:%M:%S')
                except ValueError:
                    pass
            elif 'Image DateTime' in tags:
                try:
                    datetime_taken = datetime.strptime(str(tags['Image DateTime']), '%Y:%m:%d %H:%M:%S')
                except ValueError:
                    pass
            
            return {
                'latitude': latitude,
                'longitude': longitude,
                'altitude': altitude,
                'camera_make': camera_make,
                'camera_model': camera_model,
                'datetime_taken': datetime_taken
            }
            
        except Exception as e:
            logger.error(f"exifread EXIF extraction error: {e}")
            return {}
    
    def _parse_gps_coordinates(self, gps_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Parse GPS coordinates from EXIF GPS data"""
        try:
            latitude = None
            longitude = None
            altitude = None
            
            # Parse latitude
            if 'GPSLatitude' in gps_data and 'GPSLatitudeRef' in gps_data:
                lat_deg, lat_min, lat_sec = gps_data['GPSLatitude']
                latitude = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                if gps_data['GPSLatitudeRef'] == 'S':
                    latitude = -latitude
            
            # Parse longitude
            if 'GPSLongitude' in gps_data and 'GPSLongitudeRef' in gps_data:
                lon_deg, lon_min, lon_sec = gps_data['GPSLongitude']
                longitude = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                if gps_data['GPSLongitudeRef'] == 'W':
                    longitude = -longitude
            
            # Parse altitude
            if 'GPSAltitude' in gps_data:
                altitude = float(gps_data['GPSAltitude'])
                if gps_data.get('GPSAltitudeRef') == 1:  # Below sea level
                    altitude = -altitude
            
            return latitude, longitude, altitude
            
        except Exception as e:
            logger.error(f"GPS coordinate parsing error: {e}")
            return None, None, None
    
    def _convert_to_degrees(self, value):
        """Convert GPS coordinate from exifread format to decimal degrees"""
        try:
            if not value or not value.values:
                return None
            
            d = float(value.values[0].num) / float(value.values[0].den)
            m = float(value.values[1].num) / float(value.values[1].den)
            s = float(value.values[2].num) / float(value.values[2].den)
            
            return d + (m / 60.0) + (s / 3600.0)
        except:
            return None
    
    def _extract_camera_settings(self, exif_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract camera settings from EXIF data"""
        try:
            camera_settings = {}
            
            # F-number (aperture)
            if 'FNumber' in exif_dict:
                f_num = exif_dict['FNumber']
                if hasattr(f_num, 'num') and hasattr(f_num, 'den'):
                    camera_settings['f_number'] = float(f_num.num) / float(f_num.den)
                else:
                    camera_settings['f_number'] = float(f_num)
            
            # Exposure time
            if 'ExposureTime' in exif_dict:
                exp_time = exif_dict['ExposureTime']
                if hasattr(exp_time, 'num') and hasattr(exp_time, 'den'):
                    camera_settings['exposure_time'] = f"{exp_time.num}/{exp_time.den}"
                    camera_settings['exposure_time_decimal'] = float(exp_time.num) / float(exp_time.den)
                else:
                    camera_settings['exposure_time'] = str(exp_time)
            
            # ISO
            if 'ISOSpeedRatings' in exif_dict:
                camera_settings['iso'] = int(exif_dict['ISOSpeedRatings'])
            elif 'PhotographicSensitivity' in exif_dict:
                camera_settings['iso'] = int(exif_dict['PhotographicSensitivity'])
            
            # Focal length
            if 'FocalLength' in exif_dict:
                focal = exif_dict['FocalLength']
                if hasattr(focal, 'num') and hasattr(focal, 'den'):
                    camera_settings['focal_length'] = float(focal.num) / float(focal.den)
                else:
                    camera_settings['focal_length'] = float(focal)
            
            # Flash
            if 'Flash' in exif_dict:
                camera_settings['flash'] = int(exif_dict['Flash'])
            
            # White balance
            if 'WhiteBalance' in exif_dict:
                camera_settings['white_balance'] = int(exif_dict['WhiteBalance'])
            
            return camera_settings if camera_settings else None
            
        except Exception as e:
            logger.error(f"Camera settings extraction error: {e}")
            return None
    
    def _extract_datetime(self, exif_dict: Dict[str, Any]) -> Optional[datetime]:
        """Extract datetime from EXIF data"""
        try:
            # Try DateTimeOriginal first (when photo was taken)
            if 'DateTimeOriginal' in exif_dict:
                return datetime.strptime(exif_dict['DateTimeOriginal'], '%Y:%m:%d %H:%M:%S')
            
            # Fall back to DateTime (when file was modified)
            if 'DateTime' in exif_dict:
                return datetime.strptime(exif_dict['DateTime'], '%Y:%m:%d %H:%M:%S')
            
            return None
            
        except ValueError as e:
            logger.error(f"DateTime parsing error: {e}")
            return None
    
    async def validate_coordinates(self, latitude: float, longitude: float, altitude: Optional[float] = None) -> Dict[str, Any]:
        """
        Validate GPS coordinates
        
        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees
            altitude: Optional altitude in meters
            
        Returns:
            Dict with validation results
            
        Raises:
            ValueError: If coordinates are invalid
        """
        try:
            # Validate latitude range
            if not -90 <= latitude <= 90:
                raise ValueError(f"Latitude {latitude} is out of valid range (-90 to 90)")
            
            # Validate longitude range
            if not -180 <= longitude <= 180:
                raise ValueError(f"Longitude {longitude} is out of valid range (-180 to 180)")
            
            # Validate altitude if provided
            if altitude is not None:
                # Allow reasonable altitude range (Dead Sea to Everest + some margin)
                if not -500 <= altitude <= 10000:
                    logger.warning(f"Altitude {altitude}m is outside typical range (-500 to 10000m)")
            
            return {
                "valid": True,
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude
            }
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Coordinate validation error: {e}")
            raise ValueError(f"Coordinate validation failed: {str(e)}")
    
    async def process_upload(
        self, 
        file_content: bytes, 
        filename: str, 
        content_type: str,
        manual_coordinates: Optional[Dict[str, float]] = None,
        tags: Optional[list] = None,
        description: Optional[str] = None,
        uploader_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process complete photo upload with validation, EXIF extraction, and metadata preparation
        
        Args:
            file_content: Raw file bytes
            filename: Original filename
            content_type: MIME type
            manual_coordinates: Optional manual coordinates dict with lat, lng, alt
            tags: Optional list of tags
            description: Optional description
            uploader_id: Optional uploader ID
            
        Returns:
            Dict with processed photo data ready for storage
            
        Raises:
            ValueError: If processing fails
        """
        try:
            # Validate file
            validation_result = await self.validate_file(file_content, filename, content_type)
            
            # Calculate file hash
            file_hash = await self.calculate_file_hash(file_content)
            
            # Extract EXIF data
            exif_data = await self.extract_exif_data(file_content)
            
            # Determine coordinates (manual override EXIF)
            latitude = None
            longitude = None
            altitude = None
            coordinate_source = "none"
            
            if manual_coordinates:
                latitude = manual_coordinates.get('latitude')
                longitude = manual_coordinates.get('longitude')
                altitude = manual_coordinates.get('altitude')
                coordinate_source = "manual"
            elif exif_data.get('latitude') and exif_data.get('longitude'):
                latitude = exif_data['latitude']
                longitude = exif_data['longitude']
                altitude = exif_data.get('altitude')
                coordinate_source = "exif"
            
            # Validate coordinates if available
            if latitude is not None and longitude is not None:
                await self.validate_coordinates(latitude, longitude, altitude)
            else:
                raise ValueError("No valid GPS coordinates found in EXIF data and no manual coordinates provided")
            
            # Generate unique filename
            timestamp = datetime.utcnow()
            file_extension = filename.split('.')[-1] if '.' in filename else 'jpg'
            unique_filename = f"photo_{timestamp.strftime('%Y%m%d_%H%M%S')}_{file_hash[:8]}.{file_extension}"
            
            # Prepare photo data
            photo_data = {
                "filename": unique_filename,
                "original_filename": filename,
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude,
                "timestamp": exif_data.get('datetime_taken', timestamp),
                "file_size": len(file_content),
                "mime_type": content_type,
                "camera_make": exif_data.get('camera_make'),
                "camera_model": exif_data.get('camera_model'),
                "camera_settings": exif_data.get('camera_settings'),
                "tags": tags or [],
                "description": description,
                "uploader_id": uploader_id,
                "hash_md5": file_hash,
                "processing_status": "pending",
                "coordinate_source": coordinate_source,
                "file_content": file_content,  # Include for blob upload
                "validation_result": validation_result
            }
            
            return photo_data
            
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Photo processing error: {e}")
            raise ValueError(f"Photo processing failed: {str(e)}")