"""
KML/KMZ generator service following OGC KML 2.2 standard
"""

import logging
import os
import tempfile
import zipfile
import shutil
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import requests
from io import BytesIO

from models.photo import Photo
from models.export import CoordinateSystem
from services.azure_blob_photo import AzureBlobPhotoManager


logger = logging.getLogger(__name__)


class CoordinateTransformer:
    """Coordinate system transformation utilities (WGS84 only for now)"""
    
    @staticmethod
    def transform_coordinates(
        lat: float, 
        lng: float, 
        altitude: Optional[float],
        target_system: CoordinateSystem
    ) -> Tuple[float, float, Optional[float]]:
        """
        Transform coordinates to target coordinate system
        
        For MVP, we'll keep WGS84 coordinates and add coordinate system info to KML
        Full coordinate transformation would require pyproj with proper EPSG codes
        
        Args:
            lat: Latitude in WGS84
            lng: Longitude in WGS84
            altitude: Altitude in meters
            target_system: Target coordinate system
            
        Returns:
            Tuple of (x, y, z) coordinates
        """
        if target_system == CoordinateSystem.WGS84:
            return lng, lat, altitude
        
        # For non-WGS84 systems, we'll keep WGS84 coordinates but add metadata
        # In a full implementation, this would use pyproj for proper transformation
        logger.warning(f"Coordinate transformation to {target_system} not fully implemented. Using WGS84.")
        return lng, lat, altitude
    
    @staticmethod
    def get_coordinate_system_info(coord_system: CoordinateSystem) -> Dict[str, str]:
        """Get coordinate system metadata for KML"""
        systems = {
            CoordinateSystem.WGS84: {
                "name": "WGS84 Geographic",
                "epsg": "EPSG:4326",
                "description": "World Geodetic System 1984"
            },

        }
        
        return systems.get(coord_system, systems[CoordinateSystem.WGS84])


class KMLGenerator:
    """KML file generator following OGC KML 2.2 standard"""
    
    def __init__(self, blob_manager: AzureBlobPhotoManager):
        self.blob_manager = blob_manager
        self.transformer = CoordinateTransformer()
    
    def generate_kml(
        self,
        photos: List[Photo],
        coordinate_system: CoordinateSystem = CoordinateSystem.WGS84,
        include_altitude: bool = True,
        title: str = "Photo Survey Export",
        description: str = "Geotagged photos exported from HWC Engineering Photo Log Map"
    ) -> str:
        """
        Generate KML content from photos
        
        Args:
            photos: List of Photo objects
            coordinate_system: Target coordinate system
            include_altitude: Include altitude data
            title: KML document title
            description: KML document description
            
        Returns:
            KML content as string
        """
        try:
            # Create root KML element
            kml = Element('kml')
            kml.set('xmlns', 'http://www.opengis.net/kml/2.2')
            
            # Create document
            document = SubElement(kml, 'Document')
            
            # Add document metadata
            name_elem = SubElement(document, 'name')
            name_elem.text = title
            
            desc_elem = SubElement(document, 'description')
            desc_elem.text = f"{description} - Generated: {datetime.utcnow().isoformat()}Z"
            
            # Add coordinate system info
            coord_info = self.transformer.get_coordinate_system_info(coordinate_system)
            extended_data = SubElement(document, 'ExtendedData')
            
            coord_data = SubElement(extended_data, 'Data')
            coord_data.set('name', 'coordinate_system')
            coord_value = SubElement(coord_data, 'value')
            coord_value.text = f"{coord_info['name']} ({coord_info['epsg']})"
            
            # Add styles for photo markers
            self._add_photo_styles(document)
            
            # Group photos by date for organization
            photo_groups = self._group_photos_by_date(photos)
            
            # Create folders for each date
            for date_str, date_photos in photo_groups.items():
                folder = SubElement(document, 'Folder')
                
                folder_name = SubElement(folder, 'name')
                folder_name.text = f"Photos - {date_str}"
                
                folder_desc = SubElement(folder, 'description')
                folder_desc.text = f"{len(date_photos)} photos taken on {date_str}"
                
                # Add placemarks for photos in this date group
                for photo in date_photos:
                    self._add_photo_placemark(
                        folder, 
                        photo, 
                        coordinate_system, 
                        include_altitude
                    )
            
            # Convert to properly formatted XML string
            xml_string = tostring(kml, encoding='utf-8', method='xml')
            xml_content = xml_string.decode('utf-8')
            
            # Add XML declaration and format properly
            if not xml_content.startswith('<?xml'):
                xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_content
            
            # Basic formatting for readability
            try:
                from xml.dom import minidom
                dom = minidom.parseString(xml_content)
                formatted_xml = dom.toprettyxml(indent="  ", encoding=None)
                # Remove empty lines
                lines = [line for line in formatted_xml.split('\n') if line.strip()]
                return '\n'.join(lines)
            except:
                # Fallback to unformatted if parsing fails
                return xml_content
            
        except Exception as e:
            logger.error(f"Failed to generate KML: {e}")
            raise
    
    def _add_photo_styles(self, document: Element):
        """Add KML styles for photo markers"""
        # Photo marker style
        style = SubElement(document, 'Style')
        style.set('id', 'photoMarker')
        
        icon_style = SubElement(style, 'IconStyle')
        icon_scale = SubElement(icon_style, 'scale')
        icon_scale.text = '1.0'
        
        icon = SubElement(icon_style, 'Icon')
        icon_href = SubElement(icon, 'href')
        icon_href.text = 'http://maps.google.com/mapfiles/kml/shapes/camera.png'
        
        # Label style
        label_style = SubElement(style, 'LabelStyle')
        label_scale = SubElement(label_style, 'scale')
        label_scale.text = '0.8'
        
        # Balloon style for photo popup
        balloon_style = SubElement(style, 'BalloonStyle')
        balloon_text = SubElement(balloon_style, 'text')
        # Use simple text template without CDATA to avoid XML parsing issues
        balloon_text.text = '$[name] - $[timestamp] - Camera: $[camera_info] - Tags: $[tags] - $[description]'
    
    def _group_photos_by_date(self, photos: List[Photo]) -> Dict[str, List[Photo]]:
        """Group photos by date for KML folder organization"""
        groups = {}
        
        for photo in photos:
            date_str = photo.timestamp.strftime('%Y-%m-%d')
            if date_str not in groups:
                groups[date_str] = []
            groups[date_str].append(photo)
        
        # Sort groups by date
        return dict(sorted(groups.items()))
    
    def _add_photo_placemark(
        self, 
        parent: Element, 
        photo: Photo, 
        coordinate_system: CoordinateSystem,
        include_altitude: bool
    ):
        """Add a placemark for a single photo"""
        placemark = SubElement(parent, 'Placemark')
        
        # Name
        name = SubElement(placemark, 'name')
        name.text = photo.original_filename
        
        # Description with photo metadata
        description = SubElement(placemark, 'description')
        desc_text = f"Photo: {photo.original_filename}\n"
        desc_text += f"Timestamp: {photo.timestamp.isoformat()}Z\n"
        desc_text += f"Size: {photo.file_size} bytes\n"
        
        if photo.camera_make or photo.camera_model:
            # Clean camera data by removing null bytes and extra whitespace
            camera_make = (photo.camera_make or '').replace('\x00', '').strip()
            camera_model = (photo.camera_model or '').replace('\x00', '').strip()
            camera_info = f"{camera_make} {camera_model}".strip()
            if camera_info:
                desc_text += f"Camera: {camera_info}\n"
        
        if photo.tags:
            desc_text += f"Tags: {', '.join(photo.tags)}\n"
        
        if photo.description:
            desc_text += f"Description: {photo.description}\n"
        
        description.text = desc_text
        
        # Style reference
        style_url = SubElement(placemark, 'styleUrl')
        style_url.text = '#photoMarker'
        
        # Extended data for balloon template
        extended_data = SubElement(placemark, 'ExtendedData')
        
        # Add data elements for balloon template
        # Clean camera data
        camera_make = (photo.camera_make or '').replace('\x00', '').strip()
        camera_model = (photo.camera_model or '').replace('\x00', '').strip()
        camera_info = f"{camera_make} {camera_model}".strip() or 'Unknown'
        
        data_elements = {
            'photo_url': photo.blob_url if photo.blob_url != 'https://placeholder.blob.url' else '',
            'timestamp': photo.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'coordinates': f"{photo.latitude:.6f}, {photo.longitude:.6f}",
            'camera_info': camera_info,
            'tags': ', '.join(photo.tags) if photo.tags else 'None',
            'description': photo.description or ''
        }
        
        for key, value in data_elements.items():
            data = SubElement(extended_data, 'Data')
            data.set('name', key)
            data_value = SubElement(data, 'value')
            data_value.text = str(value)
        
        # Point geometry
        point = SubElement(placemark, 'Point')
        
        # Transform coordinates
        x, y, z = self.transformer.transform_coordinates(
            photo.latitude,
            photo.longitude, 
            photo.altitude if include_altitude else None,
            coordinate_system
        )
        
        # Coordinates (longitude, latitude, altitude)
        coordinates = SubElement(point, 'coordinates')
        if include_altitude and z is not None:
            coordinates.text = f"{x},{y},{z}"
        else:
            coordinates.text = f"{x},{y}"
        
        # Altitude mode
        if include_altitude and photo.altitude is not None:
            altitude_mode = SubElement(point, 'altitudeMode')
            altitude_mode.text = 'absolute'


class KMZGenerator:
    """KMZ file generator with embedded photos"""
    
    def __init__(self, blob_manager: AzureBlobPhotoManager):
        self.blob_manager = blob_manager
        self.kml_generator = KMLGenerator(blob_manager)
    
    async def generate_kmz(
        self,
        photos: List[Photo],
        output_path: str,
        coordinate_system: CoordinateSystem = CoordinateSystem.WGS84,
        include_altitude: bool = True,
        include_photos: bool = True,
        include_thumbnails: bool = True,
        title: str = "Photo Survey Export"
    ) -> str:
        """
        Generate KMZ file with embedded photos
        
        Args:
            photos: List of Photo objects
            output_path: Path for output KMZ file
            coordinate_system: Target coordinate system
            include_altitude: Include altitude data
            include_photos: Embed full-size photos
            include_thumbnails: Embed thumbnail images
            title: KMZ document title
            
        Returns:
            Path to generated KMZ file
        """
        try:
            # Create temporary directory for KMZ contents
            with tempfile.TemporaryDirectory() as temp_dir:
                
                # Generate KML content
                kml_content = self.kml_generator.generate_kml(
                    photos=photos,
                    coordinate_system=coordinate_system,
                    include_altitude=include_altitude,
                    title=title
                )
                
                # Write KML file
                kml_path = os.path.join(temp_dir, 'doc.kml')
                with open(kml_path, 'w', encoding='utf-8') as f:
                    f.write(kml_content)
                
                # Create files directory for embedded content
                files_dir = os.path.join(temp_dir, 'files')
                os.makedirs(files_dir, exist_ok=True)
                
                # Download and embed photos
                if include_photos or include_thumbnails:
                    await self._embed_photos(
                        photos, 
                        files_dir, 
                        include_photos, 
                        include_thumbnails
                    )
                
                # Create KMZ archive
                with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as kmz:
                    # Add KML file
                    kmz.write(kml_path, 'doc.kml')
                    
                    # Add embedded files
                    for root, dirs, files in os.walk(files_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arc_path = os.path.relpath(file_path, temp_dir)
                            kmz.write(file_path, arc_path)
                
                logger.info(f"Generated KMZ file: {output_path}")
                return output_path
                
        except Exception as e:
            logger.error(f"Failed to generate KMZ: {e}")
            raise
    
    async def _embed_photos(
        self,
        photos: List[Photo],
        files_dir: str,
        include_photos: bool,
        include_thumbnails: bool
    ):
        """Download and embed photos in KMZ"""
        for i, photo in enumerate(photos):
            try:
                # Create safe filename
                safe_filename = self._make_safe_filename(photo.original_filename)
                
                # Download and embed full photo
                if include_photos and photo.blob_url:
                    photo_path = os.path.join(files_dir, safe_filename)
                    await self._download_file(photo.blob_url, photo_path)
                
                # Download and embed thumbnail
                if include_thumbnails and photo.thumbnail_url:
                    name, ext = os.path.splitext(safe_filename)
                    thumb_filename = f"{name}_thumb{ext}"
                    thumb_path = os.path.join(files_dir, thumb_filename)
                    await self._download_file(photo.thumbnail_url, thumb_path)
                
                # Update progress
                if (i + 1) % 10 == 0:
                    logger.info(f"Embedded {i + 1}/{len(photos)} photos")
                    
            except Exception as e:
                logger.error(f"Failed to embed photo {photo.id}: {e}")
                continue
    
    async def _download_file(self, url: str, output_path: str):
        """Download file from URL to local path"""
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        except Exception as e:
            logger.error(f"Failed to download file from {url}: {e}")
            raise
    
    def _make_safe_filename(self, filename: str) -> str:
        """Make filename safe for ZIP archive"""
        # Remove or replace unsafe characters
        safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
        safe_filename = ''.join(c if c in safe_chars else '_' for c in filename)
        
        # Ensure it's not empty
        if not safe_filename:
            safe_filename = "photo.jpg"
        
        return safe_filename