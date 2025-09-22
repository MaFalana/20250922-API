# HWC Photo Log Map API

A FastAPI-based service for managing geotagged photos with export functionality for civil engineering and surveying workflows.

## Features

- **Photo Upload & Processing**: Automatic EXIF GPS extraction and metadata processing
- **Geospatial Storage**: MongoDB with geospatial indexing for efficient location-based queries
- **Export Functionality**: Generate KML, KMZ, ZIP, and photos-only exports
- **Azure Blob Storage**: Scalable photo and thumbnail storage
- **Civil Engineering Ready**: Designed for construction documentation and site surveys

## API Endpoints

### Photos

- `POST /photos/upload` - Upload photos with automatic GPS extraction
- `GET /photos/` - List photos with filtering options
- `GET /photos/{photo_id}` - Get specific photo details
- `PUT /photos/{photo_id}/metadata` - Update photo tags and description
- `DELETE /photos/{photo_id}` - Delete photo

### Exports

- `POST /api/exports/photos` - Create export job (KML, KMZ, ZIP, photos-only)
- `GET /api/exports/kml` - Quick KML export (convenience endpoint)
- `GET /api/exports/kmz` - Quick KMZ export (convenience endpoint)
- `GET /api/exports/{job_id}/status` - Check export job status
- `GET /api/exports/{job_id}/download` - Get download URL for completed export
- `DELETE /api/exports/{job_id}` - Cancel export job

### Health & Monitoring

- `GET /health` - API health check
- `GET /api/exports/stats` - Export job statistics

## Coordinate Systems

**Current Implementation**: WGS84 (GPS coordinates) only

- ✅ Universal compatibility with Google Earth, web maps, and GPS devices
- ✅ Direct integration with camera EXIF GPS data
- ✅ Simplified workflow for photo documentation

**Future Enhancements** (to be implemented):

- 🔄 **Indiana State Plane East/West (EPSG:2965/2966)** - For integration with local CAD drawings and survey data
- 🔄 **UTM Zone 16N (EPSG:32616)** - For regional mapping and GIS integration
- 🔄 **Automatic coordinate transformation** using pyproj library
- 🔄 **User-selectable coordinate system preferences**

> **Note**: For most photo documentation workflows, WGS84 provides sufficient accuracy (±3-10 feet). State Plane and UTM systems are primarily needed for survey-grade precision and CAD integration.

## Storage Structure

```
Azure Blob Container: photo-log-map/
├── uploads/YYYY/MM/
│   ├── photo.jpg (original photos)
│   └── thumbnails/
│       ├── small_photo.jpg
│       ├── medium_photo.jpg
│       └── large_photo.jpg
└── exports/YYYY/MM/DD/
    ├── export_timestamp.kml
    ├── export_timestamp.kmz
    └── export_timestamp.zip
```

## Environment Variables

```bash
MONGO_CONNECTION_STRING=mongodb://...
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_STORAGE_CONTAINER=photo-log-map
API_VERSION=1.0.0
```

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Run tests
cd tests/
python test_kml_generation.py
python test_export_service.py
python test_real_photo_export.py
```

## Export Formats

### KML

- Google Earth compatible
- Camera icon markers with photo metadata
- Organized by date folders
- Balloon popups with photo details

### KMZ

- Compressed KML with embedded photos
- Direct viewing in Google Earth Pro
- Includes thumbnails for faster loading
- Offline viewing capability

### ZIP

- KML file + original photos
- Bulk photo download
- Archive for project documentation

## Civil Engineering Integration

The API is designed to integrate with common civil engineering workflows:

- **AutoCAD Civil 3D**: Import KML for site context
- **ArcGIS**: Direct KML layer import
- **Google Earth Pro**: Project visualization and client presentations
- **Field Documentation**: GPS-tagged progress photos
- **Asset Management**: Location-based photo inventory
