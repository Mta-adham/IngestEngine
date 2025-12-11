# IngestEngine - OpenStreetMap POI Extractor & Historical Analysis

A Python tool to extract Points of Interest (POIs) from OpenStreetMap for London and track their evolution over time.

## Project Structure

```
IngestEngine/
├── src/                    # Source code
│   ├── poi_extractor.py    # Main POI extraction module
│   ├── poi_history_tracker.py  # Historical data tracking
│   ├── analyze_london_evolution.py  # Evolution analysis script
│   ├── catalogue.py        # Data catalog utilities
│   └── UkDataCatalog.py    # UK data catalog
├── data/                   # Data files (CSV, JSON)
│   ├── london_pois.csv     # Extracted POI data
│   └── ...
├── config/                 # Configuration files
│   ├── evolution_framework.json
│   └── historical_analysis_plan.json
├── docs/                   # Documentation
│   ├── OSM_HISTORICAL_DATA_GUIDE.md
│   ├── OSM_UPDATE_FREQUENCY.md
│   └── README_HISTORICAL_ANALYSIS.md
├── .cache/                 # Cache files (gitignored)
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Features

- **POI Extraction**: Extract various types of POIs from OpenStreetMap:
  - Restaurants, Cafes, Hotels, Museums, Parks, Shops
- **Historical Tracking**: Track evolution of POIs over the last 20 years
- **Complete Attributes**: Extract all 1,200+ OSM attributes per POI
- **Data Export**: Export to CSV with all attributes, coordinates, descriptions, timestamps

## Installation

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

### Extract Current POIs

```bash
# From project root
python src/poi_extractor.py

# Or using Python module
python -m src.poi_extractor
```

This will:
- Extract all POI types from London
- Save results to `data/london_pois.csv`
- Include all attributes (1,200+ columns)

### Analyze Evolution

```bash
python src/analyze_london_evolution.py
```

### Track Historical Changes

```bash
python src/poi_history_tracker.py
```

## Usage

### As a Python Module

```python
from src.poi_extractor import POIExtractor

# Initialize extractor
extractor = POIExtractor("London, UK")

# Extract specific POI types
restaurants = extractor.extract_restaurants()
cafes = extractor.extract_cafes()
hotels = extractor.extract_hotels()

# Extract all POI types
all_pois = extractor.extract_all_pois()

# Save to files
extractor.save_to_csv(all_pois, "data/my_pois.csv")
```

### Custom POI Extraction

```python
from src.poi_extractor import POIExtractor

extractor = POIExtractor("London, UK")

# Extract custom POI types
hospitals = extractor.extract_pois_by_tag({"amenity": "hospital"}, "hospitals")
schools = extractor.extract_pois_by_tag({"amenity": "school"}, "schools")
```

### Historical Analysis

```python
from src.poi_history_tracker import OSMHistoryTracker

tracker = OSMHistoryTracker("London, UK")

# Track evolution over time
evolution = tracker.track_evolution(
    poi_types=['restaurants', 'cafes'],
    start_year=2005,
    end_year=2024,
    interval_years=5
)
```

## Output Format

The extracted POIs include:
- **Coordinates**: `longitude`, `latitude` (all POIs)
- **Basic Info**: `name`, `osmid`, `poi_type`
- **Description**: Multiple description fields
- **Dates/Timestamps**: 35+ date/timestamp fields
- **All OSM Attributes**: 1,200+ attributes per POI
- **Extraction Metadata**: `extraction_timestamp`, `extraction_date`, `extraction_time`

## Data Files

- `data/london_pois.csv` - Complete POI data (62,540 records, 1,212 attributes)
- `data/london_poi_evolution.csv` - Evolution tracking data
- `config/evolution_framework.json` - Evolution tracking framework
- `config/historical_analysis_plan.json` - Analysis plan

## Documentation

- **Historical Analysis Guide**: `docs/README_HISTORICAL_ANALYSIS.md`
- **OSM Historical Data Guide**: `docs/OSM_HISTORICAL_DATA_GUIDE.md`
- **Update Frequency**: `docs/OSM_UPDATE_FREQUENCY.md`

## Requirements

- Python 3.8+
- osmnx >= 1.6.0
- pandas >= 2.0.0
- geopandas >= 0.13.0
- shapely >= 2.0.0
- requests >= 2.31.0
- python-dateutil >= 2.8.0

## Project Structure Details

### `src/` - Source Code
- **poi_extractor.py**: Main POI extraction functionality
- **poi_history_tracker.py**: Historical data tracking and analysis
- **analyze_london_evolution.py**: Comprehensive evolution analysis
- **catalogue.py**: Data catalog utilities
- **UkDataCatalog.py**: UK data catalog definitions

### `data/` - Data Files
- Extracted POI data (CSV, JSON)
- Evolution tracking data
- Note: Large files are gitignored by default

### `config/` - Configuration
- Evolution framework definitions
- Analysis plans
- Configuration JSON files

### `docs/` - Documentation
- Guides and documentation
- Historical analysis documentation
- API and usage documentation

## Notes

- The script uses the Overpass API to query OpenStreetMap data
- OSM data is updated in real-time
- Large queries may take some time to complete
- Be respectful of OpenStreetMap's API usage policies
- For historical analysis, see `docs/OSM_HISTORICAL_DATA_GUIDE.md`

## License

This project uses OpenStreetMap data, which is licensed under the Open Database License (ODbL).
