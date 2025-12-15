# Source Code Organization

## 📁 New Directory Structure

All modules in `src/` are now organized by function:

```
src/
├── __init__.py              # Top-level imports
├── config.py                # Shared configuration (stays at root)
│
├── extraction/              # POI Extraction & Processing
│   ├── __init__.py
│   ├── poi_extractor.py
│   ├── poi_data_cleaner.py
│   ├── poi_change_detector.py
│   └── poi_date_extractor.py
│
├── opening_dates/            # Opening Date Estimation
│   ├── __init__.py
│   ├── building_opening_date_estimator.py
│   ├── enhanced_opening_date_estimator.py
│   ├── unified_opening_date_pipeline.py
│   ├── pipeline.py
│   └── wikidata_client.py
│
├── joining/                 # Dataset Joining
│   ├── __init__.py
│   ├── dataset_joiner.py
│   └── joining.py
│
├── data/                    # Data Loading & Utilities
│   ├── __init__.py
│   ├── data_loader.py
│   └── hours_scraper.py
│
└── catalog/                 # Catalog & Metadata
    ├── __init__.py
    ├── UkDataCatalog.py
    └── catalogue.py
```

## 🔄 Import Changes

### Old Imports (Still Work via __init__.py)
```python
from src.poi_extractor import POIExtractor
from src.building_opening_date_estimator import BuildingOpeningDateEstimator
from src.dataset_joiner import DatasetJoiner
```

### New Direct Imports (Recommended)
```python
from src.extraction.poi_extractor import POIExtractor
from src.opening_dates.building_opening_date_estimator import BuildingOpeningDateEstimator
from src.joining.dataset_joiner import DatasetJoiner
from src.data.data_loader import load_london_pois
from src.opening_dates.wikidata_client import WikidataClient
```

### Top-Level Imports (Via __init__.py)
```python
import src
extractor = src.POIExtractor()
estimator = src.BuildingOpeningDateEstimator()
loader = src.load_london_pois("data/raw/london_pois.csv")
```

## 📋 Module Groups

### 1. Extraction (`src/extraction/`)
**Purpose**: Extract and process POIs from OpenStreetMap

- `poi_extractor.py` - Extract POIs by type
- `poi_data_cleaner.py` - Clean OSM data
- `poi_change_detector.py` - Detect changes over time
- `poi_date_extractor.py` - Extract POIs at specific dates

**Usage:**
```python
from src.extraction import POIExtractor, POIDataCleaner
```

### 2. Opening Dates (`src/opening_dates/`)
**Purpose**: Estimate opening dates from multiple sources

- `building_opening_date_estimator.py` - Multi-source estimator
- `unified_opening_date_pipeline.py` - Unified pipeline
- `pipeline.py` - Wikidata enrichment
- `wikidata_client.py` - Wikidata SPARQL client
- `enhanced_opening_date_estimator.py` - Enhanced estimator

**Usage:**
```python
from src.opening_dates import (
    BuildingOpeningDateEstimator,
    unified_opening_date_pipeline,
    WikidataClient
)
```

### 3. Joining (`src/joining/`)
**Purpose**: Join multiple datasets

- `dataset_joiner.py` - Multi-strategy joining engine
- `joining.py` - Main joining orchestrator

**Usage:**
```python
from src.joining import DatasetJoiner
```

### 4. Data (`src/data/`)
**Purpose**: Data loading and utilities

- `data_loader.py` - Load POI data
- `hours_scraper.py` - Opening hours scraper (stub)

**Usage:**
```python
from src.data import load_london_pois, HoursScraper
```

### 5. Catalog (`src/catalog/`)
**Purpose**: UK data catalog definitions

- `UkDataCatalog.py` - UK data catalog
- `catalogue.py` - Catalog utilities

**Usage:**
```python
from src.catalog import create_complete_catalog
```

## ✅ Benefits

1. **Clear Organization**: Related modules grouped together
2. **Easy Navigation**: Find modules by function
3. **Better Imports**: More descriptive import paths
4. **Scalability**: Easy to add new modules
5. **Maintainability**: Clear structure shows project organization

## 🔄 Migration Guide

### For Existing Code

**Old imports still work** (via `src/__init__.py`):
```python
from src.poi_extractor import POIExtractor  # Still works!
```

**New imports recommended** (more explicit):
```python
from src.extraction.poi_extractor import POIExtractor  # Better!
```

### Updated Files

All internal imports have been updated:
- ✅ `src/extraction/*` - Updated internal imports
- ✅ `src/opening_dates/*` - Updated internal imports
- ✅ `src/joining/*` - Updated internal imports
- ✅ `scripts/*` - Updated imports
- ✅ `examples/*` - Updated imports

## 📝 Notes

- `config.py` stays at `src/` root (shared across all modules)
- All old imports still work via `src/__init__.py`
- New direct imports are recommended for clarity
- All `__init__.py` files export main classes/functions

