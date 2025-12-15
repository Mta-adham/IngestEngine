# IngestEngine

A comprehensive Python toolkit for extracting, joining, and analyzing Points of Interest (POIs) from OpenStreetMap and related UK datasets.

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Extract POIs from London
python -m src.poi_extractor

# Join datasets
python -m src.joining

# See examples/
python examples/example_compare_pois.py
```

## 📁 Project Structure

```
IngestEngine/
├── src/                    # Core modules
│   ├── poi_extractor.py           # Main POI extraction
│   ├── dataset_joiner.py          # Dataset joining engine
│   ├── building_opening_date_estimator.py  # Opening date estimation
│   ├── poi_data_cleaner.py        # Data cleaning utilities
│   ├── poi_change_detector.py     # Change detection
│   ├── poi_date_extractor.py      # Date-based extraction
│   ├── joining.py                 # Main joining script
│   ├── pipeline.py                # Wikidata enrichment pipeline
│   ├── data_loader.py             # POI data loading
│   ├── wikidata_client.py          # Wikidata SPARQL client
│   ├── config.py                  # Pipeline configuration
│   ├── hours_scraper.py           # Opening hours scraper (stub)
│   ├── UkDataCatalog.py           # UK data catalog
│   └── catalogue.py               # Catalog utilities
│
├── scripts/                # Standalone scripts & utilities
│   ├── analyze_london_evolution.py
│   ├── comprehensive_osm_poi_catalog.py
│   ├── filter_pois_with_names.py
│   ├── poi_history_tracker.py
│   └── spatial_uprn_joiner.py
│
├── examples/              # Example usage scripts
│   ├── example_compare_pois.py
│   └── changeset_query_example.py
│
├── data/                  # Data files
│   ├── raw/              # Raw input data
│   ├── processed/        # Processed outputs
│   └── exports/          # Final exports
│
├── docs/                  # Documentation
│   ├── DATASET_JOINING_GUIDE.md
│   ├── BUILDING_OPENING_DATES.md
│   ├── SPATIAL_UPRN_JOINING.md
│   └── ...
│
├── config/                # Configuration files
│   ├── evolution_framework.json
│   └── historical_analysis_plan.json
│
├── tests/                 # Test files (future)
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## ✨ Features

### 1. POI Extraction
- Extract POIs from OpenStreetMap (restaurants, cafes, hotels, museums, parks, shops, etc.)
- Extract all 1,200+ OSM attributes per POI
- Export to CSV/JSON with coordinates, descriptions, timestamps

### 2. Wikidata Enrichment
- Enrich POIs with opening dates from Wikidata
- Query Wikidata SPARQL endpoint for inception dates (P571)
- Automatic rate limiting and retry logic
- Checkpointing for resumable processing
- See [docs/WIKIDATA_ENRICHMENT.md](docs/WIKIDATA_ENRICHMENT.md) for details

### 2b. Unified Opening Date Pipeline (BEST APPROACH) ✅ VERIFIED & IMPROVED
- Combines Wikidata + Building Age + Planning + Heritage
- Maximum coverage (70-90% of POIs/properties)
- Intelligent fallback system
- **NEW**: Input validation, vectorized operations, checkpoint support
- **NEW**: Comprehensive error handling and data quality metrics
- See [docs/BEST_APPROACH_OPENING_DATES.md](docs/BEST_APPROACH_OPENING_DATES.md)
- See [docs/PIPELINE_VERIFICATION.md](docs/PIPELINE_VERIFICATION.md) for improvements

### 3. Dataset Joining
- **Postcode matching**: Join EPC + POIs by postcode
- **Coordinate matching**: Spatial joins with distance thresholds
- **UPRN matching**: Join by Unique Property Reference Number
- **Multi-column joins**: Confidence-scored matching
- **Spatial UPRN joins**: Nearest-neighbor spatial matching

### 4. Building Opening Dates
- Estimate opening dates from multiple sources:
  - Planning completion dates (high confidence)
  - Building age from OS/NGD (medium confidence)
  - Heritage records (medium confidence)

### 5. Historical Analysis
- Track POI evolution over time
- Date-based extraction
- Change detection
- Snapshot comparison

## 📖 Usage

### Extract POIs

```python
from src.poi_extractor import POIExtractor

extractor = POIExtractor("London, UK")
all_pois = extractor.extract_all_pois()
extractor.save_to_csv(all_pois, "data/processed/london_pois.csv")
```

### Enrich with Opening Dates (BEST APPROACH)

```python
# Unified pipeline (recommended - combines all methods)
from src.unified_opening_date_pipeline import unified_opening_date_pipeline

result = unified_opening_date_pipeline(
    input_file="data/raw/london_pois.csv",
    building_age_path="data/raw/os_building_age.gpkg"  # Optional
)

# Or use individual methods
from src.pipeline import run
run()  # Wikidata only

from src.building_opening_date_estimator import BuildingOpeningDateEstimator
estimator = BuildingOpeningDateEstimator(use_wikidata=True)
result = estimator.estimate_opening_dates(uprn_df, poi_name_col='name')
```

### Join Datasets

```python
from src.dataset_joiner import DatasetJoiner

joiner = DatasetJoiner(
    infrastructure_path="path/to/infrastructure.csv",
    epc_path="path/to/epc.csv",
    pois_path="data/processed/london_pois_cleaned.csv"
)

joiner.load_datasets()
results = joiner.create_comprehensive_join()
```

### Estimate Opening Dates

```python
from src.building_opening_date_estimator import BuildingOpeningDateEstimator

estimator = BuildingOpeningDateEstimator()
estimator.load_planning_data("data/raw/planning.csv")
estimator.load_building_age_data("data/raw/building_age.gpkg")

result = estimator.estimate_opening_dates(uprn_df)
```

## 📚 Documentation

- **[Dataset Joining Guide](docs/DATASET_JOINING_GUIDE.md)** - Complete guide to joining datasets
- **[Wikidata Enrichment](docs/WIKIDATA_ENRICHMENT.md)** - Enrich POIs with opening dates from Wikidata
- **[Building Opening Dates](docs/BUILDING_OPENING_DATES.md)** - How to estimate building opening dates
- **[Spatial UPRN Joining](docs/SPATIAL_UPRN_JOINING.md)** - Spatial nearest-neighbor joining
- **[OSM Historical Data Guide](docs/OSM_HISTORICAL_DATA_GUIDE.md)** - Historical data access
- **[Quick Reference](docs/QUICK_REFERENCE_OSM_FIELDS.md)** - OSM field reference

## 🛠️ Installation

```bash
# Clone repository
git clone <repository-url>
cd IngestEngine

# Install dependencies
pip install -r requirements.txt
```

## 📋 Requirements

- Python 3.8+
- osmnx >= 1.6.0
- pandas >= 2.0.0
- geopandas >= 0.13.0
- shapely >= 2.0.0
- geopy >= 2.3.0
- requests >= 2.31.0
- python-dateutil >= 2.8.0
- tqdm >= 4.65.0
- pyarrow >= 10.0.0

## 🎯 Main Scripts

### Core Scripts (src/)
- `poi_extractor.py` - Extract POIs from OSM
- `joining.py` - Main dataset joining script
- `dataset_joiner.py` - Joining engine (import as module)

### Utility Scripts (scripts/)
- `analyze_london_evolution.py` - Analyze POI evolution
- `comprehensive_osm_poi_catalog.py` - Generate POI catalog
- `spatial_uprn_joiner.py` - Standalone UPRN spatial join

### Examples (examples/)
- `example_compare_pois.py` - Compare POI snapshots
- `changeset_query_example.py` - Query OSM changesets

## 📊 Data Files

### Processed Data (data/processed/)
- `london_pois_cleaned.csv` - Cleaned POI data
- `joined_*.csv` - Joined datasets
- `osm_poi_types_with_attributes.csv` - POI catalog

### Raw Data (data/raw/)
Place your input data files here:
- Planning data
- Building age data
- UPRN data
- EPC data
- Infrastructure data

## 🔧 Configuration

Edit `config/` files to customize:
- Evolution tracking framework
- Historical analysis plans

## 📝 Notes

- OSM data is updated in real-time
- Large queries may take time
- Be respectful of API usage policies
- Data files in `data/processed/` are gitignored by default

## 🤝 Contributing

1. Keep core modules in `src/`
2. Add examples to `examples/`
3. Add utilities to `scripts/`
4. Update documentation in `docs/`

## 📄 License

This project uses OpenStreetMap data, licensed under the Open Database License (ODbL).
