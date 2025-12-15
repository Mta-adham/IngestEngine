# Source Code Organization Plan

## Current Structure (Flat)
```
src/
├── building_opening_date_estimator.py
├── catalogue.py
├── config.py
├── data_loader.py
├── dataset_joiner.py
├── enhanced_opening_date_estimator.py
├── hours_scraper.py
├── joining.py
├── pipeline.py
├── poi_change_detector.py
├── poi_data_cleaner.py
├── poi_date_extractor.py
├── poi_extractor.py
├── UkDataCatalog.py
├── unified_opening_date_pipeline.py
└── wikidata_client.py
```

## Proposed Structure (Grouped by Function)

```
src/
├── __init__.py
├── config.py                    # Shared config (stays at root)
│
├── extraction/                   # POI Extraction & Processing
│   ├── __init__.py
│   ├── poi_extractor.py
│   ├── poi_data_cleaner.py
│   ├── poi_change_detector.py
│   └── poi_date_extractor.py
│
├── opening_dates/                # Opening Date Estimation
│   ├── __init__.py
│   ├── building_opening_date_estimator.py
│   ├── enhanced_opening_date_estimator.py
│   ├── unified_opening_date_pipeline.py
│   ├── pipeline.py
│   └── wikidata_client.py
│
├── joining/                      # Dataset Joining
│   ├── __init__.py
│   ├── dataset_joiner.py
│   └── joining.py
│
├── data/                         # Data Loading & Utilities
│   ├── __init__.py
│   ├── data_loader.py
│   └── hours_scraper.py
│
└── catalog/                      # Catalog & Metadata
    ├── __init__.py
    ├── UkDataCatalog.py
    └── catalogue.py
```

## Benefits

1. **Clear Organization**: Related modules grouped together
2. **Easy Navigation**: Find modules by function
3. **Better Imports**: `from src.extraction import POIExtractor`
4. **Scalability**: Easy to add new modules in right place
5. **Maintainability**: Clear structure shows project organization

