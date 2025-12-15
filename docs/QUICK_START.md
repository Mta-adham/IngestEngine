# Quick Start Guide

Get started with IngestEngine in 5 minutes!

## Installation

```bash
pip install -r requirements.txt
```

## Basic Usage

### 1. Extract POIs

```bash
python -m src.poi_extractor
```

Output: `data/processed/london_pois.csv`

### 2. Join Datasets

```bash
python -m src.joining
```

This will:
- Load Infrastructure, EPC, and POI datasets
- Join them using multiple strategies
- Save results to `data/processed/`

### 3. Run Examples

```bash
python examples/example_compare_pois.py
```

## Common Tasks

### Extract Specific POI Types

```python
from src.poi_extractor import POIExtractor

extractor = POIExtractor("London, UK")
restaurants = extractor.extract_restaurants()
cafes = extractor.extract_cafes()
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
result = estimator.estimate_opening_dates(uprn_df)
```

## File Locations

- **Core modules**: `src/`
- **Scripts**: `scripts/`
- **Examples**: `examples/`
- **Data**: `data/processed/`
- **Documentation**: `docs/`

## Next Steps

- Read [README.md](README.md) for full documentation
- Check [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for organization details
- See [docs/DATASET_JOINING_GUIDE.md](docs/DATASET_JOINING_GUIDE.md) for joining examples

