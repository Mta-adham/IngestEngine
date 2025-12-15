# Structure Rationale: Why Move Scripts & Examples into src/?

## Current Structure (Separated)

```
IngestEngine/
├── src/           # Core modules (library code)
├── scripts/       # Standalone scripts
└── examples/      # Example code
```

## Issues with Current Structure

1. **Artificial Separation**: Scripts and examples import from `src/`, so they're tightly coupled
2. **Harder to Find**: Related code is scattered across directories
3. **Inconsistent**: Some scripts are utilities, some are examples - unclear distinction
4. **Import Complexity**: Need to manage paths between `src/`, `scripts/`, and `examples/`

## Proposed Structure (Unified)

```
IngestEngine/
└── src/
    ├── extraction/      # Core: POI extraction modules
    ├── opening_dates/   # Core: Opening date modules
    ├── joining/         # Core: Dataset joining modules
    ├── data/            # Core: Data loading modules
    ├── catalog/         # Core: Catalog modules
    ├── scripts/         # Runnable: Standalone scripts
    └── examples/        # Tutorial: Example code
```

## Benefits

1. **Everything in One Place**: All code under `src/`
2. **Clear Purpose**: Subdirectories show purpose (core vs scripts vs examples)
3. **Simpler Imports**: `from src.scripts import ...` or `from src.examples import ...`
4. **Better Organization**: Related code grouped together
5. **Easier Navigation**: One directory to explore

## Comparison

### Current (Separated)
```python
# Script imports from src
from src.extraction.poi_extractor import POIExtractor

# Run script
python scripts/analyze_london_evolution.py
```

### Proposed (Unified)
```python
# Script imports from src (same level)
from src.extraction.poi_extractor import POIExtractor

# Run script
python -m src.scripts.analyze_london_evolution
# OR
python src/scripts/analyze_london_evolution.py
```

## Recommendation

**Move `scripts/` and `examples/` into `src/`** for:
- Better organization
- Clearer structure
- Easier maintenance
- Consistent with modern Python project structure

