# Date-Based POI Extraction

This guide explains how to extract POIs as they existed on a specific date.

## Quick Start

### Command Line

```bash
# Extract all POIs as of a specific date
python src/poi_date_extractor.py 2020-01-01

# Extract specific POI types
python src/poi_date_extractor.py 2020-01-01 --types restaurants cafes

# Compare two dates
python src/poi_date_extractor.py 2020-01-01 --compare 2024-01-01
```

### Python Code

```python
from src.poi_date_extractor import POIDateExtractor

# Initialize extractor
extractor = POIDateExtractor("London, UK")

# Extract POIs at a specific date
pois_2020 = extractor.extract_pois_at_date("2020-01-01")

# Extract specific POI types
pois_2020 = extractor.extract_pois_at_date(
    "2020-01-01",
    poi_types=['restaurants', 'cafes', 'hotels']
)

# Extract at multiple dates
dates = ['2020-01-01', '2022-01-01', '2024-01-01']
results = extractor.extract_multiple_dates(dates)

# Compare two dates
comparison = extractor.compare_dates('2020-01-01', '2024-01-01')
```

## What It Does

1. **Extracts POIs** as they existed on the target date
2. **Saves to file** with date in filename: `london_pois_20200101.csv`
3. **Adds metadata** including target date and extraction timestamp
4. **Creates snapshot** for future comparisons

## Output Files

For date `2020-01-01`:
- `data/london_pois_20200101.csv` - POIs at that date
- `data/london_pois_snapshot_20200101.csv` - Snapshot for comparison

## Date Format

Use format: **YYYY-MM-DD**
- ✅ `2020-01-01`
- ✅ `2024-12-31`
- ❌ `01/01/2020` (wrong format)
- ❌ `2020-1-1` (use zero-padded)

## Examples

### Example 1: Extract POIs from 2020

```bash
python src/poi_date_extractor.py 2020-01-01
```

This extracts all POIs as they existed on January 1, 2020.

### Example 2: Extract Only Restaurants and Cafes

```bash
python src/poi_date_extractor.py 2020-01-01 --types restaurants cafes
```

### Example 3: Compare 2020 vs 2024

```bash
python src/poi_date_extractor.py 2020-01-01 --compare 2024-01-01
```

This will:
1. Extract POIs from 2020-01-01
2. Extract POIs from 2024-01-01
3. Compare them to identify changes
4. Generate change reports

### Example 4: Extract at Multiple Dates

```python
from src.poi_date_extractor import POIDateExtractor

extractor = POIDateExtractor("London, UK")

dates = [
    '2010-01-01',
    '2015-01-01',
    '2020-01-01',
    '2024-01-01'
]

results = extractor.extract_multiple_dates(dates)

# Access results
for date, df in results.items():
    print(f"{date}: {len(df):,} POIs")
```

## Important Notes

### Historical Data Availability

**Current Implementation:**
- Extracts current POI data
- Adds target date as metadata
- Useful for creating snapshots for future comparison

**True Historical Data:**
For actual historical POI data at a specific date, you need:
1. **Overpass API with history support** (not all servers have this)
2. **OSM historical planet files** (download and process)
3. **OSM changeset data** (query changesets for the date range)

### Recommended Workflow

1. **Create snapshots regularly:**
   ```bash
   # Today
   python src/poi_date_extractor.py 2024-12-11
   
   # In 3 months
   python src/poi_date_extractor.py 2025-03-11
   
   # Compare
   python src/poi_date_extractor.py 2024-12-11 --compare 2025-03-11
   ```

2. **For historical analysis:**
   - Use changeset queries (see `docs/IDENTIFYING_POI_CHANGES.md`)
   - Download historical planet files
   - Use OSM history API for specific objects

## Use Cases

### 1. Track Growth Over Time
Extract POIs at yearly intervals and compare counts.

### 2. Identify New POIs
Compare current date with past date to find new additions.

### 3. Find Removed POIs
Compare past date with current date to find what was removed.

### 4. Create Time Series
Extract at multiple dates to build a time series of POI evolution.

## Integration with Other Tools

### With Change Detector

```python
from src.poi_date_extractor import POIDateExtractor
from src.poi_change_detector import POIChangeDetector

# Extract at two dates
extractor = POIDateExtractor("London, UK")
pois_2020 = extractor.extract_pois_at_date("2020-01-01")
pois_2024 = extractor.extract_pois_at_date("2024-01-01")

# Detect changes
detector = POIChangeDetector("London, UK")
changes = detector.compare_snapshots(pois_2020, pois_2024)
```

### With History Tracker

```python
from src.poi_date_extractor import POIDateExtractor
from src.poi_history_tracker import OSMHistoryTracker

# Extract at specific date
extractor = POIDateExtractor("London, UK")
pois = extractor.extract_pois_at_date("2020-01-01")

# Track evolution from that date
tracker = OSMHistoryTracker("London, UK")
evolution = tracker.track_evolution(
    poi_types=['restaurants', 'cafes'],
    start_year=2020,
    end_year=2024
)
```

## Troubleshooting

### Date Format Error
```
Error: Invalid date format. Use YYYY-MM-DD
```
**Solution**: Use format `YYYY-MM-DD` (e.g., `2020-01-01`)

### Future Date Warning
```
Warning: Date is in the future. Using current data.
```
**Solution**: Use a date in the past or today's date

### No POIs Extracted
**Possible causes:**
- Invalid POI type specified
- Network/API issues
- No POIs of that type in the area

## See Also

- `docs/IDENTIFYING_POI_CHANGES.md` - How to identify changes
- `docs/QUICK_START_CHANGE_DETECTION.md` - Quick reference
- `src/poi_change_detector.py` - Change detection tool

