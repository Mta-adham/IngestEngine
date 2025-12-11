# Quick Start: Identifying POI Changes

## Quick Methods

### Method 1: Run the Change Detector (Easiest)

```bash
python src/poi_change_detector.py
```

This will:
- Analyze date fields in your current data
- Identify recently checked/verified POIs
- Save a snapshot for future comparison

### Method 2: Compare Snapshots

```python
from src.poi_change_detector import POIChangeDetector

detector = POIChangeDetector("London, UK")

# Load two snapshots
old_pois = detector.load_current_pois("data/london_pois_snapshot_20240101.csv")
new_pois = detector.load_current_pois("data/london_pois.csv")

# Compare
changes = detector.compare_snapshots(old_pois, new_pois)

print(f"Added: {len(changes['added'])}")
print(f"Removed: {len(changes['removed'])}")
print(f"Modified: {len(changes['modified'])}")
```

### Method 3: Analyze Date Fields

Your current data shows:
- **7,195 POIs** checked in the last year
- **4,396 POIs** checked in the last 6 months

These are likely POIs that were recently added, modified, or verified.

## What You Found

From your current data:
- **14,019 POIs** have check dates
- **7,195 POIs** were checked in the last year
- Most active: **Shops** (4,839), **Restaurants** (1,220), **Cafes** (952)

## Next Steps

1. **Extract POIs again in 1-3 months**
   - Compare with current snapshot
   - See what changed

2. **Use changeset analysis**
   - Query OSM changesets for specific date ranges
   - See exact timestamps of changes

3. **Track specific POIs**
   - Use OSM history API for individual POIs
   - See complete change history

## Files Created

- `data/london_pois_snapshot_YYYYMMDD.csv` - Snapshots for comparison
- `data/recently_checked_pois.csv` - POIs checked recently
- `data/poi_changes_*.csv` - Change detection results

## See Also

- `docs/IDENTIFYING_POI_CHANGES.md` - Complete guide
- `src/poi_change_detector.py` - Change detection tool
- `src/example_compare_pois.py` - Example code

