# Identifying Changes in POIs Over Time

This guide explains how to identify and track changes in Points of Interest (POIs) over time using OpenStreetMap data.

## Methods to Identify POI Changes

### 1. **Compare Snapshots at Different Times** (Recommended)

Compare POI data extracted at different dates to see what changed.

#### Approach:
- Extract POIs at Time T1 (e.g., 2020-01-01)
- Extract POIs at Time T2 (e.g., 2024-01-01)
- Compare the two datasets

#### What You Can Identify:
- **Added POIs**: New POIs that didn't exist at T1
- **Removed POIs**: POIs that existed at T1 but not at T2
- **Modified POIs**: POIs that exist in both but have changed attributes
- **Count Changes**: Total number of POIs by type over time

### 2. **Use OSM Changesets** (Most Detailed)

Changesets track every edit made to OSM, including when POIs were added, modified, or deleted.

#### What Changesets Tell You:
- **Exact timestamp** of when changes were made
- **What changed**: Added, modified, or deleted
- **Who made the change**: Contributor information
- **Change comments**: Why the change was made (sometimes)

### 3. **Analyze Object History** (For Specific POIs)

For individual POIs, you can query their complete history to see all versions.

#### Use Case:
- Track a specific restaurant's evolution
- See when attributes were added/modified
- Identify when a POI was created

### 4. **Use Date Fields in Current Data** (Quick Method)

Your current POI data includes date fields that indicate when things were created or last checked.

#### Fields to Look For:
- `check_date`: When POI was last verified
- `CREATEDATE`: When object was created
- `extraction_timestamp`: When you extracted the data

## Practical Implementation

### Method 1: Compare Two Snapshots

```python
from src.poi_extractor import POIExtractor
from src.poi_history_tracker import OSMHistoryTracker

# Extract POIs at two different times
extractor = POIExtractor("London, UK")

# Current POIs (you already have this)
current_pois = extractor.extract_all_pois()

# For historical comparison, you would extract at a previous date
# (Note: Requires historical data source - see below)

# Compare using the tracker
tracker = OSMHistoryTracker("London, UK")
changes = tracker.compare_snapshots(
    df_old=old_pois,  # From previous extraction
    df_new=current_pois,
    id_column='osmid'
)

print(f"Added: {len(changes['added'])} POIs")
print(f"Removed: {len(changes['removed'])} POIs")
print(f"Modified: {len(changes['modified'])} POIs")
```

### Method 2: Query Changesets

```python
from src.poi_history_tracker import OSMHistoryTracker
import requests
from datetime import datetime, timedelta

tracker = OSMHistoryTracker("London, UK")

# Get changesets for a date range
start_date = "2020-01-01"
end_date = "2024-01-01"

changesets = tracker.query_changesets(start_date, end_date)

# Parse changesets to identify POI-related changes
# (See changeset_query_example.py for detailed code)
```

### Method 3: Analyze Current Data for Change Indicators

```python
import pandas as pd

# Load your current POI data
df = pd.read_csv('data/london_pois.csv', low_memory=False)

# Find POIs with recent check dates
recent_checks = df[df['check_date'].notna()].copy()
recent_checks['check_date_parsed'] = pd.to_datetime(
    recent_checks['check_date'], errors='coerce'
)

# POIs checked in last year
from datetime import datetime, timedelta
one_year_ago = datetime.now() - timedelta(days=365)
recently_checked = recent_checks[
    recent_checks['check_date_parsed'] > one_year_ago
]

print(f"POIs checked in last year: {len(recently_checked)}")

# Group by POI type
print(recently_checked['poi_type'].value_counts())
```

## Step-by-Step: Identifying Changes

### Step 1: Extract Baseline (Current State)

You already have this: `data/london_pois.csv`

### Step 2: Extract Historical Snapshot

For true historical comparison, you need:
- Archived OSM planet files from specific dates, OR
- Changeset data for the time period

### Step 3: Compare Datasets

Use the comparison function to identify:
- Added POIs
- Removed POIs  
- Modified POIs

### Step 4: Analyze Changes

Look at:
- Which POI types changed most
- Geographic distribution of changes
- Temporal patterns (when changes occurred)

## Creating a Change Detection Script

Let me create a comprehensive script for you that identifies changes.

