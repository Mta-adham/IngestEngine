# Spatial UPRN Joining Guide

## Overview

This guide explains how to join OSM POIs with UPRN (Unique Property Reference Number) data using spatial nearest-neighbor matching. This is the most accurate method for linking OSM points to official property identifiers.

## What is UPRN?

UPRN (Unique Property Reference Number) is the UK's definitive property identifier, maintained by Ordnance Survey. Each UPRN represents a single addressable location and has:
- A unique identifier (numeric)
- Official coordinates (typically in British National Grid, EPSG:27700)
- Address information
- Postcode

## Why Spatial Joining?

Unlike postcode matching, spatial joining:
- **More accurate:** Matches based on actual geographic proximity
- **Handles coordinate differences:** UPRN point might be at building centroid, OSM POI at doorway
- **Distance-aware:** Know exactly how far each match is
- **Flexible:** Can adjust distance threshold based on use case

## Setup

### 1. Download UPRN Data

1. Visit: https://osdatahub.os.uk/downloads/open/OpenUPRN
2. Register for a free account (if needed)
3. Download the GeoPackage or Shapefile format
4. Place in your `data/` directory

**File formats supported:**
- GeoPackage (`.gpkg`) - Recommended
- Shapefile (`.shp` + associated files)
- CSV with Easting/Northing columns

### 2. Install Dependencies

```bash
pip install geopandas
```

GeoPandas should already be in your `requirements.txt`.

## Usage

### Method 1: Using the Joiner Class

```python
from src.dataset_joiner import DatasetJoiner

# Initialize joiner
joiner = DatasetJoiner(
    infrastructure_path="path/to/infrastructure.csv",
    epc_path="path/to/epc.csv",
    pois_path="data/london_pois_cleaned.csv"
)

# Load datasets
joiner.load_datasets()

# Perform spatial join
result = joiner.join_osm_to_uprn_spatial(
    uprn_path="data/os_open_uprn.gpkg",
    max_distance_meters=15.0  # Maximum distance in meters
)

# Save result
result.to_csv("data/osm_pois_with_uprn.csv", index=False)
```

### Method 2: Standalone Script

```bash
python src/spatial_uprn_joiner.py
```

The script will prompt for the UPRN file path.

### Method 3: Integrated in Comprehensive Join

When you run `python src/joining.py`, it will automatically attempt to find and use UPRN data if available in common locations.

## Parameters

### `max_distance_meters` (default: 15.0)

Maximum distance in meters for matching. Recommendations:
- **5m:** Very strict, only exact matches
- **15m:** Default, good for most use cases
- **50m:** More lenient, for areas with coordinate uncertainty
- **100m+:** Very lenient, may include false matches

### CRS Handling

The method automatically:
1. Converts OSM POIs (EPSG:4326) to British National Grid (EPSG:27700)
2. Converts UPRN data to EPSG:27700 if needed
3. Performs distance calculation in metric CRS
4. Converts results back to WGS84 (EPSG:4326)

## Output

The joined dataset includes:
- **All original OSM POI columns**
- **UPRN column:** The matched UPRN identifier
- **uprn_distance_m:** Distance in meters to the matched UPRN
- **uprn_latitude / uprn_longitude:** Coordinates of the matched UPRN
- **geometry_wkt:** WKT representation of geometry

## Example Analysis

```python
import pandas as pd

# Load results
df = pd.read_csv('data/joined_osm_pois_with_uprn_spatial.csv')

# Statistics
print(f"Total records: {len(df):,}")
print(f"Records with UPRN: {df['UPRN'].notna().sum():,}")
print(f"Average distance: {df['uprn_distance_m'].mean():.2f}m")

# Filter by distance
very_close = df[df['uprn_distance_m'] <= 5]
print(f"Very close matches (≤5m): {len(very_close):,}")

# Analyze by POI type
if 'poi_type' in df.columns:
    poi_stats = df[df['UPRN'].notna()].groupby('poi_type').agg({
        'UPRN': 'count',
        'uprn_distance_m': ['mean', 'median']
    })
    print("\nPOI types with UPRN matches:")
    print(poi_stats.sort_values(('UPRN', 'count'), ascending=False).head(10))
```

## Troubleshooting

### "UPRN file not found"

- Check the file path is correct
- Ensure the file exists and is readable
- Try providing absolute path

### "Could not detect coordinate columns"

For CSV files, ensure you have either:
- `Easting` and `Northing` columns (British National Grid)
- `lon`/`longitude` and `lat`/`latitude` columns (WGS84)

### "No matches found"

- Check that UPRN data covers your area of interest
- Increase `max_distance_meters` threshold
- Verify OSM POI coordinates are valid
- Check CRS of UPRN data

### "GeoPandas not available"

Install with:
```bash
pip install geopandas
```

## Best Practices

1. **Start with 15m threshold:** Good balance of accuracy and coverage
2. **Review distance distribution:** Check if matches are reasonable
3. **Combine with other methods:** Use spatial join + postcode matching for highest confidence
4. **Filter by distance:** Remove matches beyond reasonable threshold
5. **Validate sample:** Manually check a few matches to ensure quality

## Performance

- **Small datasets (<10K POIs):** Fast (<1 minute)
- **Medium datasets (10K-100K POIs):** Moderate (1-5 minutes)
- **Large datasets (>100K POIs):** May take 10+ minutes

For large datasets, consider:
- Sampling POIs first
- Using spatial indexing
- Processing in batches

## Next Steps

After joining:
1. Combine with EPC data using UPRN
2. Link to property databases
3. Create comprehensive property-POI datasets
4. Analyze property characteristics by POI type

