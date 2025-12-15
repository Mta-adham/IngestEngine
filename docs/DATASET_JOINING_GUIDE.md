# Dataset Joining Guide

## Overview

This guide explains how to join three datasets:
1. **Infrastructure** (2,555 records) - London infrastructure from 2000 onwards
2. **EPC (Non-domestic)** (1.3M records) - Energy Performance Certificates for non-domestic buildings
3. **POIs** (62,540 records) - Points of Interest from OpenStreetMap

## Joining Strategies

### Strategy 1: EPC + POIs (by Postcode)

**Method:** Join using normalized postcodes

**Results:**
- **361,840 joined records** (from 1.27M EPC records with postcodes and 27K POIs with postcodes)
- **Coverage:** ~28% of EPC records matched with POIs

**Useful Insights:**
- **Top POI types near EPC buildings:**
  - Shops: 234,049
  - Restaurants: 72,672
  - Cafes: 49,772
  - Hotels: 4,793
  - Museums: 319
  - Parks: 235

- **Energy Performance:**
  - Average target emissions: 38.20
  - All records are EPC Report Type 102 (non-domestic)

**Use Cases:**
- Analyze energy efficiency of buildings near different POI types
- Identify commercial areas with high energy consumption
- Study correlation between building types and nearby amenities

### Strategy 2: Infrastructure + POIs (by Coordinates)

**Method:** Spatial join using geographic coordinates (100m threshold)

**Results:**
- **6,167 joined records** (from 2,555 infrastructure records and 50K POI sample)
- **Average distance:** 59.0m
- **Coverage:** ~241% (multiple POIs can match one infrastructure item)

**Useful Insights:**
- **Infrastructure categories near POIs:**
  - Schools: 1,594
  - GP Surgeries: 1,173
  - Cinemas: 743
  - Museums: 568
  - Monuments: 556
  - Colleges: 210
  - Libraries: 204
  - Office Buildings: 197
  - Theatres: 192
  - Shopping Centres: 175

- **Top POI types near infrastructure:**
  - Shops: 3,506
  - Restaurants: 1,339
  - Cafes: 918
  - Parks: 184
  - Hotels: 158
  - Museums: 62

**Use Cases:**
- Identify amenities near schools, hospitals, and other infrastructure
- Analyze accessibility of public services
- Study urban planning and service distribution

### Strategy 3: EPC Multiple Records (by UPRN)

**Method:** Identify properties with multiple EPC assessments using UPRN

**Results:**
- **358,326 records** for **158,861 unique properties** with multiple EPCs
- **Average:** 2.26 EPC records per property
- **Max:** 86 EPC records for a single property
- **Date range:** 2010-2024

**Useful Insights:**
- **Properties with multiple assessments:**
  - 2 EPCs: 132,079 properties
  - 3+ EPCs: 26,782 properties
  - Some properties have been assessed up to 86 times (likely large commercial buildings with multiple units)

- **Energy Performance:**
  - Average emissions: 34.38
  - Median emissions: 24.45
  - Can track energy efficiency improvements over time

**Use Cases:**
- Track energy performance changes over time for the same property
- Identify properties that have been retrofitted or improved
- Analyze temporal trends in building energy efficiency
- Study properties with frequent assessments (high-turnover commercial spaces)

**Note:** UPRN is the UK's Unique Property Reference Number - the definitive identifier for properties. This is the most accurate way to match property records.

### Strategy 4: EPC + POIs (Multi-Column with Confidence Scoring)

**Method:** Join using multiple columns (postcode + city/address) with confidence scoring

**Results:**
- **9,756 joined records** (from 50K EPC sample and 50K POI sample)
- **96.5% high confidence matches** (2/2 columns match)
- **3.5% medium confidence** (1/2 columns match - postcode only)

**Useful Insights:**
- **Confidence Distribution:**
  - 2/2 columns match: 9,416 records (96.5%) - **High confidence**
  - 1/2 columns match: 340 records (3.5%) - **Medium confidence**

- **Top POI types (high confidence):**
  - Shops: 5,017
  - Restaurants: 2,585
  - Cafes: 1,626
  - Hotels: 176

**Advantages over single-column join:**
- **Higher accuracy:** Multiple matching columns reduce false positives
- **Confidence scoring:** Know which matches are most reliable
- **Flexible filtering:** Filter by confidence level based on your needs
- **Better data quality:** Matches with 2+ columns are more likely to be correct

**Use Cases:**
- When you need high-confidence matches only
- Analyzing relationships where accuracy is critical
- Filtering out ambiguous matches
- Building reliable datasets for downstream analysis

**Note:** This method joins on postcode + city/town. You can customize the join columns based on available data.

### Strategy 5: OSM POIs + UPRN (Spatial Nearest-Neighbor)

**Method:** Spatial join using GeoPandas nearest-neighbor matching

**How it works:**
1. Converts OSM POIs (lon/lat) to GeoDataFrame (EPSG:4326)
2. Loads UPRN data (typically EPSG:27700 - British National Grid)
3. Reprojects both to metric CRS (EPSG:27700) for accurate distance calculation
4. Performs nearest-neighbor spatial join with distance threshold
5. Filters matches within maximum distance (default: 15m)

**Advantages:**
- **Most accurate:** Spatial matching is more precise than postcode matching
- **Distance-aware:** Know exactly how far each POI is from its UPRN
- **Handles coordinate differences:** UPRN point might be at building centroid, POI at doorway
- **CRS-aware:** Properly handles coordinate system transformations

**Requirements:**
- OS Open UPRN data file (GeoPackage, Shapefile, or CSV)
- Download from: https://osdatahub.os.uk/downloads/open/OpenUPRN
- GeoPandas library installed

**Use Cases:**
- Attach official UPRN to OSM POIs for property identification
- Create authoritative property-POI linkages
- Combine OSM data with property databases
- Build comprehensive property datasets

**Note:** Each UPRN has a single official point coordinate. OSM POIs may have slightly different coordinates (e.g., doorway vs. building centroid), so spatial matching with distance threshold is ideal.

## Output Files

### 1. `joined_epc_pois_by_postcode.csv`
- **Size:** ~400MB (361,840 records × 1,124 columns)
- **Columns:** All EPC columns + All POI columns + `postcode_normalized`
- **Use:** Analyze energy performance in relation to nearby amenities

### 2. `joined_infrastructure_pois_by_coords.csv`
- **Size:** ~70MB (6,167 records × 1,087 columns)
- **Columns:** All Infrastructure columns + All POI columns + `distance_meters`
- **Use:** Study infrastructure accessibility and nearby amenities

### 3. `joined_epc_multiple_by_uprn.csv`
- **Size:** ~175MB (358,326 records × 48 columns)
- **Columns:** All EPC columns + `uprn_normalized`
- **Use:** Analyze properties with multiple EPC assessments over time
- **Note:** Only includes properties with 2+ EPC records

### 4. `joined_epc_pois_multi_column.csv`
- **Size:** ~11MB (9,756 records × 1,124+ columns)
- **Columns:** All EPC columns + All POI columns + `confidence_score` + `confidence_level`
- **Use:** High-confidence matches with multiple matching columns
- **Note:** Includes confidence scoring (1-2 columns match)

### 5. `joined_osm_pois_with_uprn_spatial.csv`
- **Size:** Varies (depends on matches)
- **Columns:** All OSM POI columns + UPRN + `uprn_distance_m` + `uprn_latitude` + `uprn_longitude` + `geometry_wkt`
- **Use:** OSM POIs with official UPRN property identifiers
- **Note:** Only includes matches within distance threshold (default: 15m)

### 6. `dataset_join_summary.csv`
- **Size:** Small (3 records)
- **Columns:** Dataset statistics (total records, records with coordinates/postcodes)
- **Use:** Quick reference for dataset coverage

## How to Use

### Run the Joiner

```bash
cd /Users/manal/Workspace/IngestEngine
python src/joining.py
```

### Programmatic Usage

```python
from src.dataset_joiner import DatasetJoiner

PATH = "/Users/manal/MyDocuments/Companies/Zone13/datasets"
joiner = DatasetJoiner(
    infrastructure_path=os.path.join(PATH, "london_infrastructure_2000_onwards.csv"),
    epc_path=os.path.join(PATH, "nondomestic_epc_2010_2024_complete.csv"),
    pois_path="/Users/manal/Workspace/IngestEngine/data/london_pois_cleaned.csv"
)

# Load datasets
joiner.load_datasets()

# Join by postcode
epc_pois = joiner.join_by_postcode()

# Join by coordinates
inf_pois = joiner.join_by_coordinates(distance_meters=100, max_pois=50000)

# Join by UPRN (if you have another dataset with UPRN)
# other_df = pd.read_csv('path/to/other_dataset_with_uprn.csv')
# joined_by_uprn = joiner.join_by_uprn(epc_df, other_df, uprn_col1='UPRN', uprn_col2='UPRN')

# Multi-column join with confidence scoring
join_columns = {
    'postcode': ('postcode', 'addr:postcode'),
    'city': ('POSTTOWN', 'addr:city'),
    # Add more columns as needed
}
multi_joined = joiner.join_by_multiple_columns(
    epc_df,
    pois_df,
    join_columns=join_columns,
    min_confidence=2,  # Require at least 2 columns to match
    suffixes=('_epc', '_poi')
)

# Spatial join OSM POIs to UPRN
uprn_path = "data/os_open_uprn.gpkg"  # Path to UPRN file
osm_with_uprn = joiner.join_osm_to_uprn_spatial(
    uprn_path=uprn_path,
    max_distance_meters=15.0  # Maximum distance in meters
)
```

## Useful Analysis Examples

### 1. Energy Efficiency by POI Type

```python
import pandas as pd

df = pd.read_csv('data/joined_epc_pois_by_postcode.csv')

# Average energy emissions by POI type
energy_by_poi = df.groupby('poi_type')['TARGET_EMISSIONS'].agg(['mean', 'count'])
print(energy_by_poi.sort_values('mean', ascending=False))
```

### 2. Infrastructure Accessibility

```python
df = pd.read_csv('data/joined_infrastructure_pois_by_coords.csv')

# Count POIs near each infrastructure type
pois_near_inf = df.groupby('category')['poi_type'].value_counts()
print(pois_near_inf)
```

### 3. Commercial Areas Analysis

```python
# Find areas with high shop density and energy consumption
df = pd.read_csv('data/joined_epc_pois_by_postcode.csv')
shops = df[df['poi_type'] == 'shops']
high_energy_shops = shops[shops['TARGET_EMISSIONS'] > shops['TARGET_EMISSIONS'].quantile(0.75)]
print(f"High energy commercial areas: {len(high_energy_shops)}")
```

### 4. Temporal Energy Performance Analysis

```python
# Analyze energy efficiency improvements over time
df = pd.read_csv('data/joined_epc_multiple_by_uprn.csv')
df['LODGEMENT_DATE'] = pd.to_datetime(df['LODGEMENT_DATE'])

# Group by UPRN and calculate improvement
improvements = df.groupby('uprn_normalized').apply(
    lambda x: x['TARGET_EMISSIONS'].iloc[-1] - x['TARGET_EMISSIONS'].iloc[0]
    if len(x) > 1 else None
).dropna()

print(f"Properties with improved efficiency: {(improvements < 0).sum():,}")
print(f"Average improvement: {improvements.mean():.2f}")
```

### 5. High-Confidence Multi-Column Matches

```python
# Filter to only high-confidence matches
df = pd.read_csv('data/joined_epc_pois_multi_column.csv')

# Get only matches where all columns matched
high_conf = df[df['confidence_score'] == df['confidence_score'].max()]
print(f"High confidence matches: {len(high_conf):,}")

# Analyze by confidence level
for score in sorted(df['confidence_score'].unique()):
    subset = df[df['confidence_score'] == score]
    print(f"\nConfidence {score}/{df['confidence_score'].max()}:")
    print(f"  Count: {len(subset):,}")
    if 'poi_type' in subset.columns:
        print(f"  Top POI: {subset['poi_type'].value_counts().head(3).to_dict()}")
```

### 6. Spatial UPRN Matching

```python
# Analyze spatial UPRN matches
df = pd.read_csv('data/joined_osm_pois_with_uprn_spatial.csv')

# Filter by distance
close_matches = df[df['uprn_distance_m'] <= 5]  # Within 5 meters
print(f"Very close matches (≤5m): {len(close_matches):,}")

# Analyze by POI type
if 'poi_type' in df.columns:
    poi_uprn = df[df['UPRN'].notna()].groupby('poi_type').agg({
        'UPRN': 'count',
        'uprn_distance_m': 'mean'
    })
    print("\nPOI types with UPRN matches:")
    print(poi_uprn.sort_values('UPRN', ascending=False).head(10))
```

## Performance Notes

- **Postcode join:** Fast (~30 seconds for 1.3M records)
- **Coordinate join:** Slower (~5-10 minutes for 2,555 infrastructure × 50K POIs)
  - Uses bounding box filtering for efficiency
  - Calculates exact geodesic distance only for candidates
  - Progress indicator shows processing status

## Limitations

1. **Postcode matching:** Only matches records with valid postcodes
   - EPC: 97.5% coverage (1.27M / 1.3M)
   - POIs: 43.4% coverage (27K / 62.5K)

2. **Coordinate matching:** Limited to 50K POI sample for performance
   - Can be increased by adjusting `max_pois` parameter
   - All infrastructure records processed (2,555)

3. **Distance threshold:** 100m default for coordinate matching
   - Can be adjusted based on use case
   - Larger thresholds = more matches but slower processing

4. **UPRN matching:**
   - EPC: 67.2% coverage (876K / 1.3M records have UPRN)
   - POIs: No UPRN column (would need address matching or external UPRN dataset)
   - Infrastructure: No UPRN column
   - **Note:** UPRN is the most accurate join key when available

5. **Multi-column joining:**
   - Increases confidence by requiring multiple columns to match
   - Reduces false positives compared to single-column joins
   - Confidence score shows how many columns matched (1-N)
   - Can filter by minimum confidence level
   - **Recommendation:** Use multi-column joins when accuracy is critical

6. **Spatial UPRN joining:**
   - Requires OS Open UPRN data file (download separately)
   - Most accurate method for property-POI matching
   - Uses nearest-neighbor spatial join with distance threshold
   - Handles coordinate system transformations automatically
   - **Recommendation:** Use when you need official property identifiers
   - **Setup:** Download UPRN from https://osdatahub.os.uk/downloads/open/OpenUPRN

## Next Steps

1. **Full POI dataset:** Process all 62K POIs for coordinate matching
2. **Additional joins:** Create Infrastructure + EPC join (if common keys exist)
3. **Temporal analysis:** Join with date fields to track changes over time
4. **Spatial analysis:** Use GeoPandas for more advanced spatial operations

