# OpenStreetMap Historical Data Guide for London Evolution Analysis

This guide explains how to track and analyze the evolution of London using OpenStreetMap historical data over the last 20 years.

## Methods for Accessing OSM Historical Data

### 1. **Overpass API with Date Filters** (Easiest - Current Implementation)
- **What it does**: Query OSM data as it existed at a specific date
- **Syntax**: `[date:"YYYY-MM-DD"]` in Overpass queries
- **Limitations**: May not have complete historical coverage
- **Use case**: Quick snapshots at specific dates

### 2. **OSM Changeset API** (Track Changes)
- **API**: `https://api.openstreetmap.org/api/0.6/changesets`
- **What it does**: Returns all changesets (edits) within a date range and bounding box
- **Use case**: Track when specific features were added/modified/removed
- **Example query**:
  ```
  GET /api/0.6/changesets?bbox=west,south,east,north&time=start,end
  ```

### 3. **OSM Full History Planet Files** (Most Complete)
- **Location**: https://planet.openstreetmap.org/planet/full-history/
- **What it contains**: Complete edit history of all OSM objects
- **Size**: Very large (100+ GB compressed)
- **Tools needed**: Osmosis, Osmium, or pyosmium
- **Use case**: Complete historical analysis

### 4. **OSM Archived Planet Files** (Snapshots)
- **Location**: http://planet.openstreetmap.org/cc-by-sa/
- **What it contains**: Snapshot of OSM at specific dates
- **Use case**: Compare data at different time points

### 5. **OSM History API** (Object-Level History)
- **API**: `https://api.openstreetmap.org/api/0.6/[node|way|relation]/<id>/history`
- **What it does**: Get all versions of a specific OSM object
- **Use case**: Track individual POI changes over time

## Recommended Approach for London Evolution Analysis

### Phase 1: Quick Analysis (Current Implementation)
Use the `poi_history_tracker.py` script to:
- Extract POI snapshots at 5-year intervals (2005, 2010, 2015, 2020, 2024)
- Compare counts and identify trends
- Track additions and removals

### Phase 2: Detailed Changeset Analysis
1. Query changesets for London bounding box
2. Filter by POI-related tags
3. Analyze changeset comments and metadata
4. Build timeline of changes

### Phase 3: Full History Analysis (Advanced)
1. Download OSM full history extract for London
2. Use Osmium or pyosmium to process
3. Build time-series database
4. Create visualizations

## Tools and Libraries

### Python Libraries
- **osmnx**: Current OSM data extraction
- **requests**: API queries
- **pandas**: Data analysis
- **pyosmium**: Process OSM history files (install separately)

### Command-Line Tools
- **Osmosis**: Extract and process OSM data
  ```bash
  brew install osmosis  # macOS
  # Extract London from planet file
  osmosis --read-xml planet-latest.osm.bz2 \
    --bounding-box top=51.7 left=-0.6 bottom=51.3 right=0.3 \
    --write-xml london.osm
  ```

- **Osmium**: Modern OSM data processing
  ```bash
  brew install osmium-tool  # macOS
  # Extract London history
  osmium extract --bbox -0.6,51.3,0.3,51.7 planet-history.osh.pbf \
    -o london-history.osh.pbf
  ```

## Example Workflow

### 1. Track POI Evolution Over Time
```python
from poi_history_tracker import OSMHistoryTracker

tracker = OSMHistoryTracker("London, UK")
evolution = tracker.track_evolution(
    poi_types=['restaurants', 'cafes', 'hotels'],
    start_year=2005,
    end_year=2024,
    interval_years=5
)
```

### 2. Compare Two Time Periods
```python
# Extract POIs at two different dates
pois_2010 = tracker.extract_pois_at_date(
    {'amenity': 'restaurant'}, 
    '2010-01-01', 
    'restaurants_2010'
)

pois_2024 = tracker.extract_pois_at_date(
    {'amenity': 'restaurant'}, 
    '2024-01-01', 
    'restaurants_2024'
)

# Compare
changes = tracker.compare_snapshots(pois_2010, pois_2024)
print(f"Added: {len(changes['added'])}")
print(f"Removed: {len(changes['removed'])}")
print(f"Modified: {len(changes['modified'])}")
```

### 3. Query Changesets
```python
# Get all changesets in London for a date range
changesets = tracker.query_changesets(
    start_date='2020-01-01',
    end_date='2024-01-01',
    bbox=tracker.get_place_bounds()
)
```

## Data Sources for Historical Context

### Complementary Data Sources
1. **Land Registry Price Paid Data**: Property transactions since 1995
2. **Planning Applications**: Development pipeline
3. **Census Data**: Population changes (2001, 2011, 2021)
4. **TfL Data**: Transport network evolution
5. **Google Street View Time Machine**: Visual comparison

## Output Files

The tracker generates:
- `london_poi_evolution.csv`: Time series of POI counts by type
- Comparison reports showing additions, removals, modifications

## Limitations and Considerations

1. **Data Completeness**: Early OSM data (pre-2010) may be incomplete
2. **API Rate Limits**: Overpass API has usage limits
3. **License Changes**: Some historical data may have been removed
4. **Coordinate Accuracy**: Early data may have lower accuracy
5. **Tag Evolution**: OSM tagging conventions have evolved

## Next Steps

1. Run the basic tracker to get initial evolution data
2. For deeper analysis, download OSM history extracts
3. Use Osmium to process full history files
4. Build time-series database for detailed analysis
5. Create visualizations showing London's evolution

## Resources

- [OSM Wiki - History](https://wiki.openstreetmap.org/wiki/History)
- [OSM Wiki - Changesets](https://wiki.openstreetmap.org/wiki/Changeset)
- [Overpass API Documentation](https://wiki.openstreetmap.org/wiki/Overpass_API)
- [Osmium Tool Documentation](https://osmcode.org/osmium-tool/)
- [OSM Planet Files](https://planet.openstreetmap.org/)

