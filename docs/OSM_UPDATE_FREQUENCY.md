# OpenStreetMap Data Update Frequency

## How Often is OSM Data Updated?

### Real-Time Updates (OSM Database)
**OSM Database**: Updates are **immediate** and **real-time**
- When a contributor makes an edit and saves it, the change is **instantly** applied to the OSM database
- No waiting period - edits go live immediately
- This means OSM data is continuously evolving, with thousands of edits happening daily worldwide

### Map Rendering Updates
**OpenStreetMap.org Map Display**: Updates appear within **minutes to hours**
- The "standard" map layer (Mapnik) typically reflects changes within:
  - **Minutes** for high-priority areas
  - **Few hours** for most areas
  - Depends on server load and zoom level
- Lower zoom levels may take longer to update than detailed zoom levels

### Data Extracts and Downloads

#### Planet Files
- **Full Planet**: Updated **daily** (usually around midnight UTC)
- **Regional Extracts** (e.g., Geofabrik): Updated **daily**
- **Full History Files**: Updated **weekly** (usually on Tuesdays)

#### API Updates
- **Overpass API**: Queries **live database** - always current
- **REST API**: Queries **live database** - always current
- **Changeset API**: Updates in **real-time** as edits are made

### Third-Party Services Update Frequency

Different services using OSM data update at different rates:

| Service Type | Update Frequency |
|-------------|------------------|
| Navigation apps | Weekly to monthly |
| Map rendering services | Minutes to days |
| Geocoding services | Daily to weekly |
| Routing services | Daily to weekly |
| Data extracts (Geofabrik) | Daily |

## Implications for Historical Tracking

### For Your London Evolution Analysis

#### 1. **Current Data Extraction**
- When you run `poi_extractor.py`, you get **current snapshot** of OSM
- Data is as up-to-date as the moment you query
- No delay - you're querying the live database

#### 2. **Historical Tracking**
- **Changesets**: Available in **real-time** - every edit is tracked immediately
- **History API**: Can query any object's full history at any time
- **Planet Files**: Daily snapshots available, but for historical analysis you need:
  - **Full History Files**: Weekly updates (complete edit history)
  - **Archived Planet Files**: Historical snapshots from specific dates

#### 3. **Tracking Evolution Over Time**

**Best Approach:**
1. **Current Snapshot**: Extract now (you've done this ✅)
2. **Changeset Analysis**: Query changesets for date ranges
   - Changesets are created in real-time
   - Can query any date range going back to 2005
3. **Historical Snapshots**: Use archived planet files
   - Available for specific dates
   - Can compare snapshots from different years

### Update Frequency by Data Type

#### POI Data (Restaurants, Cafes, etc.)
- **Updates**: Continuous - contributors add/edit POIs daily
- **Your extraction**: Captures current state at time of query
- **Historical tracking**: Use changesets to see when POIs were added/modified

#### Building Data
- **Updates**: Continuous - building footprints, addresses, etc.
- **Frequency**: Varies by area - active areas see more updates

#### Road Network
- **Updates**: Continuous - new roads, closures, modifications
- **Frequency**: High in urban areas, lower in rural

#### Administrative Boundaries
- **Updates**: Less frequent - changes when administrative boundaries change
- **Frequency**: Months to years

## Practical Recommendations

### For Daily/Weekly Tracking
1. **Extract current data regularly**
   - Run `poi_extractor.py` weekly/monthly
   - Compare snapshots to see changes
   - Track additions, removals, modifications

2. **Use Changeset API**
   - Query changesets for your date range
   - See exactly when edits were made
   - Filter by POI types

### For Historical Analysis (20 Years)
1. **Current Baseline**: ✅ You have this (london_pois.csv)
2. **Changeset Timeline**: Query changesets for:
   - 2005-2010 (early OSM)
   - 2010-2015 (growth period)
   - 2015-2020 (maturation)
   - 2020-2024 (recent)
3. **Archived Snapshots**: Download planet files for key years
4. **Full History**: Download full history extract for complete analysis

## Data Freshness Indicators

### In Your Current Data
Your `london_pois.csv` includes fields that indicate data freshness:

- **`check_date`**: When POI was last verified
- **`extraction_timestamp`**: When you extracted the data
- **`CREATEDATE`**: When object was created in OSM
- **`S1CAPTDATE`**: Capture date for some features

### Checking Update Activity
You can check how active OSM editing is for London:
- **OSM Statistics**: https://www.openstreetmap.org/stats/data_stats.html
- **Changeset Map**: https://www.openstreetmap.org/changeset
- **London-specific stats**: Query changesets for London bounding box

## Summary

| Aspect | Update Frequency |
|--------|------------------|
| **OSM Database** | Real-time (immediate) |
| **Map Display** | Minutes to hours |
| **Planet Files** | Daily |
| **Full History** | Weekly |
| **Your Queries** | Always current (live database) |
| **Changesets** | Real-time (every edit tracked) |

## Key Takeaway

**OSM data is continuously updated in real-time**, which means:
- ✅ Your current extraction is always up-to-date
- ✅ Changesets provide complete edit history
- ✅ Historical analysis requires archived snapshots or full history files
- ✅ For tracking evolution, combine current data with changeset analysis

For your 20-year London evolution project:
1. **Current state**: ✅ Complete (london_pois.csv)
2. **Historical tracking**: Use changesets + archived planet files
3. **Complete history**: Download full history extract for London

