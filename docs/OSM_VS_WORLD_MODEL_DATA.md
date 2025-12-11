# OSM-Specific vs World Model Data

This guide explains which OSM data fields are OSM-specific metadata (not useful for building a world model) vs. actual POI information (useful for world models).

## Summary

From your London POI data:
- **Total columns**: 1,212
- **OSM metadata** (remove): ~228 columns
- **Useful POI data** (keep): ~526 columns
- **Uncertain** (review): ~550 columns

## OSM-Specific Metadata (Not Useful for World Model)

These fields are OSM housekeeping and not useful for building a world model:

### 1. OSM Identifiers & Structure
- `osmid` - OSM internal ID
- `element_type` - OSM element type (node/way/relation)
- `id` - OSM object ID
- `element` - OSM element type

### 2. OSM Creation Metadata
- `created_by` - Which editor created it
- `CREATEDATE`, `CREATEDBY` - Creation dates
- `version` - OSM object version
- `changeset` - OSM changeset ID
- `timestamp` - OSM edit timestamp
- `user`, `uid` - OSM contributor info

### 3. Source & Reference Fields
- `source:*` - Where OSM data came from
- `ref:*` - External reference IDs
- `source_*` - Source information

### 4. OSM Verification Fields
- `check_date` - When OSM contributor verified
- `check_date:*` - Various check dates
- `FIXME` - OSM todo notes
- `note:*` - OSM contributor notes

### 5. OSM Versioning Fields
- `old_*` - Old values (OSM history)
- `was:*` - Previous values
- `disused:*` - Disused tags
- `not:*` - Negative tags

### 6. Extraction Metadata (Our Own)
- `extraction_timestamp` - When we extracted
- `extraction_date` - Extraction date
- `extraction_time` - Extraction time
- `snapshot_date` - Snapshot date
- `attr_count` - Our analysis field

### 7. Geometry (Redundant)
- `geometry` - Geometry object (we have lat/lon)
- `geometry_wkt` - WKT format (we have lat/lon)

## Useful POI Data (Keep for World Model)

These fields contain actual information about the POI:

### 1. Basic Information
- `name` - POI name
- `name:*` - Names in different languages
- `alt_name` - Alternative names
- `official_name` - Official name

### 2. Location Data
- `latitude`, `longitude` - Coordinates
- `addr:*` - Full address components
- `address` - Address information
- `location` - Location details

### 3. Contact Information
- `phone` - Phone number
- `email` - Email address
- `website` - Website URL
- `contact:*` - Contact details
- Social media: `facebook`, `twitter`, `instagram`

### 4. POI Classification
- `amenity` - Type of amenity
- `tourism` - Tourism type
- `leisure` - Leisure activity
- `shop` - Shop type
- `cuisine` - Type of cuisine
- `poi_type` - Our POI type classification

### 5. Business Information
- `opening_hours` - Opening hours
- `capacity` - Capacity
- `seats` - Number of seats
- `rooms` - Number of rooms
- `stars` - Star rating
- `price` - Price range
- `fee` - Fees

### 6. Services & Amenities
- `payment:*` - Payment methods
- `wifi` - WiFi availability
- `internet_access` - Internet access
- `parking` - Parking availability
- `wheelchair` - Wheelchair access
- `delivery` - Delivery service
- `takeaway` - Takeaway available
- `reservation` - Reservations

### 7. Descriptions
- `description` - Description
- `description:*` - Descriptions in different languages

### 8. Food & Drink Specific
- `diet:*` - Dietary options (vegan, halal, etc.)
- `alcohol` - Alcohol availability
- `food` - Food type
- `menu` - Menu information

### 9. UK-Specific (But Useful)
- `fhrs:*` - Food hygiene rating (UK-specific but useful)

## Uncertain Fields (Need Review)

These fields may or may not be useful depending on your world model needs:

- Habitat/ecology fields (e.g., `BROADHAB`, `PRIHAB`) - May be useful for environmental models
- Building-specific fields (e.g., `building:*`) - Useful if modeling buildings
- Access fields (e.g., `access:*`) - May be useful for accessibility models
- Various specialized tags - Review case by case

## How to Clean Your Data

### Option 1: Use the Data Cleaner Tool

```bash
python src/poi_data_cleaner.py
```

This will:
- Identify OSM metadata fields
- Remove them from the dataset
- Save cleaned data to `data/london_pois_cleaned.csv`

### Option 2: Use in Python

```python
from src.poi_data_cleaner import POIDataCleaner
import pandas as pd

# Load data
df = pd.read_csv('data/london_pois.csv', low_memory=False)

# Clean
cleaner = POIDataCleaner()
cleaned_df = cleaner.clean_data(df, keep_osm_metadata=False)

# Save
cleaned_df.to_csv('data/london_pois_cleaned.csv', index=False)
```

### Option 3: Manual Selection

Keep only useful fields:

```python
# Essential fields for world model
essential_fields = [
    'poi_type', 'name', 'latitude', 'longitude',
    'amenity', 'tourism', 'leisure', 'shop',
    'addr:city', 'addr:street', 'addr:postcode',
    'phone', 'email', 'website',
    'opening_hours', 'cuisine',
    'description'
]

# Filter
df_clean = df[essential_fields].copy()
```

## Recommended Fields for World Model

### Minimum Set (Essential)
- `poi_type` - Type of POI
- `name` - Name
- `latitude`, `longitude` - Location
- `amenity` / `tourism` / `leisure` / `shop` - Classification

### Standard Set (Recommended)
Add to minimum:
- `addr:city`, `addr:street`, `addr:postcode` - Address
- `phone`, `email`, `website` - Contact
- `opening_hours` - Hours
- `cuisine` - For restaurants
- `description` - Description

### Complete Set (All Useful Data)
Use the cleaned dataset which includes all useful fields (~526 columns)

## Example: Before vs After Cleaning

### Before (1,212 columns)
Includes OSM metadata like:
- `osmid`, `element_type`, `created_by`
- `check_date`, `FIXME`, `source:*`
- `extraction_timestamp`, `geometry_wkt`

### After (1,076 columns)
Removed OSM metadata, kept:
- All address fields
- All contact information
- All business details
- All service information
- All descriptions

## Files Created

- `data/london_pois_cleaned.csv` - Cleaned data without OSM metadata
- `src/poi_data_cleaner.py` - Tool to clean data

## Key Takeaway

**Remove (~228 fields):**
- OSM identifiers and structure
- OSM creation/versioning metadata
- OSM verification/check fields
- Our extraction metadata
- Redundant geometry

**Keep (~526 fields):**
- All location data
- All contact information
- All business details
- All service information
- All descriptions
- All useful attributes

The cleaned dataset is ready for building your world model of London!

