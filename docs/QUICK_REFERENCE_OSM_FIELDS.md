# Quick Reference: OSM-Specific vs Useful Fields

## What to Remove (OSM-Specific Metadata)

### Definitely Remove (136 fields removed)
- **OSM IDs**: `osmid`, `element_type`, `id`, `element`
- **Creation info**: `created_by`, `CREATEDATE`, `CREATEDBY`
- **Versioning**: `version`, `changeset`, `timestamp`, `user`, `uid`
- **Source/Ref**: `source:*`, `ref:*`, `source_*`
- **Check dates**: `check_date`, `check_date:*`
- **OSM notes**: `FIXME`, `note:*`, `fixme:*`
- **History**: `old_*`, `was:*`, `disused:*`, `not:*`
- **Our metadata**: `extraction_*`, `snapshot_*`, `attr_count`
- **Geometry**: `geometry`, `geometry_wkt` (we have lat/lon)

## What to Keep (Useful for World Model)

### Essential (Must Keep)
- `poi_type`, `name`, `latitude`, `longitude`
- `amenity`, `tourism`, `leisure`, `shop`
- `addr:*` (all address fields)
- `phone`, `email`, `website`
- `opening_hours`, `cuisine`, `description`

### Recommended (Keep)
- All `contact:*` fields
- All `payment:*` fields
- `wifi`, `parking`, `wheelchair`
- `capacity`, `seats`, `rooms`, `stars`
- `diet:*` (dietary options)
- Social media fields

## Results

- **Original**: 1,212 columns
- **Cleaned**: 1,076 columns
- **Removed**: 136 OSM metadata columns (11.2% reduction)

## Use the Cleaner

```bash
python src/poi_data_cleaner.py
```

Output: `data/london_pois_cleaned.csv`

