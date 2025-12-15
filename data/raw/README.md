# Raw Data Directory

Place your input data files here.

## Expected Files

### POI Datasets

1. **Enriched Tourism Dataset London (POIs)**
   - Source: Mendeley dataset DOI 10.17632/gw9hjn4v65.2
   - Expected file: `enriched_tourism_london.csv`
   - Columns: id, name, category, address, latitude, longitude

2. **Tourpedia London Attractions**
   - Expected file: `tourpedia_london.csv`
   - Columns: Similar structure to Enriched Tourism Dataset

3. **Generic London POIs**
   - Expected file: `london_pois.csv`
   - Columns: id, name, category, address, latitude, longitude

### Other Datasets

- Planning data
- Building age data
- UPRN data
- EPC data
- Infrastructure data

## Note

Large CSV files in this directory are gitignored by default. Add `.gitkeep` files if you need to preserve directory structure in git.

