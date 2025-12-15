# Wikidata Enrichment Pipeline

## Overview

This pipeline enriches POI datasets with opening dates from Wikidata. It queries Wikidata's SPARQL endpoint to find POIs and retrieve their inception/opening dates (property P571).

## Features

- **Data Loading**: Load POI data from various sources (Enriched Tourism Dataset, Tourpedia, etc.)
- **Wikidata Integration**: Query Wikidata SPARQL endpoint for opening dates
- **Rate Limiting**: Respects Wikidata guidelines with configurable delays
- **Retry Logic**: Automatic retry on failures
- **Checkpointing**: Saves progress periodically to resume if interrupted
- **Multiple Outputs**: Saves as both CSV and Parquet formats

## Data Sources

### Enriched Tourism Dataset London (POIs)
- **Source**: Mendeley dataset DOI 10.17632/gw9hjn4v65.2
- **Format**: CSV with columns: id, name, category, address, latitude, longitude
- **Place**: `data/raw/enriched_tourism_london.csv`

### Tourpedia London Attractions
- **Source**: Tourpedia London attractions CSV
- **Format**: Similar structure to Enriched Tourism Dataset
- **Place**: `data/raw/tourpedia_london.csv`

## Usage

### Basic Usage

```bash
# Run the complete pipeline
python -m src.pipeline

# Or from Python
from src.pipeline import run
run()
```

### Custom Input/Output

```python
from src.pipeline import run

run(
    input_file="data/raw/my_pois.csv",
    output_csv="data/processed/my_pois_enriched.csv",
    output_parquet="data/processed/my_pois_enriched.parquet"
)
```

### Step-by-Step Usage

```python
from src.data_loader import load_london_pois
from src.wikidata_client import WikidataClient
from src.pipeline import enrich_with_opening_dates

# 1. Load POIs
df = load_london_pois("data/raw/london_pois.csv")

# 2. Enrich with opening dates
df_enriched = enrich_with_opening_dates(df)

# 3. Save results
df_enriched.to_csv("data/processed/london_pois_opening_dates.csv", index=False)
```

## Module Structure

### `data_loader.py`
- `load_london_pois(path)` - Load and clean POI CSV
- `load_from_enriched_tourism_dataset()` - Load from Enriched Tourism Dataset
- `load_from_tourpedia()` - Load from Tourpedia

**Features**:
- Handles various column name variations
- Filters to valid coordinates
- Drops rows without names or coordinates
- Logs statistics

### `wikidata_client.py`
- `WikidataClient` - Client for querying Wikidata

**Methods**:
- `search_item(name, city)` - Find Wikidata Q-ID for a POI
- `get_opening_date(qid)` - Get opening date (P571) for a Q-ID
- `get_poi_info(name, city)` - Get both Q-ID and opening date

**Features**:
- Rate limiting (configurable delay)
- Retry logic with exponential backoff
- Respects Wikidata User-Agent guidelines
- Handles missing/ambiguous matches gracefully

### `pipeline.py`
- `enrich_with_opening_dates(df)` - Enrich DataFrame with opening dates
- `run()` - Run complete pipeline end-to-end

**Features**:
- Progress bars (using tqdm)
- Checkpointing (saves progress periodically)
- Resume from checkpoint if interrupted
- Logs enrichment statistics

### `config.py`
Configuration settings:
- Data directory paths
- Wikidata client settings
- Pipeline settings
- Output paths

### `hours_scraper.py` (Stub)
Future module for scraping opening hours from official websites.

**Planned Functions**:
- `fetch_opening_hours(poi_name, poi_url)` - Scrape opening hours
- `fetch_opening_hours_batch(pois)` - Batch processing
- `parse_opening_hours_text(text)` - Parse text formats
- `create_opening_hours_time_series(poi_id, start_date, end_date)` - Time series

## Output Format

The enriched dataset includes all original columns plus:

- **`opening_date`**: ISO date string (YYYY-MM-DD) from Wikidata P571
- **`wikidata_id`**: Wikidata Q-ID (e.g., "Q12345")
- **`source`**: Data source identifier ("wikidata")

### Example Output

```csv
id,name,category,address,latitude,longitude,opening_date,wikidata_id,source
1,British Museum,museum,Great Russell St,51.5194,-0.1269,1753-01-01,Q6373,wikidata
2,Tower Bridge,landmark,Tower Bridge Rd,51.5055,-0.0754,1894-06-30,Q1061,wikidata
```

## Configuration

Edit `src/config.py` to customize:

```python
# Wikidata settings
WIKIDATA_RATE_LIMIT_DELAY = 1.0  # Seconds between requests
WIKIDATA_MAX_RETRIES = 3
WIKIDATA_CITY = "London"

# Pipeline settings
CHECKPOINT_INTERVAL = 50  # Save checkpoint every N POIs
PROGRESS_BAR = True  # Show progress bar
```

## Wikidata SPARQL Examples

### Search for POI in London

```sparql
SELECT ?item WHERE {
  ?item rdfs:label ?label .
  ?item wdt:P131* wd:Q84 .  # Located in London (Q84)
  FILTER(LANG(?label) = "en")
  FILTER(CONTAINS(LCASE(?label), "british museum"))
}
LIMIT 1
```

### Get Opening Date

```sparql
SELECT ?date WHERE {
  wd:Q6373 wdt:P571 ?date .  # British Museum (Q6373) inception date (P571)
}
LIMIT 1
```

## Error Handling

- **Missing POIs**: Gracefully handles POIs not found in Wikidata
- **Rate Limiting**: Automatically delays requests to respect Wikidata guidelines
- **Network Errors**: Retries with exponential backoff
- **Checkpointing**: Can resume from last checkpoint if interrupted

## Performance

- **Rate Limit**: 1 request per second (configurable)
- **Checkpointing**: Saves progress every 50 POIs (configurable)
- **Progress Tracking**: Shows progress bar with tqdm

## Future Enhancements

1. **Opening Hours Scraping**: Implement `hours_scraper.py` to fetch current opening hours
2. **Time Series**: Create historical time series of opening hours
3. **Multiple Sources**: Add other data sources for opening dates
4. **Fuzzy Matching**: Improve POI name matching with fuzzy string matching
5. **Batch Processing**: Optimize Wikidata queries with batch SPARQL queries

## Troubleshooting

### "No module named 'tqdm'"
```bash
pip install tqdm
```

### "No module named 'pyarrow'"
```bash
pip install pyarrow
```

### Wikidata rate limiting
Increase `WIKIDATA_RATE_LIMIT_DELAY` in `config.py` (e.g., to 2.0 seconds)

### Checkpoint not resuming
Delete `data/interim/opening_dates_partial.csv` to start fresh

## References

- [Wikidata SPARQL Endpoint](https://query.wikidata.org/)
- [Wikidata User-Agent Guidelines](https://www.mediawiki.org/wiki/API:Etiquette)
- [Enriched Tourism Dataset](https://data.mendeley.com/datasets/gw9hjn4v65.2)

