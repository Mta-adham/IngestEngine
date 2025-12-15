# Building Opening Date Estimation Guide

## Overview

This guide explains how to estimate building opening dates by combining multiple data sources. There is no single official "opening date" field for every building, so we combine:

1. **Planning completion dates** (highest priority) - from London Planning Datahub
2. **Building age from OS/NGD** - estimated build year/period
3. **Heritage/listed building records** - construction dates for historic buildings

## Priority System

The estimator uses a priority system (enhanced version):

1. **Wikidata** (HIGH confidence) - **NEW**
   - For POIs with known names
   - Queries Wikidata SPARQL endpoint
   - Retrieves inception/opening date (P571 property)
   - Best for: Museums, galleries, landmarks, institutions
   - See [Wikidata Enrichment Guide](WIKIDATA_ENRICHMENT.md)

2. **Planning completion date** (HIGH confidence)
   - Most accurate for new builds (2004+)
   - Completion/occupation date is a good proxy for opening
   - Available from London Planning Datahub

3. **Building age year** (MEDIUM confidence)
   - From OS/NGD building datasets
   - Estimated build year or period
   - Often close to opening year for residential/institutional buildings

4. **Heritage construction date** (MEDIUM confidence)
   - From Historic England's National Heritage List
   - Mainly for older or significant buildings
   - May contain original construction dates

**Note:** See [Opening Date Methods Guide](OPENING_DATE_METHODS.md) for comprehensive comparison of all available methods.

## Data Sources

### 1. Planning Data (London Planning Datahub)

**Source:** https://www.london.gov.uk/what-we-do/planning/planning-databases/london-planning-datahub

**What it contains:**
- Planning permissions from 2004 onwards
- Start dates and completion dates
- Development information

**How to use:**
- Download planning data CSV or GeoPackage
- Should contain UPRN and completion_date columns
- Completion date is used as opening date proxy

**Coverage:** London only, 2004+

### 2. Building Age Data (OS/NGD)

**Source:** Ordnance Survey's building products
- OS NGD Buildings dataset
- OS Building dataset

**What it contains:**
- Building age year (estimated)
- Building age period (e.g., "1980-1989")
- Derived from multiple data sources

**How to use:**
- Download OS building data (GeoPackage/Shapefile)
- Should contain UPRN and building_age_year or building_age_period
- Age year is used as opening year (converted to Jan 1st of that year)

**Coverage:** UK-wide, various periods

### 3. Heritage Data (Historic England)

**Source:** https://historicengland.org.uk/listing/the-list/

**What it contains:**
- Listed building records
- Construction dates (sometimes)
- Descriptive text with dates

**How to use:**
- Download National Heritage List data
- Should contain UPRN and construction_date
- May need to extract dates from description text

**Coverage:** UK-wide, mainly historic buildings

## Usage

### Method 1: Standalone Estimator (with Wikidata)

```python
from src.building_opening_date_estimator import BuildingOpeningDateEstimator

# Initialize with Wikidata support
estimator = BuildingOpeningDateEstimator(use_wikidata=True)

# Load data sources
estimator.load_planning_data("data/london_planning_data.csv")
estimator.load_building_age_data("data/os_building_age.gpkg")
estimator.load_heritage_data("data/heritage_list.csv")

# Load UPRNs to estimate dates for (with POI names if available)
uprn_df = pd.read_csv("data/uprns.csv")

# Estimate opening dates (with Wikidata for POIs with names)
result = estimator.estimate_opening_dates(
    uprn_df,
    uprn_col='UPRN',
    poi_name_col='name'  # Use this column for Wikidata lookup
)

# Save results
result.to_csv("data/uprns_with_opening_dates.csv", index=False)
```

### Method 1b: Enhanced Estimator

```python
from src.enhanced_opening_date_estimator import EnhancedOpeningDateEstimator

# Use enhanced estimator with all methods
estimator = EnhancedOpeningDateEstimator(use_wikidata=True)

# Estimate with custom priority
result = estimator.estimate_opening_dates_enhanced(
    df,
    poi_name_col='name',
    priority_order=['wikidata', 'planning', 'building_age', 'heritage']
)
```

### Method 2: Integrated with Dataset Joiner

```python
from src.dataset_joiner import DatasetJoiner

joiner = DatasetJoiner(...)
joiner.load_datasets()

# If you have UPRN data with opening dates needed
uprn_df = pd.read_csv("data/uprns.csv")

# Add opening dates
result = joiner.add_opening_dates_to_uprns(
    uprn_df,
    planning_path="data/london_planning_data.csv",
    building_age_path="data/os_building_age.gpkg",
    heritage_path="data/heritage_list.csv"
)
```

### Method 3: Combine with Existing Joins

```python
# After joining OSM POIs to UPRN
osm_with_uprn = joiner.join_osm_to_uprn_spatial(...)

# Add opening dates
osm_with_dates = joiner.add_opening_dates_to_uprns(
    osm_with_uprn,
    planning_path="data/london_planning_data.csv",
    building_age_path="data/os_building_age.gpkg"
)

# Now you have POIs with UPRN and opening dates!
```

## Output Columns

The estimator adds these columns:

- **`estimated_opening_date`**: The estimated opening date (datetime)
- **`opening_date_year`**: Year only (int)
- **`opening_date_source`**: Which source was used ('planning_completion', 'building_age', 'heritage')
- **`opening_date_confidence`**: Confidence level ('high', 'medium')

## Example Analysis

```python
import pandas as pd

# Load results
df = pd.read_csv("data/uprns_with_opening_dates.csv")

# Statistics
print(f"Total UPRNs: {len(df):,}")
print(f"With dates: {df['estimated_opening_date'].notna().sum():,}")

# By source
print("\nDates by source:")
print(df['opening_date_source'].value_counts())

# By confidence
print("\nDates by confidence:")
print(df['opening_date_confidence'].value_counts())

# Date range
if df['estimated_opening_date'].notna().any():
    print(f"\nDate range:")
    print(f"  Earliest: {df['estimated_opening_date'].min()}")
    print(f"  Latest: {df['estimated_opening_date'].max()}")

# Analyze by building type (if you have building type data)
# For example, schools vs student flats
```

## Customizing Priority Order

You can change the priority order:

```python
result = estimator.estimate_opening_dates(
    uprn_df,
    priority_order=['building_age', 'planning', 'heritage']  # Building age first
)
```

## Data Source Requirements

### Planning Data Format

Required columns:
- `UPRN` (or configurable column name)
- `completion_date` (or `occupation_date`, `completion_date`)

Optional columns:
- `start_date`
- `development_type`
- `description`

### Building Age Data Format

Required columns:
- `UPRN` (or configurable column name)
- `building_age_year` OR `building_age_period`

If only period is available (e.g., "1980-1989"), the estimator extracts the start year.

### Heritage Data Format

Required columns:
- `UPRN` (or configurable column name)
- `construction_date` OR `description` (with dates in text)

The estimator can extract years from description text if no explicit date column exists.

## Limitations

1. **No universal opening date registry**: Government datasets don't store exact opening dates for every building
2. **Approximation**: Most results are build/completion years, not exact opening dates
3. **Coverage varies**: 
   - Planning data: London only, 2004+
   - Building age: UK-wide but may have gaps
   - Heritage: Only listed buildings
4. **Historic buildings**: May need manual research for exact dates

## Practical Expectations

- **Modern buildings (2004+)**: Often have planning completion dates (high confidence)
- **Typical buildings**: Usually get build year from OS/NGD (medium confidence)
- **Historic buildings**: May have heritage records (medium confidence)
- **Landmark sites**: May need manual curation for exact dates

## Next Steps

After estimating opening dates:

1. **Combine with EPC data**: Link energy performance to building age
2. **Analyze trends**: Study how building age affects energy efficiency
3. **Temporal analysis**: Track changes over time
4. **Property valuation**: Use opening dates in property analysis

## Troubleshooting

### "No dates estimated"

- Check that data sources have UPRN columns matching your data
- Verify date columns exist and are parseable
- Check that UPRNs in your data match UPRNs in source data

### "Low coverage"

- Try loading multiple data sources
- Check data source coverage for your area
- Consider manual research for important buildings

### "Date parsing errors"

- Check date column formats
- Ensure dates are in standard format (YYYY-MM-DD or similar)
- The estimator handles common formats automatically

