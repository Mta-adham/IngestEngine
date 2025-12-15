# Best Approach: Getting Building Opening Dates

## 🎯 Recommended Strategy

**Use a multi-source pipeline with intelligent fallback:**

1. **Wikidata** (for POIs with names) → Fast, accurate, global
2. **Planning completion** (for new builds) → Official, accurate  
3. **Building age** (fallback) → Broad coverage
4. **Heritage records** (for listed buildings) → Historical accuracy

## 📋 Implementation

### ✅ RECOMMENDED: Unified Pipeline (Improved)

Use the **Unified Opening Date Pipeline** - verified and improved:

```python
from src.unified_opening_date_pipeline import unified_opening_date_pipeline

# Complete pipeline with all improvements
result = unified_opening_date_pipeline(
    input_file="data/raw/london_pois.csv",
    building_age_path="data/raw/os_building_age.gpkg"  # Optional
)
```

**Key Features:**
- ✅ Input validation and data quality checks
- ✅ Vectorized operations (10-100x faster)
- ✅ Checkpoint support (resume if interrupted)
- ✅ Comprehensive error handling
- ✅ Data quality metrics and reporting
- ✅ Safe date handling (no NaT errors)

### Option 1: For POIs with Names Only

Use the **Wikidata Pipeline** - already implemented and tested:

```python
from src.pipeline import run

# Complete pipeline: Load → Enrich → Save
run(
    input_file="data/raw/london_pois.csv",
    output_csv="data/processed/london_pois_opening_dates.csv",
    output_parquet="data/processed/london_pois_opening_dates.parquet"
)
```

**Best for:**
- POIs with known names (museums, galleries, landmarks)
- Fast processing
- Exact dates when available
- Global coverage

### Option 2: For Properties via UPRN

Use the **Enhanced Building Opening Date Estimator**:

```python
from src.building_opening_date_estimator import BuildingOpeningDateEstimator

# Initialize with Wikidata support
estimator = BuildingOpeningDateEstimator(use_wikidata=True)

# Load data sources
estimator.load_planning_data("data/raw/london_planning_data.csv")
estimator.load_building_age_data("data/raw/os_building_age.gpkg")
estimator.load_heritage_data("data/raw/heritage_list.csv")

# Estimate dates (uses Wikidata if POI name available)
result = estimator.estimate_opening_dates(
    uprn_df,
    uprn_col='UPRN',
    poi_name_col='name'  # Enables Wikidata lookup
)

# Save results
result.to_csv("data/processed/uprns_with_opening_dates.csv", index=False)
```

**Best for:**
- Properties with UPRN
- Combining multiple UK data sources
- Properties without names (uses building age)

### Option 3: Combined Approach (Maximum Coverage)

**Use both pipelines together for best results:**

```python
import pandas as pd
from src.pipeline import enrich_with_opening_dates
from src.building_opening_date_estimator import BuildingOpeningDateEstimator
from src.data_loader import load_london_pois

# Step 1: Load POI data
df = load_london_pois("data/raw/london_pois.csv")

# Step 2: Enrich POIs with Wikidata (for POIs with names)
print("Enriching POIs with Wikidata...")
df_pois = enrich_with_opening_dates(df)

# Step 3: For properties without Wikidata dates, use building age estimator
print("Enriching properties with building age data...")
estimator = BuildingOpeningDateEstimator(use_wikidata=True)
estimator.load_building_age_data("data/raw/os_building_age.gpkg")

# Only process rows without opening dates
missing_dates = df_pois[df_pois['opening_date'].isna()].copy()
if len(missing_dates) > 0 and 'UPRN' in missing_dates.columns:
    # Add building age dates for those without Wikidata dates
    missing_with_age = estimator.estimate_opening_dates(
        missing_dates,
        uprn_col='UPRN',
        priority_order=['building_age', 'heritage']  # Skip Wikidata (already tried)
    )
    
    # Merge back
    df_pois.loc[missing_dates.index, 'estimated_opening_date'] = missing_with_age['estimated_opening_date']
    df_pois.loc[missing_dates.index, 'opening_date_source'] = missing_with_age['opening_date_source']

# Step 4: Save final combined results
df_pois.to_csv("data/processed/london_pois_complete_opening_dates.csv", index=False)
print(f"\n✓ Saved {len(df_pois):,} POIs with opening dates")
print(f"  Wikidata: {df_pois[df_pois['opening_date_source'] == 'wikidata'].shape[0]:,}")
print(f"  Building age: {df_pois[df_pois['opening_date_source'] == 'building_age'].shape[0]:,}")
```

## 🏆 Best Practice: Priority Order

```
For each POI/Property:

1. Try Wikidata (if name available)
   ├─ Success? → Use date (HIGH confidence)
   └─ Fail? → Continue

2. Try Planning completion (if UPRN available)
   ├─ Success? → Use date (HIGH confidence)
   └─ Fail? → Continue

3. Try Building age (if UPRN available)
   ├─ Success? → Use date (MEDIUM confidence)
   └─ Fail? → Continue

4. Try Heritage records (if listed)
   ├─ Success? → Use date (MEDIUM confidence)
   └─ Fail? → Mark as missing
```

## 📊 Expected Results

### Coverage by Method

| Method | Coverage | Accuracy | Speed |
|--------|----------|----------|-------|
| Wikidata | ~30-40% of POIs | High | Fast |
| Planning | ~10-20% (new builds) | High | Medium |
| Building Age | ~60-80% of properties | Medium | Fast |
| Heritage | ~5-10% (listed only) | Medium-High | Medium |
| **Combined** | **~70-90%** | **High-Medium** | **Medium** |

### Typical Results

- **POIs with names**: 30-40% get Wikidata dates (exact dates)
- **New builds (2004+)**: 10-20% get planning dates (exact dates)
- **All properties**: 60-80% get building age (year-level)
- **Listed buildings**: 5-10% get heritage dates (exact or year-level)
- **Overall coverage**: 70-90% with at least one date source

## 🚀 Quick Start

### Simplest Approach (RECOMMENDED)

```python
from src.unified_opening_date_pipeline import unified_opening_date_pipeline

result = unified_opening_date_pipeline("data/raw/london_pois.csv")
# That's it! Includes validation, checkpoints, and comprehensive reporting
```

### Alternative: Wikidata Only (Fast)

```python
from src.pipeline import run
run()  # Wikidata only, faster but less coverage
```

### Complete Approach (POIs + Properties)

```python
from src.building_opening_date_estimator import BuildingOpeningDateEstimator

estimator = BuildingOpeningDateEstimator(use_wikidata=True)
estimator.load_building_age_data("data/raw/os_building_age.gpkg")

result = estimator.estimate_opening_dates(
    your_df,
    poi_name_col='name'  # Enables Wikidata
)
```

## 💡 Key Insights

1. **Wikidata is best for POIs** - Fast, accurate, exact dates
2. **Building age is best for properties** - Broad coverage, year-level
3. **Planning is best for new builds** - Official, accurate
4. **Combining methods maximizes coverage** - Use all available sources

## ⚙️ Configuration

### For Maximum Accuracy

```python
# Prioritize exact dates
priority_order = ['wikidata', 'planning', 'heritage', 'building_age']
```

### For Maximum Coverage

```python
# Prioritize broad coverage
priority_order = ['wikidata', 'building_age', 'planning', 'heritage']
```

### For Speed

```python
# Skip slower methods
priority_order = ['wikidata', 'building_age']  # Skip planning/heritage
```

## 📈 Performance Tips

1. **Cache Wikidata results** - Don't re-query same POIs
2. **Batch processing** - Process in chunks for large datasets
3. **Parallel processing** - Use multiprocessing for Wikidata queries
4. **Checkpointing** - Already built into pipeline

## 🎯 Recommendation

**For your use case (London POIs + Properties):**

1. **Start with Wikidata pipeline** for POIs with names
2. **Add building age estimator** for properties via UPRN
3. **Combine results** for maximum coverage
4. **Track confidence scores** to know data quality

**This gives you:**
- ✅ High accuracy (Wikidata + Planning)
- ✅ Broad coverage (Building age)
- ✅ Historical context (Heritage)
- ✅ Confidence scoring (know what's reliable)

## 📚 Related Documentation

- [Opening Date Methods](OPENING_DATE_METHODS.md) - All 10 methods analyzed
- [Wikidata Enrichment](WIKIDATA_ENRICHMENT.md) - Wikidata pipeline guide
- [Building Opening Dates](BUILDING_OPENING_DATES.md) - Building age estimator guide
- [Best Practices](OPENING_DATE_BEST_PRACTICES.md) - Best practices guide

