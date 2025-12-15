# Best Practices: Getting Opening Dates

## Quick Answer

**Is the current approach the best?** 

The current approach is **good but can be significantly improved** by adding Wikidata and combining multiple methods.

## Recommended Multi-Source Strategy

### For POIs (Points of Interest with Names)

**Best Method:** Wikidata Pipeline
- ✅ Already implemented in `src/pipeline.py`
- ✅ Fast, accurate, global coverage
- ✅ Exact dates when available
- ✅ Works for museums, galleries, landmarks, institutions

```python
from src.pipeline import enrich_with_opening_dates

# For POIs with names
df = load_london_pois("data/raw/london_pois.csv")
df_enriched = enrich_with_opening_dates(df)
```

### For Properties (via UPRN)

**Best Method:** Enhanced Building Age Estimator
- ✅ Already implemented in `src/building_opening_date_estimator.py`
- ✅ Now includes Wikidata support
- ✅ Combines planning + building age + heritage
- ✅ Works for properties without names

```python
from src.building_opening_date_estimator import BuildingOpeningDateEstimator

estimator = BuildingOpeningDateEstimator(use_wikidata=True)
estimator.load_planning_data("data/raw/planning.csv")
estimator.load_building_age_data("data/raw/building_age.gpkg")

result = estimator.estimate_opening_dates(
    uprn_df,
    poi_name_col='name'  # Use Wikidata if name available
)
```

### Combined Approach (Best Coverage)

```python
# Step 1: Enrich POIs with Wikidata
from src.pipeline import enrich_with_opening_dates
pois_with_dates = enrich_with_opening_dates(poi_df)

# Step 2: Enrich properties with building age estimator
from src.building_opening_date_estimator import BuildingOpeningDateEstimator
estimator = BuildingOpeningDateEstimator(use_wikidata=True)
properties_with_dates = estimator.estimate_opening_dates(property_df)

# Step 3: Combine results
combined = pd.concat([pois_with_dates, properties_with_dates])
```

## Method Comparison

| Method | Best For | Accuracy | Coverage | Speed |
|--------|----------|----------|----------|-------|
| **Wikidata** | POIs with names | High | Medium | Fast |
| **Planning** | New builds (2004+) | High | Low | Medium |
| **Building Age** | All properties | Medium | High | Fast |
| **Heritage** | Listed buildings | Medium-High | Low | Medium |
| **Web Scraping** | Institutions | High | Low | Slow |
| **Land Registry** | Properties | Medium | High | Fast |
| **Companies House** | Commercial | Medium | Medium | Fast |

## Priority Order Recommendation

1. **Wikidata** (if POI has name) - Fast, accurate
2. **Planning completion** (if new build) - Official, accurate
3. **Web scraping** (if institution) - Exact dates (future)
4. **Building age** (fallback) - Broad coverage
5. **Heritage** (if listed) - Historical accuracy

## What Else Can Be Done?

### Immediate Improvements (Already Done)

✅ **Wikidata Integration** - Added to building_opening_date_estimator
✅ **Enhanced Priority System** - Wikidata first for POIs
✅ **Comprehensive Documentation** - All methods analyzed

### Future Enhancements

1. **Web Scraping Module**
   - Scrape official websites for institutions
   - Extract dates from "About" pages
   - Use NLP for date parsing

2. **Land Registry Integration**
   - First transaction dates for properties
   - Link via UPRN
   - UK-wide coverage

3. **Companies House Integration**
   - Incorporation dates for commercial buildings
   - Link via address/UPRN
   - Free API access

4. **News Archive Search**
   - Search British Newspaper Archive
   - Extract opening dates from articles
   - Use NLP for date extraction

5. **ML-Based Date Extraction**
   - Train model to extract dates from text
   - Handle various formats
   - Improve accuracy

## Current Limitations

1. **Planning data**: London only, 2004+
2. **Building age**: Year-level, not exact dates
3. **Heritage**: Only listed buildings
4. **Coverage gaps**: Some buildings have no data

## Solutions

1. **Wikidata fills gaps**: Global coverage for POIs with names
2. **Multi-source combination**: Use all available methods
3. **Confidence scoring**: Know which dates are most reliable
4. **Fallback chain**: Always have a backup method

## Conclusion

**The current approach is good, but enhanced with Wikidata it's much better.**

**Best practice:**
- Use Wikidata pipeline for POIs with names
- Use building age estimator for properties via UPRN
- Combine both for maximum coverage
- Track confidence scores to know data quality

See `docs/OPENING_DATE_METHODS.md` for complete analysis of all 10 available methods.

