# Maximizing Coverage: From 70-90% to 100%

## Current Coverage: 70-90%

### Why Not 100%?

**Current Limitations:**
1. **POIs without names** → Can't use Wikidata
2. **Properties without UPRNs** → Can't use building age/planning
3. **Very old buildings** → No planning records (pre-2004)
4. **Very new buildings** → Not yet in datasets
5. **Unregistered businesses** → Not in Companies House
6. **Private properties** → No public records
7. **Temporary structures** → Not in permanent datasets

## 🎯 Strategies to Reach 100% Coverage

### Strategy 1: Use ALL Available Data Sources ✅ (Already Done)

**Current**: 7 sources integrated
- Wikidata
- Companies House
- Planning
- Land Registry
- Building Age (OS/NGD)
- EPC
- Heritage

**Action**: Ensure all data sources are loaded and used

```python
from src.opening_dates.unified_opening_date_pipeline import unified_opening_date_pipeline

result = unified_opening_date_pipeline(
    input_file="data/raw/london_pois.csv",
    # Load ALL available sources
    companies_house_path="data/raw/companies_house.csv",
    land_registry_path="data/raw/land_registry_price_paid.csv",
    epc_path="data/raw/epc_data.csv",
    planning_path="data/raw/planning_data.csv",
    building_age_path="data/raw/os_building_age.gpkg",
    heritage_path="data/raw/heritage_list.csv",
    use_wikidata=True,
    use_building_age=True
)
```

**Expected**: 85-95% coverage (up from 70-90%)

---

### Strategy 2: Add Web Scraping for POIs

**Target**: POIs with websites but no Wikidata entry

**Approach**:
1. Extract website URLs from OSM data
2. Scrape "About" pages for opening dates
3. Use NLP to extract dates from text

**Implementation**:
```python
# Future: src/data/hours_scraper.py (currently stub)
def scrape_opening_date_from_website(url: str, name: str) -> Optional[str]:
    """
    Scrape opening date from POI website
    
    Looks for:
    - "Founded in 19XX"
    - "Established 19XX"
    - "Opened in 19XX"
    - "Since 19XX"
    """
    # TODO: Implement web scraping
    pass
```

**Expected Coverage Increase**: +5-10% for POIs with websites

---

### Strategy 3: Use Historical Maps & Aerial Imagery

**Target**: Very old buildings (pre-2004)

**Approach**:
1. Use historical Ordnance Survey maps
2. Analyze aerial imagery changes over time
3. First appearance = approximate opening date

**Data Sources**:
- **Historic OS Maps**: National Library of Scotland
- **Aerial Imagery**: Google Earth Timelapse, Historic England
- **Street View History**: Google Street View (if available)

**Expected Coverage Increase**: +5-10% for old buildings

---

### Strategy 4: Use News Archives & Historical Records

**Target**: Buildings mentioned in news articles

**Approach**:
1. Search British Newspaper Archive
2. Search local newspaper archives
3. Extract opening dates from articles

**Implementation**:
```python
def search_news_archives(name: str, address: str) -> Optional[str]:
    """
    Search news archives for opening date mentions
    
    Queries:
    - "{name} opened"
    - "{name} opening"
    - "{address} new building"
    """
    # TODO: Implement news archive search
    pass
```

**Expected Coverage Increase**: +3-5% for notable buildings

---

### Strategy 5: Use Address History & Postcode Changes

**Target**: Properties with address changes

**Approach**:
1. Use Royal Mail Postcode Address File (PAF) history
2. Track when addresses first appeared
3. Use as proxy for opening date

**Data Sources**:
- **Royal Mail PAF**: Address history
- **ONS Postcode Directory**: Postcode creation dates

**Expected Coverage Increase**: +2-5% for properties

---

### Strategy 6: Use Machine Learning Estimation

**Target**: Remaining gaps after all data sources

**Approach**:
1. Train ML model on buildings with known dates
2. Use features: location, building type, nearby buildings, etc.
3. Predict opening date for missing buildings

**Features**:
- Building type (residential, commercial, etc.)
- Location (postcode, area characteristics)
- Nearby buildings' ages
- Street characteristics
- Historical development patterns

**Expected Coverage Increase**: +5-10% with reasonable accuracy

---

### Strategy 7: Use Building Permits & Licenses

**Target**: Commercial buildings, restaurants, shops

**Approach**:
1. Scrape local authority licensing data
2. Extract first license/permit date
3. Use as opening date proxy

**Data Sources**:
- **Food Hygiene Ratings**: First rating date
- **Alcohol Licenses**: First license date
- **Business Licenses**: First license date

**Expected Coverage Increase**: +3-7% for commercial POIs

---

### Strategy 8: Use Social Media & Check-ins

**Target**: POIs with social media presence

**Approach**:
1. Query Foursquare/Swarm API for first check-in
2. Query Google Places API for establishment date
3. Query Facebook Places for creation date

**Expected Coverage Increase**: +2-5% for popular POIs

---

### Strategy 9: Use Census & Electoral Roll Data

**Target**: Residential properties

**Approach**:
1. Use census data to identify when properties first appeared
2. Use electoral roll to identify first residents
3. Use as proxy for opening date

**Data Sources**:
- **Census 2021, 2011, 2001**: Property existence
- **Electoral Roll**: First registered residents

**Expected Coverage Increase**: +3-5% for residential

---

### Strategy 10: Manual Curation for Remaining Gaps

**Target**: Final 1-5% that can't be automated

**Approach**:
1. Identify buildings still missing dates
2. Manual research for high-value properties
3. Crowdsourcing for community knowledge

**Tools**:
- Google Search
- Local history websites
- Community forums
- Historical societies

**Expected Coverage Increase**: +1-5% (manual effort)

---

## 📊 Coverage Projection

| Strategy | Current | After Strategy | Cumulative |
|----------|---------|----------------|------------|
| **Baseline** | 70-90% | - | 70-90% |
| **All 7 Sources** | 70-90% | 85-95% | 85-95% |
| **+ Web Scraping** | 85-95% | 90-98% | 90-98% |
| **+ Historical Maps** | 90-98% | 93-99% | 93-99% |
| **+ News Archives** | 93-99% | 95-99.5% | 95-99.5% |
| **+ ML Estimation** | 95-99.5% | 97-99.8% | 97-99.8% |
| **+ Manual Curation** | 97-99.8% | 99-100% | 99-100% |

---

## 🚀 Recommended Implementation Order

### Phase 1: Immediate (85-95% coverage)
1. ✅ **Use all 7 integrated sources** (already done)
2. ✅ **Ensure all data files are loaded**
3. ✅ **Optimize priority ordering**

### Phase 2: Quick Wins (90-98% coverage)
4. **Web scraping** for POIs with websites
5. **Companies House API** (real-time queries)
6. **Land Registry API** (real-time queries)

### Phase 3: Advanced (95-99% coverage)
7. **Historical maps analysis**
8. **News archive search**
9. **Social media APIs** (Foursquare, Google Places)

### Phase 4: Final Push (99-100% coverage)
10. **ML-based estimation**
11. **Manual curation** for high-value properties
12. **Crowdsourcing** for community knowledge

---

## 💡 Practical Steps to Increase Coverage

### Step 1: Ensure All Data Sources Are Loaded

```python
# Check what's missing
result = unified_opening_date_pipeline("data/raw/london_pois.csv")
missing = result[result['opening_date'].isna()]

print(f"Missing dates: {len(missing):,}")
print(f"Missing with names: {missing['name'].notna().sum():,}")
print(f"Missing with UPRN: {missing['UPRN'].notna().sum():,}")
print(f"Missing with postcode: {missing['postcode'].notna().sum():,}")
```

### Step 2: Add Missing Data Sources

```python
# Download and load additional sources
# 1. Companies House bulk data
# 2. Land Registry Price Paid Data
# 3. EPC data (domestic + non-domestic)
# 4. Planning data (if not already loaded)
```

### Step 3: Implement Web Scraping

```python
# For POIs with websites but no Wikidata
missing_with_website = missing[missing['website'].notna()]
for idx, row in missing_with_website.iterrows():
    date = scrape_opening_date_from_website(row['website'], row['name'])
    if date:
        result.loc[idx, 'opening_date'] = date
        result.loc[idx, 'opening_date_source'] = 'web_scraping'
```

### Step 4: Use ML Estimation for Remaining

```python
# Train model on buildings with known dates
from sklearn.ensemble import RandomForestRegressor

# Features: building_type, postcode, nearby_buildings_age, etc.
model = RandomForestRegressor()
model.fit(X_train, y_train)  # X = features, y = opening_year

# Predict for missing
missing_features = extract_features(missing)
predicted_years = model.predict(missing_features)
result.loc[missing.index, 'opening_date_year'] = predicted_years
result.loc[missing.index, 'opening_date_source'] = 'ml_estimation'
result.loc[missing.index, 'opening_date_confidence'] = 'low'
```

---

## ⚠️ Important Considerations

### Why 100% May Not Be Achievable

1. **Data doesn't exist**: Some buildings have no records
2. **Privacy restrictions**: Some data is not publicly available
3. **Cost**: Some data sources require payment
4. **Accuracy trade-off**: 100% coverage may mean lower accuracy
5. **Temporary structures**: Not meant to have permanent records

### Recommended Target: 95-98%

**Why 95-98% is realistic:**
- Most buildings have some record
- Remaining 2-5% are edge cases
- Better to have high accuracy on 95% than low accuracy on 100%

**For remaining 2-5%:**
- Use ML estimation with low confidence
- Mark as "estimated" in confidence field
- Allow manual override

---

## 📋 Action Plan

### Immediate Actions (This Week)

1. **Verify all 7 sources are loaded**
   ```bash
   # Check data files exist
   ls -lh data/raw/*.csv data/raw/*.gpkg
   ```

2. **Run pipeline with all sources**
   ```python
   result = unified_opening_date_pipeline(
       input_file="data/raw/london_pois.csv",
       companies_house_path="data/raw/companies_house.csv",
       land_registry_path="data/raw/land_registry_price_paid.csv",
       epc_path="data/raw/epc_data.csv",
       planning_path="data/raw/planning_data.csv",
       building_age_path="data/raw/os_building_age.gpkg",
       heritage_path="data/raw/heritage_list.csv"
   )
   ```

3. **Analyze gaps**
   ```python
   missing = result[result['opening_date'].isna()]
   print(f"Coverage: {result['opening_date'].notna().sum() / len(result) * 100:.1f}%")
   print(f"Missing: {len(missing):,}")
   ```

### Short-term (Next Month)

4. **Implement web scraping** for POIs with websites
5. **Add Companies House API** integration
6. **Add Land Registry API** integration

### Long-term (Next Quarter)

7. **Historical maps analysis**
8. **News archive search**
9. **ML-based estimation**

---

## 📊 Expected Results

### Current State
- **Coverage**: 70-90%
- **Sources**: 7 integrated
- **Accuracy**: High-Medium

### After All Strategies
- **Coverage**: 95-98% (realistic)
- **Sources**: 15+ (7 data + 8 strategies)
- **Accuracy**: High (with confidence scoring)

### Remaining 2-5%
- **ML estimation**: Low confidence
- **Manual curation**: High-value properties only
- **Marked as "estimated"**: Users know it's less reliable

---

## 🎯 Conclusion

**To reach 100% coverage:**

1. ✅ **Use all 7 integrated sources** (gets you to 85-95%)
2. **Add web scraping** (gets you to 90-98%)
3. **Add historical analysis** (gets you to 95-99%)
4. **Add ML estimation** (gets you to 97-99.8%)
5. **Manual curation** (gets you to 99-100%)

**Realistic target: 95-98%** with high accuracy, rather than 100% with lower accuracy.


