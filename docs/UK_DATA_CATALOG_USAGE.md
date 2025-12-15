# UK Data Catalog Usage Analysis

## Current Status: **NOT Using All Available Data**

The unified pipeline currently uses **only 3-4 sources** from the catalog, but the catalog contains **34+ data sources** that could be valuable for opening dates and other enrichments.

## 📊 Current Usage

### Currently Integrated Sources

1. **Planning Data** ✅
   - Source: "National Planning Applications" (from catalog)
   - Used for: Completion dates (2004+)
   - Status: Integrated in `building_opening_date_estimator.py`

2. **Building Age Data** ✅
   - Source: OS/NGD Buildings (from catalog: "OS OpenMap Local", "OS NGD Buildings")
   - Used for: Estimated build year/period
   - Status: Integrated in `building_opening_date_estimator.py`

3. **Heritage Data** ✅
   - Source: Historic England (not explicitly in catalog but referenced)
   - Used for: Construction dates for listed buildings
   - Status: Integrated in `building_opening_date_estimator.py`

4. **Wikidata** ✅
   - Source: External (not in UK catalog)
   - Used for: POI opening/inception dates
   - Status: Integrated in `pipeline.py` and `unified_opening_date_pipeline.py`

## 📋 Available But NOT Used

### High-Value Sources for Opening Dates

1. **Companies House** ⚠️ NOT USED
   - **REST API**: Company incorporation dates (1986+)
   - **Bulk Data**: Daily snapshots of all UK companies
   - **Use case**: Opening dates for businesses, shops, restaurants
   - **Coverage**: 1986+ (very comprehensive)
   - **Priority**: HIGH - Could significantly improve coverage

2. **Land Registry Price Paid Data** ⚠️ NOT USED
   - **First transaction dates**: First sale date for new builds
   - **Use case**: Opening date proxy for residential properties
   - **Coverage**: 1995+ (very comprehensive)
   - **Priority**: HIGH - Good proxy for residential opening dates

3. **EPC Data** ⚠️ NOT USED
   - **Domestic EPCs**: Construction age field (2008+)
   - **Non-Domestic EPCs**: Construction age for commercial (2008+)
   - **Use case**: Building construction age (already have OS, but EPC is more detailed)
   - **Coverage**: 2008+ (20M+ properties)
   - **Priority**: MEDIUM - Redundant with OS building age but more detailed

4. **VOA Non-Domestic Rating List** ⚠️ NOT USED
   - **Property records**: Commercial property data
   - **Use case**: Could contain opening dates for commercial properties
   - **Coverage**: 1990+ (revaluations)
   - **Priority**: LOW-MEDIUM - May not have opening dates directly

### Other Valuable Sources (Not Directly for Opening Dates)

5. **OS Code-Point Open** ⚠️ NOT USED
   - Postcode centroids (1.7M+)
   - **Use case**: Geocoding, spatial joins
   - **Priority**: HIGH - Essential for spatial operations

6. **ONS Postcode Directory** ⚠️ NOT USED
   - Postcodes linked to all census geographies
   - **Use case**: Geographic joins, demographic enrichment
   - **Priority**: HIGH - Foundation dataset

7. **Census 2021** ⚠️ NOT USED
   - Full census at OA/LSOA/MSOA level
   - **Use case**: Demographic enrichment
   - **Priority**: MEDIUM - For broader enrichment

8. **TfL Unified API** ⚠️ NOT USED
   - London transport data
   - **Use case**: Transport accessibility (affects property values)
   - **Priority**: MEDIUM - London-specific enrichment

9. **Police.uk API** ⚠️ NOT USED
   - Street-level crime data
   - **Use case**: Safety metrics (affects property values)
   - **Priority**: MEDIUM - For broader enrichment

10. **Ofcom Connected Nations** ⚠️ NOT USED
    - Broadband speeds by postcode
    - **Use case**: Connectivity metrics (affects property values)
    - **Priority**: MEDIUM - For broader enrichment

## 🎯 Recommended Integration Priority

### Phase 1: Opening Dates (High Impact)

1. **Companies House** (HIGHEST PRIORITY)
   - Incorporation date = business opening date
   - Covers: Shops, restaurants, businesses, offices
   - Implementation: Add `load_companies_house_data()` method
   - Expected coverage increase: +10-20% for commercial POIs

2. **Land Registry First Transaction** (HIGH PRIORITY)
   - First sale date = residential opening date proxy
   - Covers: Residential properties, new builds
   - Implementation: Add `load_land_registry_data()` method
   - Expected coverage increase: +15-25% for residential properties

3. **EPC Construction Age** (MEDIUM PRIORITY)
   - More detailed than OS building age
   - Covers: Properties with EPCs (2008+)
   - Implementation: Enhance existing building age with EPC data
   - Expected coverage increase: +5-10% (mostly refinement)

### Phase 2: Enrichment (Broader Value)

4. **OS Code-Point / ONSPD** (HIGH PRIORITY)
   - Foundation for spatial operations
   - Implementation: Use in spatial joins
   - Impact: Better geocoding and spatial matching

5. **Census / Demographics** (MEDIUM PRIORITY)
   - Demographic enrichment
   - Implementation: Add demographic enrichment module
   - Impact: Broader dataset value

## 📈 Expected Impact

### Current Coverage
- Wikidata: 30-40% of POIs
- Planning: 10-20% (new builds)
- Building age: 60-80% of properties
- **Total: 70-90% coverage**

### With Additional Sources
- Wikidata: 30-40% of POIs
- Planning: 10-20% (new builds)
- Building age: 60-80% of properties
- **Companies House: +10-20% for commercial POIs**
- **Land Registry: +15-25% for residential**
- **EPC: +5-10% (refinement)**
- **Total: 85-95% coverage** (estimated)

## 🔧 Implementation Plan

### Step 1: Add Companies House Integration

```python
# In building_opening_date_estimator.py
def load_companies_house_data(self, companies_house_path: str):
    """Load Companies House data with incorporation dates"""
    # Load CSV/API data
    # Extract incorporation_date
    # Join on company_name or address
    pass
```

### Step 2: Add Land Registry Integration

```python
def load_land_registry_data(self, land_registry_path: str):
    """Load Land Registry Price Paid Data"""
    # Load transaction data
    # Find first transaction per property
    # Use as opening date proxy
    pass
```

### Step 3: Enhance Priority Order

```python
priority_order = [
    'wikidata',           # POIs with names
    'companies_house',    # Businesses (NEW)
    'planning',           # New builds
    'land_registry',      # Residential (NEW)
    'building_age',       # Fallback
    'epc',                # Refinement (NEW)
    'heritage'            # Listed buildings
]
```

## 📝 Summary

**Current State:**
- ✅ Using 3-4 sources (Planning, Building Age, Heritage, Wikidata)
- ⚠️ NOT using 30+ other valuable sources
- 📊 Coverage: 70-90%

**Potential State:**
- ✅ Using 7-8 sources (add Companies House, Land Registry, EPC)
- 📊 Coverage: 85-95% (estimated)
- 🚀 Much more comprehensive dataset

**Recommendation:**
1. **Immediate**: Add Companies House integration (biggest impact)
2. **Short-term**: Add Land Registry integration
3. **Medium-term**: Add EPC refinement
4. **Long-term**: Add other enrichment sources (demographics, transport, etc.)

