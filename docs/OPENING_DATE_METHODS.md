# Comprehensive Guide: Getting Building Opening Dates

## Overview

There is no single "official opening date" field for every building. This guide covers all available methods, ranked by accuracy and coverage.

## Current Approach (Building Opening Date Estimator)

The current `building_opening_date_estimator.py` combines:

1. **Planning completion dates** (HIGH confidence) - London Planning Datahub
2. **Building age from OS/NGD** (MEDIUM confidence) - Estimated build year
3. **Heritage/listed building records** (MEDIUM confidence) - Historic England

**Limitations:**
- Planning data: London only, 2004+
- Building age: Often just a year, not exact date
- Heritage: Only covers listed buildings

## Enhanced Approach: Multi-Source Integration

### Method 1: Wikidata (NEW - Already Implemented)

**Accuracy:** HIGH (when available)  
**Coverage:** Global, millions of entities  
**Best for:** Famous landmarks, museums, institutions

**Advantages:**
- Structured data (P571 inception date)
- Global coverage
- Well-maintained
- Can be queried programmatically

**Implementation:**
```python
from src.wikidata_client import WikidataClient

client = WikidataClient()
info = client.get_poi_info("British Museum", "London")
# Returns: {'wikidata_id': 'Q6373', 'opening_date': '1753-01-01'}
```

**When to use:**
- POIs with known names
- Famous landmarks
- Museums, galleries, institutions
- Can combine with building age estimator

### Method 2: Planning Data (Current)

**Accuracy:** HIGH for new builds  
**Coverage:** London 2004+, other areas vary  
**Best for:** Modern buildings, developments

**Sources:**
- London Planning Datahub
- Local authority planning portals
- National Planning Applications API

**Advantages:**
- Official completion dates
- Very accurate for new builds
- Includes development details

**Limitations:**
- Limited historical coverage
- Not all areas have open data
- May need to parse PDFs/HTML

### Method 3: Building Age (OS/NGD) (Current)

**Accuracy:** MEDIUM (year-level)  
**Coverage:** UK-wide  
**Best for:** Residential, commercial buildings

**Sources:**
- OS NGD Buildings
- OS Building dataset
- Verisk building age data

**Advantages:**
- UK-wide coverage
- Available for most buildings
- Derived from multiple sources

**Limitations:**
- Usually just a year, not exact date
- May be estimated
- Not always accurate for opening vs construction

### Method 4: Heritage Records (Current)

**Accuracy:** MEDIUM-HIGH (when available)  
**Coverage:** Listed buildings only  
**Best for:** Historic buildings, landmarks

**Sources:**
- Historic England National Heritage List
- Historic Environment Scotland
- Cadw (Wales)

**Advantages:**
- Detailed historical information
- Often includes original construction dates
- Well-documented

**Limitations:**
- Only covers listed buildings
- May need text parsing
- Not all have explicit dates

### Method 5: Land Registry Data

**Accuracy:** MEDIUM (indirect signal)  
**Coverage:** UK-wide  
**Best for:** Properties with transaction history

**Sources:**
- Land Registry Price Paid Data
- Land Registry INSPIRE Polygons
- Title registers

**How it works:**
- First transaction date for new-build properties
- Can indicate when property became available
- Not always opening date, but close

**Advantages:**
- UK-wide coverage
- Official records
- Historical data available

**Limitations:**
- Indirect signal (transaction date ≠ opening date)
- May miss properties never sold
- Requires property matching

### Method 6: Companies House Data

**Accuracy:** MEDIUM (for commercial properties)  
**Coverage:** UK companies  
**Best for:** Commercial buildings, businesses

**Sources:**
- Companies House API
- Companies House Bulk Data

**How it works:**
- Incorporation date for companies
- Registered address can link to building
- For purpose-built commercial buildings, incorporation ≈ opening

**Advantages:**
- Free API access
- Historical data (1986+)
- Can link via address/UPRN

**Limitations:**
- Only for commercial properties
- Incorporation ≠ building opening
- Requires address matching

### Method 7: EPC Data (Energy Performance Certificates)

**Accuracy:** LOW-MEDIUM (indirect)  
**Coverage:** UK buildings with EPCs  
**Best for:** Buildings assessed for energy

**How it works:**
- First EPC date can indicate when building was first assessed
- For new builds, first EPC ≈ opening
- Building age fields in EPC data

**Advantages:**
- Large dataset (millions of records)
- Includes building age estimates
- Can link via UPRN

**Limitations:**
- EPC date ≠ opening date
- Building age may be estimated
- Not all buildings have EPCs

### Method 8: Web Scraping Official Websites

**Accuracy:** HIGH (when available)  
**Coverage:** Varies by institution  
**Best for:** Museums, galleries, major attractions

**How it works:**
- Scrape "About" or "History" pages
- Extract dates from text
- Parse structured data if available

**Advantages:**
- Often most accurate for institutions
- Can get exact dates
- Official source

**Limitations:**
- Requires custom scraping per site
- HTML structure varies
- May need NLP for text extraction
- Rate limiting concerns

### Method 9: Historical Maps and Aerial Imagery

**Accuracy:** MEDIUM (year-level)  
**Coverage:** Varies by location  
**Best for:** Buildings visible in historical imagery

**Sources:**
- Historical Ordnance Survey maps
- Aerial photography archives
- Google Earth historical imagery
- National Library of Scotland maps

**How it works:**
- Compare historical maps to identify when building appeared
- Can narrow down to specific year/decade
- Requires manual or ML-based analysis

**Advantages:**
- Visual confirmation
- Can date older buildings
- Historical context

**Limitations:**
- Time-consuming
- Year-level accuracy at best
- Requires expertise
- Not scalable

### Method 10: Social Media and News Archives

**Accuracy:** MEDIUM (when found)  
**Coverage:** Varies  
**Best for:** Recent buildings, major developments

**Sources:**
- News archives (British Newspaper Archive)
- Social media posts
- Press releases
- Local news websites

**How it works:**
- Search for building name + "opened" or "opening"
- Extract dates from articles
- Can use NLP for date extraction

**Advantages:**
- Can find exact opening dates
- Historical coverage
- Public records

**Limitations:**
- Requires text search/NLP
- May have multiple dates (planning, construction, opening)
- Not comprehensive

## Recommended Multi-Source Strategy

### Priority Order (Enhanced)

1. **Wikidata** (NEW) - For known POIs, museums, landmarks
2. **Planning completion dates** - For new builds (2004+)
3. **Web scraping** - For major institutions with official websites
4. **Building age from OS/NGD** - Fallback for general buildings
5. **Heritage records** - For listed buildings
6. **Land Registry first transaction** - For properties
7. **Companies House incorporation** - For commercial buildings
8. **EPC first assessment** - Additional signal
9. **News archives** - For recent major developments
10. **Historical maps** - For very old buildings (manual)

### Combined Approach

```python
# 1. Try Wikidata first (fast, accurate for known POIs)
wikidata_date = get_wikidata_opening_date(poi_name)

# 2. Try planning data (for new builds)
planning_date = get_planning_completion_date(uprn)

# 3. Try web scraping (for institutions)
web_date = scrape_official_website(poi_url)

# 4. Fall back to building age
building_age = get_building_age_year(uprn)

# 5. Use heritage records (for listed buildings)
heritage_date = get_heritage_construction_date(uprn)

# Combine with confidence scoring
final_date = select_best_date([
    (wikidata_date, 'high'),
    (planning_date, 'high'),
    (web_date, 'high'),
    (building_age, 'medium'),
    (heritage_date, 'medium'),
])
```

## Implementation Recommendations

### 1. Enhance Building Opening Date Estimator

Add Wikidata as first priority:

```python
# Priority order:
1. Wikidata (for POIs with names)
2. Planning completion (for new builds)
3. Web scraping (for institutions)
4. Building age (fallback)
5. Heritage (for listed buildings)
```

### 2. Add Web Scraping Module

Create `src/web_scraper.py` for official websites:
- Scrape "About" pages
- Extract dates with NLP
- Handle multiple date formats

### 3. Add News Archive Search

Create `src/news_archive_searcher.py`:
- Search British Newspaper Archive
- Extract dates from articles
- Use NLP for date parsing

### 4. Combine with Wikidata Pipeline

Integrate Wikidata enrichment with building opening date estimator:
- Use Wikidata for POIs with names
- Use building age estimator for properties via UPRN
- Combine results with confidence scoring

## Accuracy Comparison

| Method | Accuracy | Coverage | Speed | Cost |
|--------|----------|----------|-------|------|
| Wikidata | High | Medium | Fast | Free |
| Planning | High | Low (2004+) | Medium | Free |
| Web Scraping | High | Low | Slow | Free |
| Building Age | Medium | High | Fast | Free/Paid |
| Heritage | Medium-High | Low (listed only) | Medium | Free |
| Land Registry | Medium | High | Fast | Free |
| Companies House | Medium | Medium | Fast | Free |
| EPC | Low-Medium | High | Fast | Free |
| News Archives | Medium | Medium | Slow | Paid |
| Historical Maps | Medium | Low | Very Slow | Free/Paid |

## Best Practices

1. **Use multiple sources**: Don't rely on one method
2. **Confidence scoring**: Track which source provided the date
3. **Date validation**: Check dates are reasonable (not future, not too old)
4. **Fallback chain**: Always have a fallback method
5. **Caching**: Cache results to avoid repeated queries
6. **Rate limiting**: Respect API limits and terms of service
7. **Error handling**: Gracefully handle missing data

## Future Enhancements

1. **ML-based date extraction**: Train model to extract dates from text
2. **Fuzzy matching**: Better POI name matching across sources
3. **Temporal reasoning**: Use multiple signals to infer most likely date
4. **Confidence calibration**: Learn which sources are most reliable
5. **Automated web scraping**: Scale web scraping with proper tools
6. **Historical data integration**: Combine with historical OSM data

## Conclusion

The current approach is good but can be enhanced by:

1. **Adding Wikidata** as first priority (already implemented!)
2. **Adding web scraping** for major institutions
3. **Combining multiple sources** with confidence scoring
4. **Using temporal reasoning** to select best date

The best approach is a **multi-source pipeline** that tries multiple methods in priority order and selects the most confident result.

