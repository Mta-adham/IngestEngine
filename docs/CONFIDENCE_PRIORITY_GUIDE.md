# Confidence Scoring, Priority, and Data Source Selection

## 📊 Overview

This guide explains three key concepts in the IngestEngine:
1. **Confidence Scoring** - How we measure match quality
2. **Priority Ordering** - How we select which data source to use
3. **Data Source Reliability** - How we assign confidence levels

---

## 1. Confidence Scoring

### What is Confidence Scoring?

Confidence scoring measures **how certain we are that a match is correct**. It's used in two contexts:

#### A. Dataset Joining Confidence (`join_by_multiple_columns`)

**Purpose**: Measure how well two records match across multiple columns.

**How it works**:
```python
# Example: Joining EPC + POIs on postcode + address + name
join_columns = {
    'postcode': ('postcode', 'addr:postcode'),
    'address': ('ADDRESS', 'addr:street'),
    'name': ('name', 'name')
}

# Confidence score = number of matching columns
# Score starts at 1 (first column matched)
score = 1  # postcode matched

# Check if address matches
if address1 == address2:
    score += 1  # Now score = 2

# Check if name matches
if name1 == name2:
    score += 1  # Now score = 3

# Final confidence: 3/3 columns match = 100% confidence
```

**Scoring Formula**:
```
confidence_score = 1 + (number of additional matching columns)
confidence_level = f"{score}/{total_columns}"
```

**Example Scores**:
- `1/3` = Only postcode matches (33% confidence)
- `2/3` = Postcode + address match (67% confidence)
- `3/3` = All columns match (100% confidence)

**Filtering**:
```python
# Only keep matches with at least 2 columns matching
min_confidence = 2
joined = joined[joined['confidence_score'] >= min_confidence]
```

**Location**: `src/joining/dataset_joiner.py` lines 183-201

---

#### B. Opening Date Confidence (`opening_date_confidence`)

**Purpose**: Measure how reliable a data source is for opening dates.

**Confidence Levels**:
- **`'high'`** - Very reliable, precise dates
- **`'medium'`** - Reasonably reliable, approximate dates
- **`'low'`** - Less reliable, estimated dates

**How it's assigned**:

| Data Source | Confidence | Reason |
|-------------|------------|--------|
| **Wikidata** | `'high'` | Official inception dates, verified data |
| **Companies House** | `'high'` | Official incorporation dates, legal records |
| **Planning Completion** | `'high'` | Official completion dates from planning authority |
| **Land Registry** | `'medium'` | First transaction date (proxy for opening) |
| **Building Age (OS/NGD)** | `'medium'` | Estimated build year (may be approximate) |
| **EPC Construction Age** | `'medium'` | Estimated construction year |
| **Heritage Records** | `'medium'` | Historical records (may be approximate) |

**Code Example**:
```python
# Wikidata - high confidence
result.loc[mask, 'opening_date_confidence'] = 'high'

# Planning - high confidence
result.loc[mask, 'opening_date_confidence'] = 'high'

# Building age - medium confidence
result.loc[mask, 'opening_date_confidence'] = 'medium'
```

**Location**: `src/opening_dates/building_opening_date_estimator.py` lines 569, 604, 635, 660, 694, 726, 754

---

## 2. Priority Ordering

### What is Priority Ordering?

Priority ordering determines **which data source to use first** when multiple sources have dates for the same entity.

### Default Priority Order

```python
priority_order = [
    'wikidata',           # 1. Highest priority - most accurate
    'companies_house',    # 2. Official business records
    'planning',           # 3. Official completion dates
    'land_registry',      # 4. First transaction dates
    'building_age',       # 5. Estimated build year
    'epc',                # 6. EPC construction age
    'heritage'            # 7. Historical records
]
```

### How Priority Works

**Step-by-step process**:

1. **Start with highest priority source** (Wikidata)
   - If date found → Use it, mark as `'high'` confidence, **STOP**
   - If not found → Continue to next source

2. **Try next priority source** (Companies House)
   - If date found → Use it, mark as `'high'` confidence, **STOP**
   - If not found → Continue to next source

3. **Continue down the list** until a date is found

4. **Once a date is assigned, skip remaining sources** for that entity

**Code Example**:
```python
# Priority 1: Wikidata
if 'wikidata' in priority_order and date_found:
    result['opening_date'] = wikidata_date
    result['opening_date_source'] = 'wikidata'
    result['opening_date_confidence'] = 'high'
    # STOP - don't check other sources

# Priority 2: Companies House (only if Wikidata didn't find date)
if 'companies_house' in priority_order and result['opening_date'].isna():
    result['opening_date'] = companies_house_date
    result['opening_date_source'] = 'companies_house'
    result['opening_date_confidence'] = 'high'
    # STOP - don't check other sources

# ... and so on
```

**Key Principle**: **First match wins** - once a date is found from a higher-priority source, lower-priority sources are not checked.

**Location**: `src/opening_dates/building_opening_date_estimator.py` lines 520-754

---

## 3. Why This Priority Order?

### Priority Rationale

| Priority | Source | Why First? |
|----------|--------|------------|
| 1 | **Wikidata** | Most accurate for POIs, official inception dates |
| 2 | **Companies House** | Official business incorporation dates |
| 3 | **Planning** | Official completion dates for new builds |
| 4 | **Land Registry** | First transaction (proxy, but official) |
| 5 | **Building Age** | Broad coverage, but estimated |
| 6 | **EPC** | Detailed but may be approximate |
| 7 | **Heritage** | Historical, may be less precise |

### Custom Priority Order

You can customize the priority order:

```python
# Custom priority - prefer building age over planning
custom_priority = [
    'wikidata',
    'building_age',      # Moved up
    'planning',          # Moved down
    'epc',
    'heritage'
]

estimator.estimate_opening_dates(
    uprn_df,
    priority_order=custom_priority
)
```

---

## 4. Priors (Not Bayesian Priors)

**Note**: The codebase does **not** use Bayesian priors. Instead, it uses:

1. **Priority Ordering** - Determines which source to check first
2. **Confidence Levels** - Measures source reliability
3. **Confidence Scores** - Measures match quality

These are **deterministic rules**, not probabilistic priors.

---

## 5. Complete Example

### Scenario: Finding Opening Date for a Restaurant

```python
# Restaurant: "The Ivy" in London
# Has: name, UPRN, postcode, address

# Step 1: Try Wikidata (Priority 1)
wikidata_result = wikidata_client.get_poi_info("The Ivy", city="London")
# Found: opening_date = "1917-01-01", confidence = 'high'
# ✅ USE THIS - STOP checking other sources

# If Wikidata didn't find it:
# Step 2: Try Companies House (Priority 2)
# Check if "The Ivy" is a registered company
# If found: use incorporation_date, confidence = 'high'

# If still not found:
# Step 3: Try Planning (Priority 3)
# Check planning records for UPRN
# If found: use completion_date, confidence = 'high'

# If still not found:
# Step 4: Try Building Age (Priority 5)
# Check OS/NGD building age for UPRN
# If found: use building_age_year, confidence = 'medium'
```

### Dataset Joining Example

```python
# Joining EPC + POIs on multiple columns
join_columns = {
    'postcode': ('postcode', 'addr:postcode'),
    'address': ('ADDRESS', 'addr:street'),
    'name': ('name', 'name')
}

# Record 1:
#   postcode: MATCH ✓
#   address: MATCH ✓
#   name: MATCH ✓
#   confidence_score = 3/3 = 100%

# Record 2:
#   postcode: MATCH ✓
#   address: MATCH ✓
#   name: NO MATCH ✗
#   confidence_score = 2/3 = 67%

# Filter: min_confidence = 2
# Result: Both records kept (both have score >= 2)
```

---

## 6. Summary

### Confidence Scoring
- **Joining**: Number of matching columns (1/3, 2/3, 3/3)
- **Opening Dates**: Source reliability ('high', 'medium', 'low')

### Priority Ordering
- **Deterministic**: First match wins
- **Customizable**: Can change order
- **Stops early**: Once date found, doesn't check lower priorities

### No Bayesian Priors
- Uses deterministic rules
- Priority determines order
- Confidence measures quality/reliability

---

## 📚 Related Documentation

- `docs/DATASET_JOINING_GUIDE.md` - Complete joining guide
- `docs/BEST_APPROACH_OPENING_DATES.md` - Opening date estimation
- `src/joining/dataset_joiner.py` - Joining implementation
- `src/opening_dates/building_opening_date_estimator.py` - Opening date estimation

