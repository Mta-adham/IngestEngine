# Quick Reference: Code Organization

## ✅ Organized Files (6)

All files use consistent section headers and function grouping:

1. `src/building_opening_date_estimator.py` - 6 sections
2. `src/data_loader.py` - 3 sections
3. `src/config.py` - 7 sections
4. `src/wikidata_client.py` - 6 sections
5. `src/poi_extractor.py` - 7 sections
6. `src/poi_data_cleaner.py` - 6 sections

## 📋 Section Header Template

```python
# ============================================
# SECTION NAME
# ============================================
```

## 🎯 Function Grouping

Functions are grouped by purpose:

- **Data Loading**: `load_*()`, `read_*()`, `fetch_*()`
- **Extraction**: `extract_*()`
- **Processing**: `process_*()`, `transform_*()`
- **Estimation**: `estimate_*()`, `calculate_*()`
- **Joining**: `join_*()`, `merge_*()`
- **Cleaning**: `clean_*()`, `identify_*()`
- **Export**: `save_*()`, `export_*()`
- **Utilities**: `normalize_*()`, `validate_*()`

## 📝 Standard Structure

```python
"""
Module Name
===========

Brief description with usage examples.
"""

# ============================================
# IMPORTS
# ============================================
import ...

# ============================================
# CONSTANTS
# ============================================
CONSTANT = value

# ============================================
# CLASS DEFINITION
# ============================================
class ClassName:
    # ============================================
    # INITIALIZATION
    # ============================================
    def __init__(self):
        pass
    
    # ============================================
    # METHOD GROUP 1
    # ============================================
    def method1(self):
        pass
    
    # ============================================
    # METHOD GROUP 2
    # ============================================
    def method2(self):
        pass

# ============================================
# STANDALONE FUNCTIONS
# ============================================
def standalone_function():
    pass

# ============================================
# MAIN ENTRY POINT
# ============================================
if __name__ == "__main__":
    main()
```

## 🔍 Finding Functions

**Data Loading:**
- `src/data_loader.py` → `load_london_pois()`
- `src/building_opening_date_estimator.py` → All `load_*()` methods

**POI Extraction:**
- `src/poi_extractor.py` → All `extract_*()` methods

**Data Cleaning:**
- `src/poi_data_cleaner.py` → `clean_data()`, `identify_*()` methods

**Opening Dates:**
- `src/building_opening_date_estimator.py` → `estimate_opening_dates()`
- `src/wikidata_client.py` → `get_poi_info()`, `search_item()`

**Configuration:**
- `src/config.py` → All constants organized by category

## 📚 Full Documentation

- **Code Organization Guide**: `docs/CODE_ORGANIZATION.md`
- **Cleanup Summary**: `CLEANUP_SUMMARY.md`
- **Cleanup Plan**: `CLEANUP_PLAN.md`

