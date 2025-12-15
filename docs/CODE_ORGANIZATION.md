# Code Organization Guide

## Overview

All scripts are organized by functional area with clear grouping of related functions.

## 📁 Directory Structure by Function

### `src/` - Core Modules

#### 1. POI Extraction & Processing
- `poi_extractor.py` - Extract POIs from OpenStreetMap
- `poi_data_cleaner.py` - Clean OSM data for world models
- `poi_change_detector.py` - Detect changes in POIs over time
- `poi_date_extractor.py` - Extract POIs as they existed on specific dates

#### 2. Opening Date Estimation
- `building_opening_date_estimator.py` - Multi-source opening date estimator
- `unified_opening_date_pipeline.py` - Unified pipeline orchestrator
- `pipeline.py` - Wikidata enrichment pipeline
- `wikidata_client.py` - Wikidata SPARQL client
- `enhanced_opening_date_estimator.py` - Enhanced estimator (extends base)

#### 3. Dataset Joining
- `dataset_joiner.py` - Multi-strategy dataset joining engine
- `joining.py` - Main joining script/orchestrator

#### 4. Data Loading & Utilities
- `data_loader.py` - POI data loading utilities
- `config.py` - Pipeline configuration
- `hours_scraper.py` - Opening hours scraper (stub)

#### 5. Catalog & Metadata
- `UkDataCatalog.py` - UK data catalog definitions
- `catalogue.py` - Catalog utilities

### `scripts/` - Standalone Scripts

#### Historical Analysis
- `poi_history_tracker.py` - Track historical POI changes
- `analyze_london_evolution.py` - Analyze London evolution over time

#### POI Analysis
- `comprehensive_osm_poi_catalog.py` - Generate comprehensive POI catalog
- `poi_types_and_attributes_extractor.py` - Extract POI types and attributes
- `create_poi_attributes_list.py` - Create POI attributes list
- `filter_pois_with_names.py` - Filter POIs with names

#### Spatial Operations
- `spatial_uprn_joiner.py` - Standalone spatial UPRN joining

## 📋 Function Grouping Standards

Each module follows this structure:

```python
"""
Module Name
Brief description
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
# CLASSES
# ============================================
class ClassName:
    """
    Class description
    """
    
    # ============================================
    # INITIALIZATION
    # ============================================
    def __init__(self, ...):
        """Initialize"""
        pass
    
    # ============================================
    # DATA LOADING METHODS
    # ============================================
    def load_data(self, ...):
        """Load data"""
        pass
    
    # ============================================
    # PROCESSING METHODS
    # ============================================
    def process(self, ...):
        """Process data"""
        pass
    
    # ============================================
    # UTILITY METHODS
    # ============================================
    def helper(self, ...):
        """Helper function"""
        pass

# ============================================
# STANDALONE FUNCTIONS
# ============================================
def standalone_function():
    """Standalone function"""
    pass

# ============================================
# MAIN ENTRY POINT
# ============================================
if __name__ == "__main__":
    main()
```

## 🎯 Function Categories

### Data Loading Functions
- `load_*()` - Load data from files/APIs
- `read_*()` - Read data from sources
- `fetch_*()` - Fetch data from APIs

### Processing Functions
- `process_*()` - Process data
- `extract_*()` - Extract information
- `transform_*()` - Transform data
- `enrich_*()` - Enrich with additional data
- `estimate_*()` - Estimate values
- `calculate_*()` - Calculate metrics

### Joining Functions
- `join_*()` - Join datasets
- `merge_*()` - Merge data
- `match_*()` - Match records

### Utility Functions
- `normalize_*()` - Normalize data
- `validate_*()` - Validate data
- `clean_*()` - Clean data
- `format_*()` - Format output

### Analysis Functions
- `analyze_*()` - Analyze data
- `compare_*()` - Compare datasets
- `detect_*()` - Detect patterns
- `track_*()` - Track changes

## 📝 Documentation Standards

### Module Docstring
```python
"""
Module Name
===========

Brief description of the module's purpose.

Key Features:
- Feature 1
- Feature 2
- Feature 3

Usage:
    from src.module import ClassName
    instance = ClassName()
    result = instance.method()
"""
```

### Class Docstring
```python
class ClassName:
    """
    Class description.
    
    This class does X, Y, and Z.
    
    Attributes:
        attr1: Description of attr1
        attr2: Description of attr2
    
    Example:
        >>> instance = ClassName()
        >>> result = instance.method()
    """
```

### Function Docstring
```python
def function_name(param1: type, param2: type = default) -> return_type:
    """
    Brief description of what the function does.
    
    Args:
        param1: Description of param1
        param2: Description of param2 (default: default)
    
    Returns:
        Description of return value
    
    Raises:
        ExceptionType: When this exception is raised
    
    Example:
        >>> result = function_name(value1, value2)
        >>> print(result)
    """
```

## 🔍 Code Quality Standards

1. **Grouping**: Related functions grouped together with clear section headers
2. **Naming**: Consistent naming conventions (snake_case for functions, PascalCase for classes)
3. **Docstrings**: All public functions/classes have docstrings
4. **Type Hints**: Use type hints where possible
5. **Error Handling**: Proper error handling with informative messages
6. **Logging**: Use logging instead of print statements
7. **Constants**: Define constants at module level
8. **Imports**: Group imports (standard library, third-party, local)

