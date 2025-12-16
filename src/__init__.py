"""
IngestEngine - Core Modules
===========================

Main modules for POI extraction, dataset joining, and analysis.

Organized by function:
- extraction: POI extraction and processing
- opening_dates: Opening date estimation
- joining: Dataset joining
- data: Data loading and utilities
- catalog: Catalog and metadata
"""

# Import from organized submodules
from .extraction import (
    POIExtractor,
    POIDataCleaner,
    POIChangeDetector,
    POIDateExtractor
)

from .opening_dates import (
    BuildingOpeningDateEstimator,
    WikidataClient,
    unified_opening_date_pipeline,
    enrich_with_opening_dates,
    run as pipeline_run
)

from .joining import (
    DatasetJoiner
)

from .data import (
    load_london_pois,
    fetch_opening_hours
)

from .catalog import (
    create_complete_catalog
)

__all__ = [
    # Extraction
    'POIExtractor',
    'POIDataCleaner',
    'POIChangeDetector',
    'POIDateExtractor',
    # Opening Dates
    'BuildingOpeningDateEstimator',
    'WikidataClient',
    'unified_opening_date_pipeline',
    'enrich_with_opening_dates',
    'pipeline_run',
    # Joining
    'DatasetJoiner',
    # Data
    'load_london_pois',
    'fetch_opening_hours',
    # Catalog
    'create_complete_catalog',
]
