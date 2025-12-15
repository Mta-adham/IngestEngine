"""
Configuration for POI Enrichment Pipeline
==========================================

Centralized configuration for all pipeline settings.

Usage:
    from src.config import OUTPUT_CSV, WIKIDATA_RATE_LIMIT_DELAY
"""

# ============================================
# IMPORTS
# ============================================
import os
from pathlib import Path

# ============================================
# DIRECTORY PATHS
# ============================================

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW = DATA_DIR / "raw"
DATA_INTERIM = DATA_DIR / "interim"
DATA_PROCESSED = DATA_DIR / "processed"

# Create directories if they don't exist
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_INTERIM.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# ============================================
# INPUT DATA PATHS
# ============================================
ENRICHED_TOURISM_DATASET = DATA_RAW / "enriched_tourism_london.csv"
TOURPEDIA_DATASET = DATA_RAW / "tourpedia_london.csv"
LONDON_POIS_RAW = DATA_RAW / "london_pois.csv"

# ============================================
# OUTPUT PATHS
# ============================================
OUTPUT_CSV = DATA_PROCESSED / "london_pois_opening_dates.csv"
OUTPUT_PARQUET = DATA_PROCESSED / "london_pois_opening_dates.parquet"
CHECKPOINT_FILE = DATA_INTERIM / "opening_dates_partial.csv"

# ============================================
# WIKIDATA CLIENT SETTINGS
# ============================================
WIKIDATA_RATE_LIMIT_DELAY = 1.0  # Seconds between requests
WIKIDATA_MAX_RETRIES = 3
WIKIDATA_CITY = "London"

# ============================================
# PIPELINE SETTINGS
# ============================================
CHECKPOINT_INTERVAL = 50  # Save checkpoint every N POIs
PROGRESS_BAR = True  # Show progress bar

# ============================================
# LOGGING SETTINGS
# ============================================
LOG_LEVEL = "INFO"

