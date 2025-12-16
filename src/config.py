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

# ============================================
# API KEYS
# ============================================
# Load from environment variables or use defaults
# To set: export COMPANIES_HOUSE_API_KEY="your-key" in terminal
# Or create a .env file with the keys

# Companies House API
# Get your key from: https://developer.company-information.service.gov.uk/
COMPANIES_HOUSE_API_KEY = os.environ.get(
    'COMPANIES_HOUSE_API_KEY', 
    'f34ba78d-1219-4dce-9aaf-9bdc31ebc99f'  # Default key for "world" project
)

# TfL (Transport for London) API
# Get your key from: https://api-portal.tfl.gov.uk/signup
TFL_APP_ID = os.environ.get('TFL_APP_ID', '')
TFL_APP_KEY = os.environ.get('TFL_APP_KEY', '')

# EPC (Energy Performance Certificates) API
# Register at: https://epc.opendatacommunities.org/
EPC_EMAIL = os.environ.get('EPC_EMAIL', 'manal@zone13.ai')
EPC_API_KEY = os.environ.get('EPC_API_KEY', '645cef978fc53f6b16da5e76ddaf653144e205d5')

# Ordnance Survey DataHub API
# Register at: https://osdatahub.os.uk/
OS_API_KEY = os.environ.get('OS_API_KEY', 'vA1b2tAgw0372dME441EpgYdQb1jTHIQ')
OS_API_SECRET = os.environ.get('OS_API_SECRET', 'Zkp8uSAt7VBQSbAo')

# ============================================
# API RATE LIMITS (requests per minute)
# ============================================
COMPANIES_HOUSE_RATE_LIMIT = 120  # 600 per 5 min
TFL_RATE_LIMIT = 500
POLICE_UK_RATE_LIMIT = 900  # 15 per second
EPC_RATE_LIMIT = 60
NHS_RATE_LIMIT = 120
ONS_RATE_LIMIT = 60
OS_RATE_LIMIT = 100
EA_RATE_LIMIT = 120

