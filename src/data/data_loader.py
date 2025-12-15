"""
Data Loader for London POI Dataset
===================================

Loads and cleans POI data from various sources.

Usage:
    from src.data_loader import load_london_pois
    
    df = load_london_pois("data/raw/london_pois.csv")
"""

# ============================================
# IMPORTS
# ============================================
import pandas as pd
import os
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================
# DATA LOADING FUNCTIONS
# ============================================

def load_london_pois(path: str) -> pd.DataFrame:
    """
    Load London POI dataset from CSV file
    
    Args:
        path: Path to the CSV file containing POI data
        
    Returns:
        Cleaned DataFrame with columns: id, name, category, address, latitude, longitude
        
    Expected CSV columns:
        - id: POI identifier
        - name: POI name
        - category: POI category/type
        - address: Address string
        - latitude: Latitude coordinate
        - longitude: Longitude coordinate
    """
    logger.info(f"Loading London POIs from: {path}")
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"POI file not found: {path}")
    
    # Read CSV
    df = pd.read_csv(path, low_memory=False)
    logger.info(f"Loaded {len(df):,} rows from CSV")
    
    # Standardize column names (handle variations)
    column_mapping = {
        'poi_id': 'id',
        'POI_ID': 'id',
        'ID': 'id',
        'Name': 'name',
        'NAME': 'name',
        'Category': 'category',
        'CATEGORY': 'category',
        'type': 'category',
        'Type': 'category',
        'Address': 'address',
        'ADDRESS': 'address',
        'lat': 'latitude',
        'Lat': 'latitude',
        'LAT': 'latitude',
        'Latitude': 'latitude',
        'lon': 'longitude',
        'Lon': 'longitude',
        'LON': 'longitude',
        'Longitude': 'longitude',
        'lng': 'longitude',
    }
    
    # Rename columns if needed
    df = df.rename(columns=column_mapping)
    
    # Check required columns
    required_cols = ['name', 'latitude', 'longitude']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Drop rows without name
    before = len(df)
    df = df[df['name'].notna() & (df['name'].str.strip() != '')].copy()
    logger.info(f"Dropped {before - len(df):,} rows without names")
    
    # Drop rows without coordinates
    before = len(df)
    df = df[
        (df['latitude'].notna()) & 
        (df['longitude'].notna()) &
        (df['latitude'] != 0) &
        (df['longitude'] != 0)
    ].copy()
    logger.info(f"Dropped {before - len(df):,} rows without valid coordinates")
    
    # Ensure coordinates are numeric
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # Filter to valid coordinate ranges (London area)
    df = df[
        (df['latitude'] >= 51.0) & (df['latitude'] <= 52.0) &
        (df['longitude'] >= -0.6) & (df['longitude'] <= 0.3)
    ].copy()
    logger.info(f"Filtered to {len(df):,} POIs within London bounds")
    
    # Add id if missing
    if 'id' not in df.columns:
        df['id'] = range(1, len(df) + 1)
        logger.info("Added 'id' column")
    
    # Ensure id is string
    df['id'] = df['id'].astype(str)
    
    # Add category if missing
    if 'category' not in df.columns:
        df['category'] = 'attraction'
        logger.info("Added default 'category' column")
    
    # Add address if missing
    if 'address' not in df.columns:
        df['address'] = None
        logger.info("Added empty 'address' column")
    
    # Select and order columns
    output_cols = ['id', 'name', 'category', 'address', 'latitude', 'longitude']
    df = df[output_cols].copy()
    
    # Log statistics
    logger.info(f"\nFinal dataset statistics:")
    logger.info(f"  Total POIs: {len(df):,}")
    logger.info(f"  Categories: {df['category'].nunique()}")
    logger.info(f"\nCategory distribution:")
    for cat, count in df['category'].value_counts().head(10).items():
        logger.info(f"  {cat}: {count:,}")
    
    return df


# ============================================
# DATASET-SPECIFIC LOADERS
# ============================================

def load_from_enriched_tourism_dataset(path: Optional[str] = None) -> pd.DataFrame:
    """
    Load from Enriched Tourism Dataset London (POIs)
    Mendeley dataset DOI 10.17632/gw9hjn4v65.2
    
    Args:
        path: Path to the dataset CSV (default: data/raw/enriched_tourism_london.csv)
        
    Returns:
        Cleaned DataFrame
    """
    if path is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        path = os.path.join(data_dir, 'enriched_tourism_london.csv')
    
    return load_london_pois(path)


def load_from_tourpedia(path: Optional[str] = None) -> pd.DataFrame:
    """
    Load from Tourpedia London attractions CSV
    
    Args:
        path: Path to Tourpedia CSV (default: data/raw/tourpedia_london.csv)
        
    Returns:
        Cleaned DataFrame
    """
    if path is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        path = os.path.join(data_dir, 'tourpedia_london.csv')
    
    return load_london_pois(path)

