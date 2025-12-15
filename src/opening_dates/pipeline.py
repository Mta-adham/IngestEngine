"""
POI Enrichment Pipeline
Orchestrates the enrichment of POI data with opening dates from Wikidata
"""

import pandas as pd
import os
import sys
import logging
from pathlib import Path
from typing import Optional
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.data_loader import load_london_pois
from src.opening_dates.wikidata_client import WikidataClient
from src.config import (
    OUTPUT_CSV, OUTPUT_PARQUET, CHECKPOINT_FILE,
    CHECKPOINT_INTERVAL, PROGRESS_BAR,
    WIKIDATA_RATE_LIMIT_DELAY, WIKIDATA_MAX_RETRIES, WIKIDATA_CITY
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def enrich_with_opening_dates(
    df: pd.DataFrame,
    checkpoint_file: Optional[str] = None,
    checkpoint_interval: int = CHECKPOINT_INTERVAL,
    city: str = WIKIDATA_CITY
) -> pd.DataFrame:
    """
    Enrich POI DataFrame with opening dates from Wikidata
    
    Args:
        df: DataFrame with POI data (must have 'name' column)
        checkpoint_file: Path to save intermediate results (default: config.CHECKPOINT_FILE)
        checkpoint_interval: Save checkpoint every N POIs (default: 50)
        city: City name for Wikidata search (default: "London")
        
    Returns:
        DataFrame with added 'opening_date' and 'wikidata_id' columns
    """
    if checkpoint_file is None:
        checkpoint_file = str(CHECKPOINT_FILE)
    
    logger.info(f"Starting enrichment for {len(df):,} POIs")
    
    # Initialize Wikidata client
    client = WikidataClient(
        rate_limit_delay=WIKIDATA_RATE_LIMIT_DELAY,
        max_retries=WIKIDATA_MAX_RETRIES
    )
    
    # Add new columns
    df = df.copy()
    df['opening_date'] = None
    df['wikidata_id'] = None
    df['source'] = 'wikidata'
    
    # Check if checkpoint exists
    start_idx = 0
    if os.path.exists(checkpoint_file):
        logger.info(f"Loading checkpoint from {checkpoint_file}")
        checkpoint_df = pd.read_csv(checkpoint_file)
        # Find last processed index
        processed_ids = set(checkpoint_df['id'].astype(str))
        start_idx = len([i for i, row in df.iterrows() if str(row['id']) in processed_ids])
        logger.info(f"Resuming from index {start_idx}")
    
    # Process POIs
    enriched_count = 0
    missing_count = 0
    
    iterator = df.iterrows()
    if PROGRESS_BAR:
        iterator = tqdm(iterator, total=len(df), desc="Enriching POIs")
    
    for idx, row in iterator:
        # Skip if already processed (from checkpoint)
        if idx < start_idx:
            continue
        
        name = row['name']
        if pd.isna(name) or not str(name).strip():
            missing_count += 1
            continue
        
        # Query Wikidata
        try:
            info = client.get_poi_info(name, city=city)
            
            if info['wikidata_id']:
                df.at[idx, 'wikidata_id'] = info['wikidata_id']
                if info['opening_date']:
                    df.at[idx, 'opening_date'] = info['opening_date']
                    enriched_count += 1
                else:
                    missing_count += 1
            else:
                missing_count += 1
        except Exception as e:
            logger.warning(f"Error processing '{name}': {e}")
            missing_count += 1
        
        # Save checkpoint periodically
        if (idx + 1) % checkpoint_interval == 0:
            df.to_csv(checkpoint_file, index=False)
            logger.info(f"Checkpoint saved: {idx + 1}/{len(df)} POIs processed")
    
    # Final checkpoint
    df.to_csv(checkpoint_file, index=False)
    
    # Log results
    logger.info(f"\nEnrichment complete:")
    logger.info(f"  Total POIs: {len(df):,}")
    logger.info(f"  With opening dates: {enriched_count:,}")
    logger.info(f"  Without opening dates: {missing_count:,}")
    logger.info(f"  Success rate: {enriched_count/len(df)*100:.1f}%")
    
    return df


def run(
    input_file: Optional[str] = None,
    output_csv: Optional[str] = None,
    output_parquet: Optional[str] = None
):
    """
    Run the complete POI enrichment pipeline
    
    Args:
        input_file: Path to input POI CSV (default: config.LONDON_POIS_RAW)
        output_csv: Path to output CSV (default: config.OUTPUT_CSV)
        output_parquet: Path to output Parquet (default: config.OUTPUT_PARQUET)
    """
    from src.config import LONDON_POIS_RAW, OUTPUT_CSV, OUTPUT_PARQUET
    
    if input_file is None:
        input_file = str(LONDON_POIS_RAW)
    if output_csv is None:
        output_csv = str(OUTPUT_CSV)
    if output_parquet is None:
        output_parquet = str(OUTPUT_PARQUET)
    
    logger.info("=" * 70)
    logger.info("POI ENRICHMENT PIPELINE")
    logger.info("=" * 70)
    
    # Step 1: Load POIs
    logger.info("\nStep 1: Loading POI data...")
    df = load_london_pois(input_file)
    
    # Step 2: Enrich with opening dates
    logger.info("\nStep 2: Enriching with opening dates from Wikidata...")
    df_enriched = enrich_with_opening_dates(df)
    
    # Step 3: Save outputs
    logger.info("\nStep 3: Saving outputs...")
    
    # Save CSV
    df_enriched.to_csv(output_csv, index=False)
    logger.info(f"✓ Saved CSV: {output_csv}")
    logger.info(f"  Records: {len(df_enriched):,}")
    logger.info(f"  Columns: {len(df_enriched.columns)}")
    
    # Save Parquet
    try:
        df_enriched.to_parquet(output_parquet, index=False, engine='pyarrow')
        logger.info(f"✓ Saved Parquet: {output_parquet}")
    except ImportError:
        logger.warning("PyArrow not available, skipping Parquet export")
        logger.info("Install with: pip install pyarrow")
    except Exception as e:
        logger.warning(f"Could not save Parquet: {e}")
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 70)
    logger.info(f"\nOutput files:")
    logger.info(f"  CSV: {output_csv}")
    if os.path.exists(output_parquet):
        logger.info(f"  Parquet: {output_parquet}")
    
    logger.info(f"\nFinal dataset:")
    logger.info(f"  Total POIs: {len(df_enriched):,}")
    logger.info(f"  With opening dates: {df_enriched['opening_date'].notna().sum():,}")
    logger.info(f"  With Wikidata IDs: {df_enriched['wikidata_id'].notna().sum():,}")


if __name__ == "__main__":
    run()

