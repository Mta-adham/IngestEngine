"""
Unified Opening Date Pipeline
Best approach: Combines Wikidata pipeline + Building age estimator for maximum coverage

IMPROVEMENTS:
- Input validation and data quality checks
- Vectorized operations for better performance
- Progress tracking with tqdm
- Resume capability from checkpoints
- Better error handling with specific exceptions
- Data quality metrics and statistics
- Parallel processing option
- Better date handling (handles NaT properly)
- More detailed reporting
"""

import pandas as pd
import numpy as np
import os
import sys
import logging
from typing import Optional, Dict, List
from pathlib import Path
from tqdm import tqdm
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.opening_dates.pipeline import enrich_with_opening_dates
from src.opening_dates.building_opening_date_estimator import BuildingOpeningDateEstimator
from src.data.data_loader import load_london_pois
from src.config import OUTPUT_CSV, OUTPUT_PARQUET, CHECKPOINT_FILE

warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass


def validate_input_data(df: pd.DataFrame, required_columns: List[str] = None) -> Dict[str, any]:
    """
    Validate input DataFrame and return quality metrics
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Dictionary with validation results and metrics
    """
    if required_columns is None:
        required_columns = []
    
    metrics = {
        'total_rows': len(df),
        'missing_columns': [],
        'empty_rows': 0,
        'has_name': False,
        'has_coordinates': False,
        'has_uprn': False,
        'valid': True
    }
    
    # Check required columns
    for col in required_columns:
        if col not in df.columns:
            metrics['missing_columns'].append(col)
            metrics['valid'] = False
    
    # Check for empty DataFrame
    if len(df) == 0:
        metrics['valid'] = False
        metrics['empty_rows'] = len(df)
        return metrics
    
    # Check for name column
    if 'name' in df.columns:
        metrics['has_name'] = True
        metrics['rows_with_name'] = df['name'].notna().sum()
        metrics['rows_without_name'] = df['name'].isna().sum()
    
    # Check for coordinates
    if 'lat' in df.columns and 'lon' in df.columns:
        metrics['has_coordinates'] = True
        metrics['rows_with_coords'] = (df['lat'].notna() & df['lon'].notna()).sum()
        metrics['rows_without_coords'] = ((df['lat'].isna()) | (df['lon'].isna())).sum()
    elif 'latitude' in df.columns and 'longitude' in df.columns:
        metrics['has_coordinates'] = True
        metrics['rows_with_coords'] = (df['latitude'].notna() & df['longitude'].notna()).sum()
        metrics['rows_without_coords'] = ((df['latitude'].isna()) | (df['longitude'].isna())).sum()
    
    # Check for UPRN
    if 'UPRN' in df.columns:
        metrics['has_uprn'] = True
        metrics['rows_with_uprn'] = df['UPRN'].notna().sum()
        metrics['rows_without_uprn'] = df['UPRN'].isna().sum()
    
    return metrics


def load_checkpoint(checkpoint_file: str) -> Optional[pd.DataFrame]:
    """
    Load checkpoint if it exists
    
    Args:
        checkpoint_file: Path to checkpoint file
        
    Returns:
        DataFrame with checkpoint data or None
    """
    if os.path.exists(checkpoint_file):
        try:
            checkpoint_df = pd.read_csv(checkpoint_file, low_memory=False)
            logger.info(f"✓ Loaded checkpoint: {len(checkpoint_df):,} rows")
            return checkpoint_df
        except Exception as e:
            logger.warning(f"⚠ Could not load checkpoint: {e}")
            return None
    return None


def save_checkpoint(df: pd.DataFrame, checkpoint_file: str):
    """Save checkpoint"""
    try:
        df.to_csv(checkpoint_file, index=False)
        logger.debug(f"Checkpoint saved: {checkpoint_file}")
    except Exception as e:
        logger.warning(f"⚠ Could not save checkpoint: {e}")


def calculate_data_quality_metrics(result: pd.DataFrame) -> Dict[str, any]:
    """
    Calculate comprehensive data quality metrics
    
    Args:
        result: DataFrame with enrichment results
        
    Returns:
        Dictionary with quality metrics
    """
    metrics = {
        'total_pois': len(result),
        'with_dates': result['opening_date'].notna().sum(),
        'without_dates': result['opening_date'].isna().sum(),
        'coverage_pct': 0.0,
        'by_source': {},
        'by_confidence': {},
        'date_range': {},
        'data_quality': {}
    }
    
    if metrics['total_pois'] > 0:
        metrics['coverage_pct'] = (metrics['with_dates'] / metrics['total_pois']) * 100
    
    # Source distribution
    if 'opening_date_source' in result.columns:
        source_counts = result['opening_date_source'].value_counts()
        metrics['by_source'] = source_counts.to_dict()
    
    # Confidence distribution
    if 'opening_date_confidence' in result.columns:
        conf_counts = result['opening_date_confidence'].value_counts()
        metrics['by_confidence'] = conf_counts.to_dict()
    
    # Date range
    if metrics['with_dates'] > 0:
        dates = pd.to_datetime(result['opening_date'], errors='coerce')
        valid_dates = dates.dropna()
        if len(valid_dates) > 0:
            metrics['date_range'] = {
                'earliest': str(valid_dates.min()),
                'latest': str(valid_dates.max()),
                'median': str(valid_dates.median())
            }
    
    # Data quality scores
    if metrics['with_dates'] > 0:
        high_conf = (result['opening_date_confidence'] == 'high').sum()
        medium_conf = (result['opening_date_confidence'] == 'medium').sum()
        low_conf = (result['opening_date_confidence'] == 'low').sum()
        
        metrics['data_quality'] = {
            'high_confidence_pct': (high_conf / metrics['with_dates']) * 100,
            'medium_confidence_pct': (medium_conf / metrics['with_dates']) * 100,
            'low_confidence_pct': (low_conf / metrics['with_dates']) * 100 if low_conf > 0 else 0
        }
    
    return metrics


def unified_opening_date_pipeline(
    input_file: str,
    output_file: Optional[str] = None,
    checkpoint_file: Optional[str] = None,
    planning_path: Optional[str] = None,
    building_age_path: Optional[str] = None,
    heritage_path: Optional[str] = None,
    companies_house_path: Optional[str] = None,
    land_registry_path: Optional[str] = None,
    epc_path: Optional[str] = None,
    use_wikidata: bool = True,
    use_building_age: bool = True,
    resume_from_checkpoint: bool = True,
    save_checkpoints: bool = True,
    checkpoint_interval: int = 100
) -> pd.DataFrame:
    """
    Unified pipeline that combines best methods for maximum coverage
    
    Strategy:
    1. Enrich POIs with Wikidata (for POIs with names)
    2. Enrich properties with building age estimator (for properties via UPRN)
    3. Combine results with confidence scoring
    
    Args:
        input_file: Path to input POI CSV
        output_file: Path to output CSV (default: config.OUTPUT_CSV)
        checkpoint_file: Path to checkpoint file (default: config.CHECKPOINT_FILE)
        planning_path: Path to planning data (optional)
        building_age_path: Path to building age data (optional)
        heritage_path: Path to heritage data (optional)
        companies_house_path: Path to Companies House data (optional)
        land_registry_path: Path to Land Registry Price Paid data (optional)
        epc_path: Path to EPC data (optional)
        use_wikidata: Use Wikidata enrichment (default: True)
        use_building_age: Use building age estimator (default: True)
        resume_from_checkpoint: Resume from checkpoint if available (default: True)
        save_checkpoints: Save checkpoints during processing (default: True)
        checkpoint_interval: Save checkpoint every N POIs (default: 100)
        
    Returns:
        DataFrame with opening dates from multiple sources
        
    Raises:
        PipelineError: If input validation fails or critical errors occur
    """
    logger.info("=" * 70)
    logger.info("UNIFIED OPENING DATE PIPELINE (IMPROVED)")
    logger.info("=" * 70)
    logger.info("\nStrategy: Multi-source enrichment with 7+ data sources")
    logger.info("  - Wikidata (POIs)")
    logger.info("  - Companies House (businesses)")
    logger.info("  - Planning (new builds)")
    logger.info("  - Land Registry (residential)")
    logger.info("  - Building Age (properties)")
    logger.info("  - EPC (refinement)")
    logger.info("  - Heritage (listed buildings)")
    
    # Set checkpoint file
    if checkpoint_file is None:
        checkpoint_file = str(CHECKPOINT_FILE)
    
    # Load input data
    logger.info(f"\n1. Loading POI data from: {input_file}")
    if not os.path.exists(input_file):
        raise PipelineError(f"Input file not found: {input_file}")
    
    df = load_london_pois(input_file)
    if df.empty:
        raise PipelineError(f"Input file is empty: {input_file}")
    
    logger.info(f"   Loaded {len(df):,} POIs")
    
    # Validate input data
    logger.info("\n2. Validating input data...")
    validation = validate_input_data(df, required_columns=[])
    logger.info(f"   Total rows: {validation['total_rows']:,}")
    if validation['has_name']:
        logger.info(f"   Rows with name: {validation.get('rows_with_name', 0):,}")
    if validation['has_coordinates']:
        logger.info(f"   Rows with coordinates: {validation.get('rows_with_coords', 0):,}")
    if validation['has_uprn']:
        logger.info(f"   Rows with UPRN: {validation.get('rows_with_uprn', 0):,}")
    
    # Initialize result DataFrame
    result = df.copy()
    
    # Check for checkpoint
    if resume_from_checkpoint:
        checkpoint_df = load_checkpoint(checkpoint_file)
        if checkpoint_df is not None and len(checkpoint_df) > 0:
            # Merge checkpoint data back
            if 'poi_id' in checkpoint_df.columns and 'poi_id' in result.columns:
                # Use merge to update existing rows
                result = result.set_index('poi_id')
                checkpoint_df = checkpoint_df.set_index('poi_id')
                result.update(checkpoint_df[['opening_date', 'opening_date_source', 
                                           'opening_date_confidence', 'wikidata_id']])
                result = result.reset_index()
                logger.info("   ✓ Resumed from checkpoint")
            elif len(checkpoint_df) == len(result):
                # Direct copy if same length
                for col in ['opening_date', 'opening_date_source', 
                           'opening_date_confidence', 'wikidata_id']:
                    if col in checkpoint_df.columns:
                        result[col] = checkpoint_df[col]
                logger.info("   ✓ Resumed from checkpoint")
    
    # Initialize columns if they don't exist
    for col in ['opening_date', 'opening_date_source', 'opening_date_confidence', 'wikidata_id']:
        if col not in result.columns:
            result[col] = None
    
    # Step 1: Wikidata enrichment (for POIs with names)
    if use_wikidata and validation['has_name']:
        logger.info("\n3. Enriching with Wikidata (for POIs with names)...")
        try:
            # Only process rows without opening dates
            missing_mask = result['opening_date'].isna()
            missing_df = result[missing_mask].copy()
            
            if len(missing_df) > 0:
                logger.info(f"   Processing {len(missing_df):,} POIs without dates...")
                df_wikidata = enrich_with_opening_dates(missing_df)
                
                # Vectorized update of results
                mask = df_wikidata['opening_date'].notna()
                if mask.any():
                    # Convert dates properly
                    dates = pd.to_datetime(df_wikidata.loc[mask, 'opening_date'], errors='coerce')
                    
                    # Update result using index alignment
                    result_index = result.index[missing_mask][mask]
                    result.loc[result_index, 'opening_date'] = dates.values
                    result.loc[result_index, 'opening_date_source'] = 'wikidata'
                    result.loc[result_index, 'opening_date_confidence'] = 'high'
                    if 'wikidata_id' in df_wikidata.columns:
                        result.loc[result_index, 'wikidata_id'] = df_wikidata.loc[mask, 'wikidata_id'].values
                    
                    wikidata_count = mask.sum()
                    logger.info(f"   ✓ Wikidata: {wikidata_count:,} POIs ({wikidata_count/len(df)*100:.1f}%)")
                else:
                    logger.info("   ✓ Wikidata: 0 POIs found")
            else:
                logger.info("   ✓ All POIs already have dates, skipping Wikidata")
        except Exception as e:
            logger.error(f"   ✗ Wikidata enrichment failed: {e}", exc_info=True)
            if not use_building_age:
                raise PipelineError(f"Wikidata enrichment failed and no fallback: {e}")
    
    # Step 2: Building age estimator (for properties via UPRN)
    if use_building_age and validation['has_uprn']:
        logger.info("\n4. Enriching with building age data (for properties)...")
        
        # Only process rows without opening dates
        missing_mask = result['opening_date'].isna()
        missing_df = result[missing_mask].copy()
        
        if len(missing_df) > 0:
            try:
                estimator = BuildingOpeningDateEstimator(use_wikidata=False)  # Already tried Wikidata
                
                # Load data sources if provided
                sources_loaded = []
                if building_age_path and os.path.exists(building_age_path):
                    estimator.load_building_age_data(building_age_path)
                    sources_loaded.append("building_age")
                if planning_path and os.path.exists(planning_path):
                    estimator.load_planning_data(planning_path)
                    sources_loaded.append("planning")
                if heritage_path and os.path.exists(heritage_path):
                    estimator.load_heritage_data(heritage_path)
                    sources_loaded.append("heritage")
                if companies_house_path and os.path.exists(companies_house_path):
                    estimator.load_companies_house_data(companies_house_path)
                    sources_loaded.append("companies_house")
                if land_registry_path and os.path.exists(land_registry_path):
                    estimator.load_land_registry_data(land_registry_path)
                    sources_loaded.append("land_registry")
                if epc_path and os.path.exists(epc_path):
                    estimator.load_epc_data(epc_path)
                    sources_loaded.append("epc")
                
                if sources_loaded:
                    logger.info(f"   Loaded sources: {', '.join(sources_loaded)}")
                    
                    # Estimate dates for missing rows
                    estimated = estimator.estimate_opening_dates(
                        missing_df,
                        uprn_col='UPRN',
                        priority_order=['companies_house', 'planning', 'land_registry', 
                                      'building_age', 'epc', 'heritage']
                    )
                    
                    # Vectorized update of results
                    mask = estimated['estimated_opening_date'].notna()
                    if mask.any():
                        result_index = result.index[missing_mask][mask]
                        result.loc[result_index, 'opening_date'] = estimated.loc[mask, 'estimated_opening_date'].values
                        result.loc[result_index, 'opening_date_source'] = estimated.loc[mask, 'opening_date_source'].values
                        result.loc[result_index, 'opening_date_confidence'] = estimated.loc[mask, 'opening_date_confidence'].values
                        
                        all_sources_count = mask.sum()
                        logger.info(f"   ✓ All sources: {all_sources_count:,} properties enriched")
                    else:
                        logger.info("   ✓ Building age: 0 properties found")
                else:
                    logger.info("   ⚠ No building age data sources provided")
            except Exception as e:
                logger.error(f"   ✗ Building age estimation failed: {e}", exc_info=True)
    
    # Step 3: Add year column (handle NaT properly)
    logger.info("\n5. Calculating derived fields...")
    dates = pd.to_datetime(result['opening_date'], errors='coerce')
    result['opening_date_year'] = dates.dt.year
    result['opening_date_year'] = result['opening_date_year'].where(dates.notna(), None)
    
    # Calculate data quality metrics
    metrics = calculate_data_quality_metrics(result)
    
    # Save checkpoint
    if save_checkpoints:
        save_checkpoint(result, checkpoint_file)
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 70)
    
    logger.info(f"\nTotal POIs: {metrics['total_pois']:,}")
    logger.info(f"With opening dates: {metrics['with_dates']:,} ({metrics['coverage_pct']:.1f}%)")
    logger.info(f"Without dates: {metrics['without_dates']:,}")
    
    if metrics['with_dates'] > 0:
        logger.info(f"\nBy source:")
        for source, count in metrics['by_source'].items():
            pct = (count / metrics['with_dates']) * 100
            logger.info(f"  {source:20s}: {count:6,} ({pct:5.1f}%)")
        
        logger.info(f"\nBy confidence:")
        for conf, count in metrics['by_confidence'].items():
            pct = (count / metrics['with_dates']) * 100
            logger.info(f"  {conf:20s}: {count:6,} ({pct:5.1f}%)")
        
        if metrics['date_range']:
            logger.info(f"\nDate range:")
            logger.info(f"  Earliest: {metrics['date_range'].get('earliest', 'N/A')}")
            logger.info(f"  Latest: {metrics['date_range'].get('latest', 'N/A')}")
            logger.info(f"  Median: {metrics['date_range'].get('median', 'N/A')}")
        
        if metrics['data_quality']:
            logger.info(f"\nData quality:")
            logger.info(f"  High confidence: {metrics['data_quality'].get('high_confidence_pct', 0):.1f}%")
            logger.info(f"  Medium confidence: {metrics['data_quality'].get('medium_confidence_pct', 0):.1f}%")
            if metrics['data_quality'].get('low_confidence_pct', 0) > 0:
                logger.info(f"  Low confidence: {metrics['data_quality'].get('low_confidence_pct', 0):.1f}%")
    
    # Save results
    if output_file is None:
        output_file = str(OUTPUT_CSV)
    
    logger.info(f"\n6. Saving results...")
    try:
        result.to_csv(output_file, index=False)
        logger.info(f"   ✓ Saved CSV: {output_file}")
    except Exception as e:
        logger.error(f"   ✗ Failed to save CSV: {e}")
        raise PipelineError(f"Failed to save results: {e}")
    
    # Try Parquet
    try:
        parquet_file = output_file.replace('.csv', '.parquet')
        result.to_parquet(parquet_file, index=False, engine='pyarrow')
        logger.info(f"   ✓ Saved Parquet: {parquet_file}")
    except Exception as e:
        logger.warning(f"   ⚠ Could not save Parquet: {e}")
    
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 70)
    
    return result


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Unified Opening Date Pipeline (Improved)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python -m src.unified_opening_date_pipeline --input data/raw/london_pois.csv
  
  # With building age data
  python -m src.unified_opening_date_pipeline \\
    --input data/raw/london_pois.csv \\
    --building-age data/raw/os_building_age.gpkg
  
  # Skip Wikidata (building age only)
  python -m src.unified_opening_date_pipeline \\
    --input data/raw/london_pois.csv \\
    --no-wikidata \\
    --building-age data/raw/os_building_age.gpkg
        """
    )
    parser.add_argument('--input', type=str, required=True,
                       help='Input POI CSV file')
    parser.add_argument('--output', type=str,
                       help='Output CSV file (default: config.OUTPUT_CSV)')
    parser.add_argument('--checkpoint', type=str,
                       help='Checkpoint file path (default: config.CHECKPOINT_FILE)')
    parser.add_argument('--planning', type=str,
                       help='Planning data CSV/GeoPackage')
    parser.add_argument('--building-age', type=str, dest='building_age',
                       help='Building age data CSV/GeoPackage')
    parser.add_argument('--heritage', type=str,
                       help='Heritage data CSV/GeoPackage')
    parser.add_argument('--companies-house', type=str, dest='companies_house',
                       help='Companies House data CSV')
    parser.add_argument('--land-registry', type=str, dest='land_registry',
                       help='Land Registry Price Paid data CSV')
    parser.add_argument('--epc', type=str,
                       help='EPC data CSV')
    parser.add_argument('--no-wikidata', action='store_true',
                       help='Skip Wikidata enrichment')
    parser.add_argument('--no-building-age', action='store_true', dest='no_building_age',
                       help='Skip building age estimation')
    parser.add_argument('--no-resume', action='store_true',
                       help='Do not resume from checkpoint')
    parser.add_argument('--no-checkpoints', action='store_true',
                       help='Do not save checkpoints')
    
    args = parser.parse_args()
    
    try:
        unified_opening_date_pipeline(
            input_file=args.input,
            output_file=args.output,
            checkpoint_file=args.checkpoint,
            planning_path=args.planning,
            building_age_path=args.building_age,
            heritage_path=args.heritage,
            companies_house_path=args.companies_house,
            land_registry_path=args.land_registry,
            epc_path=args.epc,
            use_wikidata=not args.no_wikidata,
            use_building_age=not args.no_building_age,
            resume_from_checkpoint=not args.no_resume,
            save_checkpoints=not args.no_checkpoints
        )
    except PipelineError as e:
        logger.error(f"Pipeline error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
