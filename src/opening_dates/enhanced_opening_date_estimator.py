"""
Enhanced Opening Date Estimator
Combines multiple data sources with improved priority system and additional methods
"""

import pandas as pd
import os
import sys
from typing import Dict, Optional, List
from datetime import datetime
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.opening_dates.building_opening_date_estimator import BuildingOpeningDateEstimator
from src.opening_dates.wikidata_client import WikidataClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedOpeningDateEstimator(BuildingOpeningDateEstimator):
    """
    Enhanced estimator that combines:
    1. Wikidata (for POIs with names) - NEW
    2. Planning completion dates
    3. Web scraping (for institutions) - FUTURE
    4. Building age from OS/NGD
    5. Heritage/listed building records
    6. Land Registry first transaction - FUTURE
    7. Companies House incorporation - FUTURE
    """
    
    def __init__(self, use_wikidata: bool = True, use_web_scraping: bool = False):
        """
        Initialize enhanced estimator
        
        Args:
            use_wikidata: Use Wikidata for POI opening dates (default: True)
            use_web_scraping: Use web scraping for institutions (default: False, not yet implemented)
        """
        super().__init__(use_wikidata=use_wikidata)
        self.use_web_scraping = use_web_scraping
    
    def estimate_opening_dates_enhanced(
        self,
        df: pd.DataFrame,
        uprn_col: str = 'UPRN',
        poi_name_col: str = 'name',
        address_col: str = 'address',
        priority_order: List[str] = None
    ) -> pd.DataFrame:
        """
        Enhanced opening date estimation with multiple sources
        
        Args:
            df: DataFrame with UPRNs and optionally POI names
            uprn_col: Column name for UPRN
            poi_name_col: Column name for POI name (for Wikidata lookup)
            address_col: Column name for address (for matching)
            priority_order: Custom priority order
        
        Returns:
            DataFrame with estimated opening dates and confidence scores
        """
        if priority_order is None:
            priority_order = [
                'wikidata',      # For POIs with names
                'planning',      # For new builds
                'web_scraping', # For institutions (future)
                'building_age', # Fallback
                'heritage',     # For listed buildings
                'land_registry', # For properties (future)
                'companies_house' # For commercial (future)
            ]
        
        print("=" * 70)
        print("ENHANCED OPENING DATE ESTIMATION")
        print("=" * 70)
        print(f"\nPriority order: {' → '.join(priority_order)}")
        
        # Start with base estimator
        result = super().estimate_opening_dates(
            df,
            uprn_col=uprn_col,
            priority_order=[p for p in priority_order if p in ['planning', 'building_age', 'heritage']],
            poi_name_col=poi_name_col
        )
        
        # Add enhanced methods here as they're implemented
        # (web_scraping, land_registry, companies_house)
        
        return result
    
    def get_opening_date_sources_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary of which sources provided opening dates
        
        Args:
            df: DataFrame with opening dates estimated
        
        Returns:
            Summary DataFrame
        """
        if 'opening_date_source' not in df.columns:
            return pd.DataFrame()
        
        summary = df.groupby('opening_date_source').agg({
            'opening_date_source': 'count',
            'opening_date_confidence': lambda x: x.value_counts().to_dict()
        }).rename(columns={'opening_date_source': 'count'})
        
        return summary


def compare_opening_date_methods(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare different methods for getting opening dates
    
    Args:
        df: DataFrame with POI data
    
    Returns:
        Comparison DataFrame showing results from each method
    """
    estimator = EnhancedOpeningDateEstimator(use_wikidata=True)
    
    # Try different priority orders
    methods = {
        'wikidata_first': ['wikidata', 'planning', 'building_age', 'heritage'],
        'planning_first': ['planning', 'wikidata', 'building_age', 'heritage'],
        'building_age_first': ['building_age', 'planning', 'wikidata', 'heritage'],
    }
    
    results = {}
    for method_name, priority in methods.items():
        result = estimator.estimate_opening_dates_enhanced(
            df,
            priority_order=priority
        )
        results[method_name] = {
            'total': len(result),
            'with_dates': result['estimated_opening_date'].notna().sum(),
            'coverage': result['estimated_opening_date'].notna().sum() / len(result) * 100,
            'sources': result['opening_date_source'].value_counts().to_dict()
        }
    
    return pd.DataFrame(results).T


if __name__ == "__main__":
    print("Enhanced Opening Date Estimator")
    print("See docs/OPENING_DATE_METHODS.md for comprehensive guide")

