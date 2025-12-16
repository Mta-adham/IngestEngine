"""
NOMIS / ONS Labour Market API Client
=====================================

Access labour market and census statistics.

API Documentation: https://www.nomisweb.co.uk/api/v01/help

Usage:
    from src.clients import NOMISClient
    
    client = NOMISClient()
    data = client.get_employment_data(geography="E09000001")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class NOMISClient(BaseAPIClient):
    """
    Client for NOMIS labour market statistics.
    
    Access:
    - Employment and unemployment
    - Claimant count
    - Annual Business Survey
    - Census data
    - Earnings
    """
    
    BASE_URL = "https://www.nomisweb.co.uk/api/v01"
    
    def __init__(self, **kwargs):
        """Initialize NOMIS client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=30,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_datasets()
            return True
        except Exception:
            return False
    
    # ========================================
    # DATASETS
    # ========================================
    
    def get_datasets(self) -> List[Dict]:
        """Get list of available datasets"""
        result = self.get('/dataset/def.sdmx.json')
        
        structures = result.get('structure', {})
        keyfamilies = structures.get('keyfamilies', {})
        return keyfamilies.get('keyfamily', [])
    
    def get_datasets_df(self) -> pd.DataFrame:
        """Get datasets as DataFrame"""
        datasets = self.get_datasets()
        
        records = []
        for ds in datasets:
            records.append({
                'id': ds.get('id'),
                'name': ds.get('name', {}).get('value'),
                'agency': ds.get('agencyid'),
            })
        
        return pd.DataFrame(records)
    
    # ========================================
    # CLAIMANT COUNT
    # ========================================
    
    def get_claimant_count(
        self,
        geography: str,
        time: str = "latest"
    ) -> Dict:
        """
        Get claimant count data.
        
        Args:
            geography: Geography code (e.g., E09000001 for City of London)
            time: Time period (e.g., "latest", "2024-01")
            
        Returns:
            Claimant count data
        """
        # NM_162_1 is the Claimant Count dataset
        params = {
            'geography': geography,
            'time': time,
            'select': 'geography_name,date_name,obs_value'
        }
        
        return self.get('/dataset/NM_162_1.data.json', params=params)
    
    def get_claimant_count_df(
        self,
        geography: str,
        time: str = "latest"
    ) -> pd.DataFrame:
        """Get claimant count as DataFrame"""
        result = self.get_claimant_count(geography, time)
        
        obs = result.get('obs', [])
        return pd.DataFrame(obs)
    
    # ========================================
    # EMPLOYMENT
    # ========================================
    
    def get_employment_data(
        self,
        geography: str,
        time: str = "latest"
    ) -> Dict:
        """
        Get employment/unemployment data.
        
        Args:
            geography: Geography code
            time: Time period
            
        Returns:
            Employment data
        """
        # NM_17_5 is Annual Population Survey
        params = {
            'geography': geography,
            'time': time,
        }
        
        return self.get('/dataset/NM_17_5.data.json', params=params)
    
    # ========================================
    # BUSINESS COUNTS
    # ========================================
    
    def get_business_counts(
        self,
        geography: str,
        industry: Optional[str] = None
    ) -> Dict:
        """
        Get business counts by area.
        
        Args:
            geography: Geography code
            industry: SIC code filter
            
        Returns:
            Business count data
        """
        # NM_141_1 is UK Business Counts
        params = {
            'geography': geography,
            'time': 'latest',
        }
        
        if industry:
            params['industry'] = industry
        
        return self.get('/dataset/NM_141_1.data.json', params=params)
    
    # ========================================
    # EARNINGS
    # ========================================
    
    def get_earnings_data(
        self,
        geography: str,
        time: str = "latest"
    ) -> Dict:
        """
        Get earnings data.
        
        Args:
            geography: Geography code
            time: Time period
            
        Returns:
            Earnings data
        """
        # NM_99_1 is Annual Survey of Hours and Earnings
        params = {
            'geography': geography,
            'time': time,
        }
        
        return self.get('/dataset/NM_99_1.data.json', params=params)
    
    # ========================================
    # CENSUS DATA
    # ========================================
    
    def get_census_data(
        self,
        dataset_id: str,
        geography: str,
        measures: Optional[str] = None
    ) -> Dict:
        """
        Get Census 2021 data.
        
        Args:
            dataset_id: Census dataset ID
            geography: Geography code
            measures: Measure codes
            
        Returns:
            Census data
        """
        params = {
            'geography': geography,
        }
        
        if measures:
            params['measures'] = measures
        
        return self.get(f'/dataset/{dataset_id}.data.json', params=params)
    
    # ========================================
    # GEOGRAPHY
    # ========================================
    
    def get_geography_types(self) -> List[Dict]:
        """Get available geography types"""
        return [
            {'code': 'TYPE499', 'name': 'Local Authority Districts'},
            {'code': 'TYPE464', 'name': 'Westminster Parliamentary Constituencies'},
            {'code': 'TYPE460', 'name': 'LSOAs'},
            {'code': 'TYPE312', 'name': 'MSOAs'},
            {'code': 'TYPE265', 'name': 'Output Areas'},
            {'code': 'TYPE480', 'name': 'Travel to Work Areas'},
            {'code': 'TYPE434', 'name': 'Combined Authorities'},
        ]
    
    def get_local_authorities(self) -> pd.DataFrame:
        """Get list of local authorities"""
        result = self.get('/dataset/NM_162_1/geography/TYPE499.def.sdmx.json')
        
        geographies = result.get('structure', {}).get('codelists', {}).get('codelist', [])
        
        records = []
        for geo in geographies:
            for code in geo.get('code', []):
                records.append({
                    'code': code.get('value'),
                    'name': code.get('description', {}).get('value'),
                })
        
        return pd.DataFrame(records)
    
    # ========================================
    # COMMON DATASET IDS
    # ========================================
    
    def get_common_datasets(self) -> Dict[str, str]:
        """Get commonly used dataset IDs"""
        return {
            'claimant_count': 'NM_162_1',
            'annual_population_survey': 'NM_17_5',
            'business_counts': 'NM_141_1',
            'earnings': 'NM_99_1',
            'jobs_density': 'NM_57_1',
            'population_estimates': 'NM_2010_1',
            'census_population': 'NM_2021_1',
        }

