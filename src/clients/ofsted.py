"""
Ofsted API Client
==================

Access education inspection data for schools, childcare, and more.

API Documentation: https://www.gov.uk/government/statistical-data-sets/

Usage:
    from src.clients import OfstedClient
    
    client = OfstedClient()
    schools = client.search_schools(postcode="SW1")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class OfstedClient(BaseAPIClient):
    """
    Client for Ofsted education data.
    
    Access school inspection reports and ratings.
    Note: Ofsted doesn't have a public REST API, so we use data.gov.uk
    """
    
    BASE_URL = "https://www.compare-school-performance.service.gov.uk/api"
    GOV_DATA_URL = "https://data.gov.uk/api/action"
    
    def __init__(self, **kwargs):
        """Initialize Ofsted client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            # Test via gov.uk data portal
            self.get_dataset_info("ofsted-inspections")
            return True
        except Exception:
            return False
    
    # ========================================
    # SCHOOL SEARCH (via DfE API)
    # ========================================
    
    def search_schools(
        self,
        name: Optional[str] = None,
        postcode: Optional[str] = None,
        la_code: Optional[str] = None,
        phase: Optional[str] = None,
        page: int = 1,
        page_size: int = 50
    ) -> List[Dict]:
        """
        Search for schools.
        
        Args:
            name: School name search
            postcode: Postcode search
            la_code: Local authority code
            phase: Phase of education (primary, secondary, etc.)
            page: Page number
            page_size: Results per page
            
        Returns:
            List of schools
        """
        params = {
            'page': page,
            'pageSize': page_size
        }
        
        if name:
            params['name'] = name
        if postcode:
            params['postcode'] = postcode
        if la_code:
            params['laCode'] = la_code
        if phase:
            params['phase'] = phase
        
        try:
            result = self.get('/schools', params=params)
            return result.get('schools', result) if isinstance(result, dict) else result
        except Exception as e:
            logger.warning(f"DfE API error: {e}")
            return []
    
    def search_schools_df(self, **kwargs) -> pd.DataFrame:
        """Search schools as DataFrame"""
        schools = self.search_schools(**kwargs)
        return pd.DataFrame(schools)
    
    # ========================================
    # GOV.UK DATA PORTAL
    # ========================================
    
    def get_dataset_info(self, dataset_name: str) -> Dict:
        """Get info about a dataset from data.gov.uk"""
        import requests
        response = requests.get(
            f"{self.GOV_DATA_URL}/package_search",
            params={'q': dataset_name, 'rows': 5},
            timeout=30
        )
        return response.json()
    
    # ========================================
    # BULK DATA URLs
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk Ofsted data downloads"""
        return {
            'state_funded_schools': 'https://www.compare-school-performance.service.gov.uk/download-data',
            'ofsted_inspections': 'https://www.gov.uk/government/statistical-data-sets/monthly-management-information-ofsteds-school-inspections-outcomes',
            'school_inspection_data': 'https://www.gov.uk/government/collections/maintained-schools-inspection-outcomes',
            'childcare_providers': 'https://www.gov.uk/government/statistical-data-sets/childcare-providers-and-inspections-as-at-31-march-2024',
            'edubase': 'https://get-information-schools.service.gov.uk/Downloads',
        }
    
    def get_edubase_extract_url(self) -> str:
        """Get URL for EduBase (GIAS) school database"""
        return "https://get-information-schools.service.gov.uk/Downloads"

