"""
GOV.UK APIs Client
===================

Access various GOV.UK services including:
- Content API
- Bank Holidays
- Notify API
- Register API

API Documentation: https://www.api.gov.uk/

Usage:
    from src.clients import GovUKClient
    
    client = GovUKClient()
    holidays = client.get_bank_holidays()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class GovUKClient(BaseAPIClient):
    """
    Client for GOV.UK APIs.
    
    Access:
    - Content API
    - Bank Holidays
    - Various government registers
    """
    
    BASE_URL = "https://www.gov.uk/api"
    REGISTERS_URL = "https://registers.service.gov.uk"
    
    def __init__(self, **kwargs):
        """Initialize GOV.UK client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required for public APIs"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_bank_holidays()
            return True
        except Exception:
            return False
    
    # ========================================
    # BANK HOLIDAYS
    # ========================================
    
    def get_bank_holidays(self) -> Dict:
        """
        Get UK bank holidays for all divisions.
        
        Returns:
            Bank holidays by division (england-and-wales, scotland, northern-ireland)
        """
        import requests
        response = requests.get(
            "https://www.gov.uk/bank-holidays.json",
            timeout=30
        )
        return response.json()
    
    def get_bank_holidays_df(self, division: str = "england-and-wales") -> pd.DataFrame:
        """
        Get bank holidays as DataFrame.
        
        Args:
            division: england-and-wales, scotland, or northern-ireland
            
        Returns:
            DataFrame of bank holidays
        """
        holidays = self.get_bank_holidays()
        events = holidays.get(division, {}).get('events', [])
        return pd.DataFrame(events)
    
    def get_next_bank_holiday(self, division: str = "england-and-wales") -> Dict:
        """Get the next upcoming bank holiday"""
        from datetime import datetime
        
        df = self.get_bank_holidays_df(division)
        df['date'] = pd.to_datetime(df['date'])
        
        today = datetime.now()
        future = df[df['date'] > today]
        
        if not future.empty:
            return future.iloc[0].to_dict()
        return {}
    
    # ========================================
    # CONTENT API
    # ========================================
    
    def get_content(self, path: str) -> Dict:
        """
        Get content from GOV.UK.
        
        Args:
            path: Content path (e.g., '/vat-rates')
            
        Returns:
            Content data
        """
        path = path.lstrip('/')
        return self.get(f'/content/{path}')
    
    def search_content(
        self,
        query: str,
        count: int = 10,
        filter_format: Optional[str] = None
    ) -> Dict:
        """
        Search GOV.UK content.
        
        Args:
            query: Search query
            count: Number of results
            filter_format: Filter by format (guide, answer, etc.)
            
        Returns:
            Search results
        """
        params = {
            'q': query,
            'count': count
        }
        
        if filter_format:
            params['filter_format'] = filter_format
        
        return self.get('/search.json', params=params)
    
    # ========================================
    # GOVERNMENT REGISTERS
    # ========================================
    
    def get_register(self, register_name: str) -> List[Dict]:
        """
        Get data from a government register.
        
        Args:
            register_name: Register name (e.g., 'country', 'local-authority-eng')
            
        Returns:
            Register entries
        """
        import requests
        
        response = requests.get(
            f"https://{register_name}.register.gov.uk/records.json",
            timeout=30
        )
        
        if response.status_code != 200:
            return []
        
        return list(response.json().values())
    
    def get_countries(self) -> pd.DataFrame:
        """Get list of countries from register"""
        records = self.get_register('country')
        return pd.DataFrame(records)
    
    def get_local_authorities_england(self) -> pd.DataFrame:
        """Get English local authorities"""
        records = self.get_register('local-authority-eng')
        return pd.DataFrame(records)
    
    def get_local_authority_types(self) -> pd.DataFrame:
        """Get local authority types"""
        records = self.get_register('local-authority-type')
        return pd.DataFrame(records)
    
    # ========================================
    # POSTCODE LOOKUP
    # ========================================
    
    def get_local_authority_for_postcode(self, postcode: str) -> Dict:
        """
        Get local authority info for a postcode.
        
        Args:
            postcode: UK postcode
            
        Returns:
            Local authority information
        """
        import requests
        
        response = requests.get(
            f"https://api.postcodes.io/postcodes/{postcode}",
            timeout=30
        )
        
        if response.status_code != 200:
            return {}
        
        data = response.json().get('result', {})
        return {
            'postcode': data.get('postcode'),
            'local_authority': data.get('admin_district'),
            'local_authority_code': data.get('codes', {}).get('admin_district'),
            'region': data.get('region'),
            'country': data.get('country'),
            'constituency': data.get('parliamentary_constituency'),
        }
    
    # ========================================
    # USEFUL GOV.UK RESOURCES
    # ========================================
    
    def get_useful_apis(self) -> Dict[str, str]:
        """Get list of useful GOV.UK APIs and resources"""
        return {
            'bank_holidays': 'https://www.gov.uk/bank-holidays.json',
            'content_api': 'https://www.gov.uk/api/content/',
            'search_api': 'https://www.gov.uk/api/search.json',
            'notify_api': 'https://api.notifications.service.gov.uk/',
            'pay_api': 'https://publicapi.payments.service.gov.uk/',
            'register_country': 'https://country.register.gov.uk/',
            'register_la': 'https://local-authority-eng.register.gov.uk/',
            'statistics': 'https://www.gov.uk/government/statistics',
            'data_gov_uk': 'https://data.gov.uk/',
            'api_catalogue': 'https://www.api.gov.uk/',
        }
    
    # ========================================
    # DATA.GOV.UK
    # ========================================
    
    def search_datasets(
        self,
        query: str,
        rows: int = 10,
        organization: Optional[str] = None
    ) -> Dict:
        """
        Search data.gov.uk datasets.
        
        Args:
            query: Search query
            rows: Number of results
            organization: Filter by organization
            
        Returns:
            Search results
        """
        import requests
        
        params = {
            'q': query,
            'rows': rows
        }
        
        if organization:
            params['fq'] = f'organization:{organization}'
        
        response = requests.get(
            "https://data.gov.uk/api/action/package_search",
            params=params,
            timeout=30
        )
        
        return response.json()
    
    def get_dataset(self, dataset_name: str) -> Dict:
        """Get dataset metadata from data.gov.uk"""
        import requests
        
        response = requests.get(
            f"https://data.gov.uk/api/action/package_show?id={dataset_name}",
            timeout=30
        )
        
        return response.json()

