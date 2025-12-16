"""
Charity Commission API Client
==============================

Access registered charity data for England and Wales.

API Documentation: https://register-of-charities.charitycommission.gov.uk/

Usage:
    from src.clients import CharitiesClient
    
    client = CharitiesClient()
    charities = client.search_charities("education")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class CharitiesClient(BaseAPIClient):
    """
    Client for Charity Commission API.
    
    Access data on 170,000+ registered charities.
    """
    
    BASE_URL = "https://api.charitycommission.gov.uk/register/api"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Charity Commission client.
        
        Args:
            api_key: API key (optional - some endpoints work without)
        """
        super().__init__(
            base_url=self.BASE_URL,
            api_key=api_key,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup API key header if provided"""
        if self.api_key:
            self.session.headers['Ocp-Apim-Subscription-Key'] = self.api_key
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.search_charities("test", page_size=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # CHARITY SEARCH
    # ========================================
    
    def search_charities(
        self,
        search_term: Optional[str] = None,
        charity_type: Optional[str] = None,
        status: str = "Registered",
        income_from: Optional[int] = None,
        income_to: Optional[int] = None,
        page: int = 0,
        page_size: int = 50
    ) -> Dict:
        """
        Search for charities.
        
        Args:
            search_term: Name or keyword search
            charity_type: Type filter
            status: Registered, Removed, etc.
            income_from: Minimum annual income
            income_to: Maximum annual income
            page: Page number (0-based)
            page_size: Results per page
            
        Returns:
            Search results
        """
        params = {
            'searchText': search_term or '',
            'pageNumber': page,
            'pageSize': page_size
        }
        
        if status:
            params['status'] = status
        if income_from:
            params['incomeFrom'] = income_from
        if income_to:
            params['incomeTo'] = income_to
        
        return self.get('/allcharitydetails', params=params)
    
    def search_charities_df(self, **kwargs) -> pd.DataFrame:
        """Search charities as DataFrame"""
        result = self.search_charities(**kwargs)
        charities = result if isinstance(result, list) else result.get('charities', [])
        return pd.DataFrame(charities)
    
    # ========================================
    # SINGLE CHARITY
    # ========================================
    
    def get_charity(self, charity_number: int) -> Dict:
        """
        Get details for a single charity.
        
        Args:
            charity_number: Registered charity number
            
        Returns:
            Charity details
        """
        return self.get(f'/charitydetails/{charity_number}/0')
    
    def get_charity_accounts(self, charity_number: int) -> List[Dict]:
        """Get financial accounts for a charity"""
        return self.get(f'/charityaccounts/{charity_number}/0')
    
    def get_charity_trustees(self, charity_number: int) -> List[Dict]:
        """Get trustees for a charity"""
        return self.get(f'/charitytrustees/{charity_number}/0')
    
    def get_charity_areas(self, charity_number: int) -> List[Dict]:
        """Get areas of operation for a charity"""
        return self.get(f'/charityareaofoperation/{charity_number}/0')
    
    def get_charity_classifications(self, charity_number: int) -> List[Dict]:
        """Get classification (what/who/how) for a charity"""
        return self.get(f'/charityclassification/{charity_number}/0')
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_charities_by_postcode(
        self,
        postcode: str,
        radius_miles: float = 5.0,
        page_size: int = 100
    ) -> pd.DataFrame:
        """
        Get charities near a postcode.
        
        Note: This searches by registered address postcode area.
        """
        # Extract outcode
        outcode = postcode.split()[0] if ' ' in postcode else postcode[:4]
        
        return self.search_charities_df(
            search_term=outcode,
            page_size=page_size
        )
    
    def get_large_charities(
        self,
        min_income: int = 1000000,
        page_size: int = 100
    ) -> pd.DataFrame:
        """Get charities with income above threshold"""
        return self.search_charities_df(
            income_from=min_income,
            page_size=page_size
        )
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_bulk_download_url(self) -> str:
        """Get URL for bulk data download"""
        return "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/publicextract.charity.zip"

