"""
Care Quality Commission (CQC) API Client
=========================================

Access health and social care provider inspection data.

API Documentation: https://api.cqc.org.uk/public/v1

Usage:
    from src.clients import CQCClient
    
    client = CQCClient()
    providers = client.search_providers(postcode="SW1")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class CQCClient(BaseAPIClient):
    """
    Client for Care Quality Commission (CQC) API.
    
    Access inspection data for:
    - Hospitals
    - Care homes
    - GP practices
    - Dental practices
    - Mental health services
    """
    
    BASE_URL = "https://api.cqc.org.uk/public/v1"
    
    def __init__(self, **kwargs):
        """Initialize CQC client."""
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
            self.get_provider_types()
            return True
        except Exception:
            return False
    
    # ========================================
    # PROVIDERS
    # ========================================
    
    def search_providers(
        self,
        name: Optional[str] = None,
        postcode: Optional[str] = None,
        location_id: Optional[str] = None,
        overall_rating: Optional[str] = None,
        provider_type: Optional[str] = None,
        local_authority: Optional[str] = None,
        page: int = 1,
        per_page: int = 100
    ) -> Dict:
        """
        Search for CQC-registered providers.
        
        Args:
            name: Provider name search
            postcode: Postcode search
            location_id: CQC location ID
            overall_rating: Outstanding, Good, Requires improvement, Inadequate
            provider_type: Type filter
            local_authority: Local authority filter
            page: Page number
            per_page: Results per page
            
        Returns:
            Search results
        """
        params = {
            'page': page,
            'perPage': per_page
        }
        
        if name:
            params['providerName'] = name
        if postcode:
            params['postalCode'] = postcode
        if location_id:
            params['locationId'] = location_id
        if overall_rating:
            params['overallRating'] = overall_rating
        if provider_type:
            params['type'] = provider_type
        if local_authority:
            params['localAuthority'] = local_authority
        
        return self.get('/providers', params=params)
    
    def search_providers_df(self, **kwargs) -> pd.DataFrame:
        """Search providers as DataFrame"""
        result = self.search_providers(**kwargs)
        providers = result.get('providers', [])
        return pd.DataFrame(providers)
    
    def get_provider(self, provider_id: str) -> Dict:
        """Get details for a single provider"""
        return self.get(f'/providers/{provider_id}')
    
    # ========================================
    # LOCATIONS
    # ========================================
    
    def search_locations(
        self,
        name: Optional[str] = None,
        postcode: Optional[str] = None,
        overall_rating: Optional[str] = None,
        care_home: Optional[bool] = None,
        page: int = 1,
        per_page: int = 100
    ) -> Dict:
        """
        Search for CQC-registered locations.
        
        Args:
            name: Location name search
            postcode: Postcode search
            overall_rating: Rating filter
            care_home: Filter to care homes only
            page: Page number
            per_page: Results per page
            
        Returns:
            Search results
        """
        params = {
            'page': page,
            'perPage': per_page
        }
        
        if name:
            params['name'] = name
        if postcode:
            params['postalCode'] = postcode
        if overall_rating:
            params['overallRating'] = overall_rating
        if care_home is not None:
            params['careHome'] = 'Y' if care_home else 'N'
        
        return self.get('/locations', params=params)
    
    def search_locations_df(self, **kwargs) -> pd.DataFrame:
        """Search locations as DataFrame"""
        result = self.search_locations(**kwargs)
        locations = result.get('locations', [])
        return pd.DataFrame(locations)
    
    def get_location(self, location_id: str) -> Dict:
        """Get details for a single location"""
        return self.get(f'/locations/{location_id}')
    
    # ========================================
    # RATINGS & INSPECTIONS
    # ========================================
    
    def get_location_ratings(self, location_id: str) -> Dict:
        """Get all ratings for a location"""
        location = self.get_location(location_id)
        return {
            'current_ratings': location.get('currentRatings', {}),
            'historic_ratings': location.get('historicRatings', [])
        }
    
    def get_providers_by_rating(
        self,
        rating: str,
        per_page: int = 100
    ) -> pd.DataFrame:
        """
        Get all providers with a specific rating.
        
        Args:
            rating: Outstanding, Good, Requires improvement, Inadequate
            per_page: Results per page
            
        Returns:
            DataFrame of providers
        """
        return self.search_providers_df(
            overall_rating=rating,
            per_page=per_page
        )
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_provider_types(self) -> List[str]:
        """Get list of provider types"""
        return [
            "NHS Healthcare Organisation",
            "Independent Healthcare Org",
            "Social Care Org",
            "Dentist",
            "Independent Ambulance",
            "Primary Medical Services",
            "Primary Dental Care"
        ]
    
    def get_rating_types(self) -> List[str]:
        """Get list of rating values"""
        return [
            "Outstanding",
            "Good",
            "Requires improvement",
            "Inadequate",
            "Not yet rated"
        ]
    
    # ========================================
    # CARE HOMES
    # ========================================
    
    def get_care_homes(
        self,
        postcode: Optional[str] = None,
        local_authority: Optional[str] = None,
        rating: Optional[str] = None,
        per_page: int = 100
    ) -> pd.DataFrame:
        """
        Get care homes.
        
        Args:
            postcode: Postcode filter
            local_authority: LA filter
            rating: Rating filter
            per_page: Results per page
            
        Returns:
            DataFrame of care homes
        """
        return self.search_locations_df(
            postcode=postcode,
            overall_rating=rating,
            care_home=True,
            per_page=per_page
        )
    
    # ========================================
    # GP PRACTICES
    # ========================================
    
    def get_gp_practices(
        self,
        postcode: Optional[str] = None,
        rating: Optional[str] = None,
        per_page: int = 100
    ) -> pd.DataFrame:
        """Get GP practices from CQC"""
        return self.search_providers_df(
            postcode=postcode,
            overall_rating=rating,
            provider_type="Primary Medical Services",
            per_page=per_page
        )
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk CQC data"""
        return {
            'providers': 'https://api.cqc.org.uk/public/v1/providers',
            'locations': 'https://api.cqc.org.uk/public/v1/locations',
            'reports': 'https://www.cqc.org.uk/about-us/transparency/using-cqc-data',
        }

