"""
Food Standards Agency API Client
=================================

Access food hygiene ratings and establishment data.

API Documentation: https://api.ratings.food.gov.uk/

Usage:
    from src.clients import FoodStandardsClient
    
    client = FoodStandardsClient()
    ratings = client.get_establishments(postcode="SW1A")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class FoodStandardsClient(BaseAPIClient):
    """
    Client for Food Standards Agency (FSA) API.
    
    Free access to food hygiene ratings for UK establishments.
    """
    
    BASE_URL = "https://api.ratings.food.gov.uk"
    
    def __init__(self, **kwargs):
        """Initialize FSA client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=100,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup required headers"""
        self.session.headers['x-api-version'] = '2'
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_authorities(page_size=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # ESTABLISHMENTS
    # ========================================
    
    def get_establishments(
        self,
        name: Optional[str] = None,
        address: Optional[str] = None,
        postcode: Optional[str] = None,
        longitude: Optional[float] = None,
        latitude: Optional[float] = None,
        max_distance_limit: Optional[int] = None,
        business_type_id: Optional[int] = None,
        local_authority_id: Optional[int] = None,
        rating_key: Optional[str] = None,
        rating_operator_key: Optional[str] = None,
        sort_option_key: Optional[str] = None,
        page_number: int = 1,
        page_size: int = 100
    ) -> Dict:
        """
        Search for food establishments.
        
        Args:
            name: Business name search
            address: Address search
            postcode: Postcode search
            longitude: Longitude for radius search
            latitude: Latitude for radius search
            max_distance_limit: Max distance in miles
            business_type_id: Filter by business type
            local_authority_id: Filter by local authority
            rating_key: Filter by rating (0-5, Exempt, AwaitingInspection)
            rating_operator_key: LessThanOrEqual, Equal, GreaterThanOrEqual
            sort_option_key: Relevance, rating, distance, alpha
            page_number: Page number
            page_size: Results per page
            
        Returns:
            Search results with establishments
        """
        params = {
            'pageNumber': page_number,
            'pageSize': min(page_size, 5000)
        }
        
        if name:
            params['name'] = name
        if address:
            params['address'] = address
        if postcode:
            params['postcode'] = postcode
        if longitude and latitude:
            params['longitude'] = longitude
            params['latitude'] = latitude
        if max_distance_limit:
            params['maxDistanceLimit'] = max_distance_limit
        if business_type_id:
            params['businessTypeId'] = business_type_id
        if local_authority_id:
            params['localAuthorityId'] = local_authority_id
        if rating_key:
            params['ratingKey'] = rating_key
        if rating_operator_key:
            params['ratingOperatorKey'] = rating_operator_key
        if sort_option_key:
            params['sortOptionKey'] = sort_option_key
        
        return self.get('/Establishments', params=params)
    
    def get_establishments_df(self, **kwargs) -> pd.DataFrame:
        """Get establishments as DataFrame"""
        result = self.get_establishments(**kwargs)
        establishments = result.get('establishments', [])
        
        if not establishments:
            return pd.DataFrame()
        
        df = pd.DataFrame(establishments)
        return df
    
    def get_establishment(self, fhrs_id: int) -> Dict:
        """Get single establishment by FHRS ID"""
        return self.get(f'/Establishments/{fhrs_id}')
    
    def get_establishments_near(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
        **kwargs
    ) -> pd.DataFrame:
        """
        Get establishments near a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius_miles: Search radius in miles
            
        Returns:
            DataFrame of nearby establishments
        """
        return self.get_establishments_df(
            latitude=lat,
            longitude=lon,
            max_distance_limit=int(radius_miles),
            sort_option_key='distance',
            **kwargs
        )
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_authorities(self, page_size: int = 100) -> List[Dict]:
        """Get all local authorities"""
        result = self.get('/Authorities/basic', params={'pageSize': page_size})
        return result.get('authorities', [])
    
    def get_authorities_df(self) -> pd.DataFrame:
        """Get authorities as DataFrame"""
        authorities = self.get_authorities(page_size=500)
        return pd.DataFrame(authorities)
    
    def get_business_types(self) -> List[Dict]:
        """Get all business types"""
        result = self.get('/BusinessTypes')
        return result.get('businessTypes', [])
    
    def get_business_types_df(self) -> pd.DataFrame:
        """Get business types as DataFrame"""
        types = self.get_business_types()
        return pd.DataFrame(types)
    
    def get_regions(self) -> List[Dict]:
        """Get all regions"""
        result = self.get('/Regions')
        return result.get('regions', [])
    
    def get_ratings(self) -> List[Dict]:
        """Get all rating values"""
        result = self.get('/Ratings')
        return result.get('ratings', [])
    
    def get_scheme_types(self) -> List[Dict]:
        """Get scheme types (FHRS vs FHIS)"""
        result = self.get('/SchemeTypes')
        return result.get('schemeTypes', [])
    
    def get_countries(self) -> List[Dict]:
        """Get countries (England, Wales, Northern Ireland)"""
        result = self.get('/Countries')
        return result.get('countries', [])
    
    # ========================================
    # STATISTICS
    # ========================================
    
    def get_authority_statistics(self, authority_id: int) -> Dict:
        """Get statistics for a local authority"""
        result = self.get(f'/Authorities/{authority_id}')
        return result
    
    def get_rating_distribution(
        self,
        local_authority_id: Optional[int] = None,
        business_type_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get rating distribution.
        
        Args:
            local_authority_id: Filter by authority
            business_type_id: Filter by business type
            
        Returns:
            DataFrame with rating counts
        """
        ratings_data = []
        
        for rating in ['5', '4', '3', '2', '1', '0', 'Exempt']:
            result = self.get_establishments(
                rating_key=rating,
                rating_operator_key='Equal',
                local_authority_id=local_authority_id,
                business_type_id=business_type_id,
                page_size=1
            )
            
            ratings_data.append({
                'rating': rating,
                'count': result.get('meta', {}).get('totalCount', 0)
            })
        
        return pd.DataFrame(ratings_data)
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_all_establishments_for_authority(
        self,
        authority_id: int,
        batch_size: int = 5000
    ) -> pd.DataFrame:
        """
        Get all establishments for a local authority.
        
        Args:
            authority_id: Local authority ID
            batch_size: Results per request
            
        Returns:
            DataFrame with all establishments
        """
        all_establishments = []
        page = 1
        
        while True:
            result = self.get_establishments(
                local_authority_id=authority_id,
                page_number=page,
                page_size=batch_size
            )
            
            establishments = result.get('establishments', [])
            if not establishments:
                break
            
            all_establishments.extend(establishments)
            
            total = result.get('meta', {}).get('totalCount', 0)
            if len(all_establishments) >= total:
                break
            
            page += 1
        
        return pd.DataFrame(all_establishments)

