"""
Foursquare Places API Client
=============================

Access POI data including venue details and user activity.

API Documentation: https://developer.foursquare.com/

Usage:
    from src.clients import FoursquareClient
    
    client = FoursquareClient(api_key="your-key")
    venues = client.search_venues(lat=51.5, lon=-0.1, query="coffee")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class FoursquareClient(BaseAPIClient):
    """
    Client for Foursquare Places API.
    
    Access:
    - Place Search
    - Place Details
    - Place Photos
    - Tips & Reviews
    """
    
    BASE_URL = "https://api.foursquare.com/v3"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Foursquare client.
        
        Args:
            api_key: Foursquare API key
        """
        self.api_key = api_key or os.environ.get('FOURSQUARE_API_KEY', '')
        
        if not self.api_key:
            logger.warning("No Foursquare API key. Set FOURSQUARE_API_KEY env var.")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=self.api_key,
            rate_limit_rpm=100,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup API key header"""
        if self.api_key:
            self.session.headers['Authorization'] = self.api_key
            self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        return bool(self.api_key)
    
    # ========================================
    # PLACE SEARCH
    # ========================================
    
    def search_places(
        self,
        query: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        radius: int = 1000,
        categories: Optional[List[str]] = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Search for places.
        
        Args:
            query: Search query
            lat: Latitude
            lon: Longitude
            radius: Search radius in meters
            categories: Category IDs to filter
            limit: Max results
            
        Returns:
            List of places
        """
        params = {
            'limit': limit,
        }
        
        if lat and lon:
            params['ll'] = f'{lat},{lon}'
            params['radius'] = radius
        if query:
            params['query'] = query
        if categories:
            params['categories'] = ','.join(categories)
        
        result = self.get('/places/search', params=params)
        return result.get('results', [])
    
    def search_places_df(self, **kwargs) -> pd.DataFrame:
        """Search places as DataFrame"""
        places = self.search_places(**kwargs)
        return pd.DataFrame(places)
    
    # ========================================
    # PLACE DETAILS
    # ========================================
    
    def get_place(self, fsq_id: str) -> Dict:
        """
        Get place details.
        
        Args:
            fsq_id: Foursquare place ID
            
        Returns:
            Place details
        """
        return self.get(f'/places/{fsq_id}')
    
    def get_place_with_fields(
        self,
        fsq_id: str,
        fields: List[str]
    ) -> Dict:
        """
        Get place with specific fields.
        
        Args:
            fsq_id: Foursquare place ID
            fields: Fields to return
            
        Returns:
            Place details
        """
        params = {'fields': ','.join(fields)}
        return self.get(f'/places/{fsq_id}', params=params)
    
    def get_place_hours(self, fsq_id: str) -> Optional[Dict]:
        """Get opening hours for a place"""
        result = self.get_place_with_fields(fsq_id, ['hours'])
        return result.get('hours')
    
    def get_place_tips(self, fsq_id: str, limit: int = 10) -> List[Dict]:
        """Get tips/reviews for a place"""
        params = {'limit': limit}
        result = self.get(f'/places/{fsq_id}/tips', params=params)
        return result.get('tips', result) if isinstance(result, dict) else result
    
    def get_place_photos(self, fsq_id: str, limit: int = 10) -> List[Dict]:
        """Get photos for a place"""
        params = {'limit': limit}
        result = self.get(f'/places/{fsq_id}/photos', params=params)
        return result if isinstance(result, list) else result.get('photos', [])
    
    # ========================================
    # NEARBY SEARCH
    # ========================================
    
    def get_nearby_places(
        self,
        lat: float,
        lon: float,
        radius: int = 500,
        limit: int = 50
    ) -> pd.DataFrame:
        """Get places near a location"""
        return self.search_places_df(
            lat=lat, lon=lon, radius=radius, limit=limit
        )
    
    # ========================================
    # CATEGORIES
    # ========================================
    
    def get_categories(self) -> List[Dict]:
        """Get all place categories"""
        result = self.get('/places/categories')
        return result if isinstance(result, list) else result.get('response', {}).get('categories', [])
    
    def get_common_categories(self) -> Dict[str, str]:
        """Get common UK category IDs"""
        return {
            # Food
            '13065': 'Restaurant',
            '13032': 'Café',
            '13003': 'Bar',
            '13145': 'Fast Food',
            
            # Shopping
            '17069': 'Shopping Mall',
            '17142': 'Supermarket',
            '17057': 'Clothing Store',
            
            # Services
            '11045': 'Bank',
            '12057': 'Gym',
            '15014': 'Hotel',
            
            # Transport
            '19042': 'Train Station',
            '19046': 'Bus Station',
            '19050': 'Airport',
        }
    
    # ========================================
    # AUTOCOMPLETE
    # ========================================
    
    def autocomplete(
        self,
        query: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        radius: int = 10000,
        limit: int = 10
    ) -> List[Dict]:
        """
        Autocomplete place search.
        
        Args:
            query: Search query
            lat: Latitude for bias
            lon: Longitude for bias
            radius: Radius for bias
            limit: Max results
            
        Returns:
            Autocomplete results
        """
        params = {
            'query': query,
            'limit': limit,
        }
        
        if lat and lon:
            params['ll'] = f'{lat},{lon}'
            params['radius'] = radius
        
        result = self.get('/autocomplete', params=params)
        return result.get('results', [])
    
    # ========================================
    # UK-SPECIFIC
    # ========================================
    
    def search_uk_venues(
        self,
        query: str,
        city: str = "London"
    ) -> pd.DataFrame:
        """Search UK venues by city"""
        # Get city coordinates
        city_coords = {
            'london': (51.5074, -0.1278),
            'manchester': (53.4808, -2.2426),
            'birmingham': (52.4862, -1.8904),
            'leeds': (53.8008, -1.5491),
            'glasgow': (55.8642, -4.2518),
            'edinburgh': (55.9533, -3.1883),
            'liverpool': (53.4084, -2.9916),
            'bristol': (51.4545, -2.5879),
        }
        
        coords = city_coords.get(city.lower(), (51.5074, -0.1278))
        
        return self.search_places_df(
            query=query,
            lat=coords[0],
            lon=coords[1],
            radius=10000
        )

