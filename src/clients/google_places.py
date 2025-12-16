"""
Google Places API Client
=========================

Access POI data including business hours, ratings, and reviews.

API Documentation: https://developers.google.com/maps/documentation/places/web-service

Usage:
    from src.clients import GooglePlacesClient
    
    client = GooglePlacesClient(api_key="your-key")
    places = client.search_nearby(51.5, -0.1, radius=1000, type="restaurant")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class GooglePlacesClient(BaseAPIClient):
    """
    Client for Google Places API.
    
    Access:
    - Place Search (nearby, text, find)
    - Place Details (hours, reviews, photos)
    - Place Autocomplete
    
    Note: Requires API key with billing enabled
    """
    
    BASE_URL = "https://maps.googleapis.com/maps/api/place"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Google Places client.
        
        Args:
            api_key: Google Cloud API key
        """
        self.api_key = api_key or os.environ.get('GOOGLE_PLACES_API_KEY', '')
        
        if not self.api_key:
            logger.warning("No Google Places API key. Set GOOGLE_PLACES_API_KEY env var.")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=self.api_key,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """API key goes in query params"""
        pass
    
    def _add_key(self, params: Dict) -> Dict:
        """Add API key to params"""
        params = params or {}
        if self.api_key:
            params['key'] = self.api_key
        return params
    
    def health_check(self) -> bool:
        """Check if API is available"""
        return bool(self.api_key)
    
    # ========================================
    # NEARBY SEARCH
    # ========================================
    
    def search_nearby(
        self,
        lat: float,
        lon: float,
        radius: int = 1000,
        type: Optional[str] = None,
        keyword: Optional[str] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        open_now: bool = False
    ) -> Dict:
        """
        Search for places nearby.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Search radius in meters
            type: Place type (restaurant, cafe, etc.)
            keyword: Keyword search
            min_price: 0-4 price level
            max_price: 0-4 price level
            open_now: Only return open places
            
        Returns:
            Search results
        """
        params = self._add_key({
            'location': f'{lat},{lon}',
            'radius': radius,
        })
        
        if type:
            params['type'] = type
        if keyword:
            params['keyword'] = keyword
        if min_price is not None:
            params['minprice'] = min_price
        if max_price is not None:
            params['maxprice'] = max_price
        if open_now:
            params['opennow'] = 'true'
        
        return self.get('/nearbysearch/json', params=params)
    
    def search_nearby_df(self, **kwargs) -> pd.DataFrame:
        """Search nearby as DataFrame"""
        result = self.search_nearby(**kwargs)
        places = result.get('results', [])
        return pd.DataFrame(places)
    
    # ========================================
    # TEXT SEARCH
    # ========================================
    
    def search_text(
        self,
        query: str,
        location: Optional[str] = None,
        radius: Optional[int] = None,
        type: Optional[str] = None
    ) -> Dict:
        """
        Search places by text query.
        
        Args:
            query: Search query
            location: Location bias (lat,lng)
            radius: Search radius
            type: Place type filter
            
        Returns:
            Search results
        """
        params = self._add_key({'query': query})
        
        if location:
            params['location'] = location
        if radius:
            params['radius'] = radius
        if type:
            params['type'] = type
        
        return self.get('/textsearch/json', params=params)
    
    def search_text_df(self, query: str, **kwargs) -> pd.DataFrame:
        """Search text as DataFrame"""
        result = self.search_text(query, **kwargs)
        places = result.get('results', [])
        return pd.DataFrame(places)
    
    # ========================================
    # PLACE DETAILS
    # ========================================
    
    def get_place_details(
        self,
        place_id: str,
        fields: Optional[List[str]] = None
    ) -> Dict:
        """
        Get detailed info about a place.
        
        Args:
            place_id: Google Place ID
            fields: Fields to return (to minimize cost)
            
        Returns:
            Place details
        """
        params = self._add_key({'place_id': place_id})
        
        if fields:
            params['fields'] = ','.join(fields)
        else:
            # Default useful fields
            params['fields'] = 'name,formatted_address,geometry,opening_hours,rating,reviews,price_level,website,formatted_phone_number,types,business_status'
        
        return self.get('/details/json', params=params)
    
    def get_opening_hours(self, place_id: str) -> Optional[Dict]:
        """Get opening hours for a place"""
        result = self.get_place_details(place_id, fields=['opening_hours'])
        return result.get('result', {}).get('opening_hours')
    
    def get_place_reviews(self, place_id: str) -> List[Dict]:
        """Get reviews for a place"""
        result = self.get_place_details(place_id, fields=['reviews'])
        return result.get('result', {}).get('reviews', [])
    
    # ========================================
    # AUTOCOMPLETE
    # ========================================
    
    def autocomplete(
        self,
        input: str,
        location: Optional[str] = None,
        radius: Optional[int] = None,
        types: Optional[str] = None,
        components: str = "country:gb"
    ) -> List[Dict]:
        """
        Autocomplete place search.
        
        Args:
            input: Search input
            location: Location bias
            radius: Radius for bias
            types: Place types
            components: Country restriction
            
        Returns:
            Autocomplete predictions
        """
        params = self._add_key({
            'input': input,
            'components': components,
        })
        
        if location:
            params['location'] = location
        if radius:
            params['radius'] = radius
        if types:
            params['types'] = types
        
        result = self.get('/autocomplete/json', params=params)
        return result.get('predictions', [])
    
    # ========================================
    # UK-SPECIFIC SEARCHES
    # ========================================
    
    def search_uk_businesses(
        self,
        query: str,
        city: str = "London",
        type: Optional[str] = None
    ) -> pd.DataFrame:
        """Search UK businesses"""
        full_query = f"{query} in {city}, UK"
        return self.search_text_df(full_query, type=type)
    
    def get_restaurants_near(
        self,
        lat: float,
        lon: float,
        radius: int = 1000
    ) -> pd.DataFrame:
        """Get restaurants near a location"""
        return self.search_nearby_df(
            lat=lat, lon=lon, radius=radius, type='restaurant'
        )
    
    # ========================================
    # PLACE TYPES
    # ========================================
    
    def get_place_types(self) -> List[str]:
        """Get supported place types"""
        return [
            # Food & Drink
            'restaurant', 'cafe', 'bar', 'bakery', 'meal_takeaway',
            'meal_delivery', 'night_club',
            # Shopping
            'shopping_mall', 'supermarket', 'convenience_store',
            'clothing_store', 'shoe_store', 'jewelry_store',
            # Services
            'bank', 'atm', 'post_office', 'insurance_agency',
            'real_estate_agency', 'lawyer', 'accounting',
            # Health
            'hospital', 'doctor', 'dentist', 'pharmacy', 'veterinary_care',
            # Transport
            'airport', 'train_station', 'bus_station', 'subway_station',
            'taxi_stand', 'parking', 'gas_station', 'car_repair',
            # Leisure
            'gym', 'spa', 'movie_theater', 'museum', 'art_gallery',
            'amusement_park', 'zoo', 'park', 'stadium',
            # Accommodation
            'hotel', 'lodging',
            # Education
            'school', 'university', 'library',
            # Religious
            'church', 'mosque', 'synagogue', 'hindu_temple',
        ]

