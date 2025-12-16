"""
Street View / Street-Level Imagery Client
==========================================

Access historical street-level imagery for building dating.

Usage:
    from src.clients import StreetViewClient
    
    client = StreetViewClient(api_key="your-google-key")
    metadata = client.get_metadata(lat=51.5, lon=-0.1)
"""

import pandas as pd
from typing import Optional, Dict, List, Any, Tuple
import logging
import os
from datetime import datetime

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class StreetViewClient(BaseAPIClient):
    """
    Client for Street View metadata and imagery dating.
    
    Can help establish:
    - When buildings first appeared
    - Building modifications over time
    - Business opening dates (signage changes)
    
    Supports:
    - Google Street View (requires API key)
    - Mapillary (open source alternative)
    """
    
    GOOGLE_URL = "https://maps.googleapis.com/maps/api/streetview"
    MAPILLARY_URL = "https://graph.mapillary.com"
    
    def __init__(
        self,
        google_api_key: Optional[str] = None,
        mapillary_token: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Street View client.
        
        Args:
            google_api_key: Google Maps API key
            mapillary_token: Mapillary access token
        """
        self.google_api_key = google_api_key or os.environ.get('GOOGLE_MAPS_API_KEY', '')
        self.mapillary_token = mapillary_token or os.environ.get('MAPILLARY_TOKEN', '')
        
        super().__init__(
            base_url=self.GOOGLE_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """API keys go in query params"""
        pass
    
    def health_check(self) -> bool:
        """Check if any API is available"""
        return bool(self.google_api_key or self.mapillary_token)
    
    # ========================================
    # GOOGLE STREET VIEW METADATA
    # ========================================
    
    def get_google_metadata(
        self,
        lat: float,
        lon: float,
        radius: int = 50
    ) -> Optional[Dict]:
        """
        Get Google Street View metadata (capture date).
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Search radius in meters
            
        Returns:
            Metadata including capture date
        """
        if not self.google_api_key:
            return None
        
        import requests
        
        params = {
            'location': f'{lat},{lon}',
            'radius': radius,
            'key': self.google_api_key,
        }
        
        response = requests.get(
            f"{self.GOOGLE_URL}/metadata",
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        return None
    
    def get_capture_date(self, lat: float, lon: float) -> Optional[str]:
        """Get street view capture date for a location"""
        metadata = self.get_google_metadata(lat, lon)
        
        if metadata and metadata.get('status') == 'OK':
            return metadata.get('date')  # Format: YYYY-MM
        return None
    
    def get_historical_coverage(
        self,
        lat: float,
        lon: float
    ) -> List[str]:
        """
        Get all available capture dates for a location.
        
        Note: Requires repeated API calls, use sparingly.
        """
        # Google doesn't directly expose historical dates via API
        # Would need to iterate through time periods
        current = self.get_capture_date(lat, lon)
        return [current] if current else []
    
    # ========================================
    # MAPILLARY (Open Source Alternative)
    # ========================================
    
    def search_mapillary(
        self,
        lat: float,
        lon: float,
        radius: int = 100,
        limit: int = 50
    ) -> List[Dict]:
        """
        Search Mapillary for street-level images.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Search radius in meters
            limit: Max results
            
        Returns:
            List of images with dates
        """
        if not self.mapillary_token:
            logger.warning("Mapillary token required. Set MAPILLARY_TOKEN.")
            return []
        
        import requests
        
        # Mapillary v4 API
        bbox = self._make_bbox(lat, lon, radius)
        
        params = {
            'access_token': self.mapillary_token,
            'fields': 'id,captured_at,geometry,compass_angle',
            'bbox': f'{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}',
            'limit': limit,
        }
        
        response = requests.get(
            f"{self.MAPILLARY_URL}/images",
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json().get('data', [])
        return []
    
    def get_mapillary_timeline(
        self,
        lat: float,
        lon: float,
        radius: int = 50
    ) -> pd.DataFrame:
        """
        Get timeline of images at a location.
        
        Useful for tracking when buildings/businesses appeared.
        """
        images = self.search_mapillary(lat, lon, radius)
        
        if not images:
            return pd.DataFrame()
        
        records = []
        for img in images:
            captured_at = img.get('captured_at')
            if captured_at:
                # Convert milliseconds to datetime
                dt = datetime.fromtimestamp(captured_at / 1000)
                records.append({
                    'image_id': img.get('id'),
                    'captured_at': dt.isoformat(),
                    'year': dt.year,
                    'month': dt.month,
                    'geometry': img.get('geometry'),
                })
        
        return pd.DataFrame(records)
    
    def _make_bbox(
        self,
        lat: float,
        lon: float,
        radius_m: int
    ) -> Tuple[float, float, float, float]:
        """Create bounding box from center and radius"""
        # Rough conversion (1 degree ≈ 111km at equator)
        delta = radius_m / 111000
        return (lon - delta, lat - delta, lon + delta, lat + delta)
    
    # ========================================
    # BUILDING DATING FROM IMAGERY
    # ========================================
    
    def estimate_building_age_from_imagery(
        self,
        lat: float,
        lon: float
    ) -> Dict[str, Any]:
        """
        Estimate when a building might have been built
        based on earliest street-level imagery.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Estimate with confidence
        """
        # Try Google first
        google_date = self.get_capture_date(lat, lon)
        
        # Try Mapillary
        mapillary_df = self.get_mapillary_timeline(lat, lon)
        
        earliest_mapillary = None
        if not mapillary_df.empty:
            earliest_mapillary = mapillary_df['captured_at'].min()
        
        return {
            'earliest_google_imagery': google_date,
            'earliest_mapillary_imagery': earliest_mapillary,
            'building_present_since': earliest_mapillary or google_date,
            'confidence': 'low',
            'notes': 'Building existed when imagery was captured, may be older'
        }
    
    # ========================================
    # BUSINESS SIGNAGE DETECTION
    # ========================================
    
    def get_image_url_google(
        self,
        lat: float,
        lon: float,
        size: str = "640x480",
        heading: int = 0
    ) -> str:
        """Get Google Street View image URL"""
        if not self.google_api_key:
            return ""
        
        return (
            f"{self.GOOGLE_URL}?"
            f"location={lat},{lon}&"
            f"size={size}&"
            f"heading={heading}&"
            f"key={self.google_api_key}"
        )
    
    def get_image_url_mapillary(self, image_id: str, size: int = 1024) -> str:
        """Get Mapillary image URL"""
        if not self.mapillary_token:
            return ""
        
        return f"https://graph.mapillary.com/{image_id}/thumb?size={size}"
    
    # ========================================
    # REFERENCE
    # ========================================
    
    def get_coverage_info(self) -> Dict[str, str]:
        """Get UK street-level imagery coverage info"""
        return {
            'google_street_view': 'Most UK roads covered since 2008-2009',
            'mapillary': 'Community-contributed, growing coverage',
            'apple_look_around': 'Major cities from 2019+',
            'bing_streetside': 'Limited UK coverage',
            'earliest_uk': 'First Google coverage: 2008',
        }

