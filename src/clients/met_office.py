"""
Met Office DataPoint API Client
================================

Access UK weather data and forecasts.

API Documentation: https://www.metoffice.gov.uk/services/data/datapoint

Usage:
    from src.clients import MetOfficeClient
    
    client = MetOfficeClient(api_key="your-key")
    forecast = client.get_forecast(location_id="3772")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class MetOfficeClient(BaseAPIClient):
    """
    Client for Met Office DataPoint API.
    
    Access:
    - Weather forecasts (3-hourly, daily)
    - Observations
    - Site list
    - Weather maps
    
    Free tier: 5000 requests/day
    """
    
    BASE_URL = "http://datapoint.metoffice.gov.uk/public/data"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Met Office client.
        
        Args:
            api_key: Met Office DataPoint API key
        """
        self.api_key = api_key or os.environ.get('MET_OFFICE_API_KEY', '')
        
        if not self.api_key:
            logger.warning("No Met Office API key. Set MET_OFFICE_API_KEY env var.")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=self.api_key,
            rate_limit_rpm=100,
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
    # SITE LIST
    # ========================================
    
    def get_forecast_sites(self) -> List[Dict]:
        """Get list of forecast locations"""
        params = self._add_key({})
        result = self.get('/val/wxfcs/all/json/sitelist', params=params)
        return result.get('Locations', {}).get('Location', [])
    
    def get_forecast_sites_df(self) -> pd.DataFrame:
        """Get forecast sites as DataFrame"""
        sites = self.get_forecast_sites()
        return pd.DataFrame(sites)
    
    def get_observation_sites(self) -> List[Dict]:
        """Get list of observation locations"""
        params = self._add_key({})
        result = self.get('/val/wxobs/all/json/sitelist', params=params)
        return result.get('Locations', {}).get('Location', [])
    
    def find_nearest_site(self, lat: float, lon: float) -> Dict:
        """Find nearest forecast site to coordinates"""
        sites = self.get_forecast_sites()
        
        min_dist = float('inf')
        nearest = None
        
        for site in sites:
            site_lat = float(site.get('latitude', 0))
            site_lon = float(site.get('longitude', 0))
            dist = ((lat - site_lat)**2 + (lon - site_lon)**2)**0.5
            
            if dist < min_dist:
                min_dist = dist
                nearest = site
        
        return nearest or {}
    
    # ========================================
    # FORECASTS
    # ========================================
    
    def get_forecast(
        self,
        location_id: str,
        resolution: str = "3hourly"
    ) -> Dict:
        """
        Get weather forecast for a location.
        
        Args:
            location_id: Location ID from site list
            resolution: "3hourly" or "daily"
            
        Returns:
            Forecast data
        """
        params = self._add_key({'res': resolution})
        return self.get(f'/val/wxfcs/all/json/{location_id}', params=params)
    
    def get_forecast_df(
        self,
        location_id: str,
        resolution: str = "3hourly"
    ) -> pd.DataFrame:
        """Get forecast as DataFrame"""
        result = self.get_forecast(location_id, resolution)
        
        periods = []
        site_rep = result.get('SiteRep', {})
        dv = site_rep.get('DV', {})
        
        for location in dv.get('Location', []):
            for period in location.get('Period', []):
                date = period.get('value')
                for rep in period.get('Rep', []):
                    rep['date'] = date
                    rep['location_id'] = location_id
                    periods.append(rep)
        
        return pd.DataFrame(periods)
    
    def get_forecast_for_location(
        self,
        lat: float,
        lon: float,
        resolution: str = "3hourly"
    ) -> pd.DataFrame:
        """Get forecast for nearest site to coordinates"""
        site = self.find_nearest_site(lat, lon)
        if not site:
            return pd.DataFrame()
        return self.get_forecast_df(site.get('id'), resolution)
    
    # ========================================
    # OBSERVATIONS
    # ========================================
    
    def get_observations(self, location_id: str) -> Dict:
        """Get latest observations for a location"""
        params = self._add_key({'res': 'hourly'})
        return self.get(f'/val/wxobs/all/json/{location_id}', params=params)
    
    def get_all_observations(self) -> Dict:
        """Get latest observations from all sites"""
        params = self._add_key({'res': 'hourly'})
        return self.get('/val/wxobs/all/json/all', params=params)
    
    # ========================================
    # WEATHER PARAMETERS
    # ========================================
    
    def get_weather_types(self) -> Dict[str, str]:
        """Get weather type codes"""
        return {
            '0': 'Clear night',
            '1': 'Sunny day',
            '2': 'Partly cloudy (night)',
            '3': 'Partly cloudy (day)',
            '4': 'Not used',
            '5': 'Mist',
            '6': 'Fog',
            '7': 'Cloudy',
            '8': 'Overcast',
            '9': 'Light rain shower (night)',
            '10': 'Light rain shower (day)',
            '11': 'Drizzle',
            '12': 'Light rain',
            '13': 'Heavy rain shower (night)',
            '14': 'Heavy rain shower (day)',
            '15': 'Heavy rain',
            '16': 'Sleet shower (night)',
            '17': 'Sleet shower (day)',
            '18': 'Sleet',
            '19': 'Hail shower (night)',
            '20': 'Hail shower (day)',
            '21': 'Hail',
            '22': 'Light snow shower (night)',
            '23': 'Light snow shower (day)',
            '24': 'Light snow',
            '25': 'Heavy snow shower (night)',
            '26': 'Heavy snow shower (day)',
            '27': 'Heavy snow',
            '28': 'Thunder shower (night)',
            '29': 'Thunder shower (day)',
            '30': 'Thunder',
        }
    
    def get_parameter_codes(self) -> Dict[str, str]:
        """Get forecast parameter codes"""
        return {
            'F': 'Feels Like Temperature (°C)',
            'G': 'Wind Gust (mph)',
            'H': 'Relative Humidity (%)',
            'T': 'Temperature (°C)',
            'V': 'Visibility',
            'D': 'Wind Direction (compass)',
            'S': 'Wind Speed (mph)',
            'U': 'Max UV Index',
            'W': 'Weather Type',
            'Pp': 'Precipitation Probability (%)',
        }

