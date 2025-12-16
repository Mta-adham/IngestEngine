"""
DEFRA / Natural England API Client
===================================

Access environmental and agricultural data.

APIs:
- MAGIC (Multi-Agency Geographic Information for the Countryside)
- Air Quality
- Agricultural data

Usage:
    from src.clients import DEFRAClient
    
    client = DEFRAClient()
    air_quality = client.get_air_quality_forecast()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class DEFRAClient(BaseAPIClient):
    """
    Client for DEFRA environmental data APIs.
    
    Access:
    - Air quality forecasts and monitoring
    - Protected sites (SSSI, SAC, SPA, etc.)
    - Agricultural land data
    """
    
    # Air Quality API
    AIR_QUALITY_URL = "https://uk-air.defra.gov.uk/api"
    
    # MAGIC API (protected sites)
    MAGIC_URL = "https://magic.defra.gov.uk/api"
    
    def __init__(self, **kwargs):
        """Initialize DEFRA client."""
        super().__init__(
            base_url=self.AIR_QUALITY_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_air_quality_index()
            return True
        except Exception:
            return False
    
    # ========================================
    # AIR QUALITY
    # ========================================
    
    def get_air_quality_index(self) -> Dict:
        """Get current UK air quality index"""
        import requests
        response = requests.get(
            f"{self.AIR_QUALITY_URL}/daily-air-quality-index",
            timeout=30
        )
        return response.json() if response.status_code == 200 else {}
    
    def get_air_quality_forecast(
        self,
        location: Optional[str] = None
    ) -> Dict:
        """
        Get air quality forecast.
        
        Args:
            location: Location name (optional)
            
        Returns:
            Air quality forecast
        """
        import requests
        
        params = {}
        if location:
            params['location'] = location
        
        response = requests.get(
            f"{self.AIR_QUALITY_URL}/forecast",
            params=params,
            timeout=30
        )
        
        return response.json() if response.status_code == 200 else {}
    
    def get_monitoring_sites(self) -> List[Dict]:
        """Get air quality monitoring sites"""
        import requests
        response = requests.get(
            f"{self.AIR_QUALITY_URL}/site-info",
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json().get('sites', [])
        return []
    
    def get_monitoring_sites_df(self) -> pd.DataFrame:
        """Get monitoring sites as DataFrame"""
        sites = self.get_monitoring_sites()
        return pd.DataFrame(sites)
    
    # ========================================
    # AIR QUALITY INDEX BANDS
    # ========================================
    
    def get_aqi_bands(self) -> List[Dict]:
        """Get air quality index band definitions"""
        return [
            {'index': 1, 'band': 'Low', 'description': 'Enjoy outdoor activities'},
            {'index': 2, 'band': 'Low', 'description': 'Enjoy outdoor activities'},
            {'index': 3, 'band': 'Low', 'description': 'Enjoy outdoor activities'},
            {'index': 4, 'band': 'Moderate', 'description': 'Sensitive individuals may experience symptoms'},
            {'index': 5, 'band': 'Moderate', 'description': 'Sensitive individuals may experience symptoms'},
            {'index': 6, 'band': 'Moderate', 'description': 'Sensitive individuals may experience symptoms'},
            {'index': 7, 'band': 'High', 'description': 'Anyone may experience health effects'},
            {'index': 8, 'band': 'High', 'description': 'Anyone may experience health effects'},
            {'index': 9, 'band': 'High', 'description': 'Anyone may experience health effects'},
            {'index': 10, 'band': 'Very High', 'description': 'Everyone should reduce physical exertion'},
        ]
    
    # ========================================
    # PROTECTED SITES (from data.gov.uk)
    # ========================================
    
    def get_sssi_sites(self, limit: int = 100) -> pd.DataFrame:
        """
        Get Sites of Special Scientific Interest.
        
        Uses Natural England open data.
        """
        url = "https://services.arcgis.com/JJzESW51TqeY9uj5/arcgis/rest/services/SSSI_England/FeatureServer/0/query"
        
        import requests
        response = requests.get(
            url,
            params={
                'where': '1=1',
                'outFields': '*',
                'resultRecordCount': limit,
                'f': 'json'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            return pd.DataFrame([f.get('attributes', {}) for f in features])
        
        return pd.DataFrame()
    
    def get_aonb_areas(self, limit: int = 100) -> pd.DataFrame:
        """Get Areas of Outstanding Natural Beauty"""
        url = "https://services.arcgis.com/JJzESW51TqeY9uj5/arcgis/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0/query"
        
        import requests
        response = requests.get(
            url,
            params={
                'where': '1=1',
                'outFields': '*',
                'resultRecordCount': limit,
                'f': 'json'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            return pd.DataFrame([f.get('attributes', {}) for f in features])
        
        return pd.DataFrame()
    
    def get_national_parks(self) -> pd.DataFrame:
        """Get National Parks in England"""
        url = "https://services.arcgis.com/JJzESW51TqeY9uj5/arcgis/rest/services/National_Parks_England/FeatureServer/0/query"
        
        import requests
        response = requests.get(
            url,
            params={
                'where': '1=1',
                'outFields': '*',
                'f': 'json'
            },
            timeout=60
        )
        
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            return pd.DataFrame([f.get('attributes', {}) for f in features])
        
        return pd.DataFrame()
    
    # ========================================
    # AGRICULTURAL DATA
    # ========================================
    
    def get_agricultural_land_classification(self) -> Dict[str, str]:
        """Get agricultural land classification grades"""
        return {
            'Grade 1': 'Excellent quality agricultural land',
            'Grade 2': 'Very good quality agricultural land',
            'Grade 3a': 'Good quality agricultural land',
            'Grade 3b': 'Moderate quality agricultural land',
            'Grade 4': 'Poor quality agricultural land',
            'Grade 5': 'Very poor quality agricultural land',
            'Non-agricultural': 'Urban, woodland, water, etc.',
        }
    
    # ========================================
    # BULK DATA URLS
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk DEFRA data"""
        return {
            'air_quality_archive': 'https://uk-air.defra.gov.uk/data/',
            'sssi': 'https://naturalengland-defra.opendata.arcgis.com/datasets/sssi-england',
            'aonb': 'https://naturalengland-defra.opendata.arcgis.com/datasets/areas-of-outstanding-natural-beauty-england',
            'national_parks': 'https://naturalengland-defra.opendata.arcgis.com/datasets/national-parks-england',
            'agri_census': 'https://www.gov.uk/government/collections/agriculture-in-the-united-kingdom',
        }

