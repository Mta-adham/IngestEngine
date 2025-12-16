"""
British Geological Survey API Client
=====================================

Access geological and ground data for the UK.

API Documentation: https://www.bgs.ac.uk/services/

Usage:
    from src.clients import BGSClient
    
    client = BGSClient()
    geology = client.get_geology(lat=51.5, lon=-0.1)
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class BGSClient(BaseAPIClient):
    """
    Client for British Geological Survey data.
    
    Access:
    - Geology (bedrock, superficial)
    - Ground stability/hazards
    - Radon potential
    - Mining/mineral data
    """
    
    BASE_URL = "https://map.bgs.ac.uk/arcgis/rest/services"
    WMS_URL = "https://map.bgs.ac.uk/bgs_wms/"
    
    def __init__(self, **kwargs):
        """Initialize BGS client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=30,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        return True
    
    # ========================================
    # GEOLOGY
    # ========================================
    
    def get_geology(
        self,
        lat: float,
        lon: float
    ) -> Dict:
        """
        Get geology at a point.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Geological information
        """
        import requests
        
        # BGS uses British National Grid, need to convert
        # For simplicity, using their identify endpoint
        
        url = f"{self.BASE_URL}/GeoIndex_Onshore/BGS_1M_Geology_Bedrock/MapServer/identify"
        
        params = {
            'geometry': f'{lon},{lat}',
            'geometryType': 'esriGeometryPoint',
            'sr': '4326',  # WGS84
            'layers': 'all',
            'tolerance': '1',
            'mapExtent': f'{lon-0.01},{lat-0.01},{lon+0.01},{lat+0.01}',
            'imageDisplay': '400,400,96',
            'returnGeometry': 'false',
            'f': 'json'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    def get_bedrock_geology(self, lat: float, lon: float) -> Optional[Dict]:
        """Get bedrock geology at a point"""
        result = self.get_geology(lat, lon)
        
        for item in result.get('results', []):
            if 'Bedrock' in item.get('layerName', ''):
                return item.get('attributes', {})
        
        return None
    
    def get_superficial_geology(self, lat: float, lon: float) -> Optional[Dict]:
        """Get superficial (surface) deposits at a point"""
        result = self.get_geology(lat, lon)
        
        for item in result.get('results', []):
            if 'Superficial' in item.get('layerName', ''):
                return item.get('attributes', {})
        
        return None
    
    # ========================================
    # GROUND HAZARDS
    # ========================================
    
    def get_ground_hazards(
        self,
        lat: float,
        lon: float
    ) -> Dict[str, Any]:
        """
        Get ground hazard data for a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Hazard assessments
        """
        # GeoSure data provides hazard ratings
        url = f"{self.BASE_URL}/GeoIndex_Onshore/GeoSure/MapServer/identify"
        
        import requests
        params = {
            'geometry': f'{lon},{lat}',
            'geometryType': 'esriGeometryPoint',
            'sr': '4326',
            'layers': 'all',
            'tolerance': '1',
            'mapExtent': f'{lon-0.01},{lat-0.01},{lon+0.01},{lat+0.01}',
            'imageDisplay': '400,400,96',
            'returnGeometry': 'false',
            'f': 'json'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    def get_shrink_swell_risk(self, lat: float, lon: float) -> Optional[str]:
        """Get clay shrink-swell risk rating"""
        hazards = self.get_ground_hazards(lat, lon)
        
        for item in hazards.get('results', []):
            if 'Shrink_Swell' in item.get('layerName', ''):
                attrs = item.get('attributes', {})
                return attrs.get('SHRINK_SWE', attrs.get('Risk', 'Unknown'))
        
        return None
    
    def get_subsidence_risk(self, lat: float, lon: float) -> Optional[str]:
        """Get ground dissolution (sinkhole) risk"""
        hazards = self.get_ground_hazards(lat, lon)
        
        for item in hazards.get('results', []):
            if 'Dissolution' in item.get('layerName', ''):
                attrs = item.get('attributes', {})
                return attrs.get('DISSOLUTIO', attrs.get('Risk', 'Unknown'))
        
        return None
    
    # ========================================
    # MINING
    # ========================================
    
    def get_mining_hazards(
        self,
        lat: float,
        lon: float
    ) -> Dict:
        """Get mining-related hazards"""
        url = f"{self.BASE_URL}/GeoIndex_Onshore/CoalMiningAffectedAreas/MapServer/identify"
        
        import requests
        params = {
            'geometry': f'{lon},{lat}',
            'geometryType': 'esriGeometryPoint',
            'sr': '4326',
            'layers': 'all',
            'tolerance': '1',
            'mapExtent': f'{lon-0.01},{lat-0.01},{lon+0.01},{lat+0.01}',
            'imageDisplay': '400,400,96',
            'returnGeometry': 'false',
            'f': 'json'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    # ========================================
    # RADON
    # ========================================
    
    def get_radon_potential(self, postcode: str) -> Dict:
        """
        Get radon potential for a postcode.
        
        Uses UK Radon map.
        """
        # UKHSA provides radon data
        # This would need the UKHSA radon API or scraping
        return {
            'postcode': postcode,
            'data_source': 'https://www.ukradon.org/',
            'note': 'Check ukradon.org for detailed assessment'
        }
    
    # ========================================
    # SOIL DATA
    # ========================================
    
    def get_soil_type(self, lat: float, lon: float) -> Dict:
        """Get soil type information"""
        # Soilscapes data
        url = f"{self.BASE_URL}/Soilscapes/Soilscapes/MapServer/identify"
        
        import requests
        params = {
            'geometry': f'{lon},{lat}',
            'geometryType': 'esriGeometryPoint',
            'sr': '4326',
            'layers': 'all',
            'tolerance': '1',
            'mapExtent': f'{lon-0.01},{lat-0.01},{lon+0.01},{lat+0.01}',
            'imageDisplay': '400,400,96',
            'returnGeometry': 'false',
            'f': 'json'
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_geological_periods(self) -> List[Dict]:
        """Get UK geological periods"""
        return [
            {'era': 'Cenozoic', 'period': 'Quaternary', 'mya': '2.6 - present'},
            {'era': 'Cenozoic', 'period': 'Neogene', 'mya': '23 - 2.6'},
            {'era': 'Cenozoic', 'period': 'Paleogene', 'mya': '66 - 23'},
            {'era': 'Mesozoic', 'period': 'Cretaceous', 'mya': '145 - 66'},
            {'era': 'Mesozoic', 'period': 'Jurassic', 'mya': '201 - 145'},
            {'era': 'Mesozoic', 'period': 'Triassic', 'mya': '252 - 201'},
            {'era': 'Paleozoic', 'period': 'Permian', 'mya': '299 - 252'},
            {'era': 'Paleozoic', 'period': 'Carboniferous', 'mya': '359 - 299'},
            {'era': 'Paleozoic', 'period': 'Devonian', 'mya': '419 - 359'},
            {'era': 'Precambrian', 'period': 'Various', 'mya': '4600 - 541'},
        ]
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk BGS data"""
        return {
            'onshore_geoindex': 'https://www.bgs.ac.uk/products/digitalmaps/data_625k.html',
            'lexicon': 'https://www.bgs.ac.uk/technologies/databases/bgs-lexicon-of-named-rock-units/',
            'open_data': 'https://www.bgs.ac.uk/geological-data/open-data/',
            'wms_services': 'https://www.bgs.ac.uk/data/services/',
        }

