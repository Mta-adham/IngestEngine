"""
Historic England API Client
============================

Access heritage data including:
- Listed Buildings
- Scheduled Monuments
- Conservation Areas
- Heritage at Risk

API Documentation: https://historicengland.org.uk/listing/the-list/data-downloads/

Usage:
    from src.clients import HistoricEnglandClient
    
    client = HistoricEnglandClient()
    listings = client.search_listed_buildings("Big Ben")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class HistoricEnglandClient(BaseAPIClient):
    """
    Client for Historic England data.
    
    Access:
    - Listed Buildings (Grade I, II*, II)
    - Scheduled Monuments
    - Registered Parks and Gardens
    - Registered Battlefields
    - Protected Wrecks
    - World Heritage Sites
    - Heritage at Risk Register
    """
    
    BASE_URL = "https://historicengland.org.uk/listing/the-list"
    API_URL = "https://services-eu1.arcgis.com/ZOdPfBS3aqqDYPUQ/arcgis/rest/services"
    
    def __init__(self, **kwargs):
        """Initialize Historic England client."""
        super().__init__(
            base_url=self.API_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_listed_buildings(limit=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # LISTED BUILDINGS
    # ========================================
    
    def get_listed_buildings(
        self,
        name: Optional[str] = None,
        grade: Optional[str] = None,
        local_authority: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get listed buildings.
        
        Args:
            name: Building name search
            grade: I, II*, or II
            local_authority: LA name filter
            limit: Maximum results
            
        Returns:
            DataFrame of listed buildings
        """
        url = f"{self.API_URL}/Listed_Buildings/FeatureServer/0/query"
        
        where_clauses = ['1=1']
        
        if name:
            where_clauses.append(f"Name LIKE '%{name}%'")
        if grade:
            where_clauses.append(f"Grade = '{grade}'")
        if local_authority:
            where_clauses.append(f"LA LIKE '%{local_authority}%'")
        
        import requests
        response = requests.get(
            url,
            params={
                'where': ' AND '.join(where_clauses),
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
    
    def search_listed_buildings(
        self,
        query: str,
        limit: int = 50
    ) -> pd.DataFrame:
        """Search listed buildings by name"""
        return self.get_listed_buildings(name=query, limit=limit)
    
    def get_grade_1_buildings(self, limit: int = 100) -> pd.DataFrame:
        """Get Grade I listed buildings"""
        return self.get_listed_buildings(grade='I', limit=limit)
    
    def get_grade_2_star_buildings(self, limit: int = 100) -> pd.DataFrame:
        """Get Grade II* listed buildings"""
        return self.get_listed_buildings(grade='II*', limit=limit)
    
    # ========================================
    # SCHEDULED MONUMENTS
    # ========================================
    
    def get_scheduled_monuments(
        self,
        name: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get scheduled monuments"""
        url = f"{self.API_URL}/Scheduled_Monuments/FeatureServer/0/query"
        
        where = f"Name LIKE '%{name}%'" if name else '1=1'
        
        import requests
        response = requests.get(
            url,
            params={
                'where': where,
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
    
    # ========================================
    # PARKS AND GARDENS
    # ========================================
    
    def get_registered_parks_gardens(
        self,
        name: Optional[str] = None,
        grade: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get registered parks and gardens"""
        url = f"{self.API_URL}/Registered_Parks_and_Gardens/FeatureServer/0/query"
        
        where_clauses = ['1=1']
        if name:
            where_clauses.append(f"Name LIKE '%{name}%'")
        if grade:
            where_clauses.append(f"Grade = '{grade}'")
        
        import requests
        response = requests.get(
            url,
            params={
                'where': ' AND '.join(where_clauses),
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
    
    # ========================================
    # HERITAGE AT RISK
    # ========================================
    
    def get_heritage_at_risk(self, limit: int = 100) -> pd.DataFrame:
        """Get Heritage at Risk register entries"""
        url = f"{self.API_URL}/Heritage_at_Risk_Register/FeatureServer/0/query"
        
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
    
    # ========================================
    # WORLD HERITAGE SITES
    # ========================================
    
    def get_world_heritage_sites(self) -> pd.DataFrame:
        """Get World Heritage Sites in England"""
        url = f"{self.API_URL}/World_Heritage_Sites/FeatureServer/0/query"
        
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
    # REFERENCE DATA
    # ========================================
    
    def get_listing_grades(self) -> Dict[str, str]:
        """Get listing grade definitions"""
        return {
            'I': 'Buildings of exceptional interest (2.5% of all listed)',
            'II*': 'Particularly important buildings (5.8% of all listed)',
            'II': 'Buildings of special interest (91.7% of all listed)',
        }
    
    def get_heritage_categories(self) -> List[str]:
        """Get heritage asset categories"""
        return [
            'Listed Building',
            'Scheduled Monument',
            'Registered Park and Garden',
            'Registered Battlefield',
            'Protected Wreck',
            'World Heritage Site',
            'Conservation Area',
        ]
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk heritage data"""
        return {
            'listed_buildings': 'https://historicengland.org.uk/listing/the-list/data-downloads/',
            'nhle': 'https://historicengland.org.uk/listing/the-list/',
            'heritage_at_risk': 'https://historicengland.org.uk/advice/heritage-at-risk/',
        }

