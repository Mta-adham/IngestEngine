"""
OpenStreetMap / Overpass API Client
====================================

Access OpenStreetMap data via Overpass API.

API Documentation: https://wiki.openstreetmap.org/wiki/Overpass_API

Usage:
    from src.clients import OpenStreetMapClient
    
    client = OpenStreetMapClient()
    pubs = client.get_amenities("pub", lat=51.5, lon=-0.1, radius=1000)
"""

import pandas as pd
from typing import Optional, Dict, List, Any, Tuple
import logging
import requests
import json

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class OpenStreetMapClient(BaseAPIClient):
    """
    Client for OpenStreetMap via Overpass API.
    
    Access all OSM data including:
    - POIs (amenities, shops, etc.)
    - Buildings
    - Roads
    - Administrative boundaries
    """
    
    # Public Overpass servers
    OVERPASS_URL = "https://overpass-api.de/api/interpreter"
    OVERPASS_BACKUP = "https://overpass.kumi.systems/api/interpreter"
    
    # Nominatim for geocoding
    NOMINATIM_URL = "https://nominatim.openstreetmap.org"
    
    def __init__(self, **kwargs):
        """Initialize OSM client."""
        super().__init__(
            base_url=self.OVERPASS_URL,
            rate_limit_rpm=10,  # Be respectful of public servers
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup headers"""
        self.session.headers['Accept'] = 'application/json'
        self.session.headers['User-Agent'] = 'IngestEngine/1.0 (UK World Model)'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.query("[out:json];node(51.5,-0.1,51.51,-0.09);out 1;")
            return True
        except Exception:
            return False
    
    # ========================================
    # RAW OVERPASS QUERIES
    # ========================================
    
    def query(self, overpass_query: str, timeout: int = 60) -> Dict:
        """
        Execute raw Overpass QL query.
        
        Args:
            overpass_query: Overpass QL query string
            timeout: Request timeout
            
        Returns:
            Query results
        """
        response = requests.post(
            self.OVERPASS_URL,
            data={'data': overpass_query},
            timeout=timeout
        )
        
        if response.status_code != 200:
            # Try backup server
            response = requests.post(
                self.OVERPASS_BACKUP,
                data={'data': overpass_query},
                timeout=timeout
            )
        
        if response.status_code != 200:
            raise APIError(f"Overpass query failed: {response.text}")
        
        return response.json()
    
    def query_df(self, overpass_query: str) -> pd.DataFrame:
        """Execute query and return as DataFrame"""
        result = self.query(overpass_query)
        elements = result.get('elements', [])
        
        if not elements:
            return pd.DataFrame()
        
        # Flatten elements
        rows = []
        for elem in elements:
            row = {
                'osm_id': elem.get('id'),
                'osm_type': elem.get('type'),
                'lat': elem.get('lat') or elem.get('center', {}).get('lat'),
                'lon': elem.get('lon') or elem.get('center', {}).get('lon'),
            }
            # Add tags
            for key, value in elem.get('tags', {}).items():
                row[f'tag_{key}'] = value
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    # ========================================
    # POIs / AMENITIES
    # ========================================
    
    def get_amenities(
        self,
        amenity_type: str,
        lat: float,
        lon: float,
        radius: int = 1000
    ) -> pd.DataFrame:
        """
        Get amenities near a point.
        
        Args:
            amenity_type: OSM amenity type (pub, restaurant, hospital, etc.)
            lat: Center latitude
            lon: Center longitude
            radius: Search radius in meters
            
        Returns:
            DataFrame of amenities
        """
        query = f"""
        [out:json][timeout:30];
        (
          node["amenity"="{amenity_type}"](around:{radius},{lat},{lon});
          way["amenity"="{amenity_type}"](around:{radius},{lat},{lon});
        );
        out center;
        """
        return self.query_df(query)
    
    def get_shops(
        self,
        shop_type: Optional[str] = None,
        lat: float = None,
        lon: float = None,
        radius: int = 1000
    ) -> pd.DataFrame:
        """Get shops near a point"""
        shop_filter = f'"{shop_type}"' if shop_type else ''
        
        query = f"""
        [out:json][timeout:30];
        (
          node["shop"{f'={shop_filter}' if shop_filter else ''}](around:{radius},{lat},{lon});
          way["shop"{f'={shop_filter}' if shop_filter else ''}](around:{radius},{lat},{lon});
        );
        out center;
        """
        return self.query_df(query)
    
    def get_tourism(
        self,
        lat: float,
        lon: float,
        radius: int = 2000
    ) -> pd.DataFrame:
        """Get tourism POIs (hotels, museums, attractions)"""
        query = f"""
        [out:json][timeout:30];
        (
          node["tourism"](around:{radius},{lat},{lon});
          way["tourism"](around:{radius},{lat},{lon});
        );
        out center;
        """
        return self.query_df(query)
    
    # ========================================
    # BUILDINGS
    # ========================================
    
    def get_buildings(
        self,
        lat: float,
        lon: float,
        radius: int = 500
    ) -> pd.DataFrame:
        """Get buildings near a point"""
        query = f"""
        [out:json][timeout:60];
        way["building"](around:{radius},{lat},{lon});
        out center;
        """
        return self.query_df(query)
    
    def get_building_footprints(
        self,
        bbox: Tuple[float, float, float, float]
    ) -> List[Dict]:
        """
        Get building footprints in bounding box.
        
        Args:
            bbox: (south, west, north, east)
            
        Returns:
            List of buildings with geometry
        """
        south, west, north, east = bbox
        
        query = f"""
        [out:json][timeout:120];
        way["building"]({south},{west},{north},{east});
        out geom;
        """
        
        result = self.query(query)
        return result.get('elements', [])
    
    # ========================================
    # ADMINISTRATIVE BOUNDARIES
    # ========================================
    
    def get_boundary(
        self,
        name: str,
        admin_level: int = 8
    ) -> Dict:
        """
        Get administrative boundary by name.
        
        Args:
            name: Area name
            admin_level: OSM admin level (8=borough, 6=county, 4=country)
            
        Returns:
            Boundary data
        """
        query = f"""
        [out:json][timeout:60];
        relation["name"="{name}"]["admin_level"="{admin_level}"];
        out geom;
        """
        result = self.query(query)
        elements = result.get('elements', [])
        return elements[0] if elements else {}
    
    def get_london_boroughs(self) -> pd.DataFrame:
        """Get all London borough boundaries"""
        query = """
        [out:json][timeout:120];
        area["name"="Greater London"]->.london;
        relation["admin_level"="6"](area.london);
        out center;
        """
        return self.query_df(query)
    
    # ========================================
    # TRANSPORT
    # ========================================
    
    def get_transport_stops(
        self,
        lat: float,
        lon: float,
        radius: int = 1000
    ) -> pd.DataFrame:
        """Get public transport stops near a point"""
        query = f"""
        [out:json][timeout:30];
        (
          node["public_transport"="stop_position"](around:{radius},{lat},{lon});
          node["highway"="bus_stop"](around:{radius},{lat},{lon});
          node["railway"="station"](around:{radius},{lat},{lon});
          node["railway"="halt"](around:{radius},{lat},{lon});
        );
        out;
        """
        return self.query_df(query)
    
    # ========================================
    # GEOCODING (Nominatim)
    # ========================================
    
    def geocode(self, address: str) -> Dict:
        """
        Geocode an address.
        
        Args:
            address: Address string
            
        Returns:
            Location data
        """
        response = requests.get(
            f"{self.NOMINATIM_URL}/search",
            params={
                'q': address,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'gb'
            },
            headers={'User-Agent': 'IngestEngine/1.0'},
            timeout=30
        )
        
        results = response.json()
        return results[0] if results else {}
    
    def reverse_geocode(self, lat: float, lon: float) -> Dict:
        """Reverse geocode coordinates"""
        response = requests.get(
            f"{self.NOMINATIM_URL}/reverse",
            params={
                'lat': lat,
                'lon': lon,
                'format': 'json'
            },
            headers={'User-Agent': 'IngestEngine/1.0'},
            timeout=30
        )
        return response.json()
    
    # ========================================
    # BULK QUERIES
    # ========================================
    
    def get_all_pois_in_area(
        self,
        bbox: Tuple[float, float, float, float],
        poi_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get all POIs in a bounding box.
        
        Args:
            bbox: (south, west, north, east)
            poi_types: List of amenity types to include
            
        Returns:
            DataFrame of all POIs
        """
        south, west, north, east = bbox
        
        if poi_types:
            type_filter = '|'.join(poi_types)
            query = f"""
            [out:json][timeout:120];
            (
              node["amenity"~"{type_filter}"]({south},{west},{north},{east});
              way["amenity"~"{type_filter}"]({south},{west},{north},{east});
            );
            out center;
            """
        else:
            query = f"""
            [out:json][timeout:120];
            (
              node["amenity"]({south},{west},{north},{east});
              way["amenity"]({south},{west},{north},{east});
            );
            out center;
            """
        
        return self.query_df(query)
    
    # ========================================
    # COMMON POI TYPES
    # ========================================
    
    def get_poi_types(self) -> Dict[str, List[str]]:
        """Get common OSM POI types"""
        return {
            'food_drink': ['restaurant', 'cafe', 'pub', 'bar', 'fast_food', 'food_court'],
            'shops': ['supermarket', 'convenience', 'clothes', 'hairdresser', 'bakery'],
            'transport': ['bus_station', 'taxi', 'bicycle_rental', 'car_rental', 'fuel'],
            'health': ['hospital', 'clinic', 'doctors', 'pharmacy', 'dentist'],
            'education': ['school', 'university', 'college', 'kindergarten', 'library'],
            'leisure': ['cinema', 'theatre', 'nightclub', 'gym', 'swimming_pool'],
            'finance': ['bank', 'atm', 'bureau_de_change'],
            'tourism': ['hotel', 'hostel', 'museum', 'gallery', 'attraction'],
        }

