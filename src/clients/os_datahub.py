"""
Ordnance Survey DataHub API Client
===================================

Access OS data products including:
- OS Open UPRN (property identifiers)
- OS Code-Point (postcode coordinates)
- OS OpenMap Local (building footprints)
- OS Names API
- OS Places API

API Documentation: https://osdatahub.os.uk/docs

Usage:
    from src.clients import OSDataHubClient
    
    client = OSDataHubClient(api_key="your-api-key")
    results = client.search_places("10 Downing Street")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from pathlib import Path
import logging
import os
import requests
import zipfile
import io

from src.clients.base_client import BaseAPIClient, APIError
from src.config import OS_API_KEY

logger = logging.getLogger(__name__)


class OSDataHubClient(BaseAPIClient):
    """
    Client for Ordnance Survey DataHub APIs.
    
    Register for API key at: https://osdatahub.os.uk/
    Free tier: 1,000 transactions/month
    """
    
    BASE_URL = "https://api.os.uk"
    DOWNLOAD_URL = "https://api.os.uk/downloads/v1"
    
    # Open data download URLs
    OPEN_DATA = {
        'uprn': 'https://api.os.uk/downloads/v1/products/OpenUPRN/downloads',
        'codepoint': 'https://api.os.uk/downloads/v1/products/CodePointOpen/downloads',
        'openmap': 'https://api.os.uk/downloads/v1/products/OpenMapLocal/downloads',
        'boundary_line': 'https://api.os.uk/downloads/v1/products/BoundaryLine/downloads',
    }
    
    def __init__(
        self, 
        api_key: Optional[str] = None,
        data_dir: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize OS DataHub client.
        
        Args:
            api_key: OS DataHub API key (or uses config/env var OS_API_KEY)
            data_dir: Directory for downloaded data
        """
        api_key = api_key or OS_API_KEY
        
        if not api_key:
            logger.warning("No API key. Set OS_API_KEY in config or env var.")
        
        self.data_dir = Path(data_dir) if data_dir else Path('data/raw/os')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=api_key,
            rate_limit_rpm=100,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup API key in header"""
        if self.api_key:
            self.session.headers['key'] = self.api_key
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            # Try names API with a simple search
            self.search_names("London", limit=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # OS NAMES API
    # ========================================
    
    def search_names(
        self,
        query: str,
        bounds: Optional[tuple] = None,
        fq: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict:
        """
        Search OS Names (gazetteer).
        
        Args:
            query: Search query
            bounds: Bounding box (minx, miny, maxx, maxy) in EPSG:27700
            fq: Filter queries (e.g., ["LOCAL_TYPE:City"])
            limit: Max results
            offset: Starting offset
            
        Returns:
            Search results with named places
        """
        params = {
            'query': query,
            'maxresults': limit,
            'offset': offset,
            'key': self.api_key
        }
        
        if bounds:
            params['bounds'] = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
        if fq:
            params['fq'] = fq
        
        return self.get("/search/names/v1/find", params=params)
    
    def get_nearest_name(self, x: float, y: float) -> Dict:
        """
        Find nearest named place to coordinates.
        
        Args:
            x: Easting (EPSG:27700) or Longitude (EPSG:4326)
            y: Northing (EPSG:27700) or Latitude (EPSG:4326)
            
        Returns:
            Nearest named place
        """
        params = {
            'point': f"{x},{y}",
            'key': self.api_key
        }
        return self.get("/search/names/v1/nearest", params=params)
    
    # ========================================
    # OS PLACES API
    # ========================================
    
    def search_places(
        self,
        query: str,
        dataset: str = "DPA",
        max_results: int = 100,
        output_srs: str = "EPSG:4326"
    ) -> Dict:
        """
        Search OS Places (addresses).
        
        Args:
            query: Address search query
            dataset: Dataset to search (DPA, LPI)
            max_results: Maximum results
            output_srs: Output coordinate system
            
        Returns:
            Address search results with UPRNs
        """
        params = {
            'query': query,
            'dataset': dataset,
            'maxresults': max_results,
            'output_srs': output_srs,
            'key': self.api_key
        }
        return self.get("/search/places/v1/find", params=params)
    
    def get_place_by_uprn(self, uprn: str, output_srs: str = "EPSG:4326") -> Dict:
        """
        Get address details by UPRN.
        
        Args:
            uprn: Unique Property Reference Number
            output_srs: Output coordinate system
            
        Returns:
            Full address details
        """
        params = {
            'uprn': uprn,
            'output_srs': output_srs,
            'key': self.api_key
        }
        return self.get("/search/places/v1/uprn", params=params)
    
    def get_places_by_postcode(
        self, 
        postcode: str,
        output_srs: str = "EPSG:4326"
    ) -> Dict:
        """
        Get all addresses in a postcode.
        
        Args:
            postcode: UK postcode
            output_srs: Output coordinate system
            
        Returns:
            All addresses in postcode
        """
        params = {
            'postcode': postcode.replace(' ', ''),
            'output_srs': output_srs,
            'key': self.api_key
        }
        return self.get("/search/places/v1/postcode", params=params)
    
    def get_places_in_radius(
        self,
        x: float,
        y: float,
        radius: int = 100,
        srs: str = "EPSG:4326",
        output_srs: str = "EPSG:4326"
    ) -> Dict:
        """
        Get addresses within radius of a point.
        
        Args:
            x: X coordinate (easting or longitude)
            y: Y coordinate (northing or latitude)
            radius: Search radius in meters
            srs: Input coordinate system
            output_srs: Output coordinate system
            
        Returns:
            Addresses within radius
        """
        params = {
            'point': f"{x},{y}",
            'radius': radius,
            'srs': srs,
            'output_srs': output_srs,
            'key': self.api_key
        }
        return self.get("/search/places/v1/radius", params=params)
    
    # ========================================
    # OPEN DATA DOWNLOADS
    # ========================================
    
    def list_open_products(self) -> List[Dict]:
        """List available open data products"""
        return self.get("/downloads/v1/products", params={'key': self.api_key})
    
    def download_open_uprn(
        self,
        area: str = "GB",
        force: bool = False
    ) -> Path:
        """
        Download OS Open UPRN dataset.
        
        Args:
            area: Area code (GB for all, or specific tiles)
            force: Force re-download
            
        Returns:
            Path to downloaded file
        """
        output_file = self.data_dir / f"os_open_uprn_{area}.csv"
        
        if output_file.exists() and not force:
            logger.info(f"Using cached UPRN data: {output_file}")
            return output_file
        
        logger.info("Downloading OS Open UPRN (this may take a while)...")
        
        # Get download URL
        try:
            downloads = self.get(
                f"/downloads/v1/products/OpenUPRN/downloads",
                params={'key': self.api_key}
            )
            
            # Find CSV download
            csv_url = None
            for item in downloads:
                if item.get('format') == 'CSV' and area in item.get('area', ''):
                    csv_url = item.get('url')
                    break
            
            if not csv_url:
                raise APIError("CSV download URL not found")
            
            # Download file
            response = requests.get(csv_url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Save to file
            with open(output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded UPRN data to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Failed to download UPRN data: {e}")
            raise APIError(f"UPRN download failed: {e}")
    
    def download_codepoint(self, force: bool = False) -> Path:
        """
        Download OS Code-Point Open (postcode coordinates).
        
        Returns:
            Path to downloaded/extracted folder
        """
        output_dir = self.data_dir / "codepoint_open"
        
        if output_dir.exists() and not force:
            logger.info(f"Using cached Code-Point data: {output_dir}")
            return output_dir
        
        logger.info("Downloading OS Code-Point Open...")
        
        try:
            # Get download link
            url = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"
            
            response = requests.get(url, stream=True, timeout=300, allow_redirects=True)
            response.raise_for_status()
            
            # Extract ZIP
            output_dir.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                zf.extractall(output_dir)
            
            logger.info(f"Extracted Code-Point data to {output_dir}")
            return output_dir
            
        except Exception as e:
            logger.error(f"Failed to download Code-Point: {e}")
            raise APIError(f"Code-Point download failed: {e}")
    
    # ========================================
    # DATAFRAME EXPORTS
    # ========================================
    
    def search_places_df(self, query: str, max_results: int = 100) -> pd.DataFrame:
        """
        Search addresses and return as DataFrame.
        
        Args:
            query: Address search
            max_results: Maximum results
            
        Returns:
            DataFrame with addresses
        """
        response = self.search_places(query, max_results=max_results)
        
        results = []
        for feature in response.get('results', []):
            dpa = feature.get('DPA', {})
            results.append({
                'uprn': dpa.get('UPRN'),
                'address': dpa.get('ADDRESS'),
                'building_number': dpa.get('BUILDING_NUMBER'),
                'building_name': dpa.get('BUILDING_NAME'),
                'street': dpa.get('THOROUGHFARE_NAME'),
                'locality': dpa.get('DEPENDENT_LOCALITY'),
                'town': dpa.get('POST_TOWN'),
                'postcode': dpa.get('POSTCODE'),
                'easting': dpa.get('X_COORDINATE'),
                'northing': dpa.get('Y_COORDINATE'),
                'latitude': dpa.get('LAT'),
                'longitude': dpa.get('LNG'),
                'classification': dpa.get('CLASSIFICATION_CODE'),
                'local_authority': dpa.get('LOCAL_CUSTODIAN_CODE'),
            })
        
        return pd.DataFrame(results)
    
    def get_uprns_by_postcode_df(self, postcode: str) -> pd.DataFrame:
        """
        Get all UPRNs in a postcode as DataFrame.
        
        Args:
            postcode: UK postcode
            
        Returns:
            DataFrame with UPRNs and addresses
        """
        response = self.get_places_by_postcode(postcode)
        
        results = []
        for feature in response.get('results', []):
            dpa = feature.get('DPA', {})
            results.append({
                'uprn': dpa.get('UPRN'),
                'address': dpa.get('ADDRESS'),
                'postcode': dpa.get('POSTCODE'),
                'latitude': dpa.get('LAT'),
                'longitude': dpa.get('LNG'),
                'classification': dpa.get('CLASSIFICATION_CODE'),
            })
        
        return pd.DataFrame(results)
    
    def get_uprns_in_area_df(
        self,
        lat: float,
        lon: float,
        radius: int = 500
    ) -> pd.DataFrame:
        """
        Get all UPRNs within radius of a point.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Radius in meters
            
        Returns:
            DataFrame with UPRNs
        """
        response = self.get_places_in_radius(lon, lat, radius)
        
        results = []
        for feature in response.get('results', []):
            dpa = feature.get('DPA', {})
            results.append({
                'uprn': dpa.get('UPRN'),
                'address': dpa.get('ADDRESS'),
                'postcode': dpa.get('POSTCODE'),
                'latitude': dpa.get('LAT'),
                'longitude': dpa.get('LNG'),
                'classification': dpa.get('CLASSIFICATION_CODE'),
                'distance': feature.get('distance'),
            })
        
        return pd.DataFrame(results)

