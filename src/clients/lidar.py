"""
LIDAR / Aerial Survey Data Client
==================================

Access LIDAR and elevation data for building heights and terrain.

API Documentation: https://environment.data.gov.uk/

Usage:
    from src.clients import LIDARClient
    
    client = LIDARClient()
    elevation = client.get_dsm(lat=51.5, lon=-0.1)
"""

import pandas as pd
from typing import Optional, Dict, List, Any, Tuple
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class LIDARClient(BaseAPIClient):
    """
    Client for LIDAR and elevation data.
    
    Access:
    - Digital Surface Model (DSM) - includes buildings
    - Digital Terrain Model (DTM) - ground only
    - Building heights (DSM - DTM)
    
    Source: Environment Agency LIDAR
    """
    
    BASE_URL = "https://environment.data.gov.uk/spatialdata"
    WCS_URL = "https://environment.data.gov.uk/image/services"
    
    def __init__(self, **kwargs):
        """Initialize LIDAR client."""
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
    # DATA AVAILABILITY
    # ========================================
    
    def get_lidar_coverage(self) -> Dict[str, str]:
        """Get LIDAR coverage information"""
        return {
            'england_coverage': '~75% of England',
            'resolution_options': ['25cm', '50cm', '1m', '2m'],
            'data_type': ['DSM (Digital Surface Model)', 'DTM (Digital Terrain Model)'],
            'use_for_buildings': 'DSM - DTM gives approximate building heights',
            'download_portal': 'https://environment.data.gov.uk/DefraDataDownload/?Mode=survey',
        }
    
    def get_available_datasets(self) -> List[Dict]:
        """Get available LIDAR datasets"""
        return [
            {
                'name': 'LIDAR Composite DSM - 1m',
                'resolution': '1m',
                'type': 'DSM',
                'coverage': 'England composite',
                'url': 'https://environment.data.gov.uk/DefraDataDownload/?Mode=survey'
            },
            {
                'name': 'LIDAR Composite DTM - 1m',
                'resolution': '1m',
                'type': 'DTM',
                'coverage': 'England composite',
                'url': 'https://environment.data.gov.uk/DefraDataDownload/?Mode=survey'
            },
            {
                'name': 'LIDAR Point Cloud',
                'type': 'LAS/LAZ',
                'coverage': 'England - by tile',
                'url': 'https://environment.data.gov.uk/DefraDataDownload/?Mode=survey'
            }
        ]
    
    # ========================================
    # TILE QUERIES
    # ========================================
    
    def get_tile_index(
        self,
        bbox: Tuple[float, float, float, float]
    ) -> List[str]:
        """
        Get LIDAR tile references for a bounding box.
        
        Args:
            bbox: (min_lon, min_lat, max_lon, max_lat)
            
        Returns:
            List of tile references
        """
        import requests
        
        min_lon, min_lat, max_lon, max_lat = bbox
        
        # EA LIDAR index WFS
        url = "https://environment.data.gov.uk/spatialdata/lidar-composite-dsm-1m/wfs"
        
        params = {
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'typeName': 'lidar-composite-dsm-1m',
            'bbox': f'{min_lat},{min_lon},{max_lat},{max_lon},EPSG:4326',
            'outputFormat': 'application/json'
        }
        
        response = requests.get(url, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            return [f.get('properties', {}).get('tile_name', '') for f in features]
        
        return []
    
    # ========================================
    # BUILDING HEIGHT ESTIMATION
    # ========================================
    
    def estimate_building_height(
        self,
        dsm_height: float,
        dtm_height: float
    ) -> float:
        """
        Estimate building height from DSM and DTM.
        
        Args:
            dsm_height: Digital Surface Model height (includes buildings)
            dtm_height: Digital Terrain Model height (ground only)
            
        Returns:
            Estimated building height in meters
        """
        return max(0, dsm_height - dtm_height)
    
    def estimate_building_floors(
        self,
        height_meters: float,
        floor_height: float = 3.0
    ) -> int:
        """
        Estimate number of floors from building height.
        
        Args:
            height_meters: Building height
            floor_height: Assumed floor height (default 3m)
            
        Returns:
            Estimated floor count
        """
        if height_meters <= 0:
            return 0
        return max(1, round(height_meters / floor_height))
    
    def estimate_building_age_from_height(
        self,
        floors: int,
        area: str = "london"
    ) -> Dict[str, Any]:
        """
        Rough estimate of building age from height/floors.
        
        Args:
            floors: Number of floors
            area: Area for context
            
        Returns:
            Age estimate with confidence
        """
        # Very rough heuristics based on UK building patterns
        if floors <= 2:
            return {
                'likely_era': 'Pre-1960 or modern infill',
                'confidence': 'low',
                'notes': 'Low-rise could be Victorian, Georgian, or modern'
            }
        elif floors <= 4:
            return {
                'likely_era': 'Victorian/Edwardian or 1960s-1980s',
                'confidence': 'low',
                'notes': 'Medium-rise typical of multiple eras'
            }
        elif floors <= 10:
            return {
                'likely_era': '1960s-1990s',
                'confidence': 'medium',
                'notes': 'Post-war tower blocks common in this range'
            }
        else:
            return {
                'likely_era': '1990s-present',
                'confidence': 'medium',
                'notes': 'Tall buildings typically modern construction'
            }
    
    # ========================================
    # TERRAIN ANALYSIS
    # ========================================
    
    def get_elevation_profile_url(
        self,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float
    ) -> str:
        """Get URL for elevation profile"""
        return f"https://environment.data.gov.uk/image/services/LIDAR_DSM_1M/ImageServer/query"
    
    # ========================================
    # DOWNLOAD HELPERS
    # ========================================
    
    def get_download_portal_url(self) -> str:
        """Get URL for LIDAR download portal"""
        return "https://environment.data.gov.uk/DefraDataDownload/?Mode=survey"
    
    def get_tile_download_url(
        self,
        tile_ref: str,
        resolution: str = "1m",
        model_type: str = "DSM"
    ) -> str:
        """
        Build download URL for a specific tile.
        
        Args:
            tile_ref: Tile reference (e.g., 'TQ38')
            resolution: Resolution (1m, 2m, etc.)
            model_type: DSM or DTM
            
        Returns:
            Download URL
        """
        base = "https://environment.data.gov.uk/UserDownloads"
        return f"{base}/interactive/0d3f8b88e5e946d8a8f3eb5e318f5e93/{tile_ref}_{model_type}_{resolution}.zip"
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_os_grid_info(self) -> Dict[str, str]:
        """Get info about OS National Grid tile system"""
        return {
            'tile_size': '10km x 10km for 2-letter reference',
            'example': 'TQ38 = London area',
            'london_tiles': ['TQ27', 'TQ28', 'TQ37', 'TQ38', 'TQ47', 'TQ48'],
            'converter': 'https://gridreferencefinder.com/',
        }
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk LIDAR data"""
        return {
            'defra_download': 'https://environment.data.gov.uk/DefraDataDownload/?Mode=survey',
            'open_topography': 'https://opentopography.org/',
            'os_terrain': 'https://osdatahub.os.uk/downloads/open/Terrain50',
        }

