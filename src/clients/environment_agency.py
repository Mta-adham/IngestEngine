"""
Environment Agency API Client
==============================

Access Environment Agency data including:
- Flood risk zones
- Real-time flood warnings
- River levels
- Water quality
- LIDAR data

API Documentation: https://environment.data.gov.uk/

Usage:
    from src.clients import EnvironmentAgencyClient
    
    client = EnvironmentAgencyClient()
    flood_risk = client.get_flood_risk_for_location(51.5, -0.1)
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from pathlib import Path
import logging
import requests

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class EnvironmentAgencyClient(BaseAPIClient):
    """
    Client for Environment Agency APIs.
    
    No authentication required for most endpoints.
    """
    
    BASE_URL = "https://environment.data.gov.uk"
    FLOOD_API = "https://environment.data.gov.uk/flood-monitoring"
    
    def __init__(self, data_dir: Optional[str] = None, **kwargs):
        """
        Initialize Environment Agency client.
        
        Args:
            data_dir: Directory for downloaded data
        """
        self.data_dir = Path(data_dir) if data_dir else Path('data/raw/ea')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=None,
            rate_limit_rpm=120,
            **kwargs
        )
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            response = requests.get(f"{self.FLOOD_API}/id/floods", timeout=10)
            return response.status_code == 200
        except Exception:
            return False
    
    # ========================================
    # FLOOD WARNINGS
    # ========================================
    
    def get_current_flood_warnings(self) -> List[Dict]:
        """
        Get all current flood warnings.
        
        Returns:
            List of active flood warnings
        """
        response = requests.get(
            f"{self.FLOOD_API}/id/floods",
            timeout=30
        )
        response.raise_for_status()
        return response.json().get('items', [])
    
    def get_flood_warnings_df(self) -> pd.DataFrame:
        """Get current flood warnings as DataFrame"""
        warnings = self.get_current_flood_warnings()
        
        results = []
        for w in warnings:
            results.append({
                'flood_area_id': w.get('floodAreaID'),
                'description': w.get('description'),
                'severity': w.get('severity'),
                'severity_level': w.get('severityLevel'),
                'message': w.get('message'),
                'time_raised': w.get('timeRaised'),
                'time_severity_changed': w.get('timeSeverityChanged'),
                'easting': w.get('easting'),
                'northing': w.get('northing'),
            })
        
        return pd.DataFrame(results)
    
    def get_flood_areas(self) -> List[Dict]:
        """Get all flood area definitions"""
        response = requests.get(
            f"{self.FLOOD_API}/id/floodAreas",
            timeout=30
        )
        response.raise_for_status()
        return response.json().get('items', [])
    
    def get_flood_area(self, area_id: str) -> Dict:
        """Get details for a specific flood area"""
        response = requests.get(
            f"{self.FLOOD_API}/id/floodAreas/{area_id}",
            timeout=30
        )
        response.raise_for_status()
        return response.json().get('items', [{}])[0]
    
    # ========================================
    # MONITORING STATIONS
    # ========================================
    
    def get_stations(
        self,
        parameter: Optional[str] = None,
        qualifier: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        dist: int = 10
    ) -> List[Dict]:
        """
        Get monitoring stations.
        
        Args:
            parameter: Parameter type (level, flow, rainfall)
            qualifier: Qualifier (Stage, Downstream Stage)
            lat: Latitude for location search
            lon: Longitude for location search
            dist: Search radius in km
            
        Returns:
            List of stations
        """
        params = {}
        if parameter:
            params['parameter'] = parameter
        if qualifier:
            params['qualifier'] = qualifier
        if lat and lon:
            params['lat'] = lat
            params['long'] = lon
            params['dist'] = dist
        
        response = requests.get(
            f"{self.FLOOD_API}/id/stations",
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json().get('items', [])
    
    def get_stations_df(
        self,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        dist: int = 20
    ) -> pd.DataFrame:
        """Get monitoring stations as DataFrame"""
        stations = self.get_stations(lat=lat, lon=lon, dist=dist)
        
        results = []
        for s in stations:
            results.append({
                'station_id': s.get('stationReference'),
                'label': s.get('label'),
                'river_name': s.get('riverName'),
                'catchment': s.get('catchmentName'),
                'town': s.get('town'),
                'latitude': s.get('lat'),
                'longitude': s.get('long'),
                'easting': s.get('easting'),
                'northing': s.get('northing'),
                'status': s.get('status'),
                'date_opened': s.get('dateOpened'),
            })
        
        return pd.DataFrame(results)
    
    def get_station_readings(
        self,
        station_id: str,
        parameter: str = "level",
        latest: bool = True,
        since: Optional[str] = None
    ) -> List[Dict]:
        """
        Get readings for a station.
        
        Args:
            station_id: Station reference
            parameter: Parameter type
            latest: Get only latest reading
            since: Get readings since datetime
            
        Returns:
            List of readings
        """
        if latest:
            url = f"{self.FLOOD_API}/id/stations/{station_id}/readings?_sorted&_limit=1"
        else:
            url = f"{self.FLOOD_API}/id/stations/{station_id}/readings"
            if since:
                url += f"?since={since}"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json().get('items', [])
    
    # ========================================
    # FLOOD RISK ZONES
    # ========================================
    
    def get_flood_risk_for_location(
        self,
        lat: float,
        lon: float
    ) -> Dict:
        """
        Get flood risk assessment for a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Flood risk data including zone
        """
        # Check for nearby flood warnings
        stations = self.get_stations(lat=lat, lon=lon, dist=5)
        warnings = self.get_current_flood_warnings()
        
        # Get nearest station readings
        river_levels = []
        for station in stations[:5]:
            try:
                readings = self.get_station_readings(
                    station.get('stationReference'),
                    latest=True
                )
                if readings:
                    river_levels.append({
                        'station': station.get('label'),
                        'river': station.get('riverName'),
                        'value': readings[0].get('value'),
                        'unit': 'mAOD'
                    })
            except Exception:
                pass
        
        return {
            'latitude': lat,
            'longitude': lon,
            'nearby_stations': len(stations),
            'active_warnings': len([w for w in warnings if 
                                   abs(w.get('easting', 0) - lon*111000) < 10000]),
            'river_levels': river_levels
        }
    
    # ========================================
    # WATER QUALITY
    # ========================================
    
    def get_bathing_water_quality(self) -> List[Dict]:
        """
        Get bathing water quality data.
        
        Returns:
            List of bathing water sites with quality
        """
        response = requests.get(
            f"{self.BASE_URL}/doc/bathing-water-quality",
            params={'_format': 'json'},
            timeout=30
        )
        response.raise_for_status()
        return response.json().get('result', {}).get('items', [])
    
    def get_bathing_water_quality_df(self) -> pd.DataFrame:
        """Get bathing water quality as DataFrame"""
        sites = self.get_bathing_water_quality()
        
        results = []
        for site in sites:
            results.append({
                'site_id': site.get('bathingWater', {}).get('notation'),
                'name': site.get('bathingWater', {}).get('name'),
                'sample_date': site.get('sampleDateTime'),
                'classification': site.get('classification'),
                'easting': site.get('bathingWater', {}).get('easting'),
                'northing': site.get('bathingWater', {}).get('northing'),
            })
        
        return pd.DataFrame(results)
    
    # ========================================
    # RAINFALL
    # ========================================
    
    def get_rainfall_stations(
        self,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        dist: int = 20
    ) -> List[Dict]:
        """Get rainfall monitoring stations"""
        return self.get_stations(
            parameter='rainfall',
            lat=lat,
            lon=lon,
            dist=dist
        )
    
    def get_rainfall_readings(
        self,
        station_id: str,
        since: Optional[str] = None
    ) -> List[Dict]:
        """Get rainfall readings for a station"""
        return self.get_station_readings(
            station_id,
            parameter='rainfall',
            latest=not bool(since),
            since=since
        )
    
    # ========================================
    # LIDAR DATA
    # ========================================
    
    def get_lidar_coverage(self) -> Dict:
        """
        Get information about LIDAR coverage.
        
        Note: Actual LIDAR data download requires bulk download.
        """
        # LIDAR data is available via DEFRA Data Services Platform
        return {
            'description': 'National LIDAR Programme data',
            'coverage': 'England (rolling updates)',
            'resolution': '1m DSM/DTM',
            'download_url': 'https://environment.data.gov.uk/DefraDataDownload/?Mode=survey',
            'formats': ['ASCII Grid', 'GeoTiff'],
            'note': 'Use DEFRA Data Services Platform for bulk download'
        }
    
    # ========================================
    # LONDON SPECIFIC
    # ========================================
    
    def get_london_flood_risk_df(self) -> pd.DataFrame:
        """
        Get flood risk data for London area.
        
        Returns:
            DataFrame with monitoring stations and recent readings
        """
        # London bounding box
        london_center_lat = 51.5074
        london_center_lon = -0.1278
        
        stations = self.get_stations(
            lat=london_center_lat,
            lon=london_center_lon,
            dist=30  # 30km radius
        )
        
        results = []
        for station in stations:
            record = {
                'station_id': station.get('stationReference'),
                'name': station.get('label'),
                'river': station.get('riverName'),
                'catchment': station.get('catchmentName'),
                'town': station.get('town'),
                'latitude': station.get('lat'),
                'longitude': station.get('long'),
                'status': station.get('status'),
            }
            
            # Get latest reading
            try:
                readings = self.get_station_readings(
                    station.get('stationReference'),
                    latest=True
                )
                if readings:
                    record['latest_value'] = readings[0].get('value')
                    record['reading_time'] = readings[0].get('dateTime')
            except Exception:
                pass
            
            results.append(record)
        
        return pd.DataFrame(results)

