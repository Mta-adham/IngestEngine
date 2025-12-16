"""
Transport for London (TfL) API Client
=====================================

Fetches transport data from the TfL Unified API.

API Documentation: https://api.tfl.gov.uk/
Rate Limit: 500 requests per minute (with API key)

Usage:
    from src.clients import TfLClient
    
    client = TfLClient(app_id="your-app-id", app_key="your-app-key")
    stations = client.get_tube_stations()
    arrivals = client.get_arrivals("940GZZLUWLO")  # Waterloo
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class TfLClient(BaseAPIClient):
    """
    Client for TfL Unified API.
    
    Get your API key from: https://api-portal.tfl.gov.uk/signup
    """
    
    BASE_URL = "https://api.tfl.gov.uk"
    
    # TfL Mode IDs
    MODES = {
        'tube': 'tube',
        'dlr': 'dlr',
        'overground': 'london-overground',
        'elizabeth': 'elizabeth-line',
        'bus': 'bus',
        'tram': 'tram',
        'river-bus': 'river-bus',
        'cable-car': 'cable-car',
        'national-rail': 'national-rail',
        'cycle': 'cycle',
        'walking': 'walking'
    }
    
    def __init__(
        self, 
        app_id: Optional[str] = None,
        app_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize TfL client.
        
        Args:
            app_id: TfL App ID (or set TFL_APP_ID env var)
            app_key: TfL App Key (or set TFL_APP_KEY env var)
        """
        self.app_id = app_id or os.environ.get('TFL_APP_ID')
        self.app_key = app_key or os.environ.get('TFL_APP_KEY')
        
        if not self.app_key:
            logger.warning("No API key provided. Rate limit will be lower.")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=self.app_key,
            rate_limit_rpm=500 if self.app_key else 50,
            **kwargs
        )
    
    def _setup_auth(self):
        """Add API key to query params"""
        pass  # TfL uses query params, handled in _add_auth_params
    
    def _add_auth_params(self, params: Dict) -> Dict:
        """Add authentication parameters"""
        if self.app_key:
            params['app_key'] = self.app_key
        if self.app_id:
            params['app_id'] = self.app_id
        return params
    
    def get(self, endpoint: str, params: Optional[Dict] = None, **kwargs) -> Any:
        """Override get to add auth params"""
        params = params or {}
        params = self._add_auth_params(params)
        return super().get(endpoint, params=params, **kwargs)
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get("/Line/Meta/Modes")
            return True
        except Exception:
            return False
    
    # ========================================
    # LINE/ROUTE DATA
    # ========================================
    
    def get_all_lines(self, mode: Optional[str] = None) -> List[Dict]:
        """
        Get all lines/routes.
        
        Args:
            mode: Filter by mode (tube, dlr, overground, etc.)
            
        Returns:
            List of line data
        """
        if mode:
            return self.get(f"/Line/Mode/{mode}")
        return self.get("/Line")
    
    def get_line_status(self, line_id: str) -> Dict:
        """Get current status of a line"""
        return self.get(f"/Line/{line_id}/Status")
    
    def get_all_line_statuses(self, mode: str = "tube") -> List[Dict]:
        """Get status for all lines of a mode"""
        return self.get(f"/Line/Mode/{mode}/Status")
    
    def get_line_route(self, line_id: str) -> Dict:
        """Get route sequence for a line"""
        return self.get(f"/Line/{line_id}/Route/Sequence/outbound")
    
    # ========================================
    # STOP POINTS (STATIONS/STOPS)
    # ========================================
    
    def get_stop_points_by_mode(self, mode: str) -> List[Dict]:
        """
        Get all stop points for a mode.
        
        Args:
            mode: Transport mode (tube, bus, dlr, etc.)
            
        Returns:
            List of stop points with coordinates
        """
        return self.get(f"/StopPoint/Mode/{mode}")
    
    def get_stop_point(self, stop_id: str) -> Dict:
        """
        Get details for a specific stop.
        
        Args:
            stop_id: Stop point ID (e.g., "940GZZLUWLO")
            
        Returns:
            Stop point details
        """
        return self.get(f"/StopPoint/{stop_id}")
    
    def search_stop_points(
        self, 
        query: str, 
        modes: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Search for stop points by name.
        
        Args:
            query: Search query
            modes: Filter by modes
            
        Returns:
            Matching stop points
        """
        params = {'query': query}
        if modes:
            params['modes'] = ','.join(modes)
        return self.get("/StopPoint/Search", params=params).get('matches', [])
    
    def get_stop_points_in_radius(
        self,
        lat: float,
        lon: float,
        radius: int = 500,
        stop_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Get stop points within radius of a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius: Radius in meters
            stop_types: Filter by stop types
            
        Returns:
            List of nearby stop points
        """
        params = {
            'lat': lat,
            'lon': lon,
            'radius': radius
        }
        if stop_types:
            params['stopTypes'] = ','.join(stop_types)
        return self.get("/StopPoint", params=params).get('stopPoints', [])
    
    # ========================================
    # TUBE STATIONS
    # ========================================
    
    def get_tube_stations(self) -> pd.DataFrame:
        """
        Get all London Underground stations.
        
        Returns:
            DataFrame with station data:
            - station_id
            - station_name
            - latitude
            - longitude
            - lines (comma-separated)
            - zone
        """
        stops = self.get_stop_points_by_mode('tube')
        
        results = []
        for stop in stops.get('stopPoints', stops) if isinstance(stops, dict) else stops:
            lines = [lp.get('name') for lp in stop.get('lineModeGroups', [])]
            lines_flat = []
            for lg in stop.get('lineModeGroups', []):
                lines_flat.extend(lg.get('lineIdentifier', []))
            
            # Get zone from additional properties
            zone = None
            for prop in stop.get('additionalProperties', []):
                if prop.get('key') == 'Zone':
                    zone = prop.get('value')
                    break
            
            results.append({
                'station_id': stop.get('id') or stop.get('naptanId'),
                'station_name': stop.get('commonName'),
                'latitude': stop.get('lat'),
                'longitude': stop.get('lon'),
                'lines': ','.join(lines_flat),
                'zone': zone,
                'modes': ','.join(stop.get('modes', [])),
                'status': stop.get('status')
            })
        
        return pd.DataFrame(results)
    
    # ========================================
    # BUS STOPS
    # ========================================
    
    def get_bus_stops(self, line_id: Optional[str] = None) -> pd.DataFrame:
        """
        Get bus stops, optionally for a specific route.
        
        Args:
            line_id: Bus route number (e.g., "73")
            
        Returns:
            DataFrame with bus stop data
        """
        if line_id:
            stops = self.get(f"/Line/{line_id}/StopPoints")
        else:
            stops = self.get_stop_points_by_mode('bus')
            stops = stops.get('stopPoints', stops) if isinstance(stops, dict) else stops
        
        results = []
        for stop in stops:
            results.append({
                'stop_id': stop.get('id') or stop.get('naptanId'),
                'stop_name': stop.get('commonName'),
                'latitude': stop.get('lat'),
                'longitude': stop.get('lon'),
                'indicator': stop.get('indicator'),
                'stop_letter': stop.get('stopLetter'),
                'towards': stop.get('towards'),
            })
        
        return pd.DataFrame(results)
    
    # ========================================
    # ARRIVALS / LIVE DATA
    # ========================================
    
    def get_arrivals(self, stop_id: str) -> List[Dict]:
        """
        Get live arrivals at a stop.
        
        Args:
            stop_id: Stop point ID
            
        Returns:
            List of arriving vehicles
        """
        return self.get(f"/StopPoint/{stop_id}/Arrivals")
    
    def get_line_arrivals(self, line_id: str) -> List[Dict]:
        """Get arrivals for all stops on a line"""
        return self.get(f"/Line/{line_id}/Arrivals")
    
    # ========================================
    # BIKE POINTS (SANTANDER CYCLES)
    # ========================================
    
    def get_bike_points(self) -> pd.DataFrame:
        """
        Get all Santander Cycle docking stations.
        
        Returns:
            DataFrame with bike point data
        """
        points = self.get("/BikePoint")
        
        results = []
        for point in points:
            # Extract bike counts from additional properties
            props = {p['key']: p['value'] for p in point.get('additionalProperties', [])}
            
            results.append({
                'bike_point_id': point.get('id'),
                'name': point.get('commonName'),
                'latitude': point.get('lat'),
                'longitude': point.get('lon'),
                'num_bikes': int(props.get('NbBikes', 0)),
                'num_empty_docks': int(props.get('NbEmptyDocks', 0)),
                'num_docks': int(props.get('NbDocks', 0)),
                'installed': props.get('Installed') == 'true',
                'locked': props.get('Locked') == 'true',
            })
        
        return pd.DataFrame(results)
    
    def get_bike_point(self, bike_point_id: str) -> Dict:
        """Get details for a specific bike point"""
        return self.get(f"/BikePoint/{bike_point_id}")
    
    # ========================================
    # JOURNEY PLANNING
    # ========================================
    
    def plan_journey(
        self,
        from_location: str,
        to_location: str,
        via: Optional[str] = None,
        time_is: str = "Departing",
        journey_time: Optional[str] = None
    ) -> Dict:
        """
        Plan a journey between two locations.
        
        Args:
            from_location: Origin (postcode, station name, or lat,lon)
            to_location: Destination
            via: Optional waypoint
            time_is: "Departing" or "Arriving"
            journey_time: Time in format "HHmm" or datetime
            
        Returns:
            Journey options with routes and durations
        """
        params = {'timeIs': time_is}
        if via:
            params['via'] = via
        if journey_time:
            params['time'] = journey_time
        
        return self.get(f"/Journey/JourneyResults/{from_location}/to/{to_location}", params=params)
    
    # ========================================
    # AIR QUALITY
    # ========================================
    
    def get_air_quality(self) -> Dict:
        """
        Get current London air quality forecast.
        
        Returns:
            Air quality data with pollutant levels
        """
        return self.get("/AirQuality")
    
    # ========================================
    # CROWDING
    # ========================================
    
    def get_crowding(self, naptan_id: str, day: str = "Mon") -> Dict:
        """
        Get crowding data for a station.
        
        Args:
            naptan_id: Station NAPTAN ID
            day: Day of week (Mon, Tue, etc.)
            
        Returns:
            Crowding levels by time of day
        """
        return self.get(f"/crowding/{naptan_id}/{day}")
    
    # ========================================
    # BATCH EXPORT
    # ========================================
    
    def export_all_stations(self) -> pd.DataFrame:
        """
        Export all public transport stations/stops.
        
        Returns:
            DataFrame with all stations across modes
        """
        all_stops = []
        
        modes = ['tube', 'dlr', 'london-overground', 'elizabeth-line', 
                 'tram', 'national-rail']
        
        for mode in modes:
            logger.info(f"Fetching {mode} stops...")
            try:
                stops = self.get_stop_points_by_mode(mode)
                stop_list = stops.get('stopPoints', stops) if isinstance(stops, dict) else stops
                
                for stop in stop_list:
                    all_stops.append({
                        'stop_id': stop.get('id') or stop.get('naptanId'),
                        'stop_name': stop.get('commonName'),
                        'mode': mode,
                        'latitude': stop.get('lat'),
                        'longitude': stop.get('lon'),
                        'status': stop.get('status'),
                    })
            except Exception as e:
                logger.warning(f"Failed to fetch {mode}: {e}")
        
        return pd.DataFrame(all_stops)

