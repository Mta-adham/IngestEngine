"""
Network Rail / National Rail API Client
========================================

Access rail network data including:
- Station information
- Train services
- Disruptions
- Network infrastructure

APIs: 
- National Rail Enquiries
- Open Rail Data

Usage:
    from src.clients import NetworkRailClient
    
    client = NetworkRailClient()
    stations = client.get_stations()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class NetworkRailClient(BaseAPIClient):
    """
    Client for Network Rail and National Rail APIs.
    
    Access:
    - Station reference data
    - Live departures (requires API key)
    - Service disruptions
    """
    
    # Darwin API (Live Departure Boards)
    DARWIN_URL = "https://lite.realtime.nationalrail.co.uk/OpenLDBWS/ldb11.asmx"
    
    # Open Data Portal
    OPEN_DATA_URL = "https://opendata.nationalrail.co.uk"
    
    # ORR (Office of Rail and Road) Statistics
    ORR_URL = "https://dataportal.orr.gov.uk/api"
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Network Rail client.
        
        Args:
            api_key: National Rail API key (for live departures)
        """
        self.api_key = api_key or os.environ.get('NATIONAL_RAIL_API_KEY', '')
        
        super().__init__(
            base_url=self.OPEN_DATA_URL,
            api_key=self.api_key,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup headers"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_stations_from_csv()
            return True
        except Exception:
            return False
    
    # ========================================
    # STATION DATA
    # ========================================
    
    def get_stations_from_csv(self) -> pd.DataFrame:
        """
        Get all UK railway stations from reference data.
        
        Returns:
            DataFrame with station information
        """
        # NaPTAN (National Public Transport Access Nodes)
        url = "https://naptan.api.dft.gov.uk/v1/access-nodes"
        
        import requests
        response = requests.get(
            url,
            params={
                'dataFormat': 'csv',
                'atcoAreaCodes': '',  # All areas
                'stopTypes': 'RLY'  # Railway only
            },
            timeout=60
        )
        
        if response.status_code == 200:
            from io import StringIO
            return pd.read_csv(StringIO(response.text))
        
        return pd.DataFrame()
    
    def get_station(self, crs_code: str) -> Dict:
        """
        Get station by CRS code.
        
        Args:
            crs_code: 3-letter station code (e.g., 'KGX' for King's Cross)
            
        Returns:
            Station details
        """
        df = self.get_stations_from_csv()
        
        if 'CrsCode' in df.columns:
            station = df[df['CrsCode'] == crs_code.upper()]
            if not station.empty:
                return station.iloc[0].to_dict()
        
        return {}
    
    def search_stations(
        self,
        name: Optional[str] = None,
        postcode: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Search for stations.
        
        Args:
            name: Station name search
            postcode: Postcode search
            
        Returns:
            DataFrame of matching stations
        """
        df = self.get_stations_from_csv()
        
        if df.empty:
            return df
        
        if name and 'StopName' in df.columns:
            df = df[df['StopName'].str.contains(name, case=False, na=False)]
        
        if postcode and 'PostCode' in df.columns:
            df = df[df['PostCode'].str.startswith(postcode.upper()[:4], na=False)]
        
        return df
    
    # ========================================
    # LIVE DEPARTURES (requires API key)
    # ========================================
    
    def get_departures(
        self,
        crs: str,
        num_rows: int = 10,
        filter_crs: Optional[str] = None
    ) -> List[Dict]:
        """
        Get live departure board for a station.
        
        Requires National Rail API key.
        
        Args:
            crs: Station CRS code
            num_rows: Number of services to return
            filter_crs: Filter to services calling at this station
            
        Returns:
            List of departures
        """
        if not self.api_key:
            raise APIError("API key required for live departures. Set NATIONAL_RAIL_API_KEY.")
        
        # This would need SOAP/XML parsing for Darwin
        # Simplified version using JSON endpoint if available
        logger.warning("Live departures requires SOAP client implementation")
        return []
    
    # ========================================
    # ORR STATISTICS
    # ========================================
    
    def get_station_usage(self) -> pd.DataFrame:
        """
        Get station usage statistics from ORR.
        
        Returns:
            DataFrame with passenger numbers per station
        """
        url = "https://dataportal.orr.gov.uk/media/1842/station-usage-data.xlsx"
        
        try:
            return pd.read_excel(url, engine='openpyxl')
        except Exception as e:
            logger.warning(f"Could not fetch ORR data: {e}")
            return pd.DataFrame()
    
    def get_performance_data(self) -> pd.DataFrame:
        """Get train performance statistics"""
        url = "https://dataportal.orr.gov.uk/media/1843/ppm-data.xlsx"
        
        try:
            return pd.read_excel(url, engine='openpyxl')
        except Exception:
            return pd.DataFrame()
    
    # ========================================
    # OPERATORS
    # ========================================
    
    def get_train_operators(self) -> List[Dict]:
        """Get list of train operating companies"""
        return [
            {'code': 'AW', 'name': 'Avanti West Coast'},
            {'code': 'CC', 'name': 'c2c'},
            {'code': 'CH', 'name': 'Chiltern Railways'},
            {'code': 'CS', 'name': 'Caledonian Sleeper'},
            {'code': 'EM', 'name': 'East Midlands Railway'},
            {'code': 'ES', 'name': 'Eurostar'},
            {'code': 'GC', 'name': 'Grand Central'},
            {'code': 'GN', 'name': 'Great Northern'},
            {'code': 'GR', 'name': 'LNER'},
            {'code': 'GW', 'name': 'Great Western Railway'},
            {'code': 'GX', 'name': 'Gatwick Express'},
            {'code': 'HT', 'name': 'Hull Trains'},
            {'code': 'HX', 'name': 'Heathrow Express'},
            {'code': 'IL', 'name': 'Island Line'},
            {'code': 'LE', 'name': 'Greater Anglia'},
            {'code': 'LM', 'name': 'West Midlands Trains'},
            {'code': 'LO', 'name': 'London Overground'},
            {'code': 'LT', 'name': 'Elizabeth line'},
            {'code': 'ME', 'name': 'Merseyrail'},
            {'code': 'NT', 'name': 'Northern'},
            {'code': 'SE', 'name': 'Southeastern'},
            {'code': 'SN', 'name': 'Southern'},
            {'code': 'SR', 'name': 'ScotRail'},
            {'code': 'SW', 'name': 'South Western Railway'},
            {'code': 'TL', 'name': 'Thameslink'},
            {'code': 'TP', 'name': 'TransPennine Express'},
            {'code': 'TW', 'name': 'Tyne and Wear Metro'},
            {'code': 'VT', 'name': 'CrossCountry'},
            {'code': 'XC', 'name': 'CrossCountry'},
            {'code': 'XR', 'name': 'Elizabeth line'},
        ]
    
    # ========================================
    # BULK DATA URLS
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk rail data"""
        return {
            'naptan_rail': 'https://naptan.api.dft.gov.uk/v1/access-nodes?stopTypes=RLY',
            'station_usage': 'https://dataportal.orr.gov.uk/statistics/usage/estimates-of-station-usage/',
            'timetables': 'https://opendata.nationalrail.co.uk/',
            'network_map': 'https://www.nationalrail.co.uk/travel-information/maps/',
        }

