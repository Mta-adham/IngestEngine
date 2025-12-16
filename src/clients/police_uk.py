"""
Police.uk API Client
====================

Fetches crime data from the Police.uk API.

API Documentation: https://data.police.uk/docs/
Rate Limit: 15 requests per second (no auth required)

Usage:
    from src.clients import PoliceUKClient
    
    client = PoliceUKClient()
    crimes = client.get_crimes_at_location(51.5074, -0.1278)  # London
    street_crimes = client.get_street_level_crimes(51.5, -0.1, "2024-01")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class PoliceUKClient(BaseAPIClient):
    """
    Client for Police.uk API.
    
    No authentication required. Free to use.
    """
    
    BASE_URL = "https://data.police.uk/api"
    
    def __init__(self, **kwargs):
        """Initialize Police.uk client"""
        super().__init__(
            base_url=self.BASE_URL,
            api_key=None,  # No auth required
            rate_limit_rpm=900,  # 15 per second
            **kwargs
        )
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_forces()
            return True
        except Exception:
            return False
    
    # ========================================
    # FORCES & NEIGHBOURHOODS
    # ========================================
    
    def get_forces(self) -> List[Dict]:
        """
        Get list of all police forces.
        
        Returns:
            List of police forces with IDs
        """
        return self.get("/forces")
    
    def get_force(self, force_id: str) -> Dict:
        """Get details for a specific force"""
        return self.get(f"/forces/{force_id}")
    
    def get_neighbourhoods(self, force_id: str) -> List[Dict]:
        """
        Get neighbourhoods for a force.
        
        Args:
            force_id: Force ID (e.g., "metropolitan")
            
        Returns:
            List of neighbourhoods
        """
        return self.get(f"/{force_id}/neighbourhoods")
    
    def get_neighbourhood(self, force_id: str, neighbourhood_id: str) -> Dict:
        """Get details for a specific neighbourhood"""
        return self.get(f"/{force_id}/{neighbourhood_id}")
    
    def get_neighbourhood_boundary(self, force_id: str, neighbourhood_id: str) -> List[Dict]:
        """Get boundary polygon for a neighbourhood"""
        return self.get(f"/{force_id}/{neighbourhood_id}/boundary")
    
    def locate_neighbourhood(self, lat: float, lon: float) -> Dict:
        """
        Find the neighbourhood for a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Neighbourhood and force details
        """
        return self.get("/locate-neighbourhood", params={'q': f"{lat},{lon}"})
    
    # ========================================
    # CRIME DATA
    # ========================================
    
    def get_crime_categories(self, date: Optional[str] = None) -> List[Dict]:
        """
        Get list of crime categories.
        
        Args:
            date: Month in format "YYYY-MM" (default: latest)
            
        Returns:
            List of crime categories
        """
        params = {}
        if date:
            params['date'] = date
        return self.get("/crime-categories", params=params)
    
    def get_street_level_crimes(
        self,
        lat: float,
        lon: float,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get street-level crimes at a location.
        
        Args:
            lat: Latitude
            lon: Longitude  
            date: Month in format "YYYY-MM" (default: latest)
            
        Returns:
            List of crimes near that location
        """
        params = {'lat': lat, 'lng': lon}
        if date:
            params['date'] = date
        return self.get("/crimes-street/all-crime", params=params)
    
    def get_crimes_in_area(
        self,
        poly: List[tuple],
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get crimes within a polygon area.
        
        Args:
            poly: List of (lat, lon) tuples defining polygon
            date: Month in format "YYYY-MM"
            
        Returns:
            List of crimes in area
        """
        poly_str = ':'.join([f"{lat},{lon}" for lat, lon in poly])
        params = {'poly': poly_str}
        if date:
            params['date'] = date
        return self.get("/crimes-street/all-crime", params=params)
    
    def get_crimes_at_location(
        self,
        lat: float,
        lon: float,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get crimes at a specific location.
        
        Args:
            lat: Latitude
            lon: Longitude
            date: Month in format "YYYY-MM"
            
        Returns:
            List of crimes at location
        """
        params = {'lat': lat, 'lng': lon}
        if date:
            params['date'] = date
        return self.get("/crimes-at-location", params=params)
    
    def get_crimes_no_location(
        self,
        force_id: str,
        category: str,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get crimes with no location (force-wide).
        
        Args:
            force_id: Force ID
            category: Crime category
            date: Month in format "YYYY-MM"
            
        Returns:
            List of crimes
        """
        params = {'force': force_id, 'category': category}
        if date:
            params['date'] = date
        return self.get("/crimes-no-location", params=params)
    
    def get_crime_outcomes(
        self,
        lat: float,
        lon: float,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get crime outcomes at a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            date: Month in format "YYYY-MM"
            
        Returns:
            List of crime outcomes
        """
        params = {'lat': lat, 'lng': lon}
        if date:
            params['date'] = date
        return self.get("/outcomes-at-location", params=params)
    
    # ========================================
    # STOP AND SEARCH
    # ========================================
    
    def get_stop_and_search(
        self,
        lat: float,
        lon: float,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get stop and search data at a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            date: Month in format "YYYY-MM"
            
        Returns:
            List of stop and search incidents
        """
        params = {'lat': lat, 'lng': lon}
        if date:
            params['date'] = date
        return self.get("/stops-street", params=params)
    
    def get_stop_and_search_by_force(
        self,
        force_id: str,
        date: Optional[str] = None
    ) -> List[Dict]:
        """Get all stop and search for a force"""
        params = {'force': force_id}
        if date:
            params['date'] = date
        return self.get("/stops-force", params=params)
    
    # ========================================
    # DATA AVAILABILITY
    # ========================================
    
    def get_last_updated(self) -> Dict:
        """Get date of last data update"""
        return self.get("/crime-last-updated")
    
    def get_availability(self) -> List[Dict]:
        """
        Get data availability by force and date.
        
        Returns:
            List of available datasets
        """
        return self.get("/crimes-street-dates")
    
    # ========================================
    # BATCH OPERATIONS / DATAFRAME EXPORTS
    # ========================================
    
    def get_crimes_to_df(
        self,
        lat: float,
        lon: float,
        months: int = 12,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get crimes for multiple months as DataFrame.
        
        Args:
            lat: Latitude
            lon: Longitude
            months: Number of months to fetch (going back)
            end_date: End date in "YYYY-MM" format (default: latest)
            
        Returns:
            DataFrame with crime data
        """
        if end_date is None:
            # Get latest available date
            updated = self.get_last_updated()
            end_date = updated.get('date', datetime.now().strftime('%Y-%m'))
        
        # Parse end date and generate month list
        year, month = map(int, end_date.split('-'))
        current = datetime(year, month, 1)
        
        all_crimes = []
        
        for i in range(months):
            date_str = current.strftime('%Y-%m')
            logger.info(f"Fetching crimes for {date_str}...")
            
            try:
                crimes = self.get_street_level_crimes(lat, lon, date_str)
                for crime in crimes:
                    all_crimes.append({
                        'crime_id': crime.get('id'),
                        'category': crime.get('category'),
                        'month': crime.get('month'),
                        'latitude': crime.get('location', {}).get('latitude'),
                        'longitude': crime.get('location', {}).get('longitude'),
                        'street_name': crime.get('location', {}).get('street', {}).get('name'),
                        'outcome_status': crime.get('outcome_status', {}).get('category') if crime.get('outcome_status') else None,
                        'context': crime.get('context'),
                        'persistent_id': crime.get('persistent_id'),
                        'location_type': crime.get('location_type'),
                        'location_subtype': crime.get('location_subtype'),
                    })
            except APIError as e:
                logger.warning(f"Failed to fetch {date_str}: {e}")
            
            # Go back one month
            current = current - timedelta(days=32)
            current = current.replace(day=1)
        
        return pd.DataFrame(all_crimes)
    
    def get_crime_summary_by_category(
        self,
        lat: float,
        lon: float,
        date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get crime counts by category for a location.
        
        Returns:
            DataFrame with category and count
        """
        crimes = self.get_street_level_crimes(lat, lon, date)
        
        # Count by category
        categories = {}
        for crime in crimes:
            cat = crime.get('category', 'unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        return pd.DataFrame([
            {'category': cat, 'count': count}
            for cat, count in sorted(categories.items(), key=lambda x: -x[1])
        ])
    
    def get_crimes_in_postcode(
        self,
        postcode: str,
        date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get crimes in a postcode area.
        
        Args:
            postcode: UK postcode
            date: Month in format "YYYY-MM"
            
        Returns:
            DataFrame with crimes
            
        Note: Requires geocoding postcode first (use ONS/OS data)
        """
        # For now, this is a placeholder - would need postcode->coords lookup
        raise NotImplementedError(
            "Postcode lookup requires geocoding. Use get_crimes_to_df with lat/lon."
        )
    
    def get_forces_df(self) -> pd.DataFrame:
        """
        Get all police forces as DataFrame.
        
        Returns:
            DataFrame with force details
        """
        forces = self.get_forces()
        return pd.DataFrame(forces)
    
    def get_london_crime_hotspots(
        self,
        grid_size: float = 0.01,
        date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get crime data across London grid.
        
        Args:
            grid_size: Grid cell size in degrees
            date: Month in format "YYYY-MM"
            
        Returns:
            DataFrame with crime counts per grid cell
        """
        # London bounding box
        min_lat, max_lat = 51.28, 51.69
        min_lon, max_lon = -0.51, 0.33
        
        results = []
        lat = min_lat
        
        while lat < max_lat:
            lon = min_lon
            while lon < max_lon:
                try:
                    crimes = self.get_street_level_crimes(lat, lon, date)
                    results.append({
                        'latitude': lat,
                        'longitude': lon,
                        'crime_count': len(crimes),
                    })
                except APIError:
                    pass
                lon += grid_size
            lat += grid_size
        
        return pd.DataFrame(results)

