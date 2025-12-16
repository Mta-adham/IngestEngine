"""
DVLA Vehicle Data Client
=========================

Access vehicle registration and MOT data.

API Documentation: https://developer-portal.driver-vehicle-licensing.api.gov.uk/

Note: Requires API key registration

Usage:
    from src.clients import DVLAClient
    
    client = DVLAClient(api_key="your-key")
    vehicle = client.get_vehicle("AB12CDE")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class DVLAClient(BaseAPIClient):
    """
    Client for DVLA Vehicle Enquiry API.
    
    Access:
    - Vehicle registration data
    - MOT history (via MOT API)
    """
    
    BASE_URL = "https://driver-vehicle-licensing.api.gov.uk"
    MOT_BASE_URL = "https://beta.check-mot.service.gov.uk"
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        mot_api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize DVLA client.
        
        Args:
            api_key: DVLA Vehicle Enquiry API key
            mot_api_key: MOT History API key
        """
        self.api_key = api_key or os.environ.get('DVLA_API_KEY', '')
        self.mot_api_key = mot_api_key or os.environ.get('MOT_API_KEY', '')
        
        if not self.api_key:
            logger.warning("No DVLA API key. Set DVLA_API_KEY env var.")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=self.api_key,
            rate_limit_rpm=100,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup API key header"""
        if self.api_key:
            self.session.headers['x-api-key'] = self.api_key
        self.session.headers['Content-Type'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        # Can't easily health check without making a real request
        return bool(self.api_key)
    
    # ========================================
    # VEHICLE ENQUIRY
    # ========================================
    
    def get_vehicle(self, registration: str) -> Dict:
        """
        Get vehicle details by registration number.
        
        Args:
            registration: Vehicle registration (e.g., 'AB12CDE')
            
        Returns:
            Vehicle details
        """
        registration = registration.upper().replace(' ', '')
        
        return self.post(
            '/vehicle-enquiry/v1/vehicles',
            json={'registrationNumber': registration}
        )
    
    def get_vehicle_summary(self, registration: str) -> Dict:
        """Get simplified vehicle summary"""
        vehicle = self.get_vehicle(registration)
        
        return {
            'registration': vehicle.get('registrationNumber'),
            'make': vehicle.get('make'),
            'colour': vehicle.get('colour'),
            'fuel_type': vehicle.get('fuelType'),
            'year_of_manufacture': vehicle.get('yearOfManufacture'),
            'engine_capacity': vehicle.get('engineCapacity'),
            'co2_emissions': vehicle.get('co2Emissions'),
            'tax_status': vehicle.get('taxStatus'),
            'tax_due_date': vehicle.get('taxDueDate'),
            'mot_status': vehicle.get('motStatus'),
            'mot_expiry_date': vehicle.get('motExpiryDate'),
            'type_approval': vehicle.get('typeApproval'),
            'euro_status': vehicle.get('euroStatus'),
        }
    
    # ========================================
    # MOT HISTORY (separate API)
    # ========================================
    
    def get_mot_history(self, registration: str) -> List[Dict]:
        """
        Get MOT history for a vehicle.
        
        Args:
            registration: Vehicle registration
            
        Returns:
            List of MOT test records
        """
        if not self.mot_api_key:
            raise APIError("MOT API key required. Set MOT_API_KEY env var.")
        
        import requests
        
        response = requests.get(
            f"{self.MOT_BASE_URL}/trade/vehicles/mot-tests",
            params={'registration': registration.upper().replace(' ', '')},
            headers={
                'Accept': 'application/json+v6',
                'x-api-key': self.mot_api_key
            },
            timeout=30
        )
        
        if response.status_code != 200:
            raise APIError(f"MOT API error: {response.text}")
        
        return response.json()
    
    def get_mot_history_df(self, registration: str) -> pd.DataFrame:
        """Get MOT history as DataFrame"""
        results = self.get_mot_history(registration)
        
        if not results:
            return pd.DataFrame()
        
        # Flatten MOT tests
        tests = []
        for vehicle in results:
            for test in vehicle.get('motTests', []):
                test['registration'] = vehicle.get('registration')
                test['make'] = vehicle.get('make')
                test['model'] = vehicle.get('model')
                tests.append(test)
        
        return pd.DataFrame(tests)
    
    # ========================================
    # BULK OPERATIONS
    # ========================================
    
    def get_vehicles_batch(
        self,
        registrations: List[str]
    ) -> pd.DataFrame:
        """
        Get data for multiple vehicles.
        
        Args:
            registrations: List of registration numbers
            
        Returns:
            DataFrame with vehicle data
        """
        vehicles = []
        
        for reg in registrations:
            try:
                vehicle = self.get_vehicle_summary(reg)
                vehicles.append(vehicle)
            except Exception as e:
                logger.warning(f"Error fetching {reg}: {e}")
                vehicles.append({'registration': reg, 'error': str(e)})
        
        return pd.DataFrame(vehicles)
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_fuel_types(self) -> List[str]:
        """Get list of fuel types"""
        return [
            'PETROL',
            'DIESEL',
            'ELECTRIC',
            'HYBRID ELECTRIC',
            'GAS',
            'FUEL CELLS',
            'OTHER'
        ]
    
    def get_tax_status_types(self) -> List[str]:
        """Get list of tax status values"""
        return [
            'Taxed',
            'Untaxed',
            'SORN',
            'Not Taxed for on Road Use'
        ]

