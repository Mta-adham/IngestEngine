"""
Valuation Office Agency (VOA) API Client
=========================================

Access business rates and council tax valuation data.

API Documentation: https://voaratinglists.blob.core.windows.net/

Usage:
    from src.clients import VOAClient
    
    client = VOAClient()
    properties = client.search_business_rates(postcode="SW1A")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class VOAClient(BaseAPIClient):
    """
    Client for Valuation Office Agency data.
    
    Access:
    - Business rates valuations (2017 & 2023 lists)
    - Council tax bands
    - Rateable values
    
    Data helps establish when commercial properties started operation.
    """
    
    BASE_URL = "https://voaratinglists.blob.core.windows.net"
    FIND_BUSINESS_URL = "https://www.gov.uk/find-business-rates"
    
    def __init__(self, **kwargs):
        """Initialize VOA client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=30,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required for open data"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if data is available"""
        return True
    
    # ========================================
    # BUSINESS RATES DATA
    # ========================================
    
    def get_rating_list_summary(self) -> Dict[str, str]:
        """Get available rating list data files"""
        return {
            '2023_list': 'https://voaratinglists.blob.core.windows.net/html/rli/2023/SUMMARY.csv',
            '2017_list': 'https://voaratinglists.blob.core.windows.net/html/rli/2017/SUMMARY.csv',
            '2010_list': 'https://voaratinglists.blob.core.windows.net/html/rli/2010/SUMMARY.csv',
            'documentation': 'https://www.gov.uk/guidance/how-non-domestic-property-including-plant-and-machinery-is-valued',
        }
    
    def download_rating_list(
        self,
        year: int = 2023,
        billing_authority: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Download VOA rating list data.
        
        Args:
            year: Rating list year (2023, 2017, 2010)
            billing_authority: Filter by billing authority code
            
        Returns:
            DataFrame of rated properties
        """
        # VOA provides data as CSVs by billing authority
        base_url = f"https://voaratinglists.blob.core.windows.net/html/rli/{year}/"
        
        if billing_authority:
            url = f"{base_url}{billing_authority}.csv"
            try:
                return pd.read_csv(url)
            except Exception as e:
                logger.warning(f"Could not download {url}: {e}")
                return pd.DataFrame()
        else:
            # Return info about available data
            logger.info(f"Specify billing_authority code. Data at: {base_url}")
            return pd.DataFrame()
    
    def get_billing_authority_codes(self) -> Dict[str, str]:
        """Get London billing authority codes"""
        return {
            '5030': 'City of London',
            '5060': 'Camden',
            '5090': 'Greenwich',
            '5120': 'Hackney',
            '5150': 'Hammersmith and Fulham',
            '5180': 'Islington',
            '5210': 'Kensington and Chelsea',
            '5240': 'Lambeth',
            '5270': 'Lewisham',
            '5300': 'Southwark',
            '5330': 'Tower Hamlets',
            '5360': 'Wandsworth',
            '5390': 'Westminster',
            '5420': 'Barking and Dagenham',
            '5450': 'Barnet',
            '5480': 'Bexley',
            '5510': 'Brent',
            '5540': 'Bromley',
            '5570': 'Croydon',
            '5600': 'Ealing',
            '5630': 'Enfield',
            '5660': 'Haringey',
            '5690': 'Harrow',
            '5720': 'Havering',
            '5750': 'Hillingdon',
            '5780': 'Hounslow',
            '5810': 'Kingston upon Thames',
            '5840': 'Merton',
            '5870': 'Newham',
            '5900': 'Redbridge',
            '5930': 'Richmond upon Thames',
            '5960': 'Sutton',
            '5990': 'Waltham Forest',
        }
    
    # ========================================
    # COUNCIL TAX
    # ========================================
    
    def get_council_tax_bands(self) -> Dict[str, Dict]:
        """Get council tax band value ranges (England)"""
        return {
            'A': {'min': 0, 'max': 40000, 'fraction': '6/9'},
            'B': {'min': 40001, 'max': 52000, 'fraction': '7/9'},
            'C': {'min': 52001, 'max': 68000, 'fraction': '8/9'},
            'D': {'min': 68001, 'max': 88000, 'fraction': '9/9'},
            'E': {'min': 88001, 'max': 120000, 'fraction': '11/9'},
            'F': {'min': 120001, 'max': 160000, 'fraction': '13/9'},
            'G': {'min': 160001, 'max': 320000, 'fraction': '15/9'},
            'H': {'min': 320001, 'max': None, 'fraction': '18/9'},
        }
    
    def estimate_property_value_from_band(
        self,
        band: str
    ) -> Dict[str, Any]:
        """
        Estimate 1991 property value from council tax band.
        
        Args:
            band: Council tax band (A-H)
            
        Returns:
            Estimated value range
        """
        bands = self.get_council_tax_bands()
        band_info = bands.get(band.upper())
        
        if not band_info:
            return {}
        
        # Apply rough inflation multiplier (1991 to 2024)
        # UK house prices roughly 4-5x since 1991
        multiplier = 4.5
        
        return {
            'band': band.upper(),
            'value_1991_min': band_info['min'],
            'value_1991_max': band_info['max'],
            'estimated_current_min': int(band_info['min'] * multiplier) if band_info['min'] else 0,
            'estimated_current_max': int(band_info['max'] * multiplier) if band_info['max'] else None,
        }
    
    # ========================================
    # SPECIAL CATEGORY CODES
    # ========================================
    
    def get_special_category_codes(self) -> Dict[str, str]:
        """Get VOA special category codes for property types"""
        return {
            'CS': 'Car Spaces',
            'CW': 'Car Wash',
            'EH': 'Eating House/Restaurant',
            'EX': 'Exhibition',
            'FH': 'Fish House',
            'GP': 'Garage/Petrol Station',
            'HO': 'Hotel',
            'IF': 'Industrial',
            'LH': 'Licensed House/Pub',
            'MH': 'Music Hall',
            'OF': 'Office',
            'OH': 'Office/Retail',
            'PH': 'Public House',
            'PP': 'Petrol Pump',
            'RH': 'Retail',
            'RW': 'Retail Warehouse',
            'SH': 'Shop',
            'WH': 'Warehouse',
        }
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk VOA data"""
        return {
            'rating_list_2023': 'https://voaratinglists.blob.core.windows.net/html/rli/2023/',
            'rating_list_2017': 'https://voaratinglists.blob.core.windows.net/html/rli/2017/',
            'council_tax_list': 'https://www.gov.uk/government/statistical-data-sets/council-tax-statistics',
            'find_business_rates': 'https://www.gov.uk/find-business-rates',
        }

