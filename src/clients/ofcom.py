"""
Ofcom API Client
=================

Access telecommunications and broadband data.

API Documentation: https://www.ofcom.org.uk/research-and-data/

Usage:
    from src.clients import OfcomClient
    
    client = OfcomClient()
    coverage = client.get_broadband_coverage("SW1A 1AA")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class OfcomClient(BaseAPIClient):
    """
    Client for Ofcom telecommunications data.
    
    Access:
    - Broadband coverage
    - Mobile coverage
    - Connected Nations data
    """
    
    BASE_URL = "https://api.ofcom.org.uk"
    COVERAGE_URL = "https://checker.ofcom.org.uk/api"
    
    def __init__(self, **kwargs):
        """Initialize Ofcom client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=30,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required for public data"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if data is available"""
        return True
    
    # ========================================
    # BROADBAND COVERAGE
    # ========================================
    
    def get_broadband_coverage(self, postcode: str) -> Dict:
        """
        Get broadband coverage for a postcode.
        
        Args:
            postcode: UK postcode
            
        Returns:
            Broadband availability data
        """
        import requests
        
        # This uses the coverage checker API
        response = requests.get(
            f"{self.COVERAGE_URL}/broadband/availability",
            params={'postcode': postcode.replace(' ', '')},
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    def get_mobile_coverage(self, postcode: str) -> Dict:
        """
        Get mobile coverage for a postcode.
        
        Args:
            postcode: UK postcode
            
        Returns:
            Mobile coverage data by operator
        """
        import requests
        
        response = requests.get(
            f"{self.COVERAGE_URL}/mobile/availability",
            params={'postcode': postcode.replace(' ', '')},
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    # ========================================
    # CONNECTED NATIONS DATA
    # ========================================
    
    def get_broadband_speed_tiers(self) -> List[Dict]:
        """Get broadband speed tier definitions"""
        return [
            {'tier': 'USO', 'speed_mbps': 10, 'description': 'Universal Service Obligation minimum'},
            {'tier': 'Superfast', 'speed_mbps': 30, 'description': 'Superfast broadband'},
            {'tier': 'Ultrafast', 'speed_mbps': 100, 'description': 'Ultrafast broadband'},
            {'tier': 'Gigabit', 'speed_mbps': 1000, 'description': 'Gigabit-capable'},
        ]
    
    def get_mobile_operators(self) -> List[Dict]:
        """Get UK mobile network operators"""
        return [
            {'name': 'EE', 'parent': 'BT'},
            {'name': 'Three', 'parent': 'CK Hutchison'},
            {'name': 'O2', 'parent': 'Virgin Media O2'},
            {'name': 'Vodafone', 'parent': 'Vodafone Group'},
        ]
    
    # ========================================
    # OPEN DATA DOWNLOADS
    # ========================================
    
    def get_connected_nations_data_urls(self) -> Dict[str, str]:
        """Get URLs for Connected Nations report data"""
        return {
            'fixed_broadband': 'https://www.ofcom.org.uk/__data/assets/file/0015/239262/202305_fixed_pc_coverage_r03.csv',
            'mobile_coverage': 'https://www.ofcom.org.uk/__data/assets/file/0021/239268/202305_mobile_laua_coverage_r02.csv',
            'connected_nations_report': 'https://www.ofcom.org.uk/research-and-data/multi-sector-research/infrastructure-research/connected-nations',
        }
    
    def get_broadband_coverage_by_la(self) -> pd.DataFrame:
        """
        Get broadband coverage statistics by Local Authority.
        
        Downloads from Ofcom open data.
        """
        url = "https://www.ofcom.org.uk/__data/assets/file/0015/239262/202305_fixed_pc_coverage_r03.csv"
        
        try:
            df = pd.read_csv(url)
            return df
        except Exception as e:
            logger.warning(f"Could not fetch broadband data: {e}")
            return pd.DataFrame()
    
    # ========================================
    # SPECTRUM DATA
    # ========================================
    
    def get_spectrum_bands(self) -> List[Dict]:
        """Get UK mobile spectrum bands"""
        return [
            {'band': '700 MHz', 'use': '5G/4G', 'operators': ['EE', 'Three', 'O2', 'Vodafone']},
            {'band': '800 MHz', 'use': '4G', 'operators': ['EE', 'Three', 'O2', 'Vodafone']},
            {'band': '900 MHz', 'use': '2G/4G', 'operators': ['O2', 'Vodafone']},
            {'band': '1400 MHz', 'use': '4G SDL', 'operators': ['EE', 'Three']},
            {'band': '1800 MHz', 'use': '4G', 'operators': ['EE', 'Three', 'O2', 'Vodafone']},
            {'band': '2100 MHz', 'use': '3G', 'operators': ['EE', 'Three', 'O2', 'Vodafone']},
            {'band': '2300 MHz', 'use': '4G', 'operators': ['O2']},
            {'band': '2600 MHz', 'use': '4G', 'operators': ['EE', 'O2', 'Vodafone']},
            {'band': '3.4-3.8 GHz', 'use': '5G', 'operators': ['EE', 'Three', 'O2', 'Vodafone']},
        ]

