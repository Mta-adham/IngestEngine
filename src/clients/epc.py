"""
EPC (Energy Performance Certificate) API Client
================================================

Fetches EPC data from the Open Data Communities API.

API Documentation: https://epc.opendatacommunities.org/docs/api
Rate Limit: Be reasonable - no official limit but be respectful

Usage:
    from src.clients import EPCClient
    
    client = EPCClient(email="your@email.com", api_key="your-key")
    epcs = client.search_domestic("SW1A 1AA")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError
from src.config import EPC_EMAIL, EPC_API_KEY

logger = logging.getLogger(__name__)


class EPCClient(BaseAPIClient):
    """
    Client for EPC Open Data Communities API.
    
    Register for API key at: https://epc.opendatacommunities.org/
    """
    
    BASE_URL = "https://epc.opendatacommunities.org/api/v1"
    
    def __init__(
        self, 
        email: Optional[str] = None,
        api_key: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize EPC client.
        
        Args:
            email: Registered email (or uses config/env var EPC_EMAIL)
            api_key: EPC API key (or uses config/env var EPC_API_KEY)
        """
        self.email = email or EPC_EMAIL
        api_key = api_key or EPC_API_KEY
        
        if not api_key:
            logger.warning("No API key provided. Set EPC_API_KEY in config or env var.")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=api_key,
            rate_limit_rpm=60,  # Conservative rate limit
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup Basic Auth"""
        if self.api_key and self.email:
            self.session.auth = (self.email, self.api_key)
        elif self.api_key:
            self.session.headers['Authorization'] = f'Basic {self.api_key}'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.search_domestic(postcode="SW1A 1AA", size=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # DOMESTIC EPCs
    # ========================================
    
    def search_domestic(
        self,
        postcode: Optional[str] = None,
        local_authority: Optional[str] = None,
        constituency: Optional[str] = None,
        address: Optional[str] = None,
        energy_band: Optional[str] = None,
        from_month: Optional[str] = None,
        to_month: Optional[str] = None,
        size: int = 100,
        from_index: int = 0
    ) -> Dict[str, Any]:
        """
        Search domestic EPCs.
        
        Args:
            postcode: Postcode to search
            local_authority: Local authority code
            constituency: Constituency code
            address: Address search
            energy_band: Filter by band (A-G)
            from_month: Start month (YYYY-MM)
            to_month: End month (YYYY-MM)
            size: Results per page (max 5000)
            from_index: Starting index
            
        Returns:
            Search results with EPCs
        """
        params = {
            'size': min(size, 5000),
            'from': from_index
        }
        
        if postcode:
            params['postcode'] = postcode.replace(' ', '')
        if local_authority:
            params['local-authority'] = local_authority
        if constituency:
            params['constituency'] = constituency
        if address:
            params['address'] = address
        if energy_band:
            params['energy-band'] = energy_band
        if from_month:
            params['from-month'] = from_month
        if to_month:
            params['to-month'] = to_month
        
        return self.get("/domestic/search", params=params)
    
    def get_domestic_certificate(self, lmk_key: str) -> Dict:
        """
        Get a specific domestic EPC certificate.
        
        Args:
            lmk_key: Certificate LMK key
            
        Returns:
            Full certificate data
        """
        return self.get(f"/domestic/certificate/{lmk_key}")
    
    def get_domestic_recommendations(self, lmk_key: str) -> Dict:
        """Get improvement recommendations for a certificate"""
        return self.get(f"/domestic/recommendations/{lmk_key}")
    
    # ========================================
    # NON-DOMESTIC EPCs
    # ========================================
    
    def search_non_domestic(
        self,
        postcode: Optional[str] = None,
        local_authority: Optional[str] = None,
        address: Optional[str] = None,
        energy_band: Optional[str] = None,
        from_month: Optional[str] = None,
        to_month: Optional[str] = None,
        size: int = 100,
        from_index: int = 0
    ) -> Dict[str, Any]:
        """
        Search non-domestic (commercial) EPCs.
        
        Args:
            Same as search_domestic
            
        Returns:
            Search results with commercial EPCs
        """
        params = {
            'size': min(size, 5000),
            'from': from_index
        }
        
        if postcode:
            params['postcode'] = postcode.replace(' ', '')
        if local_authority:
            params['local-authority'] = local_authority
        if address:
            params['address'] = address
        if energy_band:
            params['energy-band'] = energy_band
        if from_month:
            params['from-month'] = from_month
        if to_month:
            params['to-month'] = to_month
        
        return self.get("/non-domestic/search", params=params)
    
    def get_non_domestic_certificate(self, lmk_key: str) -> Dict:
        """Get a specific non-domestic EPC certificate"""
        return self.get(f"/non-domestic/certificate/{lmk_key}")
    
    # ========================================
    # DISPLAY ENERGY CERTIFICATES (DECs)
    # ========================================
    
    def search_dec(
        self,
        postcode: Optional[str] = None,
        local_authority: Optional[str] = None,
        address: Optional[str] = None,
        size: int = 100,
        from_index: int = 0
    ) -> Dict[str, Any]:
        """
        Search Display Energy Certificates (public buildings).
        
        Returns:
            Search results with DECs
        """
        params = {
            'size': min(size, 5000),
            'from': from_index
        }
        
        if postcode:
            params['postcode'] = postcode.replace(' ', '')
        if local_authority:
            params['local-authority'] = local_authority
        if address:
            params['address'] = address
        
        return self.get("/display/search", params=params)
    
    # ========================================
    # BATCH OPERATIONS / DATAFRAME EXPORTS
    # ========================================
    
    def get_domestic_by_postcode_df(
        self,
        postcode: str,
        max_results: int = 1000
    ) -> pd.DataFrame:
        """
        Get all domestic EPCs for a postcode as DataFrame.
        
        Args:
            postcode: UK postcode
            max_results: Maximum results to return
            
        Returns:
            DataFrame with EPC data
        """
        all_results = []
        from_index = 0
        
        while len(all_results) < max_results:
            response = self.search_domestic(
                postcode=postcode,
                size=min(5000, max_results - len(all_results)),
                from_index=from_index
            )
            
            rows = response.get('rows', [])
            if not rows:
                break
            
            all_results.extend(rows)
            from_index += len(rows)
            
            # Check if more results available
            column_names = response.get('column-names', [])
            if len(rows) < 5000:
                break
        
        if not all_results:
            return pd.DataFrame()
        
        # Get column names from response
        column_names = response.get('column-names', [])
        
        # Convert to DataFrame
        if column_names:
            df = pd.DataFrame(all_results, columns=column_names)
        else:
            df = pd.DataFrame(all_results)
        
        return df
    
    def get_domestic_by_local_authority_df(
        self,
        local_authority: str,
        max_results: int = 10000
    ) -> pd.DataFrame:
        """
        Get domestic EPCs for a local authority.
        
        Args:
            local_authority: Local authority code (e.g., "E09000001")
            max_results: Maximum results
            
        Returns:
            DataFrame with EPC data
        """
        all_results = []
        from_index = 0
        
        while len(all_results) < max_results:
            response = self.search_domestic(
                local_authority=local_authority,
                size=min(5000, max_results - len(all_results)),
                from_index=from_index
            )
            
            rows = response.get('rows', [])
            if not rows:
                break
            
            all_results.extend(rows)
            from_index += len(rows)
            
            logger.info(f"Fetched {len(all_results)} EPCs...")
            
            if len(rows) < 5000:
                break
        
        if not all_results:
            return pd.DataFrame()
        
        column_names = response.get('column-names', [])
        if column_names:
            return pd.DataFrame(all_results, columns=column_names)
        return pd.DataFrame(all_results)
    
    def get_construction_age_summary(
        self,
        postcode: str
    ) -> pd.DataFrame:
        """
        Get construction age summary for a postcode.
        
        Returns:
            DataFrame with construction age bands and counts
        """
        df = self.get_domestic_by_postcode_df(postcode)
        
        if df.empty:
            return pd.DataFrame()
        
        # Find construction age column
        age_col = None
        for col in df.columns:
            if 'construction' in col.lower() and 'age' in col.lower():
                age_col = col
                break
        
        if not age_col:
            logger.warning("Construction age column not found")
            return pd.DataFrame()
        
        # Group by construction age
        summary = df.groupby(age_col).size().reset_index(name='count')
        summary = summary.sort_values('count', ascending=False)
        
        return summary
    
    def get_energy_efficiency_summary(
        self,
        postcode: str
    ) -> pd.DataFrame:
        """
        Get energy efficiency summary for a postcode.
        
        Returns:
            DataFrame with energy bands and counts
        """
        df = self.get_domestic_by_postcode_df(postcode)
        
        if df.empty:
            return pd.DataFrame()
        
        # Find energy rating column
        rating_col = None
        for col in df.columns:
            if 'current-energy-rating' in col.lower() or 'energy_rating' in col.lower():
                rating_col = col
                break
        
        if not rating_col:
            rating_col = 'current-energy-rating'
        
        if rating_col not in df.columns:
            return pd.DataFrame()
        
        summary = df.groupby(rating_col).size().reset_index(name='count')
        summary = summary.sort_values(rating_col)
        
        return summary
    
    def extract_opening_dates(
        self,
        postcode: str
    ) -> pd.DataFrame:
        """
        Extract construction dates/ages for buildings in a postcode.
        
        Returns:
            DataFrame with address and construction info
        """
        df = self.get_domestic_by_postcode_df(postcode)
        
        if df.empty:
            return pd.DataFrame()
        
        # Select relevant columns
        output_cols = []
        col_mapping = {
            'address': ['address', 'address1', 'ADDRESS'],
            'postcode': ['postcode', 'POSTCODE'],
            'construction_age': ['construction-age-band', 'CONSTRUCTION_AGE_BAND'],
            'inspection_date': ['inspection-date', 'INSPECTION_DATE'],
            'lodgement_date': ['lodgement-date', 'LODGEMENT_DATE'],
            'uprn': ['uprn', 'UPRN'],
            'floor_area': ['total-floor-area', 'TOTAL_FLOOR_AREA'],
            'property_type': ['property-type', 'PROPERTY_TYPE'],
        }
        
        result = pd.DataFrame()
        for output_col, candidates in col_mapping.items():
            for cand in candidates:
                if cand in df.columns:
                    result[output_col] = df[cand]
                    break
        
        return result

