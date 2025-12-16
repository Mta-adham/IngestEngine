"""
Electoral Commission API Client
================================

Access UK electoral and political party data.

API Documentation: https://www.electoralcommission.org.uk/

Usage:
    from src.clients import ElectoralCommissionClient
    
    client = ElectoralCommissionClient()
    parties = client.get_registered_parties()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class ElectoralCommissionClient(BaseAPIClient):
    """
    Client for Electoral Commission data.
    
    Access:
    - Registered political parties
    - Electoral results
    - Campaign spending
    - Donations data
    """
    
    BASE_URL = "https://search.electoralcommission.org.uk/api/search"
    
    def __init__(self, **kwargs):
        """Initialize Electoral Commission client."""
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
        try:
            self.get_registered_parties(rows=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # POLITICAL PARTIES
    # ========================================
    
    def get_registered_parties(
        self,
        start: int = 0,
        rows: int = 100,
        party_status: str = "Registered"
    ) -> List[Dict]:
        """
        Get registered political parties.
        
        Args:
            start: Start index
            rows: Number to return
            party_status: Registered, Deregistered, etc.
            
        Returns:
            List of parties
        """
        params = {
            'query': '*',
            'start': start,
            'rows': rows,
            'et': 'pp',  # Entity type: political party
            'register': 'gb',  # Great Britain
            'regStatus': party_status,
        }
        
        result = self.get('/Registrations', params=params)
        return result.get('Result', [])
    
    def get_registered_parties_df(self, rows: int = 500) -> pd.DataFrame:
        """Get registered parties as DataFrame"""
        parties = self.get_registered_parties(rows=rows)
        return pd.DataFrame(parties)
    
    def search_parties(self, name: str) -> List[Dict]:
        """Search parties by name"""
        params = {
            'query': name,
            'et': 'pp',
            'register': 'gb',
        }
        
        result = self.get('/Registrations', params=params)
        return result.get('Result', [])
    
    # ========================================
    # DONATIONS
    # ========================================
    
    def get_donations(
        self,
        start: int = 0,
        rows: int = 100,
        party_name: Optional[str] = None,
        min_value: Optional[int] = None,
        donor_status: Optional[str] = None
    ) -> List[Dict]:
        """
        Get political donations.
        
        Args:
            start: Start index
            rows: Number to return
            party_name: Filter by party
            min_value: Minimum donation value
            donor_status: Individual, Company, etc.
            
        Returns:
            List of donations
        """
        params = {
            'query': party_name or '*',
            'start': start,
            'rows': rows,
            'et': 'donation',
        }
        
        if min_value:
            params['value'] = f'>={min_value}'
        if donor_status:
            params['donorStatus'] = donor_status
        
        result = self.get('/Donations', params=params)
        return result.get('Result', [])
    
    def get_donations_df(
        self,
        rows: int = 500,
        party_name: Optional[str] = None
    ) -> pd.DataFrame:
        """Get donations as DataFrame"""
        donations = self.get_donations(rows=rows, party_name=party_name)
        return pd.DataFrame(donations)
    
    def get_large_donations(self, min_value: int = 50000) -> pd.DataFrame:
        """Get large donations over threshold"""
        donations = self.get_donations(rows=500, min_value=min_value)
        return pd.DataFrame(donations)
    
    # ========================================
    # SPENDING
    # ========================================
    
    def get_campaign_spending(
        self,
        start: int = 0,
        rows: int = 100,
        party_name: Optional[str] = None
    ) -> List[Dict]:
        """
        Get campaign spending returns.
        
        Args:
            start: Start index
            rows: Number to return
            party_name: Filter by party
            
        Returns:
            List of spending returns
        """
        params = {
            'query': party_name or '*',
            'start': start,
            'rows': rows,
            'et': 'spending',
        }
        
        result = self.get('/Spending', params=params)
        return result.get('Result', [])
    
    # ========================================
    # LOANS
    # ========================================
    
    def get_loans(
        self,
        start: int = 0,
        rows: int = 100,
        party_name: Optional[str] = None
    ) -> List[Dict]:
        """
        Get loans to political parties.
        
        Args:
            start: Start index
            rows: Number to return
            party_name: Filter by party
            
        Returns:
            List of loans
        """
        params = {
            'query': party_name or '*',
            'start': start,
            'rows': rows,
            'et': 'loan',
        }
        
        result = self.get('/Loans', params=params)
        return result.get('Result', [])
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_donor_statuses(self) -> List[str]:
        """Get types of donors"""
        return [
            'Individual',
            'Company',
            'Trade Union',
            'Unincorporated Association',
            'Limited Liability Partnership',
            'Friendly Society',
            'Impermissible Donor',
        ]
    
    def get_party_registers(self) -> List[Dict]:
        """Get available party registers"""
        return [
            {'code': 'gb', 'name': 'Great Britain'},
            {'code': 'ni', 'name': 'Northern Ireland'},
        ]
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk electoral data"""
        return {
            'donations': 'https://search.electoralcommission.org.uk/api/csv/Donations',
            'loans': 'https://search.electoralcommission.org.uk/api/csv/Loans',
            'spending': 'https://search.electoralcommission.org.uk/api/csv/Spending',
            'parties': 'https://search.electoralcommission.org.uk/api/csv/Registrations?et=pp',
            'results': 'https://www.electoralcommission.org.uk/who-we-are-and-what-we-do/elections-and-referendums/past-elections-and-referendums',
        }

