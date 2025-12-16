"""
UK Parliament API Client
=========================

Access parliamentary data including:
- Members (MPs, Lords)
- Bills and Legislation
- Divisions (votes)
- Constituencies

API Documentation: https://members-api.parliament.uk/

Usage:
    from src.clients import UKParliamentClient
    
    client = UKParliamentClient()
    mps = client.get_current_mps()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class UKParliamentClient(BaseAPIClient):
    """
    Client for UK Parliament APIs.
    
    Access:
    - Members API (MPs and Lords)
    - Bills API
    - Hansard (debates)
    - Constituencies
    """
    
    MEMBERS_URL = "https://members-api.parliament.uk/api"
    BILLS_URL = "https://bills-api.parliament.uk/api/v1"
    
    def __init__(self, **kwargs):
        """Initialize Parliament client."""
        super().__init__(
            base_url=self.MEMBERS_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_current_mps(take=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # MEMBERS - MPs
    # ========================================
    
    def get_current_mps(
        self,
        skip: int = 0,
        take: int = 100
    ) -> List[Dict]:
        """
        Get current MPs.
        
        Args:
            skip: Number to skip (pagination)
            take: Number to return
            
        Returns:
            List of MPs
        """
        result = self.get('/Members/Search', params={
            'House': 'Commons',
            'IsCurrentMember': 'true',
            'skip': skip,
            'take': take
        })
        return result.get('items', [])
    
    def get_current_mps_df(self, take: int = 650) -> pd.DataFrame:
        """Get current MPs as DataFrame"""
        mps = self.get_current_mps(take=take)
        
        records = []
        for mp in mps:
            value = mp.get('value', {})
            records.append({
                'id': value.get('id'),
                'name': value.get('nameDisplayAs'),
                'party': value.get('latestParty', {}).get('name'),
                'constituency': value.get('latestHouseMembership', {}).get('membershipFrom'),
                'gender': value.get('gender'),
            })
        
        return pd.DataFrame(records)
    
    def get_mp(self, member_id: int) -> Dict:
        """Get details for a specific MP"""
        return self.get(f'/Members/{member_id}')
    
    def search_members(
        self,
        name: str,
        house: str = "Commons"
    ) -> List[Dict]:
        """Search members by name"""
        result = self.get('/Members/Search', params={
            'Name': name,
            'House': house,
            'IsCurrentMember': 'true'
        })
        return result.get('items', [])
    
    # ========================================
    # MEMBERS - LORDS
    # ========================================
    
    def get_current_lords(self, take: int = 100) -> List[Dict]:
        """Get current Lords"""
        result = self.get('/Members/Search', params={
            'House': 'Lords',
            'IsCurrentMember': 'true',
            'take': take
        })
        return result.get('items', [])
    
    def get_current_lords_df(self, take: int = 800) -> pd.DataFrame:
        """Get current Lords as DataFrame"""
        lords = self.get_current_lords(take=take)
        
        records = []
        for lord in lords:
            value = lord.get('value', {})
            records.append({
                'id': value.get('id'),
                'name': value.get('nameDisplayAs'),
                'party': value.get('latestParty', {}).get('name'),
                'gender': value.get('gender'),
            })
        
        return pd.DataFrame(records)
    
    # ========================================
    # CONSTITUENCIES
    # ========================================
    
    def get_constituencies(self, skip: int = 0, take: int = 100) -> List[Dict]:
        """Get constituencies"""
        result = self.get('/Location/Constituency/Search', params={
            'skip': skip,
            'take': take
        })
        return result.get('items', [])
    
    def get_constituencies_df(self, take: int = 650) -> pd.DataFrame:
        """Get constituencies as DataFrame"""
        constituencies = self.get_constituencies(take=take)
        
        records = []
        for c in constituencies:
            value = c.get('value', {})
            records.append({
                'id': value.get('id'),
                'name': value.get('name'),
                'start_date': value.get('startDate'),
                'end_date': value.get('endDate'),
            })
        
        return pd.DataFrame(records)
    
    def get_mp_for_constituency(self, constituency: str) -> Dict:
        """Get current MP for a constituency"""
        result = self.get('/Location/Constituency/Search', params={
            'SearchText': constituency
        })
        
        items = result.get('items', [])
        if items:
            constituency_id = items[0].get('value', {}).get('id')
            # Get MP for this constituency
            members = self.search_members(constituency, house="Commons")
            for m in members:
                if m.get('value', {}).get('latestHouseMembership', {}).get('membershipFrom') == constituency:
                    return m
        return {}
    
    # ========================================
    # BILLS
    # ========================================
    
    def get_current_bills(
        self,
        session: Optional[str] = None,
        take: int = 50
    ) -> List[Dict]:
        """
        Get current bills.
        
        Args:
            session: Parliament session (e.g., "2024-25")
            take: Number to return
            
        Returns:
            List of bills
        """
        import requests
        
        params = {'take': take}
        if session:
            params['Session'] = session
        
        response = requests.get(
            f"{self.BILLS_URL}/Bills",
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json().get('items', [])
        return []
    
    def get_current_bills_df(self, take: int = 100) -> pd.DataFrame:
        """Get current bills as DataFrame"""
        bills = self.get_current_bills(take=take)
        
        records = []
        for bill in bills:
            records.append({
                'id': bill.get('billId'),
                'title': bill.get('shortTitle'),
                'long_title': bill.get('longTitle'),
                'bill_type': bill.get('billType'),
                'current_stage': bill.get('currentStage'),
                'originating_house': bill.get('originatingHouse'),
            })
        
        return pd.DataFrame(records)
    
    # ========================================
    # PARTIES
    # ========================================
    
    def get_parties(self) -> List[Dict]:
        """Get political parties"""
        result = self.get('/Parties/GetActive/Commons')
        return result.get('items', result) if isinstance(result, dict) else result
    
    def get_party_composition(self) -> pd.DataFrame:
        """Get current party composition of Commons"""
        mps = self.get_current_mps_df(take=700)
        return mps.groupby('party').size().reset_index(name='seats').sort_values('seats', ascending=False)
    
    # ========================================
    # REFERENCE
    # ========================================
    
    def get_house_types(self) -> List[str]:
        """Get types of houses"""
        return ['Commons', 'Lords']

