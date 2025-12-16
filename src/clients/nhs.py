"""
NHS Organisation Data Service (ODS) API Client
===============================================

Fetches NHS organization data from the ODS API.

API Documentation: https://digital.nhs.uk/services/organisation-data-service
No authentication required.

Usage:
    from src.clients import NHSClient
    
    client = NHSClient()
    gps = client.search_gp_practices("London")
    hospitals = client.get_hospitals()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class NHSClient(BaseAPIClient):
    """
    Client for NHS Organisation Data Service (ODS) API.
    
    No authentication required.
    """
    
    BASE_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0"
    
    # Organization role codes
    ROLES = {
        'gp_practice': 'RO76',      # GP Practice
        'hospital': 'RO197',         # NHS Trust Site
        'pharmacy': 'RO182',         # Community Pharmacy
        'dental': 'RO107',           # Dental Practice
        'optician': 'RO114',         # Opticians
        'nhs_trust': 'RO197',        # NHS Trust
        'ccg': 'RO98',               # Clinical Commissioning Group
        'icb': 'RO272',              # Integrated Care Board
    }
    
    def __init__(self, **kwargs):
        """Initialize NHS client"""
        super().__init__(
            base_url=self.BASE_URL,
            api_key=None,
            rate_limit_rpm=120,
            **kwargs
        )
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_organisation_roles()
            return True
        except Exception:
            return False
    
    # ========================================
    # ORGANISATION LOOKUPS
    # ========================================
    
    def get_organisation(self, org_code: str) -> Dict:
        """
        Get organisation by ODS code.
        
        Args:
            org_code: ODS organisation code
            
        Returns:
            Organisation details
        """
        return self.get(f"/organisations/{org_code}")
    
    def search_organisations(
        self,
        name: Optional[str] = None,
        postcode: Optional[str] = None,
        role_id: Optional[str] = None,
        status: str = "Active",
        limit: int = 1000,
        offset: int = 1
    ) -> Dict:
        """
        Search for organisations.
        
        Args:
            name: Organisation name search
            postcode: Postcode (partial match)
            role_id: Role code (e.g., RO76 for GP)
            status: Status filter (Active, Inactive)
            limit: Results per page
            offset: Starting offset (must be >= 1)
            
        Returns:
            Search results
        """
        params = {
            'Status': status,
            'Limit': limit,
            'Offset': max(1, offset)  # NHS API requires offset >= 1
        }
        
        if name:
            params['Name'] = name
        if postcode:
            params['PostCode'] = postcode
        if role_id:
            params['PrimaryRoleId'] = role_id
        
        return self.get("/organisations", params=params)
    
    def get_organisation_roles(self) -> List[Dict]:
        """Get list of all organisation role types"""
        return self.get("/roles")
    
    # ========================================
    # GP PRACTICES
    # ========================================
    
    def get_gp_practices(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Get GP practices.
        
        Args:
            postcode: Filter by postcode area
            name: Search by name
            limit: Maximum results
            
        Returns:
            List of GP practices
        """
        return self.search_organisations(
            name=name,
            postcode=postcode,
            role_id=self.ROLES['gp_practice'],
            limit=limit
        ).get('Organisations', [])
    
    def get_gp_practices_df(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Get GP practices as DataFrame.
        
        Returns:
            DataFrame with GP practice data
        """
        orgs = self.get_gp_practices(postcode=postcode, name=name, limit=limit)
        
        results = []
        for org in orgs:
            results.append({
                'org_code': org.get('OrgId'),
                'name': org.get('Name'),
                'status': org.get('Status'),
                'postcode': org.get('PostCode'),
                'last_change_date': org.get('LastChangeDate'),
                'org_record_class': org.get('OrgRecordClass'),
            })
        
        return pd.DataFrame(results)
    
    # ========================================
    # HOSPITALS
    # ========================================
    
    def get_hospitals(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Get NHS hospitals/trust sites.
        
        Args:
            postcode: Filter by postcode area
            name: Search by name
            limit: Maximum results
            
        Returns:
            List of hospitals
        """
        return self.search_organisations(
            name=name,
            postcode=postcode,
            role_id=self.ROLES['hospital'],
            limit=limit
        ).get('Organisations', [])
    
    def get_hospitals_df(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """Get hospitals as DataFrame"""
        orgs = self.get_hospitals(postcode=postcode, name=name, limit=limit)
        
        results = []
        for org in orgs:
            results.append({
                'org_code': org.get('OrgId'),
                'name': org.get('Name'),
                'status': org.get('Status'),
                'postcode': org.get('PostCode'),
                'last_change_date': org.get('LastChangeDate'),
            })
        
        return pd.DataFrame(results)
    
    # ========================================
    # PHARMACIES
    # ========================================
    
    def get_pharmacies(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """Get community pharmacies"""
        return self.search_organisations(
            name=name,
            postcode=postcode,
            role_id=self.ROLES['pharmacy'],
            limit=limit
        ).get('Organisations', [])
    
    def get_pharmacies_df(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """Get pharmacies as DataFrame"""
        orgs = self.get_pharmacies(postcode=postcode, name=name, limit=limit)
        
        results = []
        for org in orgs:
            results.append({
                'org_code': org.get('OrgId'),
                'name': org.get('Name'),
                'status': org.get('Status'),
                'postcode': org.get('PostCode'),
                'last_change_date': org.get('LastChangeDate'),
            })
        
        return pd.DataFrame(results)
    
    # ========================================
    # DENTAL PRACTICES
    # ========================================
    
    def get_dental_practices(
        self,
        postcode: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """Get dental practices"""
        return self.search_organisations(
            name=name,
            postcode=postcode,
            role_id=self.ROLES['dental'],
            limit=limit
        ).get('Organisations', [])
    
    # ========================================
    # FULL DETAILS
    # ========================================
    
    def get_organisation_full(self, org_code: str) -> Dict:
        """
        Get full organisation details including address.
        
        Args:
            org_code: ODS code
            
        Returns:
            Full organisation record with address
        """
        org = self.get_organisation(org_code)
        return org.get('Organisation', org)
    
    def get_organisations_with_addresses(
        self,
        org_codes: List[str]
    ) -> pd.DataFrame:
        """
        Get full details for multiple organisations.
        
        Args:
            org_codes: List of ODS codes
            
        Returns:
            DataFrame with full details including addresses
        """
        results = []
        
        for i, code in enumerate(org_codes):
            logger.info(f"Fetching {i+1}/{len(org_codes)}: {code}")
            try:
                org = self.get_organisation_full(code)
                
                # Extract address
                addr = org.get('GeoLoc', {}).get('Location', {})
                
                results.append({
                    'org_code': org.get('OrgId'),
                    'name': org.get('Name'),
                    'status': org.get('Status'),
                    'postcode': addr.get('PostCode'),
                    'address_line_1': addr.get('AddrLn1'),
                    'address_line_2': addr.get('AddrLn2'),
                    'address_line_3': addr.get('AddrLn3'),
                    'town': addr.get('Town'),
                    'county': addr.get('County'),
                    'country': addr.get('Country'),
                    'uprn': addr.get('UPRN'),
                    'latitude': addr.get('Latitude'),
                    'longitude': addr.get('Longitude'),
                    'open_date': org.get('Date', [{}])[0].get('Start') if org.get('Date') else None,
                })
            except APIError as e:
                logger.warning(f"Failed to fetch {code}: {e}")
        
        return pd.DataFrame(results)
    
    # ========================================
    # LONDON HEALTHCARE
    # ========================================
    
    def get_london_healthcare_df(self) -> pd.DataFrame:
        """
        Get all healthcare facilities in London.
        
        Returns:
            DataFrame with GPs, hospitals, pharmacies in London
        """
        london_postcodes = ['E', 'EC', 'N', 'NW', 'SE', 'SW', 'W', 'WC']
        
        all_facilities = []
        
        for prefix in london_postcodes:
            logger.info(f"Fetching {prefix}* healthcare facilities...")
            
            # GPs
            try:
                gps = self.get_gp_practices(postcode=prefix, limit=5000)
                for gp in gps:
                    all_facilities.append({
                        'org_code': gp.get('OrgId'),
                        'name': gp.get('Name'),
                        'type': 'GP Practice',
                        'postcode': gp.get('PostCode'),
                        'status': gp.get('Status'),
                    })
            except Exception as e:
                logger.warning(f"Failed to fetch GPs for {prefix}: {e}")
            
            # Pharmacies
            try:
                pharmacies = self.get_pharmacies(postcode=prefix, limit=5000)
                for ph in pharmacies:
                    all_facilities.append({
                        'org_code': ph.get('OrgId'),
                        'name': ph.get('Name'),
                        'type': 'Pharmacy',
                        'postcode': ph.get('PostCode'),
                        'status': ph.get('Status'),
                    })
            except Exception as e:
                logger.warning(f"Failed to fetch pharmacies for {prefix}: {e}")
        
        # Hospitals (search by name for London)
        try:
            hospitals = self.get_hospitals(name="London", limit=1000)
            for hosp in hospitals:
                all_facilities.append({
                    'org_code': hosp.get('OrgId'),
                    'name': hosp.get('Name'),
                    'type': 'Hospital',
                    'postcode': hosp.get('PostCode'),
                    'status': hosp.get('Status'),
                })
        except Exception as e:
            logger.warning(f"Failed to fetch hospitals: {e}")
        
        return pd.DataFrame(all_facilities)

