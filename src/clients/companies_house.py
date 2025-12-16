"""
Companies House API Client
==========================

Fetches company data from the Companies House API.

API Documentation: https://developer.company-information.service.gov.uk/
Rate Limit: 600 requests per 5 minutes (120/min)

Usage:
    from src.clients import CompaniesHouseClient
    
    client = CompaniesHouseClient(api_key="your-api-key")
    company = client.get_company("12345678")
    officers = client.get_officers("12345678")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime
import logging
import os

from src.clients.base_client import BaseAPIClient, APIError
from src.config import COMPANIES_HOUSE_API_KEY

logger = logging.getLogger(__name__)


class CompaniesHouseClient(BaseAPIClient):
    """
    Client for Companies House REST API.
    
    Get your API key from: https://developer.company-information.service.gov.uk/
    """
    
    BASE_URL = "https://api.company-information.service.gov.uk"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize Companies House client.
        
        Args:
            api_key: Companies House API key (or uses config/env var COMPANIES_HOUSE_API_KEY)
        """
        api_key = api_key or COMPANIES_HOUSE_API_KEY
        
        if not api_key:
            logger.warning("No API key provided. Set COMPANIES_HOUSE_API_KEY in config or env var")
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=api_key,
            rate_limit_rpm=120,  # 600 per 5 min = 120 per min
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup Basic Auth with API key"""
        if self.api_key:
            # Companies House uses API key as username with empty password
            self.session.auth = (self.api_key, '')
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            # Search for a known company
            self.search_companies("test", items_per_page=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # COMPANY PROFILE
    # ========================================
    
    def get_company(self, company_number: str) -> Dict[str, Any]:
        """
        Get company profile by company number.
        
        Args:
            company_number: 8-character company number (e.g., "00000006")
            
        Returns:
            Company profile data including:
            - company_name
            - company_number
            - company_status
            - date_of_creation
            - registered_office_address
            - sic_codes
            - type
        """
        company_number = company_number.zfill(8)  # Pad to 8 chars
        return self.get(f"/company/{company_number}")
    
    def search_companies(
        self, 
        query: str, 
        items_per_page: int = 20,
        start_index: int = 0
    ) -> Dict[str, Any]:
        """
        Search for companies by name.
        
        Args:
            query: Search query (company name)
            items_per_page: Results per page (max 100)
            start_index: Starting index for pagination
            
        Returns:
            Search results with company matches
        """
        return self.get("/search/companies", params={
            'q': query,
            'items_per_page': min(items_per_page, 100),
            'start_index': start_index
        })
    
    def search_companies_by_postcode(self, postcode: str) -> List[Dict]:
        """
        Find companies at a specific postcode.
        
        Args:
            postcode: UK postcode
            
        Returns:
            List of companies at that postcode
        """
        # Search using advanced search (by registered office)
        postcode_clean = postcode.upper().replace(' ', '')
        results = self.get("/advanced-search/companies", params={
            'registered_office_address': postcode_clean,
            'size': 100
        })
        return results.get('items', [])
    
    # ========================================
    # OFFICERS
    # ========================================
    
    def get_officers(
        self, 
        company_number: str,
        items_per_page: int = 35,
        start_index: int = 0
    ) -> Dict[str, Any]:
        """
        Get officers (directors, secretaries) for a company.
        
        Args:
            company_number: Company number
            items_per_page: Results per page
            start_index: Starting index
            
        Returns:
            List of officers with:
            - name
            - officer_role
            - appointed_on
            - resigned_on (if applicable)
        """
        company_number = company_number.zfill(8)
        return self.get(f"/company/{company_number}/officers", params={
            'items_per_page': items_per_page,
            'start_index': start_index
        })
    
    # ========================================
    # FILING HISTORY
    # ========================================
    
    def get_filing_history(
        self, 
        company_number: str,
        category: Optional[str] = None,
        items_per_page: int = 25,
        start_index: int = 0
    ) -> Dict[str, Any]:
        """
        Get filing history for a company.
        
        Args:
            company_number: Company number
            category: Filter by category (accounts, confirmation-statement, etc.)
            items_per_page: Results per page
            start_index: Starting index
            
        Returns:
            List of filings with dates and types
        """
        company_number = company_number.zfill(8)
        params = {
            'items_per_page': items_per_page,
            'start_index': start_index
        }
        if category:
            params['category'] = category
        return self.get(f"/company/{company_number}/filing-history", params=params)
    
    # ========================================
    # REGISTERED OFFICE ADDRESS
    # ========================================
    
    def get_registered_office(self, company_number: str) -> Dict[str, Any]:
        """
        Get registered office address.
        
        Args:
            company_number: Company number
            
        Returns:
            Address details
        """
        company_number = company_number.zfill(8)
        return self.get(f"/company/{company_number}/registered-office-address")
    
    # ========================================
    # CHARGES (MORTGAGES)
    # ========================================
    
    def get_charges(self, company_number: str) -> Dict[str, Any]:
        """
        Get charges/mortgages for a company.
        
        Args:
            company_number: Company number
            
        Returns:
            List of charges
        """
        company_number = company_number.zfill(8)
        return self.get(f"/company/{company_number}/charges")
    
    # ========================================
    # INSOLVENCY
    # ========================================
    
    def get_insolvency(self, company_number: str) -> Dict[str, Any]:
        """
        Get insolvency information.
        
        Args:
            company_number: Company number
            
        Returns:
            Insolvency details if any
        """
        company_number = company_number.zfill(8)
        return self.get(f"/company/{company_number}/insolvency")
    
    # ========================================
    # PERSONS WITH SIGNIFICANT CONTROL (PSC)
    # ========================================
    
    def get_pscs(self, company_number: str) -> Dict[str, Any]:
        """
        Get persons with significant control.
        
        Args:
            company_number: Company number
            
        Returns:
            List of PSCs (25%+ shareholders)
        """
        company_number = company_number.zfill(8)
        return self.get(f"/company/{company_number}/persons-with-significant-control")
    
    # ========================================
    # BATCH OPERATIONS
    # ========================================
    
    def get_companies_batch(
        self, 
        company_numbers: List[str],
        include_officers: bool = False,
        include_filing: bool = False
    ) -> pd.DataFrame:
        """
        Fetch multiple companies and return as DataFrame.
        
        Args:
            company_numbers: List of company numbers
            include_officers: Include officer data
            include_filing: Include recent filing
            
        Returns:
            DataFrame with company data
        """
        results = []
        total = len(company_numbers)
        
        for i, cn in enumerate(company_numbers, 1):
            try:
                logger.info(f"Fetching company {i}/{total}: {cn}")
                company = self.get_company(cn)
                
                record = {
                    'company_number': company.get('company_number'),
                    'company_name': company.get('company_name'),
                    'company_status': company.get('company_status'),
                    'incorporation_date': company.get('date_of_creation'),
                    'dissolution_date': company.get('date_of_cessation'),
                    'company_type': company.get('type'),
                    'sic_codes': ','.join(company.get('sic_codes', [])),
                    'postcode': company.get('registered_office_address', {}).get('postal_code'),
                    'address_line_1': company.get('registered_office_address', {}).get('address_line_1'),
                    'locality': company.get('registered_office_address', {}).get('locality'),
                    'country': company.get('registered_office_address', {}).get('country'),
                }
                
                if include_officers:
                    officers = self.get_officers(cn)
                    record['officers_count'] = officers.get('total_results', 0)
                    active_officers = [o for o in officers.get('items', []) 
                                      if not o.get('resigned_on')]
                    record['active_officers'] = len(active_officers)
                
                if include_filing:
                    filings = self.get_filing_history(cn, items_per_page=1)
                    if filings.get('items'):
                        record['last_filing_date'] = filings['items'][0].get('date')
                        record['last_filing_type'] = filings['items'][0].get('type')
                
                results.append(record)
                
            except APIError as e:
                logger.warning(f"Failed to fetch {cn}: {e}")
                results.append({
                    'company_number': cn,
                    'error': str(e)
                })
        
        return pd.DataFrame(results)
    
    def search_companies_to_df(
        self, 
        query: str, 
        max_results: int = 100
    ) -> pd.DataFrame:
        """
        Search companies and return as DataFrame.
        
        Args:
            query: Search query
            max_results: Maximum results to return
            
        Returns:
            DataFrame with search results
        """
        results = []
        start_index = 0
        
        while len(results) < max_results:
            response = self.search_companies(
                query, 
                items_per_page=min(100, max_results - len(results)),
                start_index=start_index
            )
            
            items = response.get('items', [])
            if not items:
                break
            
            for item in items:
                results.append({
                    'company_number': item.get('company_number'),
                    'company_name': item.get('title'),
                    'company_status': item.get('company_status'),
                    'incorporation_date': item.get('date_of_creation'),
                    'company_type': item.get('company_type'),
                    'postcode': item.get('address', {}).get('postal_code'),
                    'address_snippet': item.get('address_snippet'),
                })
            
            start_index += len(items)
            
            if start_index >= response.get('total_results', 0):
                break
        
        return pd.DataFrame(results)
    
    def get_companies_by_sic_code(
        self,
        sic_code: str,
        location: Optional[str] = None,
        max_results: int = 100
    ) -> pd.DataFrame:
        """
        Find companies by SIC code.
        
        Args:
            sic_code: SIC code (e.g., "56101" for restaurants)
            location: Optional location filter
            max_results: Maximum results
            
        Returns:
            DataFrame with matching companies
        """
        params = {
            'sic_codes': sic_code,
            'size': min(max_results, 5000)
        }
        if location:
            params['location'] = location
        
        response = self.get("/advanced-search/companies", params=params)
        items = response.get('items', [])
        
        results = []
        for item in items:
            results.append({
                'company_number': item.get('company_number'),
                'company_name': item.get('company_name'),
                'company_status': item.get('company_status'),
                'incorporation_date': item.get('date_of_creation'),
                'dissolution_date': item.get('date_of_cessation'),
                'sic_codes': ','.join(item.get('sic_codes', [])),
                'postcode': item.get('registered_office_address', {}).get('postal_code'),
            })
        
        return pd.DataFrame(results)

