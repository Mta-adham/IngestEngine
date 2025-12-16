"""
Companies House Streaming API Client
=====================================

Real-time company changes via streaming API.

API Documentation: https://developer.company-information.service.gov.uk/streaming-api

Usage:
    from src.clients import CompaniesHouseStreamingClient
    
    client = CompaniesHouseStreamingClient(api_key="your-key")
    for event in client.stream_company_events():
        print(event)
"""

import pandas as pd
from typing import Optional, Dict, List, Any, Generator
import logging
import os
import json

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class CompaniesHouseStreamingClient(BaseAPIClient):
    """
    Client for Companies House Streaming API.
    
    Real-time access to:
    - Company profile changes
    - Filing events
    - Charges
    - Insolvency events
    - Officers
    - Persons with Significant Control
    """
    
    STREAM_URL = "https://stream.companieshouse.gov.uk"
    
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Initialize streaming client.
        
        Args:
            api_key: Companies House API key
        """
        from src.config import COMPANIES_HOUSE_API_KEY
        self.api_key = api_key or COMPANIES_HOUSE_API_KEY
        
        if not self.api_key:
            logger.warning("No API key. Set COMPANIES_HOUSE_API_KEY.")
        
        super().__init__(
            base_url=self.STREAM_URL,
            api_key=self.api_key,
            rate_limit_rpm=600,
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup HTTP Basic Auth"""
        if self.api_key:
            from requests.auth import HTTPBasicAuth
            self.session.auth = HTTPBasicAuth(self.api_key, '')
    
    def health_check(self) -> bool:
        """Check if streaming is available"""
        return bool(self.api_key)
    
    # ========================================
    # STREAMING ENDPOINTS
    # ========================================
    
    def stream_companies(
        self,
        timepoint: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """
        Stream company profile changes.
        
        Args:
            timepoint: Start from specific timepoint
            
        Yields:
            Company change events
        """
        endpoint = '/companies'
        params = {}
        if timepoint:
            params['timepoint'] = timepoint
        
        yield from self._stream(endpoint, params)
    
    def stream_filings(
        self,
        timepoint: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """
        Stream filing events.
        
        Args:
            timepoint: Start from specific timepoint
            
        Yields:
            Filing events
        """
        endpoint = '/filings'
        params = {}
        if timepoint:
            params['timepoint'] = timepoint
        
        yield from self._stream(endpoint, params)
    
    def stream_officers(
        self,
        timepoint: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """
        Stream officer changes.
        
        Args:
            timepoint: Start from specific timepoint
            
        Yields:
            Officer change events
        """
        endpoint = '/officers'
        params = {}
        if timepoint:
            params['timepoint'] = timepoint
        
        yield from self._stream(endpoint, params)
    
    def stream_charges(
        self,
        timepoint: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """
        Stream charge events (mortgages, debentures).
        
        Args:
            timepoint: Start from specific timepoint
            
        Yields:
            Charge events
        """
        endpoint = '/charges'
        params = {}
        if timepoint:
            params['timepoint'] = timepoint
        
        yield from self._stream(endpoint, params)
    
    def stream_insolvency(
        self,
        timepoint: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """
        Stream insolvency events.
        
        Args:
            timepoint: Start from specific timepoint
            
        Yields:
            Insolvency events
        """
        endpoint = '/insolvency-cases'
        params = {}
        if timepoint:
            params['timepoint'] = timepoint
        
        yield from self._stream(endpoint, params)
    
    def stream_pscs(
        self,
        timepoint: Optional[int] = None
    ) -> Generator[Dict, None, None]:
        """
        Stream Persons with Significant Control changes.
        
        Args:
            timepoint: Start from specific timepoint
            
        Yields:
            PSC change events
        """
        endpoint = '/persons-with-significant-control'
        params = {}
        if timepoint:
            params['timepoint'] = timepoint
        
        yield from self._stream(endpoint, params)
    
    # ========================================
    # STREAMING HELPER
    # ========================================
    
    def _stream(
        self,
        endpoint: str,
        params: Dict
    ) -> Generator[Dict, None, None]:
        """
        Internal streaming implementation.
        
        Args:
            endpoint: Stream endpoint
            params: Query parameters
            
        Yields:
            Events from stream
        """
        import requests
        
        with requests.get(
            f"{self.STREAM_URL}{endpoint}",
            params=params,
            auth=(self.api_key, ''),
            stream=True,
            timeout=None
        ) as response:
            
            if response.status_code != 200:
                raise APIError(f"Stream error: {response.status_code}")
            
            for line in response.iter_lines():
                if line:
                    try:
                        event = json.loads(line.decode('utf-8'))
                        yield event
                    except json.JSONDecodeError:
                        continue
    
    # ========================================
    # BATCH COLLECTION
    # ========================================
    
    def collect_events(
        self,
        stream_type: str,
        max_events: int = 100,
        timepoint: Optional[int] = None
    ) -> List[Dict]:
        """
        Collect a batch of events.
        
        Args:
            stream_type: companies, filings, officers, charges, insolvency, pscs
            max_events: Maximum events to collect
            timepoint: Start from specific timepoint
            
        Returns:
            List of events
        """
        stream_methods = {
            'companies': self.stream_companies,
            'filings': self.stream_filings,
            'officers': self.stream_officers,
            'charges': self.stream_charges,
            'insolvency': self.stream_insolvency,
            'pscs': self.stream_pscs,
        }
        
        stream_func = stream_methods.get(stream_type)
        if not stream_func:
            raise ValueError(f"Unknown stream type: {stream_type}")
        
        events = []
        try:
            for event in stream_func(timepoint=timepoint):
                events.append(event)
                if len(events) >= max_events:
                    break
        except Exception as e:
            logger.warning(f"Stream ended: {e}")
        
        return events
    
    def collect_events_df(
        self,
        stream_type: str,
        max_events: int = 100,
        timepoint: Optional[int] = None
    ) -> pd.DataFrame:
        """Collect events as DataFrame"""
        events = self.collect_events(stream_type, max_events, timepoint)
        return pd.DataFrame(events)
    
    # ========================================
    # REFERENCE
    # ========================================
    
    def get_stream_types(self) -> List[str]:
        """Get available stream types"""
        return [
            'companies',
            'filings',
            'officers',
            'charges',
            'insolvency',
            'pscs',
        ]

