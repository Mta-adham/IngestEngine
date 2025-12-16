"""
Wayback Machine / Internet Archive API Client
===============================================

Access historical web snapshots for UK websites.

API Documentation: https://archive.org/help/wayback_api.php

Usage:
    from src.clients import WaybackMachineClient
    
    client = WaybackMachineClient()
    snapshots = client.get_snapshots("https://www.tesco.com")
"""

import pandas as pd
from typing import Optional, Dict, List, Any, Tuple
import logging
from datetime import datetime
import re

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class WaybackMachineClient(BaseAPIClient):
    """
    Client for Internet Archive Wayback Machine API.
    
    Access:
    - Historical snapshots of websites
    - CDX Server for bulk queries
    - Save page now functionality
    """
    
    BASE_URL = "https://archive.org"
    CDX_URL = "https://web.archive.org/cdx/search/cdx"
    AVAILABILITY_URL = "https://archive.org/wayback/available"
    
    def __init__(self, **kwargs):
        """Initialize Wayback Machine client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=15,  # Be respectful
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.check_availability("https://www.gov.uk")
            return True
        except Exception:
            return False
    
    # ========================================
    # AVAILABILITY CHECK
    # ========================================
    
    def check_availability(self, url: str, timestamp: Optional[str] = None) -> Dict:
        """
        Check if URL is archived.
        
        Args:
            url: URL to check
            timestamp: Optional timestamp (YYYYMMDDhhmmss)
            
        Returns:
            Availability info
        """
        import requests
        
        params = {'url': url}
        if timestamp:
            params['timestamp'] = timestamp
        
        response = requests.get(
            self.AVAILABILITY_URL,
            params=params,
            timeout=30
        )
        
        return response.json()
    
    def get_closest_snapshot(self, url: str, date: str) -> Optional[str]:
        """
        Get closest archived snapshot to a date.
        
        Args:
            url: URL to find
            date: Date in YYYYMMDD format
            
        Returns:
            Archived URL or None
        """
        result = self.check_availability(url, timestamp=date)
        
        snapshots = result.get('archived_snapshots', {})
        closest = snapshots.get('closest', {})
        
        if closest.get('available'):
            return closest.get('url')
        return None
    
    # ========================================
    # CDX SERVER (Bulk queries)
    # ========================================
    
    def get_snapshots(
        self,
        url: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        match_type: str = "exact",
        limit: int = 1000,
        collapse: Optional[str] = None
    ) -> List[Dict]:
        """
        Get all snapshots of a URL.
        
        Args:
            url: URL to query
            from_date: Start date (YYYYMMDD)
            to_date: End date (YYYYMMDD)
            match_type: exact, prefix, host, domain
            limit: Max results
            collapse: Collapse by field (e.g., 'timestamp:8' for daily)
            
        Returns:
            List of snapshots
        """
        import requests
        
        params = {
            'url': url,
            'output': 'json',
            'matchType': match_type,
            'limit': limit,
        }
        
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        if collapse:
            params['collapse'] = collapse
        
        response = requests.get(
            self.CDX_URL,
            params=params,
            timeout=60
        )
        
        if response.status_code != 200:
            return []
        
        lines = response.json()
        if not lines or len(lines) < 2:
            return []
        
        # First line is headers
        headers = lines[0]
        snapshots = []
        
        for line in lines[1:]:
            snapshot = dict(zip(headers, line))
            snapshots.append(snapshot)
        
        return snapshots
    
    def get_snapshots_df(self, url: str, **kwargs) -> pd.DataFrame:
        """Get snapshots as DataFrame"""
        snapshots = self.get_snapshots(url, **kwargs)
        return pd.DataFrame(snapshots)
    
    def get_first_snapshot(self, url: str) -> Optional[Dict]:
        """Get earliest snapshot of a URL"""
        snapshots = self.get_snapshots(url, limit=1)
        return snapshots[0] if snapshots else None
    
    def get_first_appearance_date(self, url: str) -> Optional[datetime]:
        """
        Get date of first web archive appearance.
        
        Useful for estimating when a business went online.
        """
        snapshot = self.get_first_snapshot(url)
        if snapshot:
            ts = snapshot.get('timestamp', '')
            if ts:
                return datetime.strptime(ts[:8], '%Y%m%d')
        return None
    
    # ========================================
    # UK-SPECIFIC QUERIES
    # ========================================
    
    def get_uk_gov_history(
        self,
        department: str,
        from_date: str = "20000101"
    ) -> pd.DataFrame:
        """Get history of UK government department website"""
        url = f"https://www.gov.uk/government/organisations/{department}"
        return self.get_snapshots_df(
            url,
            from_date=from_date,
            collapse='timestamp:6'  # Monthly
        )
    
    def get_company_website_history(
        self,
        domain: str,
        from_date: str = "19960101"
    ) -> pd.DataFrame:
        """Get history of a company's website"""
        return self.get_snapshots_df(
            domain,
            from_date=from_date,
            match_type='domain',
            collapse='timestamp:6'  # Monthly
        )
    
    def estimate_business_start_date(self, website: str) -> Optional[Dict]:
        """
        Estimate when a business started based on web presence.
        
        Args:
            website: Business website URL
            
        Returns:
            Dict with first_seen date and confidence
        """
        first = self.get_first_snapshot(website)
        
        if not first:
            return None
        
        ts = first.get('timestamp', '')
        if not ts:
            return None
        
        first_date = datetime.strptime(ts[:8], '%Y%m%d')
        
        return {
            'website': website,
            'first_archived': first_date.isoformat(),
            'archive_url': f"https://web.archive.org/web/{ts}/{first.get('original', website)}",
            'confidence': 'medium',
            'notes': 'Web presence date, actual business may have started earlier'
        }
    
    # ========================================
    # BULK OPERATIONS
    # ========================================
    
    def get_uk_retail_history(self) -> pd.DataFrame:
        """Get historical data for major UK retailers"""
        retailers = [
            'tesco.com', 'sainsburys.co.uk', 'asda.com', 'morrisons.com',
            'waitrose.com', 'ocado.com', 'boots.com', 'superdrug.com',
            'argos.co.uk', 'currys.co.uk', 'johnlewis.com', 'marksandspencer.com'
        ]
        
        all_data = []
        for retailer in retailers:
            df = self.get_snapshots_df(
                retailer,
                match_type='domain',
                limit=100,
                collapse='timestamp:4'  # Yearly
            )
            if not df.empty:
                df['retailer'] = retailer
                all_data.append(df)
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    # ========================================
    # ARCHIVE URL BUILDER
    # ========================================
    
    def build_archive_url(self, url: str, timestamp: str) -> str:
        """Build Wayback Machine URL"""
        return f"https://web.archive.org/web/{timestamp}/{url}"
    
    def get_archived_page_content(self, url: str, timestamp: str) -> Optional[str]:
        """Get content of archived page"""
        import requests
        
        archive_url = self.build_archive_url(url, timestamp)
        response = requests.get(archive_url, timeout=60)
        
        if response.status_code == 200:
            return response.text
        return None

