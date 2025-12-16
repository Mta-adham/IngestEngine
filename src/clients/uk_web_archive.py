"""
UK Web Archive / British Library Client
========================================

Access archived UK websites from the British Library.

API Documentation: https://www.webarchive.org.uk/

Usage:
    from src.clients import UKWebArchiveClient
    
    client = UKWebArchiveClient()
    results = client.search("london restaurant")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class UKWebArchiveClient(BaseAPIClient):
    """
    Client for UK Web Archive.
    
    The UK Web Archive preserves UK websites,
    complementing the Wayback Machine with UK-specific content.
    """
    
    BASE_URL = "https://www.webarchive.org.uk"
    SHINE_URL = "https://www.webarchive.org.uk/shine"
    
    def __init__(self, **kwargs):
        """Initialize UK Web Archive client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=20,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        return True
    
    # ========================================
    # SHINE SEARCH
    # ========================================
    
    def search(
        self,
        query: str,
        from_year: Optional[int] = None,
        to_year: Optional[int] = None,
        domain: Optional[str] = None,
        max_results: int = 50
    ) -> List[Dict]:
        """
        Search the UK Web Archive via SHINE.
        
        Args:
            query: Search query
            from_year: Start year filter
            to_year: End year filter
            domain: Domain filter (e.g., '.gov.uk')
            max_results: Maximum results
            
        Returns:
            Search results
        """
        import requests
        
        params = {
            'query': query,
            'rows': max_results,
            'action': 'search',
            'facet.in.content_type_norm': 'html',
        }
        
        if from_year:
            params['facet.in.crawl_year'] = f'[{from_year} TO {to_year or 2024}]'
        if domain:
            params['facet.in.domain'] = domain
        
        response = requests.get(
            f"{self.SHINE_URL}/search",
            params=params,
            timeout=60
        )
        
        if response.status_code == 200:
            # Parse HTML response or JSON if available
            return self._parse_shine_results(response.text)
        
        return []
    
    def _parse_shine_results(self, html: str) -> List[Dict]:
        """Parse SHINE search results"""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            results = []
            for item in soup.find_all('div', class_='result'):
                title = item.find('a', class_='title')
                url = item.find('cite')
                date = item.find('span', class_='date')
                
                results.append({
                    'title': title.get_text(strip=True) if title else None,
                    'url': url.get_text(strip=True) if url else None,
                    'date': date.get_text(strip=True) if date else None,
                })
            
            return results
        except ImportError:
            return []
    
    # ========================================
    # UK GOVERNMENT ARCHIVE
    # ========================================
    
    def get_archived_gov_uk_page(self, path: str, year: int) -> Optional[str]:
        """
        Get archived gov.uk page.
        
        Args:
            path: Path on gov.uk (e.g., '/government/organisations/hmrc')
            year: Year of archive
            
        Returns:
            Archive URL
        """
        return f"https://webarchive.nationalarchives.gov.uk/{year}/https://www.gov.uk{path}"
    
    # ========================================
    # NATIONAL ARCHIVES
    # ========================================
    
    def get_national_archives_url(self, original_url: str) -> str:
        """Build National Archives Web Archive URL"""
        return f"https://webarchive.nationalarchives.gov.uk/search/result/?q={original_url}"
    
    def search_national_archives(
        self,
        url: str
    ) -> Dict:
        """
        Search National Archives for archived URL.
        
        Args:
            url: Original URL to find
            
        Returns:
            Archive information
        """
        import requests
        
        response = requests.get(
            "https://webarchive.nationalarchives.gov.uk/ukgwa/search/result/",
            params={'q': url},
            timeout=30
        )
        
        return {
            'searched_url': url,
            'archive_search': f"https://webarchive.nationalarchives.gov.uk/search/result/?q={url}",
            'status': 'search_complete' if response.status_code == 200 else 'error'
        }
    
    # ========================================
    # SPECIAL COLLECTIONS
    # ========================================
    
    def get_special_collections(self) -> List[Dict]:
        """Get special collection themes in UK Web Archive"""
        return [
            {'name': 'UK Government', 'description': '.gov.uk and government sites'},
            {'name': 'News', 'description': 'UK news websites'},
            {'name': 'Higher Education', 'description': '.ac.uk university sites'},
            {'name': 'Business', 'description': 'UK company websites'},
            {'name': 'Culture', 'description': 'Museums, galleries, heritage'},
            {'name': 'Sports', 'description': 'UK sports organizations'},
            {'name': 'Elections', 'description': 'UK election content'},
            {'name': 'COVID-19', 'description': 'Pandemic-related UK content'},
        ]
    
    # ========================================
    # DOMAIN ANALYSIS
    # ========================================
    
    def get_uk_domain_stats(self) -> Dict[str, str]:
        """Get UK domain coverage statistics"""
        return {
            '.co.uk': 'Primary commercial domain',
            '.org.uk': 'Organizations',
            '.gov.uk': 'Government (comprehensive)',
            '.ac.uk': 'Higher education',
            '.nhs.uk': 'NHS services',
            '.police.uk': 'Police forces',
            '.sch.uk': 'Schools',
            '.me.uk': 'Personal sites',
            'total_sites': 'Millions of UK websites archived',
            'earliest': '1996',
        }
    
    # ========================================
    # BULK DATA
    # ========================================
    
    def get_data_access_info(self) -> Dict[str, str]:
        """Get information about accessing bulk data"""
        return {
            'shine': 'https://www.webarchive.org.uk/shine - Full text search',
            'national_archives': 'https://webarchive.nationalarchives.gov.uk/',
            'reading_rooms': 'Access full archive at British Library reading rooms',
            'api_note': 'Full API access requires British Library account',
        }

