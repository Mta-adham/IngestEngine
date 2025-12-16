"""
UK Web Scraper Client
======================

Scrape data from UK websites for POI and business information.

Usage:
    from src.clients import WebScraperClient
    
    client = WebScraperClient()
    data = client.scrape_yell("restaurant", "london")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import re
import time
from urllib.parse import urljoin, quote_plus

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class WebScraperClient(BaseAPIClient):
    """
    Web scraper for UK business directories and property sites.
    
    Sources:
    - Yell.com (Yellow Pages)
    - Thomson Local
    - 192.com (business directory)
    - Rightmove (property)
    - Zoopla (property)
    
    Note: Respect robots.txt and rate limits
    """
    
    BASE_URL = "https://www.yell.com"
    
    def __init__(self, **kwargs):
        """Initialize web scraper."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=10,  # Be respectful
            **kwargs
        )
    
    def _setup_auth(self):
        """Setup headers to look like browser"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
        })
    
    def health_check(self) -> bool:
        """Check if scraping is available"""
        return True
    
    # ========================================
    # YELL.COM (Yellow Pages)
    # ========================================
    
    def scrape_yell(
        self,
        category: str,
        location: str,
        max_pages: int = 3
    ) -> pd.DataFrame:
        """
        Scrape Yell.com business listings.
        
        Args:
            category: Business category (e.g., "restaurant", "plumber")
            location: Location (e.g., "london", "manchester")
            max_pages: Maximum pages to scrape
            
        Returns:
            DataFrame of businesses
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 required. Install with: pip install beautifulsoup4")
            return pd.DataFrame()
        
        businesses = []
        
        for page in range(1, max_pages + 1):
            url = f"https://www.yell.com/ucs/UcsSearchAction.do?keywords={quote_plus(category)}&location={quote_plus(location)}&pageNum={page}"
            
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code != 200:
                    break
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find business listings
                listings = soup.find_all('div', class_='businessCapsule')
                
                if not listings:
                    break
                
                for listing in listings:
                    business = self._parse_yell_listing(listing)
                    if business:
                        businesses.append(business)
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error scraping page {page}: {e}")
                break
        
        return pd.DataFrame(businesses)
    
    def _parse_yell_listing(self, listing) -> Optional[Dict]:
        """Parse a single Yell listing"""
        try:
            name_elem = listing.find('h2', class_='businessCapsule--name')
            name = name_elem.get_text(strip=True) if name_elem else None
            
            address_elem = listing.find('span', class_='businessCapsule--address')
            address = address_elem.get_text(strip=True) if address_elem else None
            
            phone_elem = listing.find('span', class_='business--telephoneNumber')
            phone = phone_elem.get_text(strip=True) if phone_elem else None
            
            rating_elem = listing.find('span', class_='starRating--average')
            rating = rating_elem.get_text(strip=True) if rating_elem else None
            
            website_elem = listing.find('a', class_='businessCapsule--ctaItem', href=True)
            website = website_elem.get('href') if website_elem else None
            
            return {
                'name': name,
                'address': address,
                'phone': phone,
                'rating': rating,
                'website': website,
                'source': 'yell.com'
            }
        except Exception:
            return None
    
    # ========================================
    # 192.COM BUSINESS DIRECTORY
    # ========================================
    
    def scrape_192(
        self,
        business_name: str,
        location: str
    ) -> pd.DataFrame:
        """
        Scrape 192.com business directory.
        
        Args:
            business_name: Business name to search
            location: Location to search
            
        Returns:
            DataFrame of results
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return pd.DataFrame()
        
        url = f"https://www.192.com/businesses/search/?what={quote_plus(business_name)}&where={quote_plus(location)}"
        
        try:
            response = self.session.get(url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            results = []
            listings = soup.find_all('div', class_='listing')
            
            for listing in listings[:20]:
                name = listing.find('h3')
                address = listing.find('address')
                
                results.append({
                    'name': name.get_text(strip=True) if name else None,
                    'address': address.get_text(strip=True) if address else None,
                    'source': '192.com'
                })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.warning(f"Error scraping 192.com: {e}")
            return pd.DataFrame()
    
    # ========================================
    # RIGHTMOVE (Property data)
    # ========================================
    
    def get_rightmove_url(
        self,
        location: str,
        property_type: str = "for-sale"
    ) -> str:
        """Build Rightmove search URL"""
        return f"https://www.rightmove.co.uk/property-{property_type}/find.html?locationIdentifier=REGION%5E{location}"
    
    def scrape_rightmove_prices(
        self,
        location_code: str,
        max_pages: int = 2
    ) -> pd.DataFrame:
        """
        Scrape Rightmove property listings.
        
        Note: Check Rightmove's terms of service.
        
        Args:
            location_code: Rightmove location code
            max_pages: Pages to scrape
            
        Returns:
            DataFrame of properties
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return pd.DataFrame()
        
        properties = []
        
        for page in range(max_pages):
            index = page * 24
            url = f"https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E{location_code}&index={index}"
            
            try:
                response = self.session.get(url, timeout=30)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find property cards
                cards = soup.find_all('div', class_='propertyCard')
                
                for card in cards:
                    prop = self._parse_rightmove_card(card)
                    if prop:
                        properties.append(prop)
                
                time.sleep(3)
                
            except Exception as e:
                logger.warning(f"Error: {e}")
                break
        
        return pd.DataFrame(properties)
    
    def _parse_rightmove_card(self, card) -> Optional[Dict]:
        """Parse Rightmove property card"""
        try:
            price_elem = card.find('div', class_='propertyCard-priceValue')
            price = price_elem.get_text(strip=True) if price_elem else None
            
            address_elem = card.find('address', class_='propertyCard-address')
            address = address_elem.get_text(strip=True) if address_elem else None
            
            desc_elem = card.find('h2', class_='propertyCard-title')
            description = desc_elem.get_text(strip=True) if desc_elem else None
            
            return {
                'price': price,
                'address': address,
                'description': description,
                'source': 'rightmove'
            }
        except Exception:
            return None
    
    # ========================================
    # COUNCIL WEBSITES
    # ========================================
    
    def get_council_planning_portal_urls(self) -> Dict[str, str]:
        """Get planning portal URLs for London boroughs"""
        return {
            'westminster': 'https://idoxpa.westminster.gov.uk/online-applications/',
            'camden': 'https://camdocs.camden.gov.uk/WebApps/Planning/',
            'islington': 'https://planning.islington.gov.uk/NorthgateIM/PlanningExplorer/',
            'hackney': 'https://planning.hackney.gov.uk/',
            'tower_hamlets': 'https://development.towerhamlets.gov.uk/',
            'southwark': 'https://planning.southwark.gov.uk/',
            'lambeth': 'https://planning.lambeth.gov.uk/',
            'lewisham': 'https://planning.lewisham.gov.uk/',
            'greenwich': 'https://planning.royalgreenwich.gov.uk/',
            'bexley': 'https://pa.bexley.gov.uk/',
        }
    
    # ========================================
    # BUSINESS REGISTRATION DATA
    # ========================================
    
    def scrape_fca_register(
        self,
        firm_name: str
    ) -> pd.DataFrame:
        """
        Scrape FCA Financial Services Register.
        
        Args:
            firm_name: Firm name to search
            
        Returns:
            FCA registration details
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return pd.DataFrame()
        
        url = f"https://register.fca.org.uk/s/search?q={quote_plus(firm_name)}&type=Companies"
        
        try:
            response = self.session.get(url, timeout=30)
            # FCA uses dynamic content, would need Selenium
            logger.info("FCA register requires JavaScript - use Selenium for full scraping")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Error: {e}")
            return pd.DataFrame()
    
    # ========================================
    # HELPER METHODS
    # ========================================
    
    def extract_postcode(self, text: str) -> Optional[str]:
        """Extract UK postcode from text"""
        pattern = r'[A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2}'
        match = re.search(pattern, text.upper())
        return match.group(0) if match else None
    
    def extract_phone(self, text: str) -> Optional[str]:
        """Extract UK phone number from text"""
        pattern = r'(?:0[0-9]{2,4}\s?[0-9]{3,4}\s?[0-9]{3,4}|07[0-9]{3}\s?[0-9]{6})'
        match = re.search(pattern, text)
        return match.group(0) if match else None

