"""
Wikidata Client for Querying POI Opening Dates
===============================================

Queries Wikidata SPARQL endpoint to retrieve opening/inception dates (P571).

Features:
- Rate limiting (respects Wikidata guidelines)
- Retry logic with exponential backoff
- User-Agent compliance

Usage:
    from src.wikidata_client import WikidataClient
    
    client = WikidataClient(rate_limit_delay=1.0, max_retries=3)
    info = client.get_poi_info("London Eye", city="London")
"""

# ============================================
# IMPORTS
# ============================================
import requests
import time
import logging
from typing import Optional, Dict
from urllib.parse import quote
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================
# CLASS DEFINITION
# ============================================

class WikidataClient:
    """
    Client for querying Wikidata SPARQL endpoint
    
    Respects Wikidata User-Agent guidelines and implements rate limiting
    """
    
    SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    USER_AGENT = "IngestEngine/1.0 (https://github.com/yourusername/IngestEngine; contact@example.com)"
    
    # ============================================
    # CONSTANTS
    # ============================================
    PROPERTY_INCEPTION = "P571"  # Inception/opening date
    PROPERTY_LOCATED_IN = "P131"  # Located in administrative territorial entity
    
    # ============================================
    # INITIALIZATION
    # ============================================
    
    def __init__(self, rate_limit_delay: float = 1.0, max_retries: int = 3):
        """
        Initialize Wikidata client
        
        Args:
            rate_limit_delay: Seconds to wait between requests (default: 1.0)
            max_retries: Maximum number of retry attempts (default: 3)
        """
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.last_request_time = 0
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.USER_AGENT,
            'Accept': 'application/sparql-results+json'
        })
    
    # ============================================
    # INTERNAL UTILITY METHODS
    # ============================================
    
    def _rate_limit(self):
        """Enforce rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def _execute_sparql(self, query: str) -> Optional[Dict]:
        """
        Execute SPARQL query with retry logic
        
        Args:
            query: SPARQL query string
            
        Returns:
            JSON response as dictionary, or None if failed
        """
        self._rate_limit()
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    self.SPARQL_ENDPOINT,
                    params={'query': query, 'format': 'json'},
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"SPARQL query attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"SPARQL query failed after {self.max_retries} attempts")
                    return None
        
        return None
    
    # ============================================
    # PUBLIC API METHODS
    # ============================================
    
    def search_item(self, name: str, city: str = "London") -> Optional[str]:
        """
        Search for a Wikidata item by name and location
        
        Args:
            name: POI name to search for
            city: City name to restrict search (default: "London")
            
        Returns:
            Wikidata Q-ID (e.g., "Q12345") or None if not found/ambiguous
            
        Example SPARQL query:
            SELECT ?item WHERE {
              ?item rdfs:label ?label .
              ?item wdt:P131* wd:Q84 .  # Located in London
              FILTER(LANG(?label) = "en")
              FILTER(CONTAINS(LCASE(?label), "british museum"))
            }
            LIMIT 1
        """
        if not name or not name.strip():
            return None
        
        # Clean name for search
        search_name = name.strip().lower()
        
        # SPARQL query to find item by name in London
        # Escape quotes for SPARQL
        escaped_name = search_name.replace('"', '\\"')
        query = f"""
        SELECT ?item WHERE {{
          ?item rdfs:label ?label .
          ?item wdt:P131* wd:Q84 .  # Located in London (Q84)
          FILTER(LANG(?label) = "en")
          FILTER(CONTAINS(LCASE(?label), "{escaped_name}"))
        }}
        LIMIT 1
        """
        
        try:
            result = self._execute_sparql(query)
            if result and 'results' in result and 'bindings' in result['results']:
                bindings = result['results']['bindings']
                if bindings:
                    item_uri = bindings[0]['item']['value']
                    # Extract Q-ID from URI (e.g., http://www.wikidata.org/entity/Q12345 -> Q12345)
                    qid = item_uri.split('/')[-1]
                    logger.debug(f"Found Wikidata item {qid} for '{name}'")
                    return qid
        except Exception as e:
            logger.warning(f"Error searching for '{name}': {e}")
        
        return None
    
    def get_opening_date(self, qid: str) -> Optional[str]:
        """
        Retrieve opening/inception date (P571) for a Wikidata item
        
        Args:
            qid: Wikidata Q-ID (e.g., "Q12345")
            
        Returns:
            ISO date string (YYYY-MM-DD) or None if not available
            
        Example SPARQL query:
            SELECT ?date WHERE {
              wd:Q12345 wdt:P571 ?date .
            }
        """
        if not qid or not qid.startswith('Q'):
            return None
        
        query = f"""
        SELECT ?date WHERE {{
          wd:{qid} wdt:P571 ?date .
        }}
        LIMIT 1
        """
        
        try:
            result = self._execute_sparql(query)
            if result and 'results' in result and 'bindings' in result['results']:
                bindings = result['results']['bindings']
                if bindings:
                    date_value = bindings[0]['date']['value']
                    # Parse date (Wikidata returns in various formats)
                    # Try to extract YYYY-MM-DD format
                    if 'T' in date_value:
                        date_value = date_value.split('T')[0]
                    elif '+' in date_value:
                        date_value = date_value.split('+')[0]
                    
                    # Validate it's a date-like string
                    if len(date_value) >= 4 and date_value[:4].isdigit():
                        logger.debug(f"Found opening date {date_value} for {qid}")
                        return date_value
        except Exception as e:
            logger.warning(f"Error getting opening date for {qid}: {e}")
        
        return None
    
    def get_poi_info(self, name: str, city: str = "London") -> Dict[str, Optional[str]]:
        """
        Get both Q-ID and opening date for a POI
        
        Args:
            name: POI name
            city: City name (default: "London")
            
        Returns:
            Dictionary with 'wikidata_id' and 'opening_date' keys
        """
        qid = self.search_item(name, city)
        opening_date = None
        
        if qid:
            opening_date = self.get_opening_date(qid)
        
        return {
            'wikidata_id': qid,
            'opening_date': opening_date
        }

