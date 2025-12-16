"""
HM Land Registry API Client
============================

Access UK property data including:
- Price Paid Data (property transactions)
- UK House Price Index
- Transaction Data

API Documentation: https://landregistry.data.gov.uk/

Usage:
    from src.clients import LandRegistryClient
    
    client = LandRegistryClient()
    transactions = client.get_price_paid(postcode="SW1A")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
import requests
from datetime import datetime

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class LandRegistryClient(BaseAPIClient):
    """
    Client for HM Land Registry APIs.
    
    Free access to property transaction data.
    """
    
    BASE_URL = "https://landregistry.data.gov.uk"
    SPARQL_ENDPOINT = "https://landregistry.data.gov.uk/landregistry/query"
    
    def __init__(self, **kwargs):
        """Initialize Land Registry client."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def _setup_auth(self):
        """No auth required for Land Registry"""
        self.session.headers['Accept'] = 'application/json'
    
    def health_check(self) -> bool:
        """Check if API is available"""
        try:
            self.get_house_price_index(region="united-kingdom", limit=1)
            return True
        except Exception:
            return False
    
    # ========================================
    # PRICE PAID DATA
    # ========================================
    
    def get_price_paid(
        self,
        postcode: Optional[str] = None,
        locality: Optional[str] = None,
        town: Optional[str] = None,
        district: Optional[str] = None,
        county: Optional[str] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        property_type: Optional[str] = None,
        new_build: Optional[bool] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get price paid data for property transactions.
        
        Args:
            postcode: Postcode (partial match, e.g., 'SW1A')
            locality: Street/area name
            town: Town name
            district: District name
            county: County name
            min_price: Minimum transaction price
            max_price: Maximum transaction price
            property_type: D=Detached, S=Semi, T=Terrace, F=Flat, O=Other
            new_build: True for new builds only
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            limit: Maximum results
            
        Returns:
            List of transactions
        """
        # Build SPARQL query
        filters = []
        
        if postcode:
            filters.append(f'FILTER(STRSTARTS(?postcode, "{postcode.upper()}"))')
        if locality:
            filters.append(f'FILTER(CONTAINS(UCASE(?paon), "{locality.upper()}"))')
        if town:
            filters.append(f'FILTER(CONTAINS(UCASE(?town), "{town.upper()}"))')
        if district:
            filters.append(f'FILTER(CONTAINS(UCASE(?district), "{district.upper()}"))')
        if county:
            filters.append(f'FILTER(CONTAINS(UCASE(?county), "{county.upper()}"))')
        if min_price:
            filters.append(f'FILTER(?amount >= {min_price})')
        if max_price:
            filters.append(f'FILTER(?amount <= {max_price})')
        if property_type:
            type_map = {'D': 'detached', 'S': 'semi-detached', 'T': 'terraced', 'F': 'flat-maisonette', 'O': 'other'}
            ptype = type_map.get(property_type.upper(), property_type.lower())
            filters.append(f'FILTER(?propertyType = lrcommon:{ptype})')
        if new_build is not None:
            nb = 'true' if new_build else 'false'
            filters.append(f'FILTER(?newBuild = "{nb}"^^xsd:boolean)')
        if from_date:
            filters.append(f'FILTER(?date >= "{from_date}"^^xsd:date)')
        if to_date:
            filters.append(f'FILTER(?date <= "{to_date}"^^xsd:date)')
        
        filter_str = '\n        '.join(filters)
        
        query = f"""
        PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
        PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
        
        SELECT ?transaction ?amount ?date ?propertyType ?newBuild ?postcode ?paon ?saon ?street ?town ?district ?county
        WHERE {{
            ?transaction a lrppi:TransactionRecord ;
                        lrppi:pricePaid ?amount ;
                        lrppi:transactionDate ?date ;
                        lrppi:propertyType ?propertyType ;
                        lrppi:newBuild ?newBuild ;
                        lrppi:propertyAddress ?address .
            
            ?address lrcommon:postcode ?postcode .
            OPTIONAL {{ ?address lrcommon:paon ?paon }}
            OPTIONAL {{ ?address lrcommon:saon ?saon }}
            OPTIONAL {{ ?address lrcommon:street ?street }}
            OPTIONAL {{ ?address lrcommon:town ?town }}
            OPTIONAL {{ ?address lrcommon:district ?district }}
            OPTIONAL {{ ?address lrcommon:county ?county }}
            
            {filter_str}
        }}
        ORDER BY DESC(?date)
        LIMIT {limit}
        """
        
        return self._sparql_query(query)
    
    def get_price_paid_df(self, **kwargs) -> pd.DataFrame:
        """Get price paid data as DataFrame"""
        results = self.get_price_paid(**kwargs)
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        
        # Clean up column names
        df.columns = [col.replace('.value', '') for col in df.columns]
        
        # Convert types
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        return df
    
    # ========================================
    # UK HOUSE PRICE INDEX
    # ========================================
    
    def get_house_price_index(
        self,
        region: str = "united-kingdom",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get UK House Price Index data.
        
        Args:
            region: Region identifier (e.g., 'united-kingdom', 'england', 'london')
            from_date: Start date (YYYY-MM)
            to_date: End date (YYYY-MM)
            limit: Maximum results
            
        Returns:
            List of HPI records
        """
        filters = []
        if from_date:
            filters.append(f'FILTER(?refMonth >= "{from_date}"^^xsd:gYearMonth)')
        if to_date:
            filters.append(f'FILTER(?refMonth <= "{to_date}"^^xsd:gYearMonth)')
        
        filter_str = '\n        '.join(filters)
        
        query = f"""
        PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?refMonth ?housePriceIndex ?averagePrice ?percentageChange ?salesVolume
        WHERE {{
            ?item ukhpi:refRegion <http://landregistry.data.gov.uk/id/region/{region}> ;
                  ukhpi:refMonth ?refMonth .
            OPTIONAL {{ ?item ukhpi:housePriceIndex ?housePriceIndex }}
            OPTIONAL {{ ?item ukhpi:averagePrice ?averagePrice }}
            OPTIONAL {{ ?item ukhpi:percentageChange ?percentageChange }}
            OPTIONAL {{ ?item ukhpi:salesVolume ?salesVolume }}
            {filter_str}
        }}
        ORDER BY DESC(?refMonth)
        LIMIT {limit}
        """
        
        return self._sparql_query(query)
    
    def get_house_price_index_df(self, **kwargs) -> pd.DataFrame:
        """Get HPI data as DataFrame"""
        results = self.get_house_price_index(**kwargs)
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df.columns = [col.replace('.value', '') for col in df.columns]
        
        for col in ['housePriceIndex', 'averagePrice', 'percentageChange', 'salesVolume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    # ========================================
    # REGIONAL DATA
    # ========================================
    
    def get_regions(self) -> List[Dict]:
        """Get list of available regions"""
        query = """
        PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?region ?label
        WHERE {
            ?item ukhpi:refRegion ?region .
            ?region rdfs:label ?label .
        }
        ORDER BY ?label
        """
        return self._sparql_query(query)
    
    def get_average_price_by_region(
        self,
        month: str,
        property_type: Optional[str] = None
    ) -> List[Dict]:
        """
        Get average prices across all regions for a month.
        
        Args:
            month: Reference month (YYYY-MM)
            property_type: Optional property type filter
            
        Returns:
            Regional price data
        """
        query = f"""
        PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?regionLabel ?averagePrice ?housePriceIndex ?percentageChange
        WHERE {{
            ?item ukhpi:refMonth "{month}"^^xsd:gYearMonth ;
                  ukhpi:refRegion ?region ;
                  ukhpi:averagePrice ?averagePrice .
            ?region rdfs:label ?regionLabel .
            OPTIONAL {{ ?item ukhpi:housePriceIndex ?housePriceIndex }}
            OPTIONAL {{ ?item ukhpi:percentageChange ?percentageChange }}
        }}
        ORDER BY DESC(?averagePrice)
        """
        return self._sparql_query(query)
    
    # ========================================
    # SPARQL HELPER
    # ========================================
    
    def _sparql_query(self, query: str) -> List[Dict]:
        """Execute SPARQL query"""
        response = requests.post(
            self.SPARQL_ENDPOINT,
            data={'query': query},
            headers={'Accept': 'application/sparql-results+json'},
            timeout=60
        )
        
        if response.status_code != 200:
            raise APIError(f"SPARQL query failed: {response.text}")
        
        data = response.json()
        results = []
        
        for binding in data.get('results', {}).get('bindings', []):
            row = {}
            for key, value in binding.items():
                row[key] = value.get('value')
            results.append(row)
        
        return results
    
    # ========================================
    # BULK DOWNLOADS
    # ========================================
    
    def get_bulk_download_urls(self) -> Dict[str, str]:
        """Get URLs for bulk data downloads"""
        return {
            'price_paid_complete': 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv',
            'price_paid_monthly': 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv',
            'transaction_count': 'https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads-december-2024',
        }

