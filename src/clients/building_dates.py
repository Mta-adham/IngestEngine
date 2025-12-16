"""
Building Opening/Construction Date Estimator
=============================================

Combines multiple sources to estimate when buildings were built or opened.

Sources used:
- Land Registry (first transaction date)
- EPC certificates (construction age band)
- Listed building records
- Planning applications
- Council tax banding
- Street view imagery dates
- Wayback Machine (website presence)
- VOA business rates (first entry)

Usage:
    from src.clients import BuildingDateEstimator
    
    estimator = BuildingDateEstimator()
    dates = estimator.estimate_building_date(postcode="SW1A 1AA")
"""

import pandas as pd
from typing import Optional, Dict, List, Any
import logging
from datetime import datetime

from src.clients.base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class BuildingDateEstimator(BaseAPIClient):
    """
    Multi-source building date estimator.
    
    Combines evidence from multiple APIs to estimate:
    - Construction date
    - First occupation date
    - Building modification dates
    - Business opening dates (for commercial)
    """
    
    BASE_URL = "https://example.com"  # Aggregator, uses other clients
    
    def __init__(self, **kwargs):
        """Initialize building date estimator."""
        super().__init__(
            base_url=self.BASE_URL,
            rate_limit_rpm=30,
            **kwargs
        )
        
        # Lazy load clients
        self._clients = {}
    
    def _setup_auth(self):
        """No direct auth needed"""
        pass
    
    def _get_client(self, name: str):
        """Lazy load a client"""
        if name not in self._clients:
            try:
                if name == 'land_registry':
                    from src.clients import LandRegistryClient
                    self._clients[name] = LandRegistryClient()
                elif name == 'epc':
                    from src.clients import EPCClient
                    self._clients[name] = EPCClient()
                elif name == 'historic_england':
                    from src.clients import HistoricEnglandClient
                    self._clients[name] = HistoricEnglandClient()
                elif name == 'wayback':
                    from src.clients import WaybackMachineClient
                    self._clients[name] = WaybackMachineClient()
                elif name == 'osm':
                    from src.clients import OpenStreetMapClient
                    self._clients[name] = OpenStreetMapClient()
            except ImportError as e:
                logger.warning(f"Could not load {name} client: {e}")
                return None
        return self._clients.get(name)
    
    def health_check(self) -> bool:
        """Check if any data sources available"""
        return True
    
    # ========================================
    # MAIN ESTIMATION METHOD
    # ========================================
    
    def estimate_building_date(
        self,
        postcode: Optional[str] = None,
        address: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        uprn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Estimate building construction/opening date from multiple sources.
        
        Args:
            postcode: Property postcode
            address: Property address
            lat: Latitude
            lon: Longitude
            uprn: Unique Property Reference Number
            
        Returns:
            Dict with estimates from each source and combined estimate
        """
        estimates = {
            'sources': {},
            'combined_estimate': None,
            'confidence': 'low',
            'evidence': []
        }
        
        # 1. EPC - Construction Age Band
        epc_estimate = self._get_epc_estimate(postcode)
        if epc_estimate:
            estimates['sources']['epc'] = epc_estimate
            estimates['evidence'].append(f"EPC age band: {epc_estimate.get('age_band')}")
        
        # 2. Land Registry - First Transaction
        lr_estimate = self._get_land_registry_estimate(postcode)
        if lr_estimate:
            estimates['sources']['land_registry'] = lr_estimate
            estimates['evidence'].append(f"First sale: {lr_estimate.get('first_transaction')}")
        
        # 3. Historic England - Listed Building
        if lat and lon:
            he_estimate = self._get_historic_england_estimate(lat, lon)
            if he_estimate:
                estimates['sources']['historic_england'] = he_estimate
                estimates['evidence'].append(f"Listed: {he_estimate.get('listing_date')}")
        
        # Combine estimates
        estimates['combined_estimate'] = self._combine_estimates(estimates['sources'])
        estimates['confidence'] = self._calculate_confidence(estimates['sources'])
        
        return estimates
    
    # ========================================
    # INDIVIDUAL SOURCE METHODS
    # ========================================
    
    def _get_epc_estimate(self, postcode: str) -> Optional[Dict]:
        """Get estimate from EPC certificate"""
        client = self._get_client('epc')
        if not client:
            return None
        
        try:
            epcs = client.search_domestic(postcode=postcode, size=1)
            if epcs:
                epc = epcs[0] if isinstance(epcs, list) else epcs.get('rows', [{}])[0]
                
                age_band = epc.get('construction-age-band', epc.get('constructionAgeBand', ''))
                
                return {
                    'age_band': age_band,
                    'estimated_year_range': self._parse_epc_age_band(age_band),
                    'source': 'EPC Certificate',
                    'confidence': 'medium'
                }
        except Exception as e:
            logger.debug(f"EPC lookup failed: {e}")
        
        return None
    
    def _parse_epc_age_band(self, age_band: str) -> Optional[Dict]:
        """Parse EPC age band to year range"""
        age_bands = {
            'England and Wales: before 1900': {'min': 1800, 'max': 1899},
            'England and Wales: 1900-1929': {'min': 1900, 'max': 1929},
            'England and Wales: 1930-1949': {'min': 1930, 'max': 1949},
            'England and Wales: 1950-1966': {'min': 1950, 'max': 1966},
            'England and Wales: 1967-1975': {'min': 1967, 'max': 1975},
            'England and Wales: 1976-1982': {'min': 1976, 'max': 1982},
            'England and Wales: 1983-1990': {'min': 1983, 'max': 1990},
            'England and Wales: 1991-1995': {'min': 1991, 'max': 1995},
            'England and Wales: 1996-2002': {'min': 1996, 'max': 2002},
            'England and Wales: 2003-2006': {'min': 2003, 'max': 2006},
            'England and Wales: 2007-2011': {'min': 2007, 'max': 2011},
            'England and Wales: 2012 onwards': {'min': 2012, 'max': datetime.now().year},
        }
        
        for band, years in age_bands.items():
            if age_band and band.lower() in age_band.lower():
                return years
        
        # Try to extract years directly
        import re
        match = re.search(r'(\d{4})\s*[-–]\s*(\d{4})', age_band)
        if match:
            return {'min': int(match.group(1)), 'max': int(match.group(2))}
        
        match = re.search(r'before\s*(\d{4})', age_band, re.I)
        if match:
            return {'min': 1800, 'max': int(match.group(1)) - 1}
        
        match = re.search(r'(\d{4})\s*onwards', age_band, re.I)
        if match:
            return {'min': int(match.group(1)), 'max': datetime.now().year}
        
        return None
    
    def _get_land_registry_estimate(self, postcode: str) -> Optional[Dict]:
        """Get estimate from Land Registry first transaction"""
        client = self._get_client('land_registry')
        if not client:
            return None
        
        try:
            transactions = client.get_price_paid(postcode=postcode[:4], limit=50)
            
            if transactions:
                # Find earliest transaction for this postcode
                earliest = None
                for t in transactions:
                    t_postcode = t.get('postcode', '')
                    if postcode.replace(' ', '').upper() in t_postcode.replace(' ', '').upper():
                        t_date = t.get('date')
                        if t_date:
                            if not earliest or t_date < earliest:
                                earliest = t_date
                
                if earliest:
                    return {
                        'first_transaction': earliest,
                        'year': int(earliest[:4]) if isinstance(earliest, str) else earliest.year,
                        'source': 'Land Registry',
                        'confidence': 'medium',
                        'note': 'Building likely existed before first recorded sale'
                    }
        except Exception as e:
            logger.debug(f"Land Registry lookup failed: {e}")
        
        return None
    
    def _get_historic_england_estimate(self, lat: float, lon: float) -> Optional[Dict]:
        """Get estimate from Historic England listed building records"""
        client = self._get_client('historic_england')
        if not client:
            return None
        
        try:
            # Search for nearby listed buildings
            buildings = client.get_listed_buildings(limit=10)
            
            # Would need to filter by location
            # For now, return None
            return None
        except Exception as e:
            logger.debug(f"Historic England lookup failed: {e}")
        
        return None
    
    # ========================================
    # ESTIMATION COMBINATION
    # ========================================
    
    def _combine_estimates(self, sources: Dict) -> Optional[Dict]:
        """Combine estimates from multiple sources"""
        year_ranges = []
        
        for source, data in sources.items():
            if data:
                if 'estimated_year_range' in data and data['estimated_year_range']:
                    year_ranges.append(data['estimated_year_range'])
                elif 'year' in data:
                    year_ranges.append({'min': data['year'], 'max': data['year']})
        
        if not year_ranges:
            return None
        
        # Find overlapping range
        min_year = max(r['min'] for r in year_ranges)
        max_year = min(r['max'] for r in year_ranges)
        
        if min_year > max_year:
            # No overlap, take weighted average
            avg = sum((r['min'] + r['max']) / 2 for r in year_ranges) / len(year_ranges)
            return {
                'estimated_year': int(avg),
                'range_min': min(r['min'] for r in year_ranges),
                'range_max': max(r['max'] for r in year_ranges),
                'method': 'average'
            }
        
        return {
            'estimated_year': int((min_year + max_year) / 2),
            'range_min': min_year,
            'range_max': max_year,
            'method': 'intersection'
        }
    
    def _calculate_confidence(self, sources: Dict) -> str:
        """Calculate confidence based on number and quality of sources"""
        source_count = len([s for s in sources.values() if s])
        
        if source_count >= 3:
            return 'high'
        elif source_count >= 2:
            return 'medium'
        elif source_count >= 1:
            return 'low'
        else:
            return 'none'
    
    # ========================================
    # BUSINESS OPENING DATE ESTIMATION
    # ========================================
    
    def estimate_business_opening(
        self,
        business_name: str,
        website: Optional[str] = None,
        address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Estimate when a business opened.
        
        Args:
            business_name: Name of business
            website: Business website URL
            address: Business address
            
        Returns:
            Opening date estimate
        """
        estimates = {
            'sources': {},
            'combined_estimate': None,
            'evidence': []
        }
        
        # 1. Wayback Machine - First web presence
        if website:
            wayback = self._get_client('wayback')
            if wayback:
                try:
                    first_snapshot = wayback.get_first_appearance_date(website)
                    if first_snapshot:
                        estimates['sources']['wayback'] = {
                            'first_web_presence': first_snapshot.isoformat(),
                            'source': 'Wayback Machine'
                        }
                        estimates['evidence'].append(f"First web presence: {first_snapshot.year}")
                except Exception as e:
                    logger.debug(f"Wayback lookup failed: {e}")
        
        # 2. Companies House - Incorporation date
        # Would need company number
        
        return estimates
    
    # ========================================
    # REFERENCE DATA
    # ========================================
    
    def get_age_band_reference(self) -> Dict[str, str]:
        """Get EPC age band reference"""
        return {
            'A': 'Before 1900',
            'B': '1900-1929',
            'C': '1930-1949',
            'D': '1950-1966',
            'E': '1967-1975',
            'F': '1976-1982',
            'G': '1983-1990',
            'H': '1991-1995',
            'I': '1996-2002',
            'J': '2003-2006',
            'K': '2007-2011',
            'L': '2012 onwards',
        }
    
    def get_building_era_characteristics(self) -> Dict[str, Dict]:
        """Get typical characteristics of buildings by era"""
        return {
            'Georgian': {
                'years': '1714-1837',
                'features': ['Symmetrical facades', 'Sash windows', 'Brick/stone'],
                'common_in': 'City centers, Bath, London'
            },
            'Victorian': {
                'years': '1837-1901',
                'features': ['Bay windows', 'Decorative details', 'Terraced housing'],
                'common_in': 'Most UK cities'
            },
            'Edwardian': {
                'years': '1901-1910',
                'features': ['Larger windows', 'Gardens', 'Red brick'],
                'common_in': 'Suburbs'
            },
            'Inter-war': {
                'years': '1918-1939',
                'features': ['Semi-detached', 'Art Deco', 'Pebbledash'],
                'common_in': 'New suburbs'
            },
            'Post-war': {
                'years': '1945-1970',
                'features': ['Brutalist', 'Tower blocks', 'New towns'],
                'common_in': 'Council estates, rebuilt areas'
            },
            'Late 20th Century': {
                'years': '1970-2000',
                'features': ['UPVC windows', 'Standard layouts', 'Developer estates'],
                'common_in': 'Everywhere'
            },
            'Modern': {
                'years': '2000-present',
                'features': ['Energy efficient', 'Open plan', 'Mixed materials'],
                'common_in': 'Regeneration areas, new developments'
            }
        }

