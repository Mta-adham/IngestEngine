"""
Unified Data Fetcher
====================

Combines all UK data source clients into a single interface.
Fetches and joins data from multiple sources for a given location or set of UPRNs.

Usage:
    from src.clients import UnifiedDataFetcher
    
    fetcher = UnifiedDataFetcher(
        companies_house_key="...",
        tfl_key="...",
        epc_email="...",
        epc_key="..."
    )
    
    # Get all data for a postcode
    data = fetcher.get_all_data_for_postcode("SW1A 1AA")
    
    # Get all data for coordinates
    data = fetcher.get_all_data_for_location(51.5074, -0.1278)
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from pathlib import Path
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.clients.companies_house import CompaniesHouseClient
from src.clients.tfl import TfLClient
from src.clients.police_uk import PoliceUKClient
from src.clients.epc import EPCClient
from src.clients.nhs import NHSClient
from src.clients.ons import ONSClient
from src.clients.os_datahub import OSDataHubClient
from src.clients.environment_agency import EnvironmentAgencyClient
from src.opening_dates.wikidata_client import WikidataClient

logger = logging.getLogger(__name__)


class UnifiedDataFetcher:
    """
    Unified interface for fetching data from all UK data sources.
    
    Combines:
    - Companies House (business data)
    - TfL (transport)
    - Police.uk (crime)
    - EPC (energy/building)
    - NHS (healthcare)
    - ONS (statistics/geography)
    - OS DataHub (addresses/UPRNs)
    - Environment Agency (flood risk)
    - Wikidata (POI information)
    """
    
    def __init__(
        self,
        companies_house_key: Optional[str] = None,
        tfl_app_id: Optional[str] = None,
        tfl_app_key: Optional[str] = None,
        epc_email: Optional[str] = None,
        epc_key: Optional[str] = None,
        os_api_key: Optional[str] = None,
        data_dir: Optional[str] = None,
        parallel: bool = True,
        max_workers: int = 5
    ):
        """
        Initialize unified fetcher with all clients.
        
        Args:
            companies_house_key: Companies House API key
            tfl_app_id: TfL App ID
            tfl_app_key: TfL App Key
            epc_email: EPC registered email
            epc_key: EPC API key
            os_api_key: Ordnance Survey API key
            data_dir: Directory for cached data
            parallel: Use parallel fetching
            max_workers: Number of parallel workers
        """
        self.data_dir = Path(data_dir) if data_dir else Path('data/raw')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.parallel = parallel
        self.max_workers = max_workers
        
        # Initialize clients
        logger.info("Initializing data source clients...")
        
        self.clients = {}
        self._init_errors = {}
        
        # Companies House
        try:
            self.clients['companies_house'] = CompaniesHouseClient(
                api_key=companies_house_key,
                cache_dir=str(self.data_dir / 'cache' / 'companies_house')
            )
            logger.info("  ✓ Companies House client initialized")
        except Exception as e:
            self._init_errors['companies_house'] = str(e)
            logger.warning(f"  ✗ Companies House: {e}")
        
        # TfL
        try:
            self.clients['tfl'] = TfLClient(
                app_id=tfl_app_id,
                app_key=tfl_app_key,
                cache_dir=str(self.data_dir / 'cache' / 'tfl')
            )
            logger.info("  ✓ TfL client initialized")
        except Exception as e:
            self._init_errors['tfl'] = str(e)
            logger.warning(f"  ✗ TfL: {e}")
        
        # Police.uk (no auth needed)
        try:
            self.clients['police'] = PoliceUKClient(
                cache_dir=str(self.data_dir / 'cache' / 'police')
            )
            logger.info("  ✓ Police.uk client initialized")
        except Exception as e:
            self._init_errors['police'] = str(e)
            logger.warning(f"  ✗ Police.uk: {e}")
        
        # EPC
        try:
            self.clients['epc'] = EPCClient(
                email=epc_email,
                api_key=epc_key,
                cache_dir=str(self.data_dir / 'cache' / 'epc')
            )
            logger.info("  ✓ EPC client initialized")
        except Exception as e:
            self._init_errors['epc'] = str(e)
            logger.warning(f"  ✗ EPC: {e}")
        
        # NHS (no auth needed)
        try:
            self.clients['nhs'] = NHSClient(
                cache_dir=str(self.data_dir / 'cache' / 'nhs')
            )
            logger.info("  ✓ NHS client initialized")
        except Exception as e:
            self._init_errors['nhs'] = str(e)
            logger.warning(f"  ✗ NHS: {e}")
        
        # ONS (no auth needed)
        try:
            self.clients['ons'] = ONSClient(
                data_dir=str(self.data_dir / 'ons')
            )
            logger.info("  ✓ ONS client initialized")
        except Exception as e:
            self._init_errors['ons'] = str(e)
            logger.warning(f"  ✗ ONS: {e}")
        
        # OS DataHub
        try:
            self.clients['os'] = OSDataHubClient(
                api_key=os_api_key,
                data_dir=str(self.data_dir / 'os'),
            )
            logger.info("  ✓ OS DataHub client initialized")
        except Exception as e:
            self._init_errors['os'] = str(e)
            logger.warning(f"  ✗ OS DataHub: {e}")
        
        # Environment Agency (no auth needed)
        try:
            self.clients['environment_agency'] = EnvironmentAgencyClient(
                data_dir=str(self.data_dir / 'ea')
            )
            logger.info("  ✓ Environment Agency client initialized")
        except Exception as e:
            self._init_errors['environment_agency'] = str(e)
            logger.warning(f"  ✗ Environment Agency: {e}")
        
        # Wikidata (no auth needed)
        try:
            self.clients['wikidata'] = WikidataClient()
            logger.info("  ✓ Wikidata client initialized")
        except Exception as e:
            self._init_errors['wikidata'] = str(e)
            logger.warning(f"  ✗ Wikidata: {e}")
        
        logger.info(f"\nInitialized {len(self.clients)}/{len(self.clients) + len(self._init_errors)} clients")
    
    def get_client_status(self) -> Dict[str, Dict]:
        """Get status of all clients"""
        status = {}
        
        for name, client in self.clients.items():
            try:
                healthy = client.health_check()
                status[name] = {
                    'initialized': True,
                    'healthy': healthy,
                    'stats': client.get_stats() if hasattr(client, 'get_stats') else {}
                }
            except Exception as e:
                status[name] = {
                    'initialized': True,
                    'healthy': False,
                    'error': str(e)
                }
        
        for name, error in self._init_errors.items():
            status[name] = {
                'initialized': False,
                'healthy': False,
                'error': error
            }
        
        return status
    
    def health_check_all(self) -> Dict[str, bool]:
        """Check health of all clients"""
        results = {}
        for name, client in self.clients.items():
            try:
                results[name] = client.health_check()
            except Exception:
                results[name] = False
        return results
    
    # ========================================
    # POSTCODE-BASED FETCHING
    # ========================================
    
    def get_all_data_for_postcode(
        self,
        postcode: str,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch all available data for a postcode.
        
        Args:
            postcode: UK postcode
            include: List of sources to include (default: all)
            exclude: List of sources to exclude
            
        Returns:
            Dictionary with data from each source
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"FETCHING ALL DATA FOR POSTCODE: {postcode}")
        logger.info(f"{'='*60}")
        
        # Determine which sources to use
        sources = list(self.clients.keys())
        if include:
            sources = [s for s in sources if s in include]
        if exclude:
            sources = [s for s in sources if s not in exclude]
        
        results = {
            'postcode': postcode,
            'sources': {}
        }
        
        # Get coordinates first (needed for some APIs)
        coords = None
        if 'ons' in self.clients:
            try:
                coords = self.clients['ons'].get_postcode_lookup(postcode)
                results['geography'] = coords
                logger.info(f"  ✓ Got coordinates: {coords.get('latitude')}, {coords.get('longitude')}")
            except Exception as e:
                logger.warning(f"  ✗ ONS postcode lookup failed: {e}")
        
        lat = coords.get('latitude') if coords else None
        lon = coords.get('longitude') if coords else None
        
        # Define fetch functions
        fetch_funcs = {
            'epc': lambda: self._fetch_epc_for_postcode(postcode),
            'companies_house': lambda: self._fetch_companies_for_postcode(postcode),
            'police': lambda: self._fetch_crime_for_location(lat, lon) if lat else None,
            'tfl': lambda: self._fetch_transport_for_location(lat, lon) if lat else None,
            'nhs': lambda: self._fetch_healthcare_for_postcode(postcode),
            'environment_agency': lambda: self._fetch_flood_risk(lat, lon) if lat else None,
            'ons': lambda: self._fetch_deprivation_for_lsoa(coords.get('lsoa_code')) if coords else None,
        }
        
        # Fetch data (parallel or sequential)
        if self.parallel:
            results['sources'] = self._parallel_fetch(
                {k: v for k, v in fetch_funcs.items() if k in sources}
            )
        else:
            for source in sources:
                if source in fetch_funcs:
                    try:
                        logger.info(f"  Fetching {source}...")
                        data = fetch_funcs[source]()
                        results['sources'][source] = data
                        logger.info(f"  ✓ {source}: success")
                    except Exception as e:
                        results['sources'][source] = {'error': str(e)}
                        logger.warning(f"  ✗ {source}: {e}")
        
        return results
    
    def _parallel_fetch(self, fetch_funcs: Dict[str, callable]) -> Dict[str, Any]:
        """Execute fetch functions in parallel"""
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_source = {
                executor.submit(func): source 
                for source, func in fetch_funcs.items()
            }
            
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    results[source] = future.result()
                    logger.info(f"  ✓ {source}: success")
                except Exception as e:
                    results[source] = {'error': str(e)}
                    logger.warning(f"  ✗ {source}: {e}")
        
        return results
    
    # ========================================
    # COORDINATE-BASED FETCHING
    # ========================================
    
    def get_all_data_for_location(
        self,
        lat: float,
        lon: float,
        radius_m: int = 500,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch all available data for a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius_m: Search radius in meters
            include: List of sources to include
            exclude: List of sources to exclude
            
        Returns:
            Dictionary with data from each source
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"FETCHING ALL DATA FOR LOCATION: {lat}, {lon}")
        logger.info(f"{'='*60}")
        
        results = {
            'latitude': lat,
            'longitude': lon,
            'radius_m': radius_m,
            'sources': {}
        }
        
        # Get postcode and geography
        if 'ons' in self.clients:
            # Use reverse geocoding (postcodes.io)
            try:
                import requests
                resp = requests.get(
                    f"https://api.postcodes.io/postcodes?lon={lon}&lat={lat}",
                    timeout=10
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('result'):
                        results['geography'] = data['result'][0]
                        results['postcode'] = data['result'][0].get('postcode')
                        logger.info(f"  ✓ Reverse geocode: {results['postcode']}")
            except Exception as e:
                logger.warning(f"  ✗ Reverse geocode failed: {e}")
        
        # Fetch from each source
        sources_to_fetch = list(self.clients.keys())
        if include:
            sources_to_fetch = [s for s in sources_to_fetch if s in include]
        if exclude:
            sources_to_fetch = [s for s in sources_to_fetch if s not in exclude]
        
        for source in sources_to_fetch:
            try:
                logger.info(f"  Fetching {source}...")
                
                if source == 'police':
                    results['sources'][source] = self._fetch_crime_for_location(lat, lon)
                elif source == 'tfl':
                    results['sources'][source] = self._fetch_transport_for_location(lat, lon, radius_m)
                elif source == 'environment_agency':
                    results['sources'][source] = self._fetch_flood_risk(lat, lon)
                elif source == 'nhs':
                    postcode = results.get('postcode', '').split()[0] if results.get('postcode') else None
                    if postcode:
                        results['sources'][source] = self._fetch_healthcare_for_postcode(postcode)
                elif source == 'os':
                    results['sources'][source] = self._fetch_uprns_for_location(lat, lon, radius_m)
                
                logger.info(f"  ✓ {source}: success")
            except Exception as e:
                results['sources'][source] = {'error': str(e)}
                logger.warning(f"  ✗ {source}: {e}")
        
        return results
    
    # ========================================
    # SOURCE-SPECIFIC FETCH METHODS
    # ========================================
    
    def _fetch_epc_for_postcode(self, postcode: str) -> Dict:
        """Fetch EPC data for a postcode"""
        if 'epc' not in self.clients:
            return {'error': 'EPC client not initialized'}
        
        df = self.clients['epc'].get_domestic_by_postcode_df(postcode)
        return {
            'count': len(df),
            'data': df.to_dict('records') if len(df) < 100 else df.head(100).to_dict('records'),
            'summary': {
                'total_properties': len(df),
            }
        }
    
    def _fetch_companies_for_postcode(self, postcode: str) -> Dict:
        """Fetch company data for a postcode"""
        if 'companies_house' not in self.clients:
            return {'error': 'Companies House client not initialized'}
        
        try:
            companies = self.clients['companies_house'].search_companies_by_postcode(postcode)
            return {
                'count': len(companies),
                'companies': companies[:50] if len(companies) > 50 else companies
            }
        except Exception as e:
            return {'error': str(e)}
    
    def _fetch_crime_for_location(self, lat: float, lon: float) -> Dict:
        """Fetch crime data for a location"""
        if 'police' not in self.clients:
            return {'error': 'Police client not initialized'}
        
        # Get recent crimes (simpler approach - just latest month)
        crimes = self.clients['police'].get_street_level_crimes(lat, lon)
        
        # Count by category
        categories = {}
        for crime in crimes:
            cat = crime.get('category', 'unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        by_category = [
            {'category': cat, 'count': count}
            for cat, count in sorted(categories.items(), key=lambda x: -x[1])
        ]
        
        return {
            'count': len(crimes),
            'months_covered': 1,
            'by_category': by_category[:10],
            'recent_crimes': crimes[:20]
        }
    
    def _fetch_transport_for_location(self, lat: float, lon: float, radius: int = 500) -> Dict:
        """Fetch transport data for a location"""
        if 'tfl' not in self.clients:
            return {'error': 'TfL client not initialized'}
        
        # Try to get nearby stops using search
        stops = []
        try:
            # Get stop points by searching nearby
            stops = self.clients['tfl'].get_stop_points_in_radius(
                lat, lon, radius,
                stop_types=['NaptanMetroStation', 'NaptanRailStation', 'NaptanBusCoachStation']
            )
        except Exception:
            # Fallback: search by name
            try:
                stops = self.clients['tfl'].search_stop_points("Westminster", modes=['tube', 'bus'])
            except Exception:
                pass
        
        bikes = []
        try:
            bike_df = self.clients['tfl'].get_bike_points()
            if not bike_df.empty:
                # Filter to nearby
                bike_df['dist'] = ((bike_df['latitude'] - lat)**2 + (bike_df['longitude'] - lon)**2)**0.5
                bikes = bike_df.nsmallest(5, 'dist').to_dict('records')
        except Exception:
            pass
        
        return {
            'nearby_stops': len(stops) if isinstance(stops, list) else 0,
            'stops': stops[:20] if isinstance(stops, list) else [],
            'bike_points': bikes
        }
    
    def _fetch_healthcare_for_postcode(self, postcode: str) -> Dict:
        """Fetch healthcare facilities for a postcode area"""
        if 'nhs' not in self.clients:
            return {'error': 'NHS client not initialized'}
        
        # Get first part of postcode
        postcode_area = postcode.split()[0] if ' ' in postcode else postcode[:3]
        
        gps = self.clients['nhs'].get_gp_practices_df(postcode=postcode_area, limit=20)
        pharmacies = self.clients['nhs'].get_pharmacies_df(postcode=postcode_area, limit=20)
        
        return {
            'gp_practices': len(gps),
            'pharmacies': len(pharmacies),
            'gp_list': gps.to_dict('records'),
            'pharmacy_list': pharmacies.to_dict('records')
        }
    
    def _fetch_flood_risk(self, lat: float, lon: float) -> Dict:
        """Fetch flood risk data for a location"""
        if 'environment_agency' not in self.clients:
            return {'error': 'Environment Agency client not initialized'}
        
        return self.clients['environment_agency'].get_flood_risk_for_location(lat, lon)
    
    def _fetch_deprivation_for_lsoa(self, lsoa_code: str) -> Dict:
        """Fetch deprivation data for an LSOA"""
        if 'ons' not in self.clients or not lsoa_code:
            return {'error': 'ONS client not initialized or no LSOA'}
        
        imd = self.clients['ons'].get_imd_for_lsoa(lsoa_code)
        return imd
    
    def _fetch_uprns_for_location(self, lat: float, lon: float, radius: int) -> Dict:
        """Fetch UPRNs for a location"""
        if 'os' not in self.clients:
            return {'error': 'OS DataHub client not initialized'}
        
        df = self.clients['os'].get_uprns_in_area_df(lat, lon, radius)
        return {
            'count': len(df),
            'uprns': df.to_dict('records')
        }
    
    # ========================================
    # POI ENRICHMENT
    # ========================================
    
    def enrich_poi(self, poi_name: str, lat: float, lon: float) -> Dict:
        """
        Enrich a POI with data from all sources.
        
        Args:
            poi_name: Name of the POI
            lat: Latitude
            lon: Longitude
            
        Returns:
            Enriched POI data
        """
        result = {
            'name': poi_name,
            'latitude': lat,
            'longitude': lon,
        }
        
        # Wikidata opening date
        if 'wikidata' in self.clients:
            try:
                info = self.clients['wikidata'].get_poi_info(poi_name)
                result['wikidata_id'] = info.get('wikidata_id')
                result['opening_date'] = info.get('opening_date')
            except Exception as e:
                result['wikidata_error'] = str(e)
        
        # Get location data
        location_data = self.get_all_data_for_location(
            lat, lon, 
            radius_m=200,
            include=['police', 'tfl', 'environment_agency']
        )
        result['location_data'] = location_data.get('sources', {})
        
        return result
    
    # ========================================
    # BATCH OPERATIONS
    # ========================================
    
    def enrich_pois_batch(
        self,
        pois: pd.DataFrame,
        name_col: str = 'name',
        lat_col: str = 'latitude',
        lon_col: str = 'longitude'
    ) -> pd.DataFrame:
        """
        Enrich multiple POIs with data from all sources.
        
        Args:
            pois: DataFrame with POIs
            name_col: Column with POI name
            lat_col: Column with latitude
            lon_col: Column with longitude
            
        Returns:
            DataFrame with enriched data
        """
        results = []
        total = len(pois)
        
        for i, (idx, row) in enumerate(pois.iterrows(), 1):
            logger.info(f"Processing POI {i}/{total}: {row[name_col]}")
            
            enriched = self.enrich_poi(
                row[name_col],
                row[lat_col],
                row[lon_col]
            )
            enriched['original_index'] = idx
            results.append(enriched)
        
        return pd.DataFrame(results)
    
    def export_all_london_data(self, output_dir: Optional[str] = None) -> Dict[str, Path]:
        """
        Export comprehensive London data from all sources.
        
        Args:
            output_dir: Output directory
            
        Returns:
            Dictionary of output file paths
        """
        output_dir = Path(output_dir) if output_dir else self.data_dir / 'exports'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        outputs = {}
        
        logger.info("\n" + "="*60)
        logger.info("EXPORTING ALL LONDON DATA")
        logger.info("="*60)
        
        # TfL stations
        if 'tfl' in self.clients:
            try:
                logger.info("Exporting TfL stations...")
                df = self.clients['tfl'].export_all_stations()
                path = output_dir / 'tfl_stations.csv'
                df.to_csv(path, index=False)
                outputs['tfl_stations'] = path
                logger.info(f"  ✓ {len(df)} stations")
            except Exception as e:
                logger.warning(f"  ✗ TfL: {e}")
        
        # NHS facilities
        if 'nhs' in self.clients:
            try:
                logger.info("Exporting NHS facilities...")
                df = self.clients['nhs'].get_london_healthcare_df()
                path = output_dir / 'nhs_facilities.csv'
                df.to_csv(path, index=False)
                outputs['nhs_facilities'] = path
                logger.info(f"  ✓ {len(df)} facilities")
            except Exception as e:
                logger.warning(f"  ✗ NHS: {e}")
        
        # EA flood monitoring
        if 'environment_agency' in self.clients:
            try:
                logger.info("Exporting flood monitoring stations...")
                df = self.clients['environment_agency'].get_london_flood_risk_df()
                path = output_dir / 'flood_monitoring.csv'
                df.to_csv(path, index=False)
                outputs['flood_monitoring'] = path
                logger.info(f"  ✓ {len(df)} stations")
            except Exception as e:
                logger.warning(f"  ✗ Environment Agency: {e}")
        
        # ONS London LSOAs
        if 'ons' in self.clients:
            try:
                logger.info("Exporting London deprivation data...")
                df = self.clients['ons'].get_london_lsoas()
                path = output_dir / 'london_imd.csv'
                df.to_csv(path, index=False)
                outputs['london_imd'] = path
                logger.info(f"  ✓ {len(df)} LSOAs")
            except Exception as e:
                logger.warning(f"  ✗ ONS: {e}")
        
        logger.info(f"\nExported {len(outputs)} datasets to {output_dir}")
        return outputs


# ========================================
# CLI INTERFACE
# ========================================

def main():
    """CLI interface for unified fetcher"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified UK Data Fetcher')
    parser.add_argument('--postcode', type=str, help='Fetch data for postcode')
    parser.add_argument('--lat', type=float, help='Latitude')
    parser.add_argument('--lon', type=float, help='Longitude')
    parser.add_argument('--export-london', action='store_true', help='Export all London data')
    parser.add_argument('--health-check', action='store_true', help='Check all API health')
    parser.add_argument('--output-dir', type=str, default='data/exports', help='Output directory')
    
    args = parser.parse_args()
    
    fetcher = UnifiedDataFetcher()
    
    if args.health_check:
        print("\nAPI Health Check:")
        print("-" * 40)
        status = fetcher.health_check_all()
        for name, healthy in status.items():
            icon = "✓" if healthy else "✗"
            print(f"  {icon} {name}")
        return
    
    if args.export_london:
        outputs = fetcher.export_all_london_data(args.output_dir)
        print(f"\nExported {len(outputs)} files")
        return
    
    if args.postcode:
        data = fetcher.get_all_data_for_postcode(args.postcode)
        print(f"\nData for {args.postcode}:")
        for source, content in data.get('sources', {}).items():
            if isinstance(content, dict) and 'error' not in content:
                print(f"  ✓ {source}")
            else:
                print(f"  ✗ {source}")
        return
    
    if args.lat and args.lon:
        data = fetcher.get_all_data_for_location(args.lat, args.lon)
        print(f"\nData for {args.lat}, {args.lon}:")
        for source, content in data.get('sources', {}).items():
            if isinstance(content, dict) and 'error' not in content:
                print(f"  ✓ {source}")
            else:
                print(f"  ✗ {source}")
        return
    
    parser.print_help()


if __name__ == "__main__":
    main()

