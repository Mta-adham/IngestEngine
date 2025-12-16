"""
ONS (Office for National Statistics) Data Client
=================================================

Downloads and processes ONS statistical data including:
- Postcode Directory (ONSPD)
- Census data
- Indices of Multiple Deprivation (IMD)
- Geographic boundaries

API/Download: https://geoportal.statistics.gov.uk/

Usage:
    from src.clients import ONSClient
    
    client = ONSClient()
    postcodes = client.download_postcode_directory()
    imd = client.download_imd()
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from pathlib import Path
import logging
import os
import zipfile
import io
import requests

from src.clients.base_client import BaseAPIClient, APIError

logger = logging.getLogger(__name__)


class ONSClient(BaseAPIClient):
    """
    Client for ONS Open Geography Portal and Statistics.
    
    Most data is bulk download, some APIs available.
    """
    
    BASE_URL = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
    GEOPORTAL_URL = "https://geoportal.statistics.gov.uk"
    
    # Known dataset URLs
    DATASETS = {
        'onspd': 'https://www.arcgis.com/sharing/rest/content/items/eeb3a84a-8cdf-4e7c-84be-ed8f1272c09c/data',
        'imd_2019': 'https://assets.publishing.service.gov.uk/media/5d8b3b4bed915d0373c44f92/File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators.xlsx',
        'lsoa_boundaries': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V2/FeatureServer/0/query',
    }
    
    def __init__(self, data_dir: Optional[str] = None, **kwargs):
        """
        Initialize ONS client.
        
        Args:
            data_dir: Directory to store downloaded data
        """
        self.data_dir = Path(data_dir) if data_dir else Path('data/raw/ons')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        super().__init__(
            base_url=self.BASE_URL,
            api_key=None,
            rate_limit_rpm=60,
            **kwargs
        )
    
    def health_check(self) -> bool:
        """Check if services are available"""
        try:
            response = requests.head(self.GEOPORTAL_URL, timeout=10)
            return response.status_code == 200
        except Exception:
            return False
    
    # ========================================
    # POSTCODE DIRECTORY
    # ========================================
    
    def get_postcode_lookup(self, postcode: str) -> Dict:
        """
        Get geographic codes for a postcode via API.
        
        Args:
            postcode: UK postcode
            
        Returns:
            Dict with OA, LSOA, MSOA, LAD codes and coordinates
        """
        # Use Postcodes.io as a quick lookup
        postcode_clean = postcode.replace(' ', '').upper()
        
        response = requests.get(
            f"https://api.postcodes.io/postcodes/{postcode_clean}",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 200:
                result = data.get('result', {})
                return {
                    'postcode': result.get('postcode'),
                    'latitude': result.get('latitude'),
                    'longitude': result.get('longitude'),
                    'oa_code': result.get('codes', {}).get('oa'),
                    'lsoa_code': result.get('lsoa'),
                    'lsoa_name': result.get('lsoa'),
                    'msoa_code': result.get('msoa'),
                    'msoa_name': result.get('msoa'),
                    'lad_code': result.get('codes', {}).get('admin_district'),
                    'lad_name': result.get('admin_district'),
                    'region': result.get('region'),
                    'country': result.get('country'),
                    'parliamentary_constituency': result.get('parliamentary_constituency'),
                }
        
        return {}
    
    def bulk_postcode_lookup(self, postcodes: List[str]) -> pd.DataFrame:
        """
        Look up multiple postcodes.
        
        Args:
            postcodes: List of postcodes
            
        Returns:
            DataFrame with geographic codes
        """
        results = []
        
        # Batch lookup (100 at a time via postcodes.io)
        batch_size = 100
        
        for i in range(0, len(postcodes), batch_size):
            batch = postcodes[i:i + batch_size]
            
            try:
                response = requests.post(
                    "https://api.postcodes.io/postcodes",
                    json={'postcodes': batch},
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for item in data.get('result', []):
                        if item.get('result'):
                            r = item['result']
                            results.append({
                                'postcode': r.get('postcode'),
                                'latitude': r.get('latitude'),
                                'longitude': r.get('longitude'),
                                'oa_code': r.get('codes', {}).get('oa'),
                                'lsoa_code': r.get('lsoa'),
                                'msoa_code': r.get('msoa'),
                                'lad_code': r.get('codes', {}).get('admin_district'),
                                'lad_name': r.get('admin_district'),
                                'region': r.get('region'),
                            })
                        else:
                            results.append({
                                'postcode': item.get('query'),
                                'error': 'Not found'
                            })
                            
            except Exception as e:
                logger.warning(f"Batch lookup failed: {e}")
            
            logger.info(f"Processed {min(i + batch_size, len(postcodes))}/{len(postcodes)} postcodes")
        
        return pd.DataFrame(results)
    
    # ========================================
    # INDICES OF MULTIPLE DEPRIVATION
    # ========================================
    
    def download_imd(self, force: bool = False) -> pd.DataFrame:
        """
        Download Indices of Multiple Deprivation 2019.
        
        Args:
            force: Force re-download even if file exists
            
        Returns:
            DataFrame with IMD data by LSOA
        """
        cache_file = self.data_dir / 'imd_2019.csv'
        
        if cache_file.exists() and not force:
            logger.info(f"Loading cached IMD data from {cache_file}")
            return pd.read_csv(cache_file)
        
        logger.info("Downloading IMD 2019 data...")
        
        try:
            response = requests.get(self.DATASETS['imd_2019'], timeout=60)
            response.raise_for_status()
            
            # Read Excel file
            df = pd.read_excel(io.BytesIO(response.content), sheet_name=0)
            
            # Standardize column names
            df.columns = [c.strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]
            
            # Save to cache
            df.to_csv(cache_file, index=False)
            logger.info(f"Saved IMD data to {cache_file}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to download IMD: {e}")
            raise APIError(f"IMD download failed: {e}")
    
    def get_imd_for_lsoa(self, lsoa_code: str) -> Dict:
        """
        Get IMD data for a specific LSOA.
        
        Args:
            lsoa_code: LSOA code (e.g., "E01000001")
            
        Returns:
            IMD scores and ranks
        """
        df = self.download_imd()
        
        # Find LSOA code column
        lsoa_col = None
        for col in df.columns:
            if 'lsoa' in col.lower() and 'code' in col.lower():
                lsoa_col = col
                break
        
        if not lsoa_col:
            lsoa_col = df.columns[0]
        
        row = df[df[lsoa_col] == lsoa_code]
        
        if row.empty:
            return {}
        
        return row.iloc[0].to_dict()
    
    # ========================================
    # CENSUS DATA
    # ========================================
    
    def query_census_table(
        self,
        table_code: str,
        geography: str = "lsoa",
        area_codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Query Census 2021 data table.
        
        Args:
            table_code: Census table code (e.g., "TS001")
            geography: Geography level (oa, lsoa, msoa, lad)
            area_codes: Optional list of area codes to filter
            
        Returns:
            DataFrame with census data
        """
        # Census 2021 API endpoint
        api_url = "https://api.beta.ons.gov.uk/v1/population-types/UR/census-observations"
        
        params = {
            'dimensions': f'{geography},{table_code}',
        }
        
        try:
            response = requests.get(api_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            observations = data.get('observations', [])
            return pd.DataFrame(observations)
            
        except Exception as e:
            logger.warning(f"Census API query failed: {e}")
            return pd.DataFrame()
    
    # ========================================
    # GEOGRAPHIC BOUNDARIES
    # ========================================
    
    def get_lsoa_boundaries(
        self,
        area_codes: Optional[List[str]] = None,
        bbox: Optional[tuple] = None
    ) -> Any:
        """
        Get LSOA boundary polygons.
        
        Args:
            area_codes: Filter by LSOA codes
            bbox: Bounding box (min_x, min_y, max_x, max_y)
            
        Returns:
            GeoJSON or GeoDataFrame with boundaries
        """
        params = {
            'where': '1=1',
            'outFields': '*',
            'f': 'geojson',
            'outSR': '4326'
        }
        
        if area_codes:
            codes_str = "','".join(area_codes)
            params['where'] = f"LSOA21CD IN ('{codes_str}')"
        
        if bbox:
            params['geometry'] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            params['geometryType'] = 'esriGeometryEnvelope'
            params['spatialRel'] = 'esriSpatialRelIntersects'
        
        try:
            response = requests.get(
                self.DATASETS['lsoa_boundaries'],
                params=params,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get LSOA boundaries: {e}")
            return None
    
    # ========================================
    # LONDON DATA
    # ========================================
    
    def get_london_lsoas(self) -> pd.DataFrame:
        """
        Get all LSOA codes and data for London.
        
        Returns:
            DataFrame with London LSOAs and IMD data
        """
        # London borough codes (Local Authority District)
        london_lads = [
            'E09000001', 'E09000002', 'E09000003', 'E09000004', 'E09000005',
            'E09000006', 'E09000007', 'E09000008', 'E09000009', 'E09000010',
            'E09000011', 'E09000012', 'E09000013', 'E09000014', 'E09000015',
            'E09000016', 'E09000017', 'E09000018', 'E09000019', 'E09000020',
            'E09000021', 'E09000022', 'E09000023', 'E09000024', 'E09000025',
            'E09000026', 'E09000027', 'E09000028', 'E09000029', 'E09000030',
            'E09000031', 'E09000032', 'E09000033'
        ]
        
        # Get IMD data
        imd_df = self.download_imd()
        
        # Filter to London
        # Find LAD column
        lad_col = None
        for col in imd_df.columns:
            if 'local_authority' in col.lower() and 'code' in col.lower():
                lad_col = col
                break
        
        if lad_col:
            london_imd = imd_df[imd_df[lad_col].isin(london_lads)]
            return london_imd
        
        return imd_df
    
    def get_deprivation_summary(
        self,
        lsoa_codes: List[str]
    ) -> pd.DataFrame:
        """
        Get deprivation summary for multiple LSOAs.
        
        Args:
            lsoa_codes: List of LSOA codes
            
        Returns:
            DataFrame with IMD ranks and deciles
        """
        imd_df = self.download_imd()
        
        # Find LSOA code column
        lsoa_col = None
        for col in imd_df.columns:
            if 'lsoa' in col.lower() and 'code' in col.lower():
                lsoa_col = col
                break
        
        if not lsoa_col:
            lsoa_col = imd_df.columns[0]
        
        return imd_df[imd_df[lsoa_col].isin(lsoa_codes)]

