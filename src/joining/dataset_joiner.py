"""
Dataset Joiner - Join Infrastructure, EPC, and POI datasets
Creates useful joined datasets for analysis
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, Tuple, Optional
from geopy.distance import geodesic
import warnings
warnings.filterwarnings('ignore')

try:
    import geopandas as gpd
    from geopandas import points_from_xy
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
    warnings.warn("GeoPandas not available. Spatial joins will be disabled.")


class DatasetJoiner:
    """Join multiple datasets using various strategies"""
    
    def __init__(self, infrastructure_path: str, epc_path: str, pois_path: str):
        """
        Initialize the joiner
        
        Args:
            infrastructure_path: Path to infrastructure dataset
            epc_path: Path to EPC dataset
            pois_path: Path to POIs dataset
        """
        self.infrastructure_path = infrastructure_path
        self.epc_path = epc_path
        self.pois_path = pois_path
        
        self.infrastructure_df = None
        self.epc_df = None
        self.pois_df = None
    
    def load_datasets(self):
        """Load all three datasets"""
        print("=" * 70)
        print("LOADING DATASETS")
        print("=" * 70)
        
        print("\n1. Loading Infrastructure dataset...")
        self.infrastructure_df = pd.read_csv(self.infrastructure_path, low_memory=False)
        print(f"   Loaded {len(self.infrastructure_df):,} records")
        
        print("\n2. Loading EPC dataset...")
        self.epc_df = pd.read_csv(self.epc_path, low_memory=False)
        print(f"   Loaded {len(self.epc_df):,} records")
        
        print("\n3. Loading POIs dataset...")
        self.pois_df = pd.read_csv(self.pois_path, low_memory=False)
        print(f"   Loaded {len(self.pois_df):,} records")
        
        return self.infrastructure_df, self.epc_df, self.pois_df
    
    def normalize_postcode(self, postcode: str) -> Optional[str]:
        """Normalize postcode for matching"""
        if pd.isna(postcode):
            return None
        postcode_str = str(postcode).upper().strip()
        # Remove spaces and format
        postcode_str = postcode_str.replace(' ', '')
        if len(postcode_str) >= 5:
            # Format as SW1A1AA
            return postcode_str
        return None
    
    def normalize_uprn(self, uprn) -> Optional[float]:
        """Normalize UPRN for matching"""
        if pd.isna(uprn):
            return None
        try:
            # Convert to float/int for consistent matching
            uprn_val = float(uprn)
            # UPRNs are typically positive integers
            if uprn_val > 0:
                return uprn_val
        except (ValueError, TypeError):
            pass
        return None
    
    def normalize_text(self, text: str) -> Optional[str]:
        """Normalize text for matching (lowercase, strip, remove special chars)"""
        if pd.isna(text):
            return None
        text_str = str(text).lower().strip()
        # Remove extra spaces and special characters for matching
        text_str = ' '.join(text_str.split())
        return text_str if text_str else None
    
    def join_by_multiple_columns(self, df1: pd.DataFrame, df2: pd.DataFrame,
                                 join_columns: Dict[str, Tuple[str, str]],
                                 min_confidence: int = 1,
                                 suffixes: Tuple[str, str] = ('_1', '_2')) -> pd.DataFrame:
        """
        Join datasets using multiple columns with confidence scoring
        
        Args:
            df1: First DataFrame
            df2: Second DataFrame
            join_columns: Dict mapping column names to (col1, col2) tuples
                         e.g., {'postcode': ('postcode', 'addr:postcode'),
                                'address': ('ADDRESS', 'addr:street')}
            min_confidence: Minimum number of matching columns required (1- len(join_columns))
            suffixes: Suffixes for overlapping column names
        
        Returns:
            Joined DataFrame with confidence_score column
        """
        print("\n" + "=" * 70)
        print("JOINING BY MULTIPLE COLUMNS")
        print("=" * 70)
        
        print(f"\nJoin columns: {list(join_columns.keys())}")
        print(f"Minimum confidence (matching columns): {min_confidence}/{len(join_columns)}")
        
        # Prepare normalized columns for both datasets
        df1_clean = df1.copy()
        df2_clean = df2.copy()
        
        normalized_cols = {}
        
        for col_name, (col1, col2) in join_columns.items():
            # Check if columns exist
            if col1 not in df1_clean.columns:
                print(f"  ⚠ Column '{col1}' not found in dataset 1")
                continue
            if col2 not in df2_clean.columns:
                print(f"  ⚠ Column '{col2}' not found in dataset 2")
                continue
            
            # Normalize based on column type
            if 'uprn' in col_name.lower():
                df1_clean[f'{col_name}_norm'] = df1_clean[col1].apply(self.normalize_uprn)
                df2_clean[f'{col_name}_norm'] = df2_clean[col2].apply(self.normalize_uprn)
            elif 'postcode' in col_name.lower():
                df1_clean[f'{col_name}_norm'] = df1_clean[col1].apply(self.normalize_postcode)
                df2_clean[f'{col_name}_norm'] = df2_clean[col2].apply(self.normalize_postcode)
            else:
                # Text normalization
                df1_clean[f'{col_name}_norm'] = df1_clean[col1].apply(self.normalize_text)
                df2_clean[f'{col_name}_norm'] = df2_clean[col2].apply(self.normalize_text)
            
            normalized_cols[col_name] = f'{col_name}_norm'
            
            # Count non-null values
            df1_count = df1_clean[f'{col_name}_norm'].notna().sum()
            df2_count = df2_clean[f'{col_name}_norm'].notna().sum()
            print(f"  {col_name:20s}: Dataset1={df1_count:,}/{len(df1_clean):,}, Dataset2={df2_count:,}/{len(df2_clean):,}")
        
        if not normalized_cols:
            print("  ⚠ No valid join columns found")
            return pd.DataFrame()
        
        # Start with first column join
        first_col = list(normalized_cols.values())[0]
        print(f"\n  Starting with base join on: {list(normalized_cols.keys())[0]}")
        
        # Initial join on first column
        joined = pd.merge(
            df1_clean,
            df2_clean,
            left_on=first_col,
            right_on=first_col,
            how='inner',
            suffixes=suffixes
        )
        print(f"  Base join matches: {len(joined):,}")
        
        if len(joined) == 0:
            return pd.DataFrame()
        
        # Calculate confidence score for each match
        print(f"  Calculating confidence scores...")
        
        # Build confidence scores vectorized
        score = pd.Series([1] * len(joined), index=joined.index)  # First column already matched
        
        for col_name, norm_col in list(normalized_cols.items())[1:]:  # Skip first (already matched)
            # Get the normalized column names with suffixes
            col1_name = f'{norm_col}{suffixes[0]}'
            col2_name = f'{norm_col}{suffixes[1]}'
            
            # Check if columns exist (they should after merge)
            if col1_name in joined.columns and col2_name in joined.columns:
                # Vectorized comparison
                both_notna = joined[col1_name].notna() & joined[col2_name].notna()
                matches = (joined[col1_name] == joined[col2_name]) & both_notna
                score += matches.astype(int)
        
        joined['confidence_score'] = score.values
        joined['confidence_level'] = joined['confidence_score'].apply(
            lambda x: f"{x}/{len(normalized_cols)}"
        )
        
        # Filter by minimum confidence
        if min_confidence > 1:
            before = len(joined)
            joined = joined[joined['confidence_score'] >= min_confidence]
            print(f"  After filtering (min_confidence={min_confidence}): {len(joined):,} (removed {before - len(joined):,})")
        
        # Summary statistics
        print(f"\n  Confidence score distribution:")
        score_dist = joined['confidence_score'].value_counts().sort_index()
        for score, count in score_dist.items():
            pct = count / len(joined) * 100
            print(f"    {score}/{len(normalized_cols)} columns match: {count:,} ({pct:.1f}%)")
        
        print(f"\n  ✓ Final joined records: {len(joined):,}")
        
        return joined
    
    def join_by_uprn(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                     uprn_col1: str = 'UPRN', uprn_col2: str = 'UPRN',
                     suffixes: Tuple[str, str] = ('_1', '_2')) -> pd.DataFrame:
        """
        Join two datasets by UPRN (Unique Property Reference Number)
        
        Args:
            df1: First DataFrame
            df2: Second DataFrame
            uprn_col1: UPRN column name in first DataFrame
            uprn_col2: UPRN column name in second DataFrame
            suffixes: Suffixes for overlapping column names
        
        Returns:
            Joined DataFrame
        """
        print("\n" + "=" * 70)
        print("JOINING BY UPRN")
        print("=" * 70)
        
        # Check if UPRN columns exist
        if uprn_col1 not in df1.columns:
            print(f"  ⚠ Column '{uprn_col1}' not found in first dataset")
            print(f"  Available columns: {[c for c in df1.columns if 'uprn' in c.lower() or 'property' in c.lower() or 'reference' in c.lower()]}")
            return pd.DataFrame()
        
        if uprn_col2 not in df2.columns:
            print(f"  ⚠ Column '{uprn_col2}' not found in second dataset")
            print(f"  Available columns: {[c for c in df2.columns if 'uprn' in c.lower() or 'property' in c.lower() or 'reference' in c.lower()]}")
            return pd.DataFrame()
        
        # Prepare UPRNs
        print("\nPreparing UPRNs...")
        
        df1_clean = df1.copy()
        df1_clean['uprn_normalized'] = df1_clean[uprn_col1].apply(self.normalize_uprn)
        df1_with_uprn = df1_clean[df1_clean['uprn_normalized'].notna()].copy()
        print(f"  Dataset 1 records with UPRN: {len(df1_with_uprn):,} / {len(df1):,} ({len(df1_with_uprn)/len(df1)*100:.1f}%)")
        print(f"  Unique UPRNs in dataset 1: {df1_with_uprn['uprn_normalized'].nunique():,}")
        
        df2_clean = df2.copy()
        df2_clean['uprn_normalized'] = df2_clean[uprn_col2].apply(self.normalize_uprn)
        df2_with_uprn = df2_clean[df2_clean['uprn_normalized'].notna()].copy()
        print(f"  Dataset 2 records with UPRN: {len(df2_with_uprn):,} / {len(df2):,} ({len(df2_with_uprn)/len(df2)*100:.1f}%)")
        print(f"  Unique UPRNs in dataset 2: {df2_with_uprn['uprn_normalized'].nunique():,}")
        
        # Join by normalized UPRN
        print("\nJoining by UPRN...")
        joined = pd.merge(
            df1_with_uprn,
            df2_with_uprn,
            on='uprn_normalized',
            how='inner',
            suffixes=suffixes
        )
        
        print(f"  ✓ Joined records: {len(joined):,}")
        
        if len(joined) > 0:
            # Check for multiple matches per UPRN
            uprn_counts = joined['uprn_normalized'].value_counts()
            multiple_matches = (uprn_counts > 1).sum()
            if multiple_matches > 0:
                print(f"  ⚠ {multiple_matches:,} UPRNs have multiple matches (one-to-many join)")
                print(f"     Max matches per UPRN: {uprn_counts.max()}")
        
        return joined
    
    def join_osm_to_uprn_spatial(self, osm_df: pd.DataFrame = None, 
                                  uprn_path: str = None,
                                  osm_lon_col: str = 'longitude',
                                  osm_lat_col: str = 'latitude',
                                  max_distance_meters: float = 15.0,
                                  uprn_crs: str = 'EPSG:27700') -> pd.DataFrame:
        """
        Join OSM POIs to UPRN using spatial nearest-neighbor join
        
        Args:
            osm_df: DataFrame with OSM POI data (default: self.pois_df)
            uprn_path: Path to UPRN GeoPackage/Shapefile/CSV file
            osm_lon_col: Column name for longitude in OSM data
            osm_lat_col: Column name for latitude in OSM data
            max_distance_meters: Maximum distance in meters for matching
            uprn_crs: CRS of UPRN data (default: EPSG:27700 for OS Open UPRN)
        
        Returns:
            Joined DataFrame with UPRN attached to OSM POIs
        """
        if not GEOPANDAS_AVAILABLE:
            print("  ⚠ GeoPandas not available. Install with: pip install geopandas")
            return pd.DataFrame()
        
        print("\n" + "=" * 70)
        print("SPATIAL JOIN: OSM POIs TO UPRN")
        print("=" * 70)
        
        # Use default OSM data if not provided
        if osm_df is None:
            if self.pois_df is None:
                print("  ⚠ No OSM data available. Load datasets first.")
                return pd.DataFrame()
            osm_df = self.pois_df
        
        # Check if UPRN path is provided
        if uprn_path is None:
            print("  ⚠ UPRN file path not provided.")
            print("  Expected: Path to OS Open UPRN GeoPackage/Shapefile/CSV")
            print("  Download from: https://osdatahub.os.uk/downloads/open/OpenUPRN")
            return pd.DataFrame()
        
        if not os.path.exists(uprn_path):
            print(f"  ⚠ UPRN file not found: {uprn_path}")
            return pd.DataFrame()
        
        # Check OSM columns
        if osm_lon_col not in osm_df.columns or osm_lat_col not in osm_df.columns:
            print(f"  ⚠ OSM columns not found: {osm_lon_col}, {osm_lat_col}")
            print(f"  Available columns: {list(osm_df.columns)[:10]}...")
            return pd.DataFrame()
        
        print(f"\n1. Loading and preparing OSM POIs...")
        # Filter OSM data to records with valid coordinates
        osm_clean = osm_df[
            (osm_df[osm_lon_col].notna()) & 
            (osm_df[osm_lat_col].notna())
        ].copy()
        print(f"   OSM records with coordinates: {len(osm_clean):,} / {len(osm_df):,}")
        
        # Create GeoDataFrame from OSM (assume WGS84)
        gdf_osm = gpd.GeoDataFrame(
            osm_clean,
            geometry=points_from_xy(osm_clean[osm_lon_col], osm_clean[osm_lat_col]),
            crs="EPSG:4326"
        )
        print(f"   Created OSM GeoDataFrame (EPSG:4326)")
        
        print(f"\n2. Loading UPRN data...")
        # Load UPRN data
        try:
            if uprn_path.endswith('.csv'):
                # If CSV, assume it has Easting/Northing or lon/lat columns
                uprn_df = pd.read_csv(uprn_path, low_memory=False)
                # Try to detect coordinate columns
                if 'Easting' in uprn_df.columns and 'Northing' in uprn_df.columns:
                    gdf_uprn = gpd.GeoDataFrame(
                        uprn_df,
                        geometry=points_from_xy(uprn_df['Easting'], uprn_df['Northing']),
                        crs=uprn_crs
                    )
                elif 'lon' in uprn_df.columns.lower() or 'longitude' in uprn_df.columns:
                    lon_col = [c for c in uprn_df.columns if 'lon' in c.lower()][0]
                    lat_col = [c for c in uprn_df.columns if 'lat' in c.lower()][0]
                    gdf_uprn = gpd.GeoDataFrame(
                        uprn_df,
                        geometry=points_from_xy(uprn_df[lon_col], uprn_df[lat_col]),
                        crs="EPSG:4326"
                    )
                else:
                    print(f"  ⚠ Could not detect coordinate columns in UPRN CSV")
                    print(f"  Expected: Easting/Northing or lon/lat columns")
                    return pd.DataFrame()
            else:
                # GeoPackage or Shapefile
                gdf_uprn = gpd.read_file(uprn_path)
                if gdf_uprn.crs is None:
                    print(f"  ⚠ UPRN file has no CRS, assuming {uprn_crs}")
                    gdf_uprn.set_crs(uprn_crs, inplace=True)
        except Exception as e:
            print(f"  ⚠ Error loading UPRN file: {str(e)}")
            return pd.DataFrame()
        
        print(f"   Loaded {len(gdf_uprn):,} UPRN records")
        print(f"   UPRN CRS: {gdf_uprn.crs}")
        
        # Check for UPRN column
        uprn_col = None
        for col in ['UPRN', 'uprn', 'UPRN_SOURCE']:
            if col in gdf_uprn.columns:
                uprn_col = col
                break
        
        if uprn_col is None:
            print(f"  ⚠ UPRN column not found. Available columns: {list(gdf_uprn.columns)[:10]}")
            return pd.DataFrame()
        
        print(f"\n3. Reprojecting to metric CRS for distance calculation...")
        # Reproject both to a metric CRS (British National Grid) for accurate distance
        gdf_osm_m = gdf_osm.to_crs("EPSG:27700")
        gdf_uprn_m = gdf_uprn.to_crs("EPSG:27700")
        print(f"   Reprojected to EPSG:27700 (British National Grid)")
        
        print(f"\n4. Performing nearest-neighbor spatial join...")
        print(f"   Max distance: {max_distance_meters}m")
        
        # Keep only UPRN column and geometry for join
        gdf_uprn_small = gdf_uprn_m[[uprn_col, 'geometry']].copy()
        
        # Perform nearest-neighbor join
        joined = gpd.sjoin_nearest(
            gdf_osm_m,
            gdf_uprn_small,
            how="left",
            distance_col="uprn_distance_m",
            max_distance=max_distance_meters
        )
        
        print(f"   Initial matches: {len(joined):,}")
        
        # Filter by distance
        before_filter = len(joined)
        joined = joined[joined['uprn_distance_m'].notna()].copy()
        print(f"   After distance filter: {len(joined):,} (removed {before_filter - len(joined):,})")
        
        # Reproject back to WGS84 for consistency
        print(f"\n5. Reprojecting back to WGS84...")
        joined = joined.to_crs("EPSG:4326")
        
        # Convert back to regular DataFrame (drop geometry if not needed)
        # Keep geometry as WKT or lat/lon
        if 'geometry' in joined.columns:
            joined['geometry_wkt'] = joined['geometry'].apply(lambda x: x.wkt if pd.notna(x) else None)
            # Extract lat/lon from geometry
            joined['uprn_latitude'] = joined['geometry'].y
            joined['uprn_longitude'] = joined['geometry'].x
        
        # Convert to regular DataFrame
        result_df = pd.DataFrame(joined.drop(columns=['geometry', 'index_right'], errors='ignore'))
        
        print(f"\n  ✓ Final joined records: {len(result_df):,}")
        if len(result_df) > 0:
            print(f"   Records with UPRN: {result_df[uprn_col].notna().sum():,}")
            print(f"   Average distance: {result_df['uprn_distance_m'].mean():.2f}m")
            print(f"   Median distance: {result_df['uprn_distance_m'].median():.2f}m")
            print(f"   Max distance: {result_df['uprn_distance_m'].max():.2f}m")
        
        return result_df
    
    def join_by_postcode(self, distance_threshold: float = 0.01) -> pd.DataFrame:
        """
        Join datasets by postcode
        
        Args:
            distance_threshold: Maximum distance in degrees for coordinate matching
        
        Returns:
            Joined DataFrame
        """
        print("\n" + "=" * 70)
        print("JOINING BY POSTCODE")
        print("=" * 70)
        
        # Prepare postcodes
        print("\nPreparing postcodes...")
        
        # EPC postcodes
        epc_clean = self.epc_df.copy()
        if 'postcode' in epc_clean.columns:
            epc_clean['postcode_normalized'] = epc_clean['postcode'].apply(self.normalize_postcode)
            epc_clean = epc_clean[epc_clean['postcode_normalized'].notna()]
            print(f"  EPC records with postcodes: {len(epc_clean):,}")
        
        # POI postcodes
        pois_clean = self.pois_df.copy()
        if 'addr:postcode' in pois_clean.columns:
            pois_clean['postcode_normalized'] = pois_clean['addr:postcode'].apply(self.normalize_postcode)
            pois_clean = pois_clean[pois_clean['postcode_normalized'].notna()]
            print(f"  POI records with postcodes: {len(pois_clean):,}")
        
        # Join EPC and POIs by postcode
        print("\nJoining EPC and POIs by postcode...")
        joined = pd.merge(
            epc_clean,
            pois_clean,
            on='postcode_normalized',
            how='inner',
            suffixes=('_epc', '_poi')
        )
        print(f"  Joined records: {len(joined):,}")
        
        return joined
    
    def join_by_coordinates(self, distance_meters: float = 100, max_inf: int = None, max_pois: int = None) -> pd.DataFrame:
        """
        Join datasets by geographic coordinates (optimized for large datasets)
        
        Args:
            distance_meters: Maximum distance in meters for matching
            max_inf: Maximum infrastructure records to process (None for all)
            max_pois: Maximum POI records to process (None for all)
        
        Returns:
            Joined DataFrame
        """
        print("\n" + "=" * 70)
        print("JOINING BY COORDINATES")
        print("=" * 70)
        
        # Prepare coordinates
        print("\nPreparing coordinates...")
        
        # Infrastructure coordinates
        inf_clean = self.infrastructure_df.copy()
        if 'lat' in inf_clean.columns and 'lon' in inf_clean.columns:
            inf_clean = inf_clean[
                (inf_clean['lat'].notna()) & 
                (inf_clean['lon'].notna())
            ].copy()
            print(f"  Infrastructure records with coordinates: {len(inf_clean):,}")
        else:
            print("  ⚠ Infrastructure dataset missing lat/lon columns")
            return pd.DataFrame()
        
        # POI coordinates
        pois_clean = self.pois_df.copy()
        if 'latitude' in pois_clean.columns and 'longitude' in pois_clean.columns:
            pois_clean = pois_clean[
                (pois_clean['latitude'].notna()) & 
                (pois_clean['longitude'].notna())
            ].copy()
            print(f"  POI records with coordinates: {len(pois_clean):,}")
        else:
            print("  ⚠ POI dataset missing latitude/longitude columns")
            return pd.DataFrame()
        
        # Limit datasets for efficiency
        if max_inf and len(inf_clean) > max_inf:
            print(f"  Limiting infrastructure data to {max_inf:,} records...")
            inf_clean = inf_clean.head(max_inf)
        
        if max_pois and len(pois_clean) > max_pois:
            print(f"  Limiting POI data to {max_pois:,} records...")
            pois_clean = pois_clean.head(max_pois)
        
        print(f"\n  Joining (distance threshold: {distance_meters}m)...")
        print(f"  This may take a while...")
        
        # Convert distance threshold to degrees (approximate)
        # 1 degree latitude ≈ 111 km, so 100m ≈ 0.0009 degrees
        distance_degrees = distance_meters / 111000
        
        joined_records = []
        total_inf = len(inf_clean)
        
        for idx, (inf_idx, inf_row) in enumerate(inf_clean.iterrows(), 1):
            if idx % 100 == 0:
                print(f"    Processing infrastructure {idx:,}/{total_inf:,}...", end='\r')
            
            inf_lat = inf_row['lat']
            inf_lon = inf_row['lon']
            
            # Find nearby POIs using bounding box (much faster)
            nearby = pois_clean[
                (pois_clean['latitude'] >= inf_lat - distance_degrees) &
                (pois_clean['latitude'] <= inf_lat + distance_degrees) &
                (pois_clean['longitude'] >= inf_lon - distance_degrees) &
                (pois_clean['longitude'] <= inf_lon + distance_degrees)
            ]
            
            # Calculate exact distances only for candidates
            for poi_idx, poi_row in nearby.iterrows():
                poi_lat = poi_row['latitude']
                poi_lon = poi_row['longitude']
                
                distance = geodesic((inf_lat, inf_lon), (poi_lat, poi_lon)).meters
                
                if distance <= distance_meters:
                    # Merge rows
                    merged = {**inf_row.to_dict(), **poi_row.to_dict()}
                    merged['distance_meters'] = distance
                    joined_records.append(merged)
        
        print()  # New line after progress
        
        if joined_records:
            joined = pd.DataFrame(joined_records)
            print(f"  ✓ Joined records: {len(joined):,}")
        else:
            joined = pd.DataFrame()
            print(f"  ⚠ No matches found within {distance_meters}m")
        
        return joined
    
    def create_comprehensive_join(self, output_dir: str = None) -> Dict[str, pd.DataFrame]:
        """
        Create comprehensive joined datasets using multiple strategies
        
        Returns:
            Dictionary of joined DataFrames
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        print("=" * 70)
        print("COMPREHENSIVE DATASET JOINING")
        print("=" * 70)
        
        # Load datasets
        self.load_datasets()
        
        results = {}
        
        # Strategy 1: Join EPC and POIs by postcode
        print("\n" + "=" * 70)
        print("STRATEGY 1: EPC + POIs (by postcode)")
        print("=" * 70)
        try:
            joined_postcode = self.join_by_postcode()
            if not joined_postcode.empty:
                output_file = os.path.join(output_dir, 'joined_epc_pois_by_postcode.csv')
                joined_postcode.to_csv(output_file, index=False)
                print(f"\n✓ Saved to: {output_file}")
                print(f"  Records: {len(joined_postcode):,}")
                results['epc_pois_postcode'] = joined_postcode
        except Exception as e:
            print(f"  Error: {str(e)}")
        
        # Strategy 2: Join Infrastructure and POIs by coordinates
        print("\n" + "=" * 70)
        print("STRATEGY 2: Infrastructure + POIs (by coordinates)")
        print("=" * 70)
        try:
            # Process all infrastructure (it's small: ~2,500 records)
            # But limit POIs for efficiency (process first 50k)
            joined_coords = self.join_by_coordinates(distance_meters=100, max_inf=None, max_pois=50000)
            if not joined_coords.empty:
                output_file = os.path.join(output_dir, 'joined_infrastructure_pois_by_coords.csv')
                joined_coords.to_csv(output_file, index=False)
                print(f"\n✓ Saved to: {output_file}")
                print(f"  Records: {len(joined_coords):,}")
                results['infrastructure_pois_coords'] = joined_coords
        except Exception as e:
            print(f"  Error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # Strategy 3: Join EPC with itself by UPRN (to find multiple EPCs per property)
        print("\n" + "=" * 70)
        print("STRATEGY 3: EPC Self-Join (by UPRN)")
        print("=" * 70)
        try:
            # Prepare EPC data with UPRN
            epc_with_uprn = self.epc_df.copy()
            epc_with_uprn['uprn_normalized'] = epc_with_uprn['UPRN'].apply(self.normalize_uprn)
            epc_with_uprn = epc_with_uprn[epc_with_uprn['uprn_normalized'].notna()].copy()
            
            # Find UPRNs with multiple EPC records
            uprn_counts = epc_with_uprn['uprn_normalized'].value_counts()
            multiple_epc_uprns = uprn_counts[uprn_counts > 1].index.tolist()
            
            print(f"\n  Properties with multiple EPC records: {len(multiple_epc_uprns):,}")
            print(f"  Total EPC records for these properties: {uprn_counts[uprn_counts > 1].sum():,}")
            
            if len(multiple_epc_uprns) > 0:
                # Create joined dataset with all EPC records for properties with multiple assessments
                epc_multiple = epc_with_uprn[epc_with_uprn['uprn_normalized'].isin(multiple_epc_uprns)].copy()
                
                # Sort by UPRN and date if available
                if 'LODGEMENT_DATE' in epc_multiple.columns:
                    epc_multiple['LODGEMENT_DATE'] = pd.to_datetime(epc_multiple['LODGEMENT_DATE'], errors='coerce')
                    epc_multiple = epc_multiple.sort_values(['uprn_normalized', 'LODGEMENT_DATE'])
                
                output_file = os.path.join(output_dir, 'joined_epc_multiple_by_uprn.csv')
                epc_multiple.to_csv(output_file, index=False)
                print(f"\n✓ Saved to: {output_file}")
                print(f"  Records: {len(epc_multiple):,}")
                print(f"  Unique properties: {epc_multiple['uprn_normalized'].nunique():,}")
                results['epc_multiple_uprn'] = epc_multiple
        except Exception as e:
            print(f"  Error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # Strategy 4: Multi-column join (EPC + POIs with confidence scoring)
        print("\n" + "=" * 70)
        print("STRATEGY 4: EPC + POIs (Multi-Column with Confidence)")
        print("=" * 70)
        try:
            # For large datasets, sample for efficiency
            # You can adjust these limits or remove them for full dataset
            epc_sample_size = min(50000, len(self.epc_df))
            pois_sample_size = min(50000, len(self.pois_df))
            
            if epc_sample_size < len(self.epc_df) or pois_sample_size < len(self.pois_df):
                print(f"  Sampling for efficiency: EPC={epc_sample_size:,}, POIs={pois_sample_size:,}")
                epc_sample = self.epc_df.head(epc_sample_size).copy()
                pois_sample = self.pois_df.head(pois_sample_size).copy()
            else:
                epc_sample = self.epc_df
                pois_sample = self.pois_df
            
            # Join on postcode + address components for higher confidence
            join_cols = {
                'postcode': ('postcode', 'addr:postcode'),
            }
            
            # Add address matching if available
            if 'ADDRESS' in epc_sample.columns and 'addr:street' in pois_sample.columns:
                join_cols['address'] = ('ADDRESS', 'addr:street')
            elif 'ADDRESS2' in epc_sample.columns and 'addr:housenumber' in pois_sample.columns:
                join_cols['address2'] = ('ADDRESS2', 'addr:housenumber')
            
            # Add city/town matching
            if 'POSTTOWN' in epc_sample.columns and 'addr:city' in pois_sample.columns:
                join_cols['city'] = ('POSTTOWN', 'addr:city')
            
            if len(join_cols) > 1:
                epc_pois_multi = self.join_by_multiple_columns(
                    epc_sample,
                    pois_sample,
                    join_columns=join_cols,
                    min_confidence=1,  # At least postcode must match
                    suffixes=('_epc', '_poi')
                )
                
                if not epc_pois_multi.empty:
                    output_file = os.path.join(output_dir, 'joined_epc_pois_multi_column.csv')
                    epc_pois_multi.to_csv(output_file, index=False)
                    print(f"\n✓ Saved to: {output_file}")
                    print(f"  Records: {len(epc_pois_multi):,}")
                    print(f"  Average confidence: {epc_pois_multi['confidence_score'].mean():.2f}/{len(join_cols)}")
                    results['epc_pois_multi'] = epc_pois_multi
            else:
                print("  ⚠ Only one join column available, skipping multi-column join")
        except Exception as e:
            print(f"  Error: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # Strategy 5: Spatial join OSM POIs to UPRN (if UPRN file provided)
        print("\n" + "=" * 70)
        print("STRATEGY 5: OSM POIs + UPRN (Spatial Nearest-Neighbor)")
        print("=" * 70)
        
        # Check for UPRN file in common locations
        possible_uprn_paths = [
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'os_open_uprn.gpkg'),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'os_open_uprn.shp'),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'uprn.csv'),
            '/Users/manal/MyDocuments/Companies/Zone13/datasets/os_open_uprn.gpkg',
            '/Users/manal/MyDocuments/Companies/Zone13/datasets/uprn.gpkg',
        ]
        
        uprn_path = None
        for path in possible_uprn_paths:
            if os.path.exists(path):
                uprn_path = path
                break
        
        if uprn_path:
            try:
                osm_uprn_joined = self.join_osm_to_uprn_spatial(
                    uprn_path=uprn_path,
                    max_distance_meters=15.0
                )
                
                if not osm_uprn_joined.empty:
                    output_file = os.path.join(output_dir, 'joined_osm_pois_with_uprn_spatial.csv')
                    osm_uprn_joined.to_csv(output_file, index=False)
                    print(f"\n✓ Saved to: {output_file}")
                    print(f"  Records: {len(osm_uprn_joined):,}")
                    results['osm_uprn_spatial'] = osm_uprn_joined
            except Exception as e:
                print(f"  Error: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print("  ⚠ UPRN file not found in common locations")
            print("  To use spatial UPRN joining, provide UPRN file path:")
            print("  - Download OS Open UPRN from: https://osdatahub.os.uk/downloads/open/OpenUPRN")
            print("  - Place in data/ directory or specify path in join_osm_to_uprn_spatial()")
        
        # Strategy 6: Create summary statistics
        print("\n" + "=" * 70)
        print("STRATEGY 6: Summary Statistics")
        print("=" * 70)
        summary = self.create_summary_statistics()
        if not summary.empty:
            output_file = os.path.join(output_dir, 'dataset_join_summary.csv')
            summary.to_csv(output_file, index=False)
            print(f"\n✓ Saved summary to: {output_file}")
            results['summary'] = summary
        
        return results
    
    def add_opening_dates_to_uprns(self, uprn_df: pd.DataFrame,
                                   planning_path: str = None,
                                   building_age_path: str = None,
                                   heritage_path: str = None,
                                   uprn_col: str = 'UPRN') -> pd.DataFrame:
        """
        Add estimated opening dates to UPRN DataFrame
        
        This is a convenience method that uses BuildingOpeningDateEstimator
        
        Args:
            uprn_df: DataFrame with UPRNs
            planning_path: Path to planning data
            building_age_path: Path to building age data
            heritage_path: Path to heritage data
            uprn_col: Column name for UPRN
        
        Returns:
            DataFrame with estimated opening dates added
        """
        try:
            import sys
            import os
            # Add src directory to path if needed
            src_dir = os.path.dirname(os.path.abspath(__file__))
            if src_dir not in sys.path:
                sys.path.insert(0, src_dir)
            from src.opening_dates.building_opening_date_estimator import BuildingOpeningDateEstimator
            
            estimator = BuildingOpeningDateEstimator()
            
            # Load data sources if provided
            if planning_path:
                estimator.load_planning_data(planning_path)
            if building_age_path:
                estimator.load_building_age_data(building_age_path)
            if heritage_path:
                estimator.load_heritage_data(heritage_path)
            
            # Estimate dates
            result = estimator.estimate_opening_dates(uprn_df, uprn_col=uprn_col)
            
            return result
            
        except ImportError:
            print("  ⚠ BuildingOpeningDateEstimator not available")
            return uprn_df
        except Exception as e:
            print(f"  ⚠ Error estimating opening dates: {str(e)}")
            return uprn_df
    
    def create_summary_statistics(self) -> pd.DataFrame:
        """Create summary statistics about the datasets"""
        summary_data = []
        
        # Dataset sizes
        summary_data.append({
            'metric': 'total_records',
            'infrastructure': len(self.infrastructure_df),
            'epc': len(self.epc_df),
            'pois': len(self.pois_df)
        })
        
        # Coordinate coverage
        if 'lat' in self.infrastructure_df.columns and 'lon' in self.infrastructure_df.columns:
            inf_with_coords = self.infrastructure_df[
                (self.infrastructure_df['lat'].notna()) & 
                (self.infrastructure_df['lon'].notna())
            ]
            summary_data.append({
                'metric': 'records_with_coordinates',
                'infrastructure': len(inf_with_coords),
                'epc': 0,  # EPC may not have coordinates
                'pois': len(self.pois_df[
                    (self.pois_df['latitude'].notna()) & 
                    (self.pois_df['longitude'].notna())
                ])
            })
        
        # Postcode coverage
        if 'postcode' in self.epc_df.columns:
            epc_with_postcode = self.epc_df[self.epc_df['postcode'].notna()]
            summary_data.append({
                'metric': 'records_with_postcode',
                'infrastructure': 0,
                'epc': len(epc_with_postcode),
                'pois': len(self.pois_df[self.pois_df['addr:postcode'].notna()]) if 'addr:postcode' in self.pois_df.columns else 0
            })
        
        return pd.DataFrame(summary_data)


def main():
    """Main function"""
    PATH = "/Users/manal/MyDocuments/Companies/Zone13/datasets"
    dataset1 = os.path.join(PATH, "london_infrastructure_2000_onwards.csv")
    dataset2 = os.path.join(PATH, "nondomestic_epc_2010_2024_complete.csv")
    dataset3 = "/Users/manal/Workspace/IngestEngine/data/london_pois_cleaned.csv"
    
    joiner = DatasetJoiner(dataset1, dataset2, dataset3)
    results = joiner.create_comprehensive_join()
    
    print("\n" + "=" * 70)
    print("JOINING COMPLETE")
    print("=" * 70)
    print(f"\nCreated {len(results)} joined datasets")
    print("\nFiles created:")
    print("  - joined_epc_pois_by_postcode.csv")
    print("  - joined_infrastructure_pois_by_coords.csv")
    print("  - dataset_join_summary.csv")


if __name__ == "__main__":
    main()

