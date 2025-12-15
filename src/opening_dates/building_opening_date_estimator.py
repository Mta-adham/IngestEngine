"""
Building Opening Date Estimator
================================

Combines multiple UK data sources to estimate building opening dates per UPRN.

Data Sources (Priority Order):
1. Wikidata (for POIs with names)
2. Companies House (incorporation dates for businesses)
3. Planning completion dates (for new builds)
4. Land Registry (first transaction dates for residential)
5. Building age from OS/NGD (broad coverage)
6. EPC construction age (detailed refinement)
7. Heritage/listed building records (historical accuracy)

Usage:
    from src.building_opening_date_estimator import BuildingOpeningDateEstimator
    
    estimator = BuildingOpeningDateEstimator(use_wikidata=True)
    estimator.load_planning_data("data/planning.csv")
    estimator.load_building_age_data("data/building_age.gpkg")
    result = estimator.estimate_opening_dates(uprn_df)
"""

# ============================================
# IMPORTS
# ============================================
import pandas as pd
import numpy as np
import os
from typing import Dict, Optional, Tuple
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


# ============================================
# CLASS DEFINITION
# ============================================

class BuildingOpeningDateEstimator:
    """
    Estimate building opening dates by combining multiple UK data sources.
    
    Combines 7 data sources with intelligent priority system for maximum coverage.
    """
    
    # ============================================
    # INITIALIZATION
    # ============================================
    
    def __init__(self, use_wikidata: bool = True):
        """
        Initialize the estimator
        
        Args:
            use_wikidata: Whether to use Wikidata for POI opening dates (default: True)
        """
        self.planning_df = None
        self.building_age_df = None
        self.heritage_df = None
        self.companies_house_df = None
        self.land_registry_df = None
        self.epc_df = None
        self.use_wikidata = use_wikidata
        self.wikidata_client = None
        
        if use_wikidata:
            try:
                from src.opening_dates.wikidata_client import WikidataClient
                self.wikidata_client = WikidataClient()
            except ImportError:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning("WikidataClient not available. Install dependencies.")
                self.use_wikidata = False
    
    # ============================================
    # DATA LOADING METHODS
    # ============================================
    
    def load_planning_data(self, planning_path: str, 
                          uprn_col: str = 'UPRN',
                          completion_date_col: str = 'completion_date',
                          start_date_col: str = 'start_date') -> pd.DataFrame:
        """
        Load planning data with completion dates
        
        Args:
            planning_path: Path to planning data CSV/GeoPackage
            uprn_col: Column name for UPRN
            completion_date_col: Column name for completion date
            start_date_col: Column name for start date
        
        Returns:
            DataFrame with planning data
        """
        print(f"\nLoading planning data from: {planning_path}")
        
        if not os.path.exists(planning_path):
            print(f"  ⚠ Planning data file not found")
            return pd.DataFrame()
        
        try:
            if planning_path.endswith('.csv'):
                df = pd.read_csv(planning_path, low_memory=False)
            else:
                # Try GeoPandas for GeoPackage/Shapefile
                try:
                    import geopandas as gpd
                    gdf = gpd.read_file(planning_path)
                    df = pd.DataFrame(gdf.drop(columns=['geometry'], errors='ignore'))
                except:
                    print(f"  ⚠ Could not read planning file")
                    return pd.DataFrame()
            
            # Check for required columns
            if uprn_col not in df.columns:
                print(f"  ⚠ UPRN column '{uprn_col}' not found")
                print(f"  Available columns: {list(df.columns)[:10]}")
                return pd.DataFrame()
            
            # Parse dates
            date_cols = []
            if completion_date_col in df.columns:
                df[completion_date_col] = pd.to_datetime(df[completion_date_col], errors='coerce')
                date_cols.append(completion_date_col)
            if start_date_col in df.columns:
                df[start_date_col] = pd.to_datetime(df[start_date_col], errors='coerce')
                date_cols.append(start_date_col)
            
            # Filter to records with UPRN and dates
            df = df[df[uprn_col].notna()].copy()
            if date_cols:
                df = df[df[date_cols].notna().any(axis=1)].copy()
            
            print(f"  ✓ Loaded {len(df):,} planning records")
            if completion_date_col in df.columns:
                records_with_completion = df[completion_date_col].notna().sum()
                print(f"  Records with completion dates: {records_with_completion:,}")
            
            self.planning_df = df
            return df
            
        except Exception as e:
            print(f"  ⚠ Error loading planning data: {str(e)}")
            return pd.DataFrame()
    
    def load_building_age_data(self, building_age_path: str,
                              uprn_col: str = 'UPRN',
                              age_year_col: str = 'building_age_year',
                              age_period_col: str = 'building_age_period') -> pd.DataFrame:
        """
        Load building age data from OS/NGD
        
        Args:
            building_age_path: Path to building age data
            uprn_col: Column name for UPRN
            age_year_col: Column name for building age year
            age_period_col: Column name for building age period
        
        Returns:
            DataFrame with building age data
        """
        print(f"\nLoading building age data from: {building_age_path}")
        
        if not os.path.exists(building_age_path):
            print(f"  ⚠ Building age file not found")
            return pd.DataFrame()
        
        try:
            if building_age_path.endswith('.csv'):
                df = pd.read_csv(building_age_path, low_memory=False)
            else:
                try:
                    import geopandas as gpd
                    gdf = gpd.read_file(building_age_path)
                    df = pd.DataFrame(gdf.drop(columns=['geometry'], errors='ignore'))
                except:
                    print(f"  ⚠ Could not read building age file")
                    return pd.DataFrame()
            
            if uprn_col not in df.columns:
                print(f"  ⚠ UPRN column '{uprn_col}' not found")
                return pd.DataFrame()
            
            # Extract year from period if needed
            if age_period_col in df.columns and age_year_col not in df.columns:
                # Try to extract year from period (e.g., "1980-1989" -> 1980)
                df[age_year_col] = df[age_period_col].str.extract(r'(\d{4})').astype(float)
            
            df = df[df[uprn_col].notna()].copy()
            
            print(f"  ✓ Loaded {len(df):,} building age records")
            if age_year_col in df.columns:
                records_with_year = df[age_year_col].notna().sum()
                print(f"  Records with age year: {records_with_year:,}")
            
            self.building_age_df = df
            return df
            
        except Exception as e:
            print(f"  ⚠ Error loading building age data: {str(e)}")
            return pd.DataFrame()
    
    def load_heritage_data(self, heritage_path: str,
                          uprn_col: str = 'UPRN',
                          construction_date_col: str = 'construction_date',
                          description_col: str = 'description') -> pd.DataFrame:
        """
        Load heritage/listed building data
        
        Args:
            heritage_path: Path to heritage data
            uprn_col: Column name for UPRN
            construction_date_col: Column name for construction date
            description_col: Column name for description (may contain dates)
        
        Returns:
            DataFrame with heritage data
        """
        print(f"\nLoading heritage data from: {heritage_path}")
        
        if not os.path.exists(heritage_path):
            print(f"  ⚠ Heritage data file not found")
            return pd.DataFrame()
        
        try:
            if heritage_path.endswith('.csv'):
                df = pd.read_csv(heritage_path, low_memory=False)
            else:
                try:
                    import geopandas as gpd
                    gdf = gpd.read_file(heritage_path)
                    df = pd.DataFrame(gdf.drop(columns=['geometry'], errors='ignore'))
                except:
                    print(f"  ⚠ Could not read heritage file")
                    return pd.DataFrame()
            
            if uprn_col not in df.columns:
                print(f"  ⚠ UPRN column '{uprn_col}' not found")
                return pd.DataFrame()
            
            # Parse construction date
            if construction_date_col in df.columns:
                df[construction_date_col] = pd.to_datetime(df[construction_date_col], errors='coerce')
            
            # Try to extract dates from description
            if description_col in df.columns and construction_date_col not in df.columns:
                # Extract 4-digit years from description
                years = df[description_col].str.extractall(r'(\d{4})')[0].astype(float)
                if len(years) > 0:
                    # Take earliest year mentioned
                    df[construction_date_col] = years.groupby(years.index.get_level_values(0)).min()
            
            df = df[df[uprn_col].notna()].copy()
            
            print(f"  ✓ Loaded {len(df):,} heritage records")
            if construction_date_col in df.columns:
                records_with_date = df[construction_date_col].notna().sum()
                print(f"  Records with construction dates: {records_with_date:,}")
            
            self.heritage_df = df
            return df
            
        except Exception as e:
            print(f"  ⚠ Error loading heritage data: {str(e)}")
            return pd.DataFrame()
    
    def load_companies_house_data(self, companies_house_path: str,
                                  company_name_col: str = 'company_name',
                                  incorporation_date_col: str = 'incorporation_date',
                                  company_number_col: str = 'company_number',
                                  address_col: str = 'registered_address') -> pd.DataFrame:
        """
        Load Companies House data with incorporation dates
        
        Args:
            companies_house_path: Path to Companies House data CSV
            company_name_col: Column name for company name
            incorporation_date_col: Column name for incorporation date
            company_number_col: Column name for company number
            address_col: Column name for registered address
        
        Returns:
            DataFrame with Companies House data
        """
        print(f"\nLoading Companies House data from: {companies_house_path}")
        
        if not os.path.exists(companies_house_path):
            print(f"  ⚠ Companies House data file not found")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(companies_house_path, low_memory=False)
            
            # Check for required columns
            if incorporation_date_col not in df.columns:
                # Try alternative column names
                for col in df.columns:
                    if 'incorporation' in col.lower() or 'date_of_incorporation' in col.lower():
                        incorporation_date_col = col
                        break
            
            if incorporation_date_col not in df.columns:
                print(f"  ⚠ Incorporation date column not found")
                print(f"  Available columns: {list(df.columns)[:10]}")
                return pd.DataFrame()
            
            # Parse dates
            df[incorporation_date_col] = pd.to_datetime(df[incorporation_date_col], errors='coerce')
            
            # Filter to records with dates
            df = df[df[incorporation_date_col].notna()].copy()
            
            print(f"  ✓ Loaded {len(df):,} Companies House records")
            print(f"  Records with incorporation dates: {len(df):,}")
            
            self.companies_house_df = df
            return df
            
        except Exception as e:
            print(f"  ⚠ Error loading Companies House data: {str(e)}")
            return pd.DataFrame()
    
    def load_land_registry_data(self, land_registry_path: str,
                                postcode_col: str = 'postcode',
                                address_col: str = 'address',
                                date_col: str = 'date',
                                price_col: str = 'price',
                                property_type_col: str = 'property_type',
                                new_build_col: str = 'new_build') -> pd.DataFrame:
        """
        Load Land Registry Price Paid Data and find first transaction dates
        
        Args:
            land_registry_path: Path to Land Registry data CSV
            postcode_col: Column name for postcode
            address_col: Column name for address
            date_col: Column name for transaction date
            price_col: Column name for price
            property_type_col: Column name for property type
            new_build_col: Column name for new build flag
        
        Returns:
            DataFrame with first transaction dates per property
        """
        print(f"\nLoading Land Registry data from: {land_registry_path}")
        
        if not os.path.exists(land_registry_path):
            print(f"  ⚠ Land Registry data file not found")
            return pd.DataFrame()
        
        try:
            # Read in chunks if file is large
            chunk_size = 100000
            chunks = []
            
            for chunk in pd.read_csv(land_registry_path, chunksize=chunk_size, low_memory=False):
                chunks.append(chunk)
            
            df = pd.concat(chunks, ignore_index=True)
            
            # Find date column
            if date_col not in df.columns:
                for col in df.columns:
                    if 'date' in col.lower() and 'transfer' in col.lower():
                        date_col = col
                        break
                    elif 'date' in col.lower():
                        date_col = col
                        break
            
            if date_col not in df.columns:
                print(f"  ⚠ Date column not found")
                print(f"  Available columns: {list(df.columns)[:10]}")
                return pd.DataFrame()
            
            # Parse dates
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df[df[date_col].notna()].copy()
            
            # Find first transaction per property (using postcode + address as key)
            if postcode_col in df.columns and address_col in df.columns:
                # Group by postcode and address, take earliest date
                first_transactions = df.groupby([postcode_col, address_col])[date_col].min().reset_index()
                first_transactions.columns = [postcode_col, address_col, 'first_transaction_date']
                
                # Merge back to get full record details
                df = df.merge(
                    first_transactions,
                    on=[postcode_col, address_col],
                    how='inner'
                )
                df = df[df[date_col] == df['first_transaction_date']].copy()
                df = df.drop_duplicates(subset=[postcode_col, address_col])
            else:
                # If no postcode/address, just use date column as-is
                df['first_transaction_date'] = df[date_col]
            
            print(f"  ✓ Loaded {len(df):,} Land Registry records")
            print(f"  Unique properties with first transaction dates: {len(df):,}")
            
            self.land_registry_df = df
            return df
            
        except Exception as e:
            print(f"  ⚠ Error loading Land Registry data: {str(e)}")
            return pd.DataFrame()
    
    def load_epc_data(self, epc_path: str,
                     uprn_col: str = 'UPRN',
                     construction_age_col: str = 'construction_age',
                     construction_age_band_col: str = 'construction_age_band',
                     postcode_col: str = 'postcode',
                     address_col: str = 'address') -> pd.DataFrame:
        """
        Load EPC data with construction age
        
        Args:
            epc_path: Path to EPC data CSV
            uprn_col: Column name for UPRN
            construction_age_col: Column name for construction age year
            construction_age_band_col: Column name for construction age band
            postcode_col: Column name for postcode
            address_col: Column name for address
        
        Returns:
            DataFrame with EPC data
        """
        print(f"\nLoading EPC data from: {epc_path}")
        
        if not os.path.exists(epc_path):
            print(f"  ⚠ EPC data file not found")
            return pd.DataFrame()
        
        try:
            # EPC files can be very large, read in chunks
            chunk_size = 100000
            chunks = []
            
            for chunk in pd.read_csv(epc_path, chunksize=chunk_size, low_memory=False):
                chunks.append(chunk)
            
            df = pd.concat(chunks, ignore_index=True)
            
            # Find construction age column
            if construction_age_col not in df.columns:
                for col in df.columns:
                    if 'construction' in col.lower() and 'age' in col.lower() and 'year' in col.lower():
                        construction_age_col = col
                        break
                    elif 'construction' in col.lower() and 'age' in col.lower():
                        construction_age_col = col
                        break
            
            # If we have age band but not year, try to extract year from band
            if construction_age_col not in df.columns and construction_age_band_col in df.columns:
                # Extract year from age band (e.g., "2007-2011" -> 2007)
                df['construction_age_year'] = df[construction_age_band_col].str.extract(r'(\d{4})').astype(float)
                construction_age_col = 'construction_age_year'
            
            if construction_age_col not in df.columns:
                print(f"  ⚠ Construction age column not found")
                print(f"  Available columns: {list(df.columns)[:10]}")
                return pd.DataFrame()
            
            # Convert to numeric if needed
            if df[construction_age_col].dtype == 'object':
                df[construction_age_col] = pd.to_numeric(df[construction_age_col], errors='coerce')
            
            # Filter to records with construction age
            df = df[df[construction_age_col].notna()].copy()
            
            print(f"  ✓ Loaded {len(df):,} EPC records")
            print(f"  Records with construction age: {len(df):,}")
            
            self.epc_df = df
            return df
            
        except Exception as e:
            print(f"  ⚠ Error loading EPC data: {str(e)}")
            return pd.DataFrame()
    
    # ============================================
    # ESTIMATION METHODS
    # ============================================
    
    def estimate_opening_dates(self, uprn_df: pd.DataFrame,
                               uprn_col: str = 'UPRN',
                               priority_order: list = None,
                               poi_name_col: str = None,
                               **kwargs) -> pd.DataFrame:
        """
        Estimate opening dates for UPRNs using priority system
        
        Priority order (default):
        1. Wikidata (if POI name available and use_wikidata=True)
        2. Companies House (incorporation dates for businesses)
        3. Planning completion date
        4. Land Registry (first transaction dates for residential)
        5. Building age year from OS/NGD
        6. EPC construction age (refinement)
        7. Heritage construction date
        
        Args:
            uprn_df: DataFrame with UPRNs to estimate dates for
            uprn_col: Column name for UPRN
            priority_order: List of data source priorities 
                          ['wikidata', 'companies_house', 'planning', 'land_registry', 
                           'building_age', 'epc', 'heritage']
            poi_name_col: Column name for POI name (for Wikidata lookup)
            company_name_col: Column name for company name (for Companies House lookup)
            address_col: Column name for address (for Land Registry lookup)
            postcode_col: Column name for postcode (for Land Registry lookup)
        
        Returns:
            DataFrame with estimated opening dates
        """
        if priority_order is None:
            priority_order = ['wikidata', 'companies_house', 'planning', 'land_registry', 
                            'building_age', 'epc', 'heritage']
        
        # Extract optional parameters
        company_name_col = kwargs.get('company_name_col', 'company_name')
        address_col = kwargs.get('address_col', 'address')
        postcode_col = kwargs.get('postcode_col', 'postcode')
        
        print("\n" + "=" * 70)
        print("ESTIMATING BUILDING OPENING DATES")
        print("=" * 70)
        
        result = uprn_df.copy()
        result['estimated_opening_date'] = None
        result['opening_date_source'] = None
        result['opening_date_year'] = None
        result['opening_date_confidence'] = None
        result['wikidata_id'] = None
        result['company_number'] = None  # For Companies House
        
        sources_used = {
            'wikidata': 0,
            'companies_house': 0,
            'planning': 0,
            'land_registry': 0,
            'building_age': 0,
            'epc': 0,
            'heritage': 0,
            'none': 0
        }
        
        # Priority 1: Wikidata (if POI name available)
        if 'wikidata' in priority_order and self.use_wikidata and self.wikidata_client:
            if poi_name_col and poi_name_col in result.columns:
                print("\n0. Using Wikidata for POI opening dates...")
                wikidata_count = 0
                
                for idx, row in result.iterrows():
                    if pd.isna(result.at[idx, 'estimated_opening_date']):
                        poi_name = row.get(poi_name_col)
                        if pd.notna(poi_name) and str(poi_name).strip():
                            try:
                                info = self.wikidata_client.get_poi_info(str(poi_name), city="London")
                                if info['opening_date']:
                                    result.at[idx, 'estimated_opening_date'] = pd.to_datetime(
                                        info['opening_date'], errors='coerce'
                                    )
                                    result.at[idx, 'opening_date_source'] = 'wikidata'
                                    result.at[idx, 'opening_date_confidence'] = 'high'
                                    result.at[idx, 'opening_date_year'] = result.at[idx, 'estimated_opening_date'].year
                                    result.at[idx, 'wikidata_id'] = info['wikidata_id']
                                    wikidata_count += 1
                            except Exception as e:
                                logger.debug(f"Wikidata lookup failed for '{poi_name}': {e}")
                
                sources_used['wikidata'] = wikidata_count
                print(f"   ✓ Assigned {wikidata_count:,} dates from Wikidata")
        
        # Priority 2: Companies House incorporation dates
        if 'companies_house' in priority_order and self.companies_house_df is not None and len(self.companies_house_df) > 0:
            print("\n1. Using Companies House incorporation dates...")
            incorporation_col = 'incorporation_date'
            if incorporation_col not in self.companies_house_df.columns:
                for col in self.companies_house_df.columns:
                    if 'incorporation' in col.lower():
                        incorporation_col = col
                        break
            
            if incorporation_col in self.companies_house_df.columns:
                # Try to join on company name or address
                if company_name_col in result.columns:
                    # Join on company name
                    companies_merged = pd.merge(
                        result,
                        self.companies_house_df[[company_name_col, incorporation_col, 'company_number']],
                        on=company_name_col,
                        how='left',
                        suffixes=('', '_companies')
                    )
                    
                    mask = companies_merged[incorporation_col].notna() & result['estimated_opening_date'].isna()
                    result.loc[mask, 'estimated_opening_date'] = companies_merged.loc[mask, incorporation_col]
                    result.loc[mask, 'opening_date_source'] = 'companies_house'
                    result.loc[mask, 'opening_date_confidence'] = 'high'
                    result.loc[mask, 'opening_date_year'] = result.loc[mask, 'estimated_opening_date'].dt.year
                    if 'company_number' in companies_merged.columns:
                        result.loc[mask, 'company_number'] = companies_merged.loc[mask, 'company_number']
                    
                    sources_used['companies_house'] = mask.sum()
                    print(f"   ✓ Assigned {sources_used['companies_house']:,} dates from Companies House")
        
        # Priority 3: Planning completion dates
        if 'planning' in priority_order and self.planning_df is not None and len(self.planning_df) > 0:
            print("\n2. Using planning completion dates...")
            planning_col = 'completion_date'
            if planning_col not in self.planning_df.columns:
                # Try alternative column names
                for col in self.planning_df.columns:
                    if 'completion' in col.lower() or 'occupation' in col.lower():
                        planning_col = col
                        break
            
            if planning_col in self.planning_df.columns:
                planning_merged = pd.merge(
                    result,
                    self.planning_df[[uprn_col, planning_col]],
                    on=uprn_col,
                    how='left',
                    suffixes=('', '_planning')
                )
                
                mask = planning_merged[planning_col].notna() & result['estimated_opening_date'].isna()
                result.loc[mask, 'estimated_opening_date'] = planning_merged.loc[mask, planning_col]
                result.loc[mask, 'opening_date_source'] = 'planning_completion'
                result.loc[mask, 'opening_date_confidence'] = 'high'
                result.loc[mask, 'opening_date_year'] = result.loc[mask, 'estimated_opening_date'].dt.year
                
                sources_used['planning'] = mask.sum()
                print(f"   ✓ Assigned {sources_used['planning']:,} dates from planning data")
        
        # Priority 4: Land Registry first transaction dates
        if 'land_registry' in priority_order and self.land_registry_df is not None and len(self.land_registry_df) > 0:
            print("\n3. Using Land Registry first transaction dates...")
            transaction_col = 'first_transaction_date'
            
            if transaction_col in self.land_registry_df.columns:
                # Try to join on postcode + address
                if postcode_col in result.columns and address_col in result.columns:
                    land_registry_merged = pd.merge(
                        result,
                        self.land_registry_df[[postcode_col, address_col, transaction_col]],
                        on=[postcode_col, address_col],
                        how='left',
                        suffixes=('', '_land_registry')
                    )
                    
                    mask = land_registry_merged[transaction_col].notna() & result['estimated_opening_date'].isna()
                    result.loc[mask, 'estimated_opening_date'] = land_registry_merged.loc[mask, transaction_col]
                    result.loc[mask, 'opening_date_source'] = 'land_registry'
                    result.loc[mask, 'opening_date_confidence'] = 'medium'
                    result.loc[mask, 'opening_date_year'] = result.loc[mask, 'estimated_opening_date'].dt.year
                    
                    sources_used['land_registry'] = mask.sum()
                    print(f"   ✓ Assigned {sources_used['land_registry']:,} dates from Land Registry")
        
        # Priority 5: Building age year
        if 'building_age' in priority_order and self.building_age_df is not None and len(self.building_age_df) > 0:
            print("\n4. Using building age year...")
            age_col = 'building_age_year'
            if age_col not in self.building_age_df.columns:
                # Try alternative column names
                for col in self.building_age_df.columns:
                    if 'age' in col.lower() and 'year' in col.lower():
                        age_col = col
                        break
            
            if age_col in self.building_age_df.columns:
                age_merged = pd.merge(
                    result,
                    self.building_age_df[[uprn_col, age_col]],
                    on=uprn_col,
                    how='left',
                    suffixes=('', '_age')
                )
                
                mask = age_merged[age_col].notna() & result['estimated_opening_date'].isna()
                # Convert year to date (use Jan 1st of that year)
                result.loc[mask, 'opening_date_year'] = age_merged.loc[mask, age_col].astype(int)
                result.loc[mask, 'estimated_opening_date'] = pd.to_datetime(
                    result.loc[mask, 'opening_date_year'].astype(str) + '-01-01',
                    errors='coerce'
                )
                result.loc[mask, 'opening_date_source'] = 'building_age'
                result.loc[mask, 'opening_date_confidence'] = 'medium'
                
                sources_used['building_age'] = mask.sum()
                print(f"   ✓ Assigned {sources_used['building_age']:,} dates from building age")
        
        # Priority 6: EPC construction age
        if 'epc' in priority_order and self.epc_df is not None and len(self.epc_df) > 0:
            print("\n5. Using EPC construction age...")
            epc_age_col = 'construction_age'
            if epc_age_col not in self.epc_df.columns:
                for col in self.epc_df.columns:
                    if 'construction' in col.lower() and 'age' in col.lower():
                        epc_age_col = col
                        break
            
            if epc_age_col in self.epc_df.columns and uprn_col in self.epc_df.columns:
                epc_merged = pd.merge(
                    result,
                    self.epc_df[[uprn_col, epc_age_col]],
                    on=uprn_col,
                    how='left',
                    suffixes=('', '_epc')
                )
                
                mask = epc_merged[epc_age_col].notna() & result['estimated_opening_date'].isna()
                # Convert construction age to date (use Jan 1st of that year)
                result.loc[mask, 'opening_date_year'] = epc_merged.loc[mask, epc_age_col].astype(int)
                result.loc[mask, 'estimated_opening_date'] = pd.to_datetime(
                    result.loc[mask, 'opening_date_year'].astype(str) + '-01-01',
                    errors='coerce'
                )
                result.loc[mask, 'opening_date_source'] = 'epc'
                result.loc[mask, 'opening_date_confidence'] = 'medium'
                
                sources_used['epc'] = mask.sum()
                print(f"   ✓ Assigned {sources_used['epc']:,} dates from EPC data")
        
        # Priority 7: Heritage construction dates
        if 'heritage' in priority_order and self.heritage_df is not None and len(self.heritage_df) > 0:
            print("\n6. Using heritage construction dates...")
            heritage_col = 'construction_date'
            if heritage_col not in self.heritage_df.columns:
                # Try alternative column names
                for col in self.heritage_df.columns:
                    if 'construction' in col.lower() or 'built' in col.lower():
                        heritage_col = col
                        break
            
            if heritage_col in self.heritage_df.columns:
                heritage_merged = pd.merge(
                    result,
                    self.heritage_df[[uprn_col, heritage_col]],
                    on=uprn_col,
                    how='left',
                    suffixes=('', '_heritage')
                )
                
                mask = heritage_merged[heritage_col].notna() & result['estimated_opening_date'].isna()
                result.loc[mask, 'estimated_opening_date'] = heritage_merged.loc[mask, heritage_col]
                result.loc[mask, 'opening_date_source'] = 'heritage'
                result.loc[mask, 'opening_date_confidence'] = 'medium'
                result.loc[mask, 'opening_date_year'] = result.loc[mask, 'estimated_opening_date'].dt.year
                
                sources_used['heritage'] = mask.sum()
                print(f"   ✓ Assigned {sources_used['heritage']:,} dates from heritage data")
        
        # Summary
        sources_used['none'] = result['estimated_opening_date'].isna().sum()
        
        print("\n" + "=" * 70)
        print("OPENING DATE ESTIMATION SUMMARY")
        print("=" * 70)
        print(f"\nTotal UPRNs: {len(result):,}")
        print(f"With estimated dates: {result['estimated_opening_date'].notna().sum():,}")
        print(f"Without dates: {sources_used['none']:,}")
        
        print(f"\nSource breakdown:")
        for source, count in sources_used.items():
            if source != 'none':
                pct = count / len(result) * 100 if len(result) > 0 else 0
                print(f"  {source:15s}: {count:6,} ({pct:5.1f}%)")
        
        if result['estimated_opening_date'].notna().sum() > 0:
            print(f"\nDate range:")
            print(f"  Earliest: {result['estimated_opening_date'].min()}")
            print(f"  Latest: {result['estimated_opening_date'].max()}")
            
            print(f"\nConfidence levels:")
            conf_dist = result['opening_date_confidence'].value_counts()
            for conf, count in conf_dist.items():
                print(f"  {conf:10s}: {count:6,}")
        
        return result


# ============================================
# MAIN ENTRY POINT
# ============================================

def main():
    """Example usage"""
    print("=" * 70)
    print("BUILDING OPENING DATE ESTIMATOR")
    print("=" * 70)
    
    estimator = BuildingOpeningDateEstimator()
    
    # Example: Load data sources
    # estimator.load_planning_data("data/london_planning_data.csv")
    # estimator.load_building_age_data("data/os_building_age.gpkg")
    # estimator.load_heritage_data("data/heritage_list.csv")
    
    # Example: Estimate dates for UPRNs
    # uprn_df = pd.read_csv("data/uprns.csv")
    # result = estimator.estimate_opening_dates(uprn_df)
    # result.to_csv("data/uprns_with_opening_dates.csv", index=False)
    
    print("\nSee docs/BUILDING_OPENING_DATES.md for detailed usage")


if __name__ == "__main__":
    main()

