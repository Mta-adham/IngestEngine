"""
POI Data Cleaner - Filter OSM-specific metadata from useful POI data
=====================================================================

Identifies and removes OSM-specific fields that aren't useful for building
a world model, keeping only the actual POI information.

Usage:
    from src.poi_data_cleaner import POIDataCleaner
    
    cleaner = POIDataCleaner()
    cleaned_df = cleaner.clean_data(df)
"""

# ============================================
# IMPORTS
# ============================================
import pandas as pd
import os
from typing import List, Dict, Set


# ============================================
# CLASS DEFINITION
# ============================================

class POIDataCleaner:
    """Clean POI data by removing OSM-specific metadata"""
    
    # ============================================
    # CONSTANTS - OSM METADATA PATTERNS
    # ============================================
    
    # OSM-specific fields to exclude (not useful for world model)
    # NOTE: Keeping geometry, extraction metadata, source/ref fields, and dates per user request
    OSM_METADATA_PATTERNS = [
        # OSM identifiers and structure (but keep element_type for reference)
        'osmid', 'osm_', '^id$',  # Keep element_type, keep geometry
        
        # OSM creation metadata (but keep dates)
        'created_by', 'createdby', 'CREATEDBY',  # Keep CREATEDATE and other dates
        
        # Check dates - REMOVED from exclusion (user wants to keep all dates)
        # '^check_date', 'check_date:',
        
        # Extraction metadata - REMOVED from exclusion (user wants to keep)
        # '^extraction_', '^snapshot_', 'attr_count',
        
        # Geometry - REMOVED from exclusion (user wants to keep)
        # 'geometry', 'geometry_wkt',
        
        # Source and reference fields - REMOVED from exclusion (user wants to keep)
        # '^source:', '^source_', '^ref:', '^ref_',
        
        # OSM-specific tags
        '^fixme', '^FIXME', '^note:', '^note_',
        '^old_', '^was:', '^disused:', '^not:',
        '^dontimport:', '^case:',
        
        # OSM versioning
        'version', 'changeset', 'timestamp',
        
        # OSM contributor info
        'user', 'uid',
    ]
    
    # Fields that ARE useful (keep these)
    USEFUL_PATTERNS = [
        # Basic info
        'name', '^name:', 'alt_name', 'official_name',
        
        # Location
        'latitude', 'longitude', 'coordinates',
        '^addr:', 'address', 'location',
        
        # Contact
        'phone', 'email', 'website', '^contact:',
        '^facebook', '^twitter', '^instagram',
        
        # POI type
        'amenity', 'tourism', 'leisure', 'shop',
        'cuisine', 'type', 'poi_type',
        
        # Business info
        'opening_hours', 'hours', 'opening',
        'capacity', 'seats', 'rooms', 'stars',
        'rating', 'price', 'fee',
        
        # Services
        'payment', '^payment:', 'wifi', 'internet',
        'parking', 'wheelchair', 'accessibility',
        'delivery', 'takeaway', 'reservation',
        
        # Descriptions
        'description', '^description:',
        
        # Food/drink specific
        'diet', '^diet:', 'alcohol', 'food',
        'drink', 'menu', 'breakfast', 'lunch',
        
        # UK-specific but useful
        '^fhrs:',  # Food hygiene rating
    ]
    
    # ============================================
    # INITIALIZATION
    # ============================================
    
    def __init__(self):
        """Initialize the data cleaner"""
        pass
    
    # ============================================
    # IDENTIFICATION METHODS
    # ============================================
    
    def identify_osm_metadata(self, df: pd.DataFrame) -> Set[str]:
        """
        Identify OSM-specific metadata columns
        
        Args:
            df: DataFrame with POI data
        
        Returns:
            Set of column names that are OSM metadata
        """
        osm_columns = set()
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check against OSM metadata patterns
            for pattern in self.OSM_METADATA_PATTERNS:
                import re
                if re.search(pattern, col_lower):
                    osm_columns.add(col)
                    break
        
        return osm_columns
    
    def identify_useful_fields(self, df: pd.DataFrame) -> Set[str]:
        """
        Identify useful POI data columns
        
        Args:
            df: DataFrame with POI data
        
        Returns:
            Set of column names that are useful for world model
        """
        useful_columns = set()
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check against useful patterns
            for pattern in self.USEFUL_PATTERNS:
                import re
                if re.search(pattern, col_lower):
                    useful_columns.add(col)
                    break
        
        return useful_columns
    
    # ============================================
    # CLEANING METHODS
    # ============================================
    
    def clean_data(self, df: pd.DataFrame, 
                   keep_osm_metadata: bool = False,
                   keep_uncertain: bool = True) -> pd.DataFrame:
        """
        Clean POI data by removing OSM-specific metadata
        
        Args:
            df: DataFrame with POI data
            keep_osm_metadata: If True, keep OSM metadata columns
            keep_uncertain: If True, keep columns that are uncertain
        
        Returns:
            Cleaned DataFrame with only useful POI data
        """
        print("=" * 70)
        print("CLEANING POI DATA - REMOVING OSM METADATA")
        print("=" * 70)
        
        # Identify columns
        osm_metadata = self.identify_osm_metadata(df)
        useful_fields = self.identify_useful_fields(df)
        
        # All columns
        all_columns = set(df.columns)
        
        # Uncertain columns (not in either category)
        uncertain = all_columns - osm_metadata - useful_fields
        
        print(f"\nColumn Analysis:")
        print(f"  Total columns: {len(all_columns)}")
        print(f"  OSM metadata: {len(osm_metadata)}")
        print(f"  Useful POI data: {len(useful_fields)}")
        print(f"  Uncertain: {len(uncertain)}")
        
        # Determine columns to keep
        columns_to_keep = set()
        
        if keep_osm_metadata:
            columns_to_keep.update(osm_metadata)
        else:
            print(f"\n  Removing {len(osm_metadata)} OSM metadata columns")
        
        columns_to_keep.update(useful_fields)
        
        if keep_uncertain:
            columns_to_keep.update(uncertain)
            print(f"  Keeping {len(uncertain)} uncertain columns")
        else:
            print(f"  Removing {len(uncertain)} uncertain columns")
        
        # Create cleaned DataFrame
        columns_to_keep = sorted(list(columns_to_keep))
        cleaned_df = df[columns_to_keep].copy()
        
        print(f"\n  Kept {len(columns_to_keep)} columns")
        print(f"  Removed {len(df.columns) - len(columns_to_keep)} columns")
        
        return cleaned_df
    
    def generate_field_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate a report categorizing all fields
        
        Args:
            df: DataFrame with POI data
        
        Returns:
            Dictionary with categorized fields
        """
        osm_metadata = self.identify_osm_metadata(df)
        useful_fields = self.identify_useful_fields(df)
        all_columns = set(df.columns)
        uncertain = all_columns - osm_metadata - useful_fields
        
        return {
            'osm_metadata': sorted(list(osm_metadata)),
            'useful_fields': sorted(list(useful_fields)),
            'uncertain': sorted(list(uncertain)),
            'total': len(df.columns),
            'counts': {
                'osm_metadata': len(osm_metadata),
                'useful_fields': len(useful_fields),
                'uncertain': len(uncertain)
            }
        }
    
    def save_cleaned_data(self, df: pd.DataFrame, 
                         output_path: str = None,
                         keep_osm_metadata: bool = False) -> str:
        """
        Clean and save POI data
        
        Args:
            df: DataFrame with POI data
            output_path: Output file path
            keep_osm_metadata: Whether to keep OSM metadata
        
        Returns:
            Path to saved file
        """
        cleaned_df = self.clean_data(df, keep_osm_metadata=keep_osm_metadata)
        
        if output_path is None:
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            output_path = os.path.join(data_dir, 'london_pois_cleaned.csv')
        
        cleaned_df.to_csv(output_path, index=False)
        print(f"\n✓ Saved cleaned data to: {output_path}")
        print(f"  Records: {len(cleaned_df):,}")
        print(f"  Columns: {len(cleaned_df.columns)}")
        
        return output_path


def main():
    """Main function"""
    import sys
    
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    input_file = os.path.join(data_dir, 'london_pois.csv')
    
    if not os.path.exists(input_file):
        print(f"Error: File not found: {input_file}")
        return
    
    print("=" * 70)
    print("POI DATA CLEANER - REMOVING OSM METADATA")
    print("=" * 70)
    
    # Load data
    print(f"\nLoading data from {input_file}...")
    df = pd.read_csv(input_file, low_memory=False)
    print(f"Loaded {len(df):,} records with {len(df.columns)} columns")
    
    # Clean
    cleaner = POIDataCleaner()
    
    # Generate report
    report = cleaner.generate_field_report(df)
    
    print("\n" + "=" * 70)
    print("FIELD CATEGORIZATION REPORT")
    print("=" * 70)
    print(f"\nOSM Metadata (not useful for world model): {report['counts']['osm_metadata']}")
    print(f"Useful POI Data (for world model): {report['counts']['useful_fields']}")
    print(f"Uncertain: {report['counts']['uncertain']}")
    
    # Show examples
    print(f"\n\nOSM Metadata Examples (first 15):")
    for col in report['osm_metadata'][:15]:
        print(f"  - {col}")
    
    print(f"\n\nUseful POI Data Examples (first 15):")
    for col in report['useful_fields'][:15]:
        print(f"  - {col}")
    
    # Clean and save
    print("\n" + "=" * 70)
    output_file = cleaner.save_cleaned_data(df, keep_osm_metadata=False)
    
    print("\n" + "=" * 70)
    print("CLEANING COMPLETE")
    print("=" * 70)
    print(f"\nCleaned data saved to: {output_file}")
    print(f"Original columns: {len(df.columns)}")
    print(f"Cleaned columns: {len(pd.read_csv(output_file, nrows=1).columns)}")
    print(f"Removed: {len(df.columns) - len(pd.read_csv(output_file, nrows=1).columns)} OSM metadata columns")


if __name__ == "__main__":
    main()

