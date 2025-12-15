"""
Standalone script for spatial joining OSM POIs to UPRN
Can be run independently if you have UPRN data
"""

import os
import sys
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.joining.dataset_joiner import DatasetJoiner


def main():
    """Main function for spatial UPRN joining"""
    print("=" * 70)
    print("SPATIAL JOIN: OSM POIs TO UPRN")
    print("=" * 70)
    
    # Paths
    pois_path = "/Users/manal/Workspace/IngestEngine/data/london_pois_cleaned.csv"
    
    # UPRN file path - update this to your UPRN file location
    uprn_path = input("\nEnter path to UPRN file (GeoPackage/Shapefile/CSV): ").strip()
    
    if not uprn_path or not os.path.exists(uprn_path):
        print("\n⚠ UPRN file not found.")
        print("\nTo download OS Open UPRN:")
        print("1. Visit: https://osdatahub.os.uk/downloads/open/OpenUPRN")
        print("2. Download the GeoPackage or Shapefile")
        print("3. Place in data/ directory")
        print("\nOr provide the full path when prompted.")
        return
    
    # Initialize joiner
    joiner = DatasetJoiner(
        infrastructure_path="",  # Not needed for this join
        epc_path="",  # Not needed for this join
        pois_path=pois_path
    )
    
    # Load POI data
    print("\nLoading POI data...")
    joiner.load_datasets()
    
    # Perform spatial join
    result = joiner.join_osm_to_uprn_spatial(
        uprn_path=uprn_path,
        max_distance_meters=15.0
    )
    
    if not result.empty:
        # Save result
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'data',
            'osm_pois_with_uprn_spatial.csv'
        )
        result.to_csv(output_path, index=False)
        print(f"\n✓ Saved to: {output_path}")
        print(f"  Records: {len(result):,}")
        
        # Find UPRN column
        uprn_col = None
        for col in ['UPRN', 'uprn', 'UPRN_SOURCE']:
            if col in result.columns:
                uprn_col = col
                break
        
        if uprn_col:
            matched = result[uprn_col].notna().sum()
            print(f"  Records with UPRN: {matched:,} ({matched/len(result)*100:.1f}%)")
    else:
        print("\n⚠ No matches found. Check:")
        print("  - UPRN file format and CRS")
        print("  - OSM POI coordinates")
        print("  - Distance threshold")


if __name__ == "__main__":
    main()

