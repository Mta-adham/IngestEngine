"""
Filter POIs to show only those with names
Creates a new CSV file with only POIs that have names
"""

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def filter_pois_with_names(input_file=None, output_file=None):
    """
    Filter POIs to keep only those with names
    
    Args:
        input_file: Input CSV file (default: data/london_pois.csv)
        output_file: Output CSV file (default: data/london_pois_with_names.csv)
    """
    if input_file is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        input_file = os.path.join(data_dir, 'london_pois.csv')
    
    if output_file is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        output_file = os.path.join(data_dir, 'london_pois_with_names.csv')
    
    print("=" * 70)
    print("FILTERING POIs WITH NAMES")
    print("=" * 70)
    
    print(f"\nLoading: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)
    print(f"Loaded: {len(df):,} POIs")
    
    # Filter to only POIs with names
    df_with_names = df[df['name'].notna()].copy()
    
    print(f"\nPOIs with names: {len(df_with_names):,}")
    print(f"POIs without names (removed): {len(df) - len(df_with_names):,}")
    
    # Save
    df_with_names.to_csv(output_file, index=False)
    print(f"\n✓ Saved to: {output_file}")
    print(f"  Records: {len(df_with_names):,}")
    print(f"  Columns: {len(df_with_names.columns)}")
    
    # Show sample
    print(f"\nSample POIs with names:")
    for idx, row in df_with_names.head(10).iterrows():
        print(f"  {row.get('poi_type', 'N/A'):12s} | {row.get('name', 'N/A')}")
    
    return df_with_names


if __name__ == "__main__":
    filter_pois_with_names()

