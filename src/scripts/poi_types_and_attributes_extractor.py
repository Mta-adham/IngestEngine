"""
Extract exhaustive list of all POI types and their associated attributes from OSM data
Creates a comprehensive CSV mapping POI types to all possible attributes
"""

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def extract_poi_types_and_attributes(csv_file=None):
    """
    Extract all POI types and their associated attributes
    
    Args:
        csv_file: Path to POI CSV file (default: data/london_pois.csv)
    
    Returns:
        DataFrame with POI types and their attributes
    """
    if csv_file is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        csv_file = os.path.join(data_dir, 'london_pois.csv')
    
    print("=" * 70)
    print("EXTRACTING ALL POI TYPES AND ATTRIBUTES")
    print("=" * 70)
    
    print(f"\nLoading data from {csv_file}...")
    df = pd.read_csv(csv_file, low_memory=False)
    print(f"Loaded {len(df):,} POIs with {len(df.columns)} columns")
    
    # Get all POI types
    if 'poi_type' not in df.columns:
        print("ERROR: poi_type column not found!")
        return pd.DataFrame()
    
    poi_types = df['poi_type'].unique()
    print(f"\nFound {len(poi_types)} POI types: {', '.join(poi_types)}")
    
    # For each POI type, find all attributes that have values
    results = []
    
    for poi_type in poi_types:
        print(f"\nAnalyzing {poi_type}...")
        type_df = df[df['poi_type'] == poi_type]
        
        # Find all columns that have at least one non-null value for this POI type
        attributes_with_values = []
        
        for col in df.columns:
            if col == 'poi_type':
                continue
            
            # Count non-null values for this POI type
            non_null_count = type_df[col].notna().sum()
            total_count = len(type_df)
            
            if non_null_count > 0:
                # Calculate percentage
                pct = (non_null_count / total_count * 100) if total_count > 0 else 0
                
                # Get sample values
                sample_values = type_df[col].dropna().unique()[:5].tolist()
                sample_values_str = '; '.join([str(v) for v in sample_values if str(v) != 'nan'])[:200]
                
                attributes_with_values.append({
                    'poi_type': poi_type,
                    'attribute': col,
                    'non_null_count': non_null_count,
                    'total_count': total_count,
                    'coverage_percent': round(pct, 2),
                    'sample_values': sample_values_str
                })
        
        print(f"  Found {len(attributes_with_values)} attributes with values")
        results.extend(attributes_with_values)
    
    # Create DataFrame
    results_df = pd.DataFrame(results)
    
    # Sort by POI type, then by coverage percentage
    results_df = results_df.sort_values(['poi_type', 'coverage_percent'], ascending=[True, False])
    
    return results_df


def create_comprehensive_poi_catalog():
    """
    Create a comprehensive catalog of all OSM POI types and their attributes
    """
    print("=" * 70)
    print("CREATING COMPREHENSIVE POI CATALOG")
    print("=" * 70)
    
    # Extract from data
    attributes_df = extract_poi_types_and_attributes()
    
    if attributes_df.empty:
        print("No data extracted")
        return
    
    # Save detailed mapping
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    output_file = os.path.join(data_dir, 'osm_poi_types_and_attributes.csv')
    attributes_df.to_csv(output_file, index=False)
    print(f"\n✓ Saved detailed mapping to: {output_file}")
    print(f"  Total records: {len(attributes_df):,}")
    
    # Create summary by POI type
    summary_data = []
    for poi_type in attributes_df['poi_type'].unique():
        type_attrs = attributes_df[attributes_df['poi_type'] == poi_type]
        summary_data.append({
            'poi_type': poi_type,
            'total_attributes': len(type_attrs),
            'high_coverage_attributes': len(type_attrs[type_attrs['coverage_percent'] >= 50]),
            'medium_coverage_attributes': len(type_attrs[(type_attrs['coverage_percent'] >= 10) & (type_attrs['coverage_percent'] < 50)]),
            'low_coverage_attributes': len(type_attrs[type_attrs['coverage_percent'] < 10]),
            'most_common_attributes': '; '.join(type_attrs.head(10)['attribute'].tolist())
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_file = os.path.join(data_dir, 'osm_poi_types_summary.csv')
    summary_df.to_csv(summary_file, index=False)
    print(f"✓ Saved summary to: {summary_file}")
    
    # Create attribute frequency across all POI types
    attribute_freq = attributes_df.groupby('attribute').agg({
        'poi_type': 'count',
        'coverage_percent': 'mean'
    }).reset_index()
    attribute_freq.columns = ['attribute', 'used_in_poi_types', 'avg_coverage_percent']
    attribute_freq = attribute_freq.sort_values('used_in_poi_types', ascending=False)
    
    freq_file = os.path.join(data_dir, 'osm_attributes_frequency.csv')
    attribute_freq.to_csv(freq_file, index=False)
    print(f"✓ Saved attribute frequency to: {freq_file}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\nPOI Types analyzed: {len(summary_df)}")
    for _, row in summary_df.iterrows():
        print(f"\n{row['poi_type'].upper()}:")
        print(f"  Total attributes: {row['total_attributes']}")
        print(f"  High coverage (≥50%): {row['high_coverage_attributes']}")
        print(f"  Medium coverage (10-50%): {row['medium_coverage_attributes']}")
        print(f"  Low coverage (<10%): {row['low_coverage_attributes']}")
    
    print(f"\n\nMost common attributes (used across multiple POI types):")
    for _, row in attribute_freq.head(20).iterrows():
        print(f"  {row['attribute']:40s} - Used in {row['used_in_poi_types']} POI types")
    
    return attributes_df, summary_df, attribute_freq


def main():
    """Main function"""
    create_comprehensive_poi_catalog()


if __name__ == "__main__":
    main()

