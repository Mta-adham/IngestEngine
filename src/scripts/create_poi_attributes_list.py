"""
Create a readable list of POI types with their associated attributes
Formatted for easy reading
"""

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def create_readable_poi_attributes_list():
    """
    Create a readable CSV with POI types and their attributes
    One row per POI type, with attributes listed
    """
    print("=" * 70)
    print("CREATING READABLE POI TYPES AND ATTRIBUTES LIST")
    print("=" * 70)
    
    # Load comprehensive catalog
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    catalog_file = os.path.join(data_dir, 'osm_comprehensive_poi_catalog.csv')
    
    if not os.path.exists(catalog_file):
        print("Comprehensive catalog not found. Creating it first...")
        from comprehensive_osm_poi_catalog import create_comprehensive_osm_poi_catalog
        create_comprehensive_osm_poi_catalog()
    
    catalog = pd.read_csv(catalog_file)
    
    print(f"\nLoaded catalog: {len(catalog):,} entries")
    print(f"POI types: {catalog['poi_type'].nunique()}")
    print(f"Attributes: {catalog['attribute'].nunique()}")
    
    # Group by POI type and collect all attributes
    poi_attributes_list = []
    
    for (category, poi_type), group in catalog.groupby(['poi_category', 'poi_type']):
        attributes = sorted(group['attribute'].unique().tolist())
        
        # Categorize attributes
        common_attrs = [a for a in attributes if group[group['attribute'] == a]['is_common'].iloc[0]]
        category_attrs = [a for a in attributes if group[group['attribute'] == a]['is_category_specific'].iloc[0]]
        other_attrs = [a for a in attributes if a not in common_attrs and a not in category_attrs]
        
        poi_attributes_list.append({
            'poi_category': category,
            'poi_type': poi_type,
            'osm_tag': group['osm_tag'].iloc[0],
            'total_attributes': len(attributes),
            'common_attributes_count': len(common_attrs),
            'category_specific_count': len(category_attrs),
            'other_attributes_count': len(other_attrs),
            'all_attributes': '; '.join(attributes),
            'common_attributes': '; '.join(common_attrs[:50]),  # Limit for readability
            'category_specific_attributes': '; '.join(category_attrs[:50]),
            'key_attributes': '; '.join(common_attrs[:20])  # Most important
        })
    
    result_df = pd.DataFrame(poi_attributes_list)
    result_df = result_df.sort_values(['poi_category', 'poi_type'])
    
    # Save
    output_file = os.path.join(data_dir, 'osm_poi_types_with_attributes.csv')
    result_df.to_csv(output_file, index=False)
    
    print(f"\n✓ Saved readable list to: {output_file}")
    print(f"  POI types: {len(result_df)}")
    print(f"  Columns: {len(result_df.columns)}")
    
    # Create a summary by category
    summary = result_df.groupby('poi_category').agg({
        'poi_type': 'count',
        'total_attributes': 'mean'
    }).reset_index()
    summary.columns = ['poi_category', 'poi_types_count', 'avg_attributes_per_type']
    summary = summary.sort_values('poi_types_count', ascending=False)
    
    summary_file = os.path.join(data_dir, 'osm_poi_categories_summary.csv')
    summary.to_csv(summary_file, index=False)
    
    print(f"\n✓ Saved category summary to: {summary_file}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY BY CATEGORY")
    print("=" * 70)
    for _, row in summary.iterrows():
        print(f"\n{row['poi_category'].upper()}:")
        print(f"  POI types: {int(row['poi_types_count'])}")
        print(f"  Average attributes per type: {row['avg_attributes_per_type']:.0f}")
    
    # Show sample
    print("\n" + "=" * 70)
    print("SAMPLE POI TYPES WITH ATTRIBUTES")
    print("=" * 70)
    for idx, row in result_df.head(5).iterrows():
        print(f"\n{row['poi_category']}.{row['poi_type']} ({row['osm_tag']}):")
        print(f"  Total attributes: {row['total_attributes']}")
        print(f"  Key attributes: {row['key_attributes'][:200]}...")
    
    return result_df, summary


if __name__ == "__main__":
    create_readable_poi_attributes_list()

