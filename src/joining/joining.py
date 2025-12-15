"""
Join three datasets: Infrastructure, EPC, and POIs
Creates useful joined datasets for analysis
"""

import os
import sys
import pandas as pd

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.joining.dataset_joiner import DatasetJoiner

# Dataset paths
PATH = "/Users/manal/MyDocuments/Companies/Zone13/datasets"
dataset1 = os.path.join(PATH, "london_infrastructure_2000_onwards.csv")
dataset2 = os.path.join(PATH, "nondomestic_epc_2010_2024_complete.csv")
dataset3 = "/Users/manal/Workspace/IngestEngine/data/london_pois_cleaned.csv"


def main():
    """Main joining function"""
    print("=" * 70)
    print("JOINING THREE DATASETS")
    print("=" * 70)
    print("\nDatasets:")
    print(f"  1. Infrastructure: {dataset1}")
    print(f"  2. EPC (Non-domestic): {dataset2}")
    print(f"  3. POIs: {dataset3}")
    
    # Initialize joiner
    joiner = DatasetJoiner(dataset1, dataset2, dataset3)
    
    # Create comprehensive joins
    results = joiner.create_comprehensive_join()
    
    # Display results summary
    print("\n" + "=" * 70)
    print("JOINING RESULTS SUMMARY")
    print("=" * 70)
    
    for key, df in results.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            print(f"\n{key}:")
            print(f"  Records: {len(df):,}")
            print(f"  Columns: {len(df.columns)}")
            if 'distance_meters' in df.columns:
                print(f"  Avg distance: {df['distance_meters'].mean():.1f}m")
    
    print("\n" + "=" * 70)
    print("USEFUL INSIGHTS FROM JOINED DATA")
    print("=" * 70)
    
    # Analyze joined data
    if 'epc_pois_postcode' in results:
        analyze_epc_pois(results['epc_pois_postcode'])
    
    if 'infrastructure_pois_coords' in results:
        analyze_infrastructure_pois(results['infrastructure_pois_coords'])
    
    if 'epc_multiple_uprn' in results:
        analyze_epc_uprn(results['epc_multiple_uprn'])
    
    if 'epc_pois_multi' in results:
        analyze_multi_column_join(results['epc_pois_multi'])
    
    if 'osm_uprn_spatial' in results:
        analyze_spatial_uprn_join(results['osm_uprn_spatial'])
    
    print("\n✓ Joining complete! Check the 'data/' directory for output files.")


def analyze_epc_pois(df):
    """Analyze EPC + POIs joined data"""
    print("\n1. EPC + POIs Analysis (by postcode):")
    print(f"   Total joined records: {len(df):,}")
    
    if 'poi_type' in df.columns:
        poi_counts = df['poi_type'].value_counts().head(10)
        print(f"\n   Top POI types near EPC buildings:")
        for poi_type, count in poi_counts.items():
            print(f"     {poi_type}: {count:,}")
    
    if 'TARGET_EMISSIONS' in df.columns:
        print(f"\n   EPC Energy Performance:")
        print(f"     Average target emissions: {df['TARGET_EMISSIONS'].mean():.2f}")
    
    if 'REPORT_TYPE' in df.columns:
        report_types = df['REPORT_TYPE'].value_counts()
        print(f"\n   EPC Report Types:")
        for rtype, count in report_types.items():
            print(f"     {rtype}: {count:,}")


def analyze_infrastructure_pois(df):
    """Analyze Infrastructure + POIs joined data"""
    print("\n2. Infrastructure + POIs Analysis (by coordinates):")
    print(f"   Total joined records: {len(df):,}")
    
    if 'category' in df.columns:
        categories = df['category'].value_counts()
        print(f"\n   Infrastructure categories near POIs:")
        for cat, count in categories.items():
            print(f"     {cat}: {count:,}")
    
    if 'distance_meters' in df.columns:
        print(f"\n   Distance statistics:")
        print(f"     Average: {df['distance_meters'].mean():.1f}m")
        print(f"     Median: {df['distance_meters'].median():.1f}m")
        print(f"     Max: {df['distance_meters'].max():.1f}m")
    
    if 'poi_type' in df.columns:
        poi_counts = df['poi_type'].value_counts().head(10)
        print(f"\n   Top POI types near infrastructure:")
        for poi_type, count in poi_counts.items():
            print(f"     {poi_type}: {count:,}")


def analyze_epc_uprn(df):
    """Analyze EPC multiple records by UPRN"""
    print("\n3. EPC Multiple Records by UPRN:")
    print(f"   Total records: {len(df):,}")
    
    if 'uprn_normalized' in df.columns:
        unique_properties = df['uprn_normalized'].nunique()
        print(f"   Unique properties: {unique_properties:,}")
        
        # Count EPCs per property
        epcs_per_property = df['uprn_normalized'].value_counts()
        print(f"\n   EPC records per property:")
        print(f"     Average: {epcs_per_property.mean():.2f}")
        print(f"     Median: {epcs_per_property.median():.0f}")
        print(f"     Max: {epcs_per_property.max()}")
        print(f"     Properties with 2 EPCs: {(epcs_per_property == 2).sum():,}")
        print(f"     Properties with 3+ EPCs: {(epcs_per_property >= 3).sum():,}")
    
    if 'LODGEMENT_DATE' in df.columns:
        df['LODGEMENT_DATE'] = pd.to_datetime(df['LODGEMENT_DATE'], errors='coerce')
        date_range = df['LODGEMENT_DATE'].agg(['min', 'max'])
        print(f"\n   Date range:")
        print(f"     Earliest: {date_range['min']}")
        print(f"     Latest: {date_range['max']}")
    
    if 'TARGET_EMISSIONS' in df.columns:
        print(f"\n   Energy Performance:")
        print(f"     Average emissions: {df['TARGET_EMISSIONS'].mean():.2f}")
        print(f"     Median emissions: {df['TARGET_EMISSIONS'].median():.2f}")


def analyze_multi_column_join(df):
    """Analyze multi-column join results"""
    print("\n4. Multi-Column Join Analysis (with Confidence Scoring):")
    print(f"   Total records: {len(df):,}")
    
    if 'confidence_score' in df.columns:
        max_score = df['confidence_score'].max()
        avg_score = df['confidence_score'].mean()
        print(f"\n   Confidence Scores:")
        print(f"     Average: {avg_score:.2f}/{max_score}")
        print(f"     Median: {df['confidence_score'].median():.0f}/{max_score}")
        print(f"     Max: {max_score}/{max_score}")
        
        # Distribution
        score_dist = df['confidence_score'].value_counts().sort_index()
        print(f"\n   Confidence Distribution:")
        for score, count in score_dist.items():
            pct = count / len(df) * 100
            print(f"     {score}/{max_score} columns match: {count:,} ({pct:.1f}%)")
        
        # High confidence matches
        high_conf = df[df['confidence_score'] == max_score]
        if len(high_conf) > 0:
            print(f"\n   High Confidence Matches ({max_score}/{max_score}):")
            print(f"     Count: {len(high_conf):,} ({len(high_conf)/len(df)*100:.1f}%)")
            if 'poi_type' in high_conf.columns:
                poi_counts = high_conf['poi_type'].value_counts().head(5)
                print(f"     Top POI types:")
                for poi_type, count in poi_counts.items():
                    print(f"       {poi_type}: {count:,}")
    
    if 'poi_type' in df.columns:
        poi_counts = df['poi_type'].value_counts().head(10)
        print(f"\n   Top POI types (all confidence levels):")
        for poi_type, count in poi_counts.items():
            print(f"     {poi_type}: {count:,}")


def analyze_spatial_uprn_join(df):
    """Analyze spatial UPRN join results"""
    print("\n5. Spatial UPRN Join Analysis:")
    print(f"   Total records: {len(df):,}")
    
    # Find UPRN column
    uprn_col = None
    for col in ['UPRN', 'uprn', 'UPRN_SOURCE']:
        if col in df.columns:
            uprn_col = col
            break
    
    if uprn_col:
        records_with_uprn = df[uprn_col].notna().sum()
        unique_uprns = df[uprn_col].nunique()
        print(f"\n   UPRN Matching:")
        print(f"     Records with UPRN: {records_with_uprn:,} ({records_with_uprn/len(df)*100:.1f}%)")
        print(f"     Unique UPRNs matched: {unique_uprns:,}")
    
    if 'uprn_distance_m' in df.columns:
        print(f"\n   Distance Statistics:")
        print(f"     Average: {df['uprn_distance_m'].mean():.2f}m")
        print(f"     Median: {df['uprn_distance_m'].median():.2f}m")
        print(f"     Min: {df['uprn_distance_m'].min():.2f}m")
        print(f"     Max: {df['uprn_distance_m'].max():.2f}m")
        
        # Distance distribution
        print(f"\n   Distance Distribution:")
        bins = [0, 5, 10, 15, 20, 50, 100, float('inf')]
        labels = ['0-5m', '5-10m', '10-15m', '15-20m', '20-50m', '50-100m', '100m+']
        df['distance_bin'] = pd.cut(df['uprn_distance_m'], bins=bins, labels=labels)
        dist_dist = df['distance_bin'].value_counts().sort_index()
        for bin_label, count in dist_dist.items():
            pct = count / len(df) * 100
            print(f"     {bin_label}: {count:,} ({pct:.1f}%)")
    
    if 'poi_type' in df.columns and uprn_col:
        matched = df[df[uprn_col].notna()]
        if len(matched) > 0:
            poi_counts = matched['poi_type'].value_counts().head(10)
            print(f"\n   Top POI types with UPRN matches:")
            for poi_type, count in poi_counts.items():
                print(f"     {poi_type}: {count:,}")


if __name__ == "__main__":
    import pandas as pd
    main()
