"""
Example: How to Compare POI Snapshots and Identify Changes

This script demonstrates how to identify changes in POIs over time
by comparing two snapshots.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.poi_change_detector import POIChangeDetector
import pandas as pd


def example_compare_snapshots():
    """Example: Compare two POI snapshots"""
    
    print("=" * 70)
    print("EXAMPLE: Comparing POI Snapshots")
    print("=" * 70)
    
    detector = POIChangeDetector("London, UK")
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    
    # Load current POIs
    current_file = os.path.join(data_dir, 'london_pois.csv')
    current_pois = detector.load_current_pois(current_file)
    
    # Check if we have a previous snapshot
    snapshot_files = [f for f in os.listdir(data_dir) 
                     if f.startswith('london_pois_snapshot_') and f.endswith('.csv')]
    
    if len(snapshot_files) > 0:
        # Use the most recent snapshot
        snapshot_files.sort(reverse=True)
        if len(snapshot_files) > 1:
            # Compare with previous snapshot
            old_file = os.path.join(data_dir, snapshot_files[1])  # Second most recent
            new_file = os.path.join(data_dir, snapshot_files[0])  # Most recent
            
            print(f"\nComparing:")
            print(f"  Old: {snapshot_files[1]}")
            print(f"  New: {snapshot_files[0]}")
            
            old_pois = pd.read_csv(old_file, low_memory=False)
            new_pois = pd.read_csv(new_file, low_memory=False)
            
            # Compare
            changes = detector.compare_snapshots(old_pois, new_pois)
            
            # Analyze by type
            summary = detector.analyze_changes_by_type(changes)
            if not summary.empty:
                print("\n" + "=" * 70)
                print("CHANGES BY POI TYPE")
                print("=" * 70)
                print(summary)
            
            # Save results
            detector.save_change_report(changes, summary)
            
        else:
            print(f"\nFound snapshot: {snapshot_files[0]}")
            print("Extract POIs again in the future to compare changes")
    else:
        print("\nNo previous snapshots found.")
        print("The current POI data has been saved as a snapshot.")
        print("Extract POIs again later to compare and identify changes.")


def example_analyze_date_fields():
    """Example: Analyze date fields to identify recently changed POIs"""
    
    print("\n" + "=" * 70)
    print("EXAMPLE: Analyzing Date Fields")
    print("=" * 70)
    
    detector = POIChangeDetector("London, UK")
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    current_file = os.path.join(data_dir, 'london_pois.csv')
    
    current_pois = detector.load_current_pois(current_file)
    
    # Analyze date fields
    date_analysis = detector.identify_recent_changes_by_dates(current_pois)
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total POIs: {date_analysis['total_pois']:,}")
    print(f"With check dates: {date_analysis['with_check_dates']:,}")
    print(f"Recently checked (last year): {date_analysis['recently_checked']:,}")
    
    if date_analysis['by_poi_type']:
        print("\nRecently checked by type:")
        for poi_type, count in sorted(date_analysis['by_poi_type'].items(), 
                                      key=lambda x: x[1], reverse=True):
            print(f"  {poi_type:15s}: {count:>6,}")


def example_find_specific_changes():
    """Example: Find specific types of changes"""
    
    print("\n" + "=" * 70)
    print("EXAMPLE: Finding Specific Changes")
    print("=" * 70)
    
    detector = POIChangeDetector("London, UK")
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    current_file = os.path.join(data_dir, 'london_pois.csv')
    
    df = detector.load_current_pois(current_file)
    
    # Find POIs with recent check dates (likely changed/verified recently)
    if 'check_date' in df.columns:
        df['check_date_parsed'] = pd.to_datetime(df['check_date'], errors='coerce')
        from datetime import datetime, timedelta
        
        # Last 6 months
        six_months_ago = datetime.now() - timedelta(days=180)
        recently_checked = df[df['check_date_parsed'] > six_months_ago]
        
        print(f"\nPOIs checked in last 6 months: {len(recently_checked):,}")
        
        if 'poi_type' in recently_checked.columns:
            print("\nBy type:")
            print(recently_checked['poi_type'].value_counts())
        
        # Save recently checked POIs
        if len(recently_checked) > 0:
            output_file = os.path.join(data_dir, 'recently_checked_pois.csv')
            recently_checked.to_csv(output_file, index=False)
            print(f"\n✓ Saved to: {output_file}")


if __name__ == "__main__":
    print("=" * 70)
    print("POI CHANGE IDENTIFICATION - EXAMPLES")
    print("=" * 70)
    
    # Example 1: Compare snapshots
    example_compare_snapshots()
    
    # Example 2: Analyze date fields
    example_analyze_date_fields()
    
    # Example 3: Find specific changes
    example_find_specific_changes()
    
    print("\n" + "=" * 70)
    print("EXAMPLES COMPLETE")
    print("=" * 70)
    print("\nFor more information, see:")
    print("  - docs/IDENTIFYING_POI_CHANGES.md")
    print("  - src/poi_change_detector.py")

