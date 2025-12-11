"""
POI Change Detection Tool
Identifies changes in POIs over time by comparing snapshots and analyzing changesets
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.poi_extractor import POIExtractor
from src.poi_history_tracker import OSMHistoryTracker


class POIChangeDetector:
    """Detect and analyze changes in POIs over time"""
    
    def __init__(self, place: str = "London, UK"):
        """
        Initialize the change detector
        
        Args:
            place: Name of the place to analyze
        """
        self.place = place
        self.extractor = POIExtractor(place)
        self.tracker = OSMHistoryTracker(place)
    
    def load_current_pois(self, filepath: str = None) -> pd.DataFrame:
        """
        Load current POI data
        
        Args:
            filepath: Path to CSV file (default: data/london_pois.csv)
        
        Returns:
            DataFrame with current POIs
        """
        if filepath is None:
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            filepath = os.path.join(data_dir, 'london_pois.csv')
        
        if os.path.exists(filepath):
            print(f"Loading POIs from {filepath}...")
            df = pd.read_csv(filepath, low_memory=False)
            print(f"Loaded {len(df):,} POIs")
            return df
        else:
            print(f"File not found: {filepath}")
            print("Extracting current POIs...")
            return self.extractor.extract_all_pois()
    
    def identify_recent_changes_by_dates(self, df: pd.DataFrame) -> Dict:
        """
        Identify potentially changed POIs based on date fields in current data
        
        Args:
            df: DataFrame with current POIs
        
        Returns:
            Dictionary with change analysis
        """
        print("\n" + "=" * 70)
        print("ANALYZING POI CHANGES BASED ON DATE FIELDS")
        print("=" * 70)
        
        results = {
            'total_pois': len(df),
            'with_check_dates': 0,
            'recently_checked': 0,
            'by_poi_type': {},
            'date_field_analysis': {}
        }
        
        # Find date fields
        date_fields = [col for col in df.columns 
                      if any(x in col.lower() for x in ['date', 'created', 'check_date', 'timestamp'])]
        
        print(f"\nFound {len(date_fields)} date-related fields")
        
        # Analyze check_date field
        if 'check_date' in df.columns:
            check_dates = df[df['check_date'].notna()].copy()
            results['with_check_dates'] = len(check_dates)
            
            if len(check_dates) > 0:
                try:
                    check_dates['check_date_parsed'] = pd.to_datetime(
                        check_dates['check_date'], errors='coerce'
                    )
                    
                    # Recently checked (last year)
                    one_year_ago = datetime.now() - timedelta(days=365)
                    recently_checked = check_dates[
                        check_dates['check_date_parsed'] > one_year_ago
                    ]
                    results['recently_checked'] = len(recently_checked)
                    
                    print(f"\nPOIs with check_date: {len(check_dates):,}")
                    print(f"Recently checked (last year): {len(recently_checked):,}")
                    
                    if 'poi_type' in recently_checked.columns:
                        results['by_poi_type'] = recently_checked['poi_type'].value_counts().to_dict()
                        print("\nRecently checked by type:")
                        for poi_type, count in list(results['by_poi_type'].items())[:10]:
                            print(f"  {poi_type}: {count:,}")
                
                except Exception as e:
                    print(f"Error parsing dates: {e}")
        
        # Analyze extraction timestamp
        if 'extraction_timestamp' in df.columns:
            extraction_dates = df['extraction_timestamp'].notna().sum()
            results['date_field_analysis']['extraction_timestamp'] = extraction_dates
            print(f"\nPOIs with extraction timestamp: {extraction_dates:,}")
        
        return results
    
    def compare_snapshots(self, df_old: pd.DataFrame, df_new: pd.DataFrame,
                         id_column: str = 'osmid') -> Dict[str, pd.DataFrame]:
        """
        Compare two POI snapshots to identify changes
        
        Args:
            df_old: Older snapshot
            df_new: Newer snapshot
            id_column: Column to use for matching POIs
        
        Returns:
            Dictionary with 'added', 'removed', 'modified' DataFrames
        """
        print("\n" + "=" * 70)
        print("COMPARING POI SNAPSHOTS")
        print("=" * 70)
        
        if df_old.empty:
            print("Old snapshot is empty - all POIs are new")
            return {
                'added': df_new,
                'removed': pd.DataFrame(),
                'modified': pd.DataFrame()
            }
        
        if df_new.empty:
            print("New snapshot is empty - all POIs were removed")
            return {
                'added': pd.DataFrame(),
                'removed': df_old,
                'modified': pd.DataFrame()
            }
        
        old_ids = set(df_old[id_column].dropna())
        new_ids = set(df_new[id_column].dropna())
        
        print(f"Old snapshot: {len(old_ids):,} POIs")
        print(f"New snapshot: {len(new_ids):,} POIs")
        
        # Added POIs
        added_ids = new_ids - old_ids
        added = df_new[df_new[id_column].isin(added_ids)].copy()
        
        # Removed POIs
        removed_ids = old_ids - new_ids
        removed = df_old[df_old[id_column].isin(removed_ids)].copy()
        
        # Modified POIs (exist in both but attributes changed)
        common_ids = old_ids & new_ids
        print(f"\nCommon POIs: {len(common_ids):,}")
        print("Checking for modifications...")
        
        modified = []
        key_attrs = ['name', 'amenity', 'tourism', 'leisure', 'shop', 'cuisine', 
                     'opening_hours', 'phone', 'website', 'addr:street']
        
        # Sample check (full comparison can be slow)
        sample_size = min(1000, len(common_ids))
        sample_ids = list(common_ids)[:sample_size]
        
        for poi_id in sample_ids:
            old_poi = df_old[df_old[id_column] == poi_id]
            new_poi = df_new[df_new[id_column] == poi_id]
            
            if len(old_poi) > 0 and len(new_poi) > 0:
                old_poi = old_poi.iloc[0]
                new_poi = new_poi.iloc[0]
                
                # Check if key attributes changed
                changed = False
                changes = []
                
                for attr in key_attrs:
                    if attr in old_poi.index and attr in new_poi.index:
                        old_val = str(old_poi[attr]) if pd.notna(old_poi[attr]) else ''
                        new_val = str(new_poi[attr]) if pd.notna(new_poi[attr]) else ''
                        
                        if old_val != new_val:
                            changed = True
                            changes.append(f"{attr}: '{old_val}' -> '{new_val}'")
                
                if changed:
                    new_poi_copy = new_poi.copy()
                    new_poi_copy['change_details'] = '; '.join(changes)
                    modified.append(new_poi_copy)
        
        modified_df = pd.DataFrame(modified) if modified else pd.DataFrame()
        
        print(f"\nChanges detected:")
        print(f"  Added: {len(added):,} POIs")
        print(f"  Removed: {len(removed):,} POIs")
        print(f"  Modified: {len(modified_df):,} POIs (checked {sample_size:,} of {len(common_ids):,})")
        
        return {
            'added': added,
            'removed': removed,
            'modified': modified_df
        }
    
    def analyze_changes_by_type(self, changes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Analyze changes grouped by POI type
        
        Args:
            changes: Dictionary from compare_snapshots
        
        Returns:
            DataFrame with summary by POI type
        """
        summary_data = []
        
        for change_type, df in changes.items():
            if not df.empty and 'poi_type' in df.columns:
                type_counts = df['poi_type'].value_counts()
                for poi_type, count in type_counts.items():
                    summary_data.append({
                        'change_type': change_type,
                        'poi_type': poi_type,
                        'count': count
                    })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df = summary_df.pivot_table(
                index='poi_type',
                columns='change_type',
                values='count',
                fill_value=0
            )
            return summary_df
        else:
            return pd.DataFrame()
    
    def save_change_report(self, changes: Dict[str, pd.DataFrame], 
                          summary: pd.DataFrame = None,
                          output_dir: str = None):
        """
        Save change detection results to files
        
        Args:
            changes: Dictionary with change DataFrames
            summary: Summary DataFrame
            output_dir: Output directory (default: data/)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save individual change files
        for change_type, df in changes.items():
            if not df.empty:
                filename = os.path.join(output_dir, f"poi_changes_{change_type}_{timestamp}.csv")
                df.to_csv(filename, index=False)
                print(f"  Saved {change_type}: {filename}")
        
        # Save summary
        if summary is not None and not summary.empty:
            filename = os.path.join(output_dir, f"poi_changes_summary_{timestamp}.csv")
            summary.to_csv(filename)
            print(f"  Saved summary: {filename}")
        
        # Save JSON report
        report = {
            'timestamp': timestamp,
            'extraction_date': datetime.now().isoformat(),
            'changes': {
                'added': len(changes.get('added', pd.DataFrame())),
                'removed': len(changes.get('removed', pd.DataFrame())),
                'modified': len(changes.get('modified', pd.DataFrame()))
            }
        }
        
        if summary is not None and not summary.empty:
            report['summary_by_type'] = summary.to_dict()
        
        filename = os.path.join(output_dir, f"poi_changes_report_{timestamp}.json")
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"  Saved report: {filename}")


def main():
    """Main function to detect POI changes"""
    print("=" * 70)
    print("POI CHANGE DETECTION TOOL")
    print("=" * 70)
    
    detector = POIChangeDetector("London, UK")
    
    # Load current POIs
    current_pois = detector.load_current_pois()
    
    if current_pois.empty:
        print("No POIs loaded. Exiting.")
        return
    
    # Method 1: Analyze date fields in current data
    date_analysis = detector.identify_recent_changes_by_dates(current_pois)
    
    # Method 2: Compare with previous snapshot (if available)
    print("\n" + "=" * 70)
    print("SNAPSHOT COMPARISON")
    print("=" * 70)
    print("\nTo compare snapshots:")
    print("1. Extract POIs at Time T1 (save to data/london_pois_T1.csv)")
    print("2. Extract POIs at Time T2 (save to data/london_pois_T2.csv)")
    print("3. Load both and compare")
    
    # Check if previous snapshot exists
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    previous_files = [f for f in os.listdir(data_dir) 
                     if 'london_pois' in f and f.endswith('.csv') and 'with_metadata' not in f]
    
    if len(previous_files) > 1:
        print(f"\nFound multiple POI files. Comparing...")
        # You could load and compare here
    else:
        print("\nOnly one POI file found. To detect changes:")
        print("  - Extract POIs again in the future")
        print("  - Compare with this snapshot")
        print("  - Or use changeset analysis (see docs)")
    
    # Save current snapshot with timestamp for future comparison
    timestamp = datetime.now().strftime("%Y%m%d")
    snapshot_file = os.path.join(data_dir, f"london_pois_snapshot_{timestamp}.csv")
    current_pois.to_csv(snapshot_file, index=False)
    print(f"\n✓ Saved current snapshot: {snapshot_file}")
    print("  Use this for future comparisons")
    
    print("\n" + "=" * 70)
    print("CHANGE DETECTION COMPLETE")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Extract POIs again in the future")
    print("2. Use compare_snapshots() to identify changes")
    print("3. Query changesets for detailed edit history")
    print("4. See docs/IDENTIFYING_POI_CHANGES.md for more methods")


if __name__ == "__main__":
    main()

