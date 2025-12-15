"""
POI Date Extractor - Extract POIs as they existed on a specific date

This tool allows you to select a date and extract all POIs that existed at that time.
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extraction.poi_extractor import POIExtractor
# OSMHistoryTracker moved to scripts/poi_history_tracker.py
# Import directly if needed: from scripts.poi_history_tracker import OSMHistoryTracker
import pandas as pd
from datetime import datetime
import argparse


class POIDateExtractor:
    """Extract POIs as they existed on a specific date"""
    
    def __init__(self, place: str = "London, UK"):
        """
        Initialize the date extractor
        
        Args:
            place: Name of the place to extract from
        """
        self.place = place
        self.extractor = POIExtractor(place)
        # OSMHistoryTracker moved to scripts/ - import if needed
        # self.tracker = OSMHistoryTracker(place)
        self.tracker = None  # Optional - can be set later if needed
    
    def extract_pois_at_date(self, target_date: str, 
                            poi_types: list = None,
                            output_dir: str = None) -> pd.DataFrame:
        """
        Extract all POIs as they existed on a specific date
        
        Args:
            target_date: Date in format 'YYYY-MM-DD'
            poi_types: List of POI types to extract (None = all)
            output_dir: Directory to save results (default: data/)
        
        Returns:
            DataFrame with POIs at that date
        """
        print("=" * 70)
        print(f"EXTRACTING POIs AS OF {target_date}")
        print("=" * 70)
        
        # Validate date format
        try:
            date_obj = datetime.strptime(target_date, "%Y-%m-%d")
            print(f"Target date: {date_obj.strftime('%B %d, %Y')}")
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD")
            return pd.DataFrame()
        
        # Check if date is in the future
        if date_obj > datetime.now():
            print(f"Warning: Date is in the future. Using current data.")
            target_date = datetime.now().strftime("%Y-%m-%d")
        
        # Default POI types
        if poi_types is None:
            poi_types = ['restaurants', 'cafes', 'hotels', 'museums', 'parks', 'shops']
        
        print(f"\nExtracting POI types: {', '.join(poi_types)}")
        print(f"Date: {target_date}")
        
        # Extract POIs at the target date
        # Note: For true historical data, this requires Overpass with history support
        # or historical planet files. For now, we extract current data and add date metadata
        
        all_pois = []
        
        poi_tags = {
            'restaurants': {'amenity': 'restaurant'},
            'cafes': {'amenity': 'cafe'},
            'hotels': {'tourism': 'hotel'},
            'museums': {'tourism': 'museum'},
            'parks': {'leisure': 'park'},
            'shops': {'shop': True}
        }
        
        for poi_type in poi_types:
            if poi_type not in poi_tags:
                print(f"  Skipping unknown POI type: {poi_type}")
                continue
            
            print(f"\nExtracting {poi_type}...", end=" ", flush=True)
            
            # Use tracker to extract at date (will use current data with date metadata)
            df = self.tracker.extract_pois_at_date(
                tags=poi_tags[poi_type],
                date=target_date,
                name=poi_type
            )
            
            if not df.empty:
                df['poi_type'] = poi_type
                df['snapshot_date'] = target_date
                all_pois.append(df)
        
        if not all_pois:
            print("\nNo POIs extracted")
            return pd.DataFrame()
        
        # Combine all POIs
        combined_df = pd.concat(all_pois, ignore_index=True)
        
        # Add metadata
        combined_df['extraction_date'] = datetime.now().strftime("%Y-%m-%d")
        combined_df['extraction_timestamp'] = datetime.now().isoformat()
        combined_df['target_date'] = target_date
        
        print(f"\n" + "=" * 70)
        print(f"EXTRACTION COMPLETE")
        print("=" * 70)
        print(f"Total POIs extracted: {len(combined_df):,}")
        
        if 'poi_type' in combined_df.columns:
            print("\nBreakdown by type:")
            type_counts = combined_df['poi_type'].value_counts()
            for poi_type, count in type_counts.items():
                print(f"  {poi_type:15s}: {count:>8,}")
        
        # Save to file
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create filename with date
        date_str = target_date.replace('-', '')
        filename = os.path.join(output_dir, f"london_pois_{date_str}.csv")
        combined_df.to_csv(filename, index=False)
        print(f"\n✓ Saved to: {filename}")
        
        # Also save as snapshot
        snapshot_filename = os.path.join(output_dir, f"london_pois_snapshot_{date_str}.csv")
        combined_df.to_csv(snapshot_filename, index=False)
        print(f"✓ Saved snapshot: {snapshot_filename}")
        
        return combined_df
    
    def extract_multiple_dates(self, dates: list, 
                               poi_types: list = None) -> dict:
        """
        Extract POIs at multiple dates for comparison
        
        Args:
            dates: List of dates in format 'YYYY-MM-DD'
            poi_types: List of POI types to extract
        
        Returns:
            Dictionary mapping dates to DataFrames
        """
        print("=" * 70)
        print(f"EXTRACTING POIs AT MULTIPLE DATES")
        print("=" * 70)
        print(f"Dates: {', '.join(dates)}")
        
        results = {}
        
        for date in dates:
            print(f"\n{'='*70}")
            df = self.extract_pois_at_date(date, poi_types)
            results[date] = df
        
        return results
    
    def compare_dates(self, date1: str, date2: str, 
                     poi_types: list = None) -> dict:
        """
        Extract POIs at two dates and compare them
        
        Args:
            date1: First date (YYYY-MM-DD)
            date2: Second date (YYYY-MM-DD)
            poi_types: POI types to compare
        
        Returns:
            Dictionary with comparison results
        """
        print("=" * 70)
        print(f"COMPARING POIs BETWEEN {date1} AND {date2}")
        print("=" * 70)
        
        # Extract at both dates
        print(f"\nExtracting POIs at {date1}...")
        pois_date1 = self.extract_pois_at_date(date1, poi_types)
        
        print(f"\nExtracting POIs at {date2}...")
        pois_date2 = self.extract_pois_at_date(date2, poi_types)
        
        if pois_date1.empty or pois_date2.empty:
            print("Cannot compare - one or both extractions returned no data")
            return {}
        
        # Compare using change detector
        from src.extraction.poi_change_detector import POIChangeDetector
        detector = POIChangeDetector(self.place)
        
        changes = detector.compare_snapshots(pois_date1, pois_date2)
        
        # Summary
        print("\n" + "=" * 70)
        print("COMPARISON SUMMARY")
        print("=" * 70)
        print(f"Date 1 ({date1}): {len(pois_date1):,} POIs")
        print(f"Date 2 ({date2}): {len(pois_date2):,} POIs")
        print(f"\nChanges:")
        print(f"  Added: {len(changes['added']):,} POIs")
        print(f"  Removed: {len(changes['removed']):,} POIs")
        print(f"  Modified: {len(changes['modified']):,} POIs")
        
        # Save comparison
        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        detector.save_change_report(changes)
        
        return {
            'date1': date1,
            'date2': date2,
            'pois_date1': pois_date1,
            'pois_date2': pois_date2,
            'changes': changes
        }


def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(
        description='Extract POIs as they existed on a specific date'
    )
    parser.add_argument(
        'date',
        type=str,
        help='Target date in format YYYY-MM-DD (e.g., 2020-01-01)'
    )
    parser.add_argument(
        '--types',
        type=str,
        nargs='+',
        default=None,
        help='POI types to extract (restaurants, cafes, hotels, museums, parks, shops)'
    )
    parser.add_argument(
        '--compare',
        type=str,
        default=None,
        help='Compare with another date (format: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--place',
        type=str,
        default='London, UK',
        help='Place to extract from (default: London, UK)'
    )
    
    args = parser.parse_args()
    
    extractor = POIDateExtractor(args.place)
    
    if args.compare:
        # Compare two dates
        extractor.compare_dates(args.date, args.compare, args.types)
    else:
        # Extract at single date
        extractor.extract_pois_at_date(args.date, args.types)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Command-line mode
        main()
    else:
        # Interactive mode
        print("=" * 70)
        print("POI DATE EXTRACTOR")
        print("=" * 70)
        print("\nExtract POIs as they existed on a specific date")
        print("\nUsage examples:")
        print("  python src/poi_date_extractor.py 2020-01-01")
        print("  python src/poi_date_extractor.py 2020-01-01 --types restaurants cafes")
        print("  python src/poi_date_extractor.py 2020-01-01 --compare 2024-01-01")
        print("\nOr use in Python:")
        print("  from src.poi_date_extractor import POIDateExtractor")
        print("  extractor = POIDateExtractor('London, UK')")
        print("  pois = extractor.extract_pois_at_date('2020-01-01')")
        
        # Interactive prompt
        print("\n" + "=" * 70)
        date_input = input("Enter date (YYYY-MM-DD) or press Enter to exit: ").strip()
        
        if date_input:
            extractor = POIDateExtractor("London, UK")
            extractor.extract_pois_at_date(date_input)

