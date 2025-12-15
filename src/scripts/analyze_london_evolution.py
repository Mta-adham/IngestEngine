"""
Practical Script to Analyze London's Evolution Using OSM Historical Data

This script provides multiple approaches to track London's evolution:
1. Current data with metadata (what we can do now)
2. Changeset analysis (track edits over time)
3. Comparison framework (compare different time periods)
"""

import pandas as pd
from datetime import datetime
import sys
import os
import json

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extraction.poi_extractor import POIExtractor
# Note: poi_history_tracker is now in scripts/ - import directly if needed
# from poi_history_tracker import OSMHistoryTracker


def analyze_current_with_metadata():
    """
    Analyze current POIs and extract metadata that indicates age/creation
    This gives us clues about when POIs were added
    """
    print("=" * 70)
    print("ANALYZING CURRENT POIs WITH METADATA")
    print("=" * 70)
    
    extractor = POIExtractor("London, UK")
    all_pois = extractor.extract_all_pois()
    
    if all_pois.empty:
        print("No POIs extracted")
        return
    
    # Look for date-related fields
    date_fields = [col for col in all_pois.columns 
                   if any(x in col.lower() for x in ['date', 'created', 'start', 'check_date'])]
    
    print(f"\nFound {len(date_fields)} date-related fields:")
    for field in date_fields[:10]:
        non_null = all_pois[field].notna().sum()
        print(f"  - {field}: {non_null:,} non-null values")
    
    # Analyze by POI type
    print("\n" + "=" * 70)
    print("POI COUNTS BY TYPE (Current State)")
    print("=" * 70)
    
    if 'poi_type' in all_pois.columns:
        type_counts = all_pois['poi_type'].value_counts()
        for poi_type, count in type_counts.items():
            print(f"  {poi_type.capitalize():15s}: {count:>8,}")
    
    # Save with all metadata
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)
    extractor.save_to_csv(all_pois, os.path.join(data_dir, "london_pois.csv"))
    
    return all_pois


def create_evolution_framework():
    """
    Create a framework for tracking evolution
    This sets up the structure for historical analysis
    """
    print("\n" + "=" * 70)
    print("CREATING EVOLUTION TRACKING FRAMEWORK")
    print("=" * 70)
    
    # Define time periods
    time_periods = {
        '2005-2009': {'start': '2005-01-01', 'end': '2009-12-31', 'label': 'Early OSM (2005-2009)'},
        '2010-2014': {'start': '2010-01-01', 'end': '2014-12-31', 'label': 'Growth Period (2010-2014)'},
        '2015-2019': {'start': '2015-01-01', 'end': '2019-12-31', 'label': 'Maturation (2015-2019)'},
        '2020-2024': {'start': '2020-01-01', 'end': '2024-12-31', 'label': 'Recent (2020-2024)'},
    }
    
    # Create tracking structure
    evolution_structure = {
        'time_periods': time_periods,
        'poi_types': ['restaurants', 'cafes', 'hotels', 'museums', 'parks', 'shops'],
        'metrics': ['count', 'with_names', 'with_addresses', 'with_websites', 'with_coordinates'],
        'created_at': datetime.now().isoformat()
    }
    
    # Save framework
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
    os.makedirs(config_dir, exist_ok=True)
    with open(os.path.join(config_dir, 'evolution_framework.json'), 'w') as f:
        json.dump(evolution_structure, f, indent=2)
    
    print("\nEvolution framework created:")
    print(f"  Time periods: {len(time_periods)}")
    print(f"  POI types: {len(evolution_structure['poi_types'])}")
    print(f"  Metrics: {len(evolution_structure['metrics'])}")
    print("\nSaved to: evolution_framework.json")
    
    return evolution_structure


def generate_historical_analysis_plan():
    """
    Generate a plan for historical analysis using different data sources
    """
    print("\n" + "=" * 70)
    print("HISTORICAL ANALYSIS PLAN")
    print("=" * 70)
    
    plan = {
        'phase_1_current_analysis': {
            'description': 'Analyze current POI data with metadata',
            'output': 'london_pois.csv',
            'status': 'Complete'
        },
        'phase_2_changeset_analysis': {
            'description': 'Query OSM changesets API for edit history',
            'method': 'Use OSM API: GET /api/0.6/changesets?bbox=...&time=...',
            'output': 'london_changesets.csv',
            'status': 'Ready to implement'
        },
        'phase_3_overpass_history': {
            'description': 'Use Overpass API with [date:"YYYY-MM-DD"] syntax',
            'method': 'Overpass queries with date filters',
            'output': 'london_pois_by_date.csv',
            'status': 'Requires Overpass server with history support'
        },
        'phase_4_full_history': {
            'description': 'Download and process OSM full history files',
            'method': 'Download from planet.openstreetmap.org/planet/full-history/',
            'tools': ['Osmium', 'Osmosis'],
            'output': 'london_full_history.osh',
            'status': 'Advanced - requires large downloads'
        },
        'phase_5_archived_snapshots': {
            'description': 'Compare archived planet file snapshots',
            'method': 'Download archived planet files from different years',
            'output': 'london_snapshots_comparison.csv',
            'status': 'Requires multiple large file downloads'
        }
    }
    
    print("\nRecommended approach for London evolution analysis:")
    print("\n1. START HERE: Current Analysis with Metadata")
    print("   - Extract all current POIs")
    print("   - Analyze date fields (created, check_date, etc.)")
    print("   - Build baseline dataset")
    print("   ✓ This script does this")
    
    print("\n2. Changeset Analysis (Next Step)")
    print("   - Query OSM changesets API for London bounding box")
    print("   - Filter by POI-related tags")
    print("   - Build timeline of additions/modifications")
    print("   - Code example in OSM_HISTORICAL_DATA_GUIDE.md")
    
    print("\n3. Overpass Historical Queries")
    print("   - Use Overpass API with date filters")
    print("   - Extract POIs as they existed at specific dates")
    print("   - Compare snapshots")
    print("   - Note: Requires Overpass server with history support")
    
    print("\n4. Full History Analysis (Advanced)")
    print("   - Download OSM full history extract for London")
    print("   - Process with Osmium tool")
    print("   - Build complete time-series database")
    print("   - See OSM_HISTORICAL_DATA_GUIDE.md for details")
    
    # Save plan
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
    os.makedirs(config_dir, exist_ok=True)
    with open(os.path.join(config_dir, 'historical_analysis_plan.json'), 'w') as f:
        json.dump(plan, f, indent=2)
    
    print("\n✓ Analysis plan saved to: historical_analysis_plan.json")
    
    return plan


def create_changeset_query_example():
    """
    Create example code for querying OSM changesets
    """
    example_code = '''
# Example: Query OSM Changesets for London
import requests
from datetime import datetime

# London bounding box
bbox = (-0.6, 51.3, 0.3, 51.7)  # west, south, east, north

# Date range
start_date = "2020-01-01"
end_date = "2024-01-01"

# OSM Changeset API
url = "https://api.openstreetmap.org/api/0.6/changesets"
params = {
    'bbox': f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
    'time': f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
    'closed': 'true'
}

response = requests.get(url, params=params)
# Parse XML response to get changeset data
'''
    
    src_dir = os.path.dirname(__file__)
    with open(os.path.join(src_dir, 'changeset_query_example.py'), 'w') as f:
        f.write(example_code)
    
    print("\n✓ Example changeset query code saved to: changeset_query_example.py")


def main():
    """Main analysis function"""
    print("=" * 70)
    print("LONDON EVOLUTION ANALYSIS - COMPREHENSIVE APPROACH")
    print("=" * 70)
    print("\nThis script provides multiple approaches to track London's evolution")
    print("over the last 20 years using OpenStreetMap data.\n")
    
    # Step 1: Analyze current data with metadata
    current_pois = analyze_current_with_metadata()
    
    # Step 2: Create evolution framework
    framework = create_evolution_framework()
    
    # Step 3: Generate analysis plan
    plan = generate_historical_analysis_plan()
    
    # Step 4: Create example code
    create_changeset_query_example()
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("\nFiles created:")
    print("  1. london_pois.csv - Current POIs (cleaned, no OSM metadata)")
    print("  2. evolution_framework.json - Structure for tracking evolution")
    print("  3. historical_analysis_plan.json - Detailed analysis plan")
    print("  4. changeset_query_example.py - Example code for changeset queries")
    print("\nNext steps:")
    print("  1. Review OSM_HISTORICAL_DATA_GUIDE.md for detailed methods")
    print("  2. Implement changeset analysis (see changeset_query_example.py)")
    print("  3. For full history, download OSM history files and use Osmium")
    print("  4. Compare current data with historical snapshots")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

