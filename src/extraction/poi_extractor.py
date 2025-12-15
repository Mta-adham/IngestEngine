"""
Point of Interest (POI) Extractor for London using OpenStreetMap
================================================================

Extracts various types of POIs from OpenStreetMap for London.

Features:
- Extract POIs by type (restaurants, cafes, hotels, museums, parks, shops, etc.)
- Extract all OSM attributes per POI
- Export to CSV/JSON with coordinates, descriptions, timestamps

Usage:
    from src.poi_extractor import POIExtractor
    
    extractor = POIExtractor(place="London, UK")
    restaurants = extractor.extract_restaurants()
    all_pois = extractor.extract_all_pois()
"""

# ============================================
# IMPORTS
# ============================================
import osmnx as ox
import pandas as pd
from typing import List, Dict, Optional
import json
import warnings
from datetime import datetime

# Suppress performance warnings from geopandas
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


# ============================================
# CLASS DEFINITION
# ============================================

class POIExtractor:
    """Extract Points of Interest from OpenStreetMap for London"""
    
    # ============================================
    # INITIALIZATION
    # ============================================
    
    def __init__(self, place: str = "London, UK"):
        """
        Initialize the POI extractor
        
        Args:
            place: Name of the place to extract POIs from (default: "London, UK")
        """
        self.place = place
    
    # ============================================
    # UTILITY METHODS
    # ============================================
    
    @staticmethod
    def _extract_coordinates(geom):
        """Extract longitude and latitude from geometry"""
        if geom.geom_type == 'Point':
            return pd.Series({'longitude': geom.x, 'latitude': geom.y})
        else:
            # For polygons/other shapes, use centroid
            centroid = geom.centroid
            return pd.Series({'longitude': centroid.x, 'latitude': centroid.y})
    
    def extract_pois_by_tag(self, tags: Dict[str, str], name: str = "POI") -> pd.DataFrame:
        """
        Extract POIs based on OSM tags
        
        Args:
            tags: Dictionary of OSM tags to filter by (e.g., {"amenity": "restaurant"})
            name: Name for the POI type (for logging)
            
        Returns:
            DataFrame containing the extracted POIs
        """
        print(f"Extracting {name}...", end=" ", flush=True)
        try:
            # Use features_from_place which is simpler and more reliable
            pois = ox.features_from_place(self.place, tags=tags)
            
            if len(pois) == 0:
                print("No entries found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            pois_df = pois.reset_index()
            
            # Extract coordinates from geometry - ALWAYS ensure longitude and latitude exist
            if 'geometry' in pois_df.columns:
                # Always extract coordinates to ensure they're present
                coords = pois_df['geometry'].apply(self._extract_coordinates)
                # Update or add coordinates
                if 'longitude' in pois_df.columns:
                    pois_df['longitude'] = pois_df['longitude'].fillna(coords['longitude'])
                else:
                    pois_df['longitude'] = coords['longitude']
                
                if 'latitude' in pois_df.columns:
                    pois_df['latitude'] = pois_df['latitude'].fillna(coords['latitude'])
                else:
                    pois_df['latitude'] = coords['latitude']
                
                # Also save geometry as WKT (Well-Known Text) for reference
                pois_df['geometry_wkt'] = pois_df['geometry'].apply(lambda x: x.wkt if hasattr(x, 'wkt') else str(x))
            
            # Remove duplicate columns (keep first occurrence)
            pois_df = pois_df.loc[:, ~pois_df.columns.duplicated()]
            
            # Ensure critical fields exist (add as None if missing)
            critical_fields = {
                'longitude': None,
                'latitude': None,
                'description': None,
                'name': None,
                'osmid': None,
                'element_type': None
            }
            
            for field, default_value in critical_fields.items():
                if field not in pois_df.columns:
                    pois_df[field] = default_value
            
            print(f"Found {len(pois_df)} entries")
            return pois_df
                
        except Exception as e:
            print(f"Error: {str(e)}")
            return pd.DataFrame()
    
    # ============================================
    # POI EXTRACTION METHODS
    # ============================================
    
    def extract_restaurants(self) -> pd.DataFrame:
        """Extract restaurants from London"""
        return self.extract_pois_by_tag({"amenity": "restaurant"}, "restaurants")
    
    def extract_cafes(self) -> pd.DataFrame:
        """Extract cafes from London"""
        return self.extract_pois_by_tag({"amenity": "cafe"}, "cafes")
    
    def extract_hotels(self) -> pd.DataFrame:
        """Extract hotels from London"""
        return self.extract_pois_by_tag({"tourism": "hotel"}, "hotels")
    
    def extract_museums(self) -> pd.DataFrame:
        """Extract museums from London"""
        return self.extract_pois_by_tag({"tourism": "museum"}, "museums")
    
    def extract_parks(self) -> pd.DataFrame:
        """Extract parks from London"""
        return self.extract_pois_by_tag({"leisure": "park"}, "parks")
    
    def extract_shops(self) -> pd.DataFrame:
        """Extract shops from London"""
        return self.extract_pois_by_tag({"shop": True}, "shops")
    
    def extract_all_pois(self, poi_types: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Extract multiple types of POIs
        
        Args:
            poi_types: List of POI types to extract. If None, extracts all common types.
                      Options: 'restaurants', 'cafes', 'hotels', 'museums', 'parks', 'shops'
        
        Returns:
            Combined DataFrame of all POIs
        """
        if poi_types is None:
            poi_types = ['restaurants', 'cafes', 'hotels', 'museums', 'parks', 'shops']
        
        all_pois = []
        extractors = {
            'restaurants': self.extract_restaurants,
            'cafes': self.extract_cafes,
            'hotels': self.extract_hotels,
            'museums': self.extract_museums,
            'parks': self.extract_parks,
            'shops': self.extract_shops
        }
        
        for poi_type in poi_types:
            if poi_type in extractors:
                df = extractors[poi_type]()
                if not df.empty:
                    df['poi_type'] = poi_type
                    all_pois.append(df)
        
        if not all_pois:
            return pd.DataFrame()
        
        # ============================================
        # Combine and standardize all POI DataFrames
        # ============================================
        
        # Get all unique columns from all DataFrames
        all_columns = set()
        for df in all_pois:
            all_columns.update(df.columns)
        
        # Standardize columns across all DataFrames
        standardized_pois = []
        for df in all_pois:
            # Add missing columns
            for col in all_columns:
                if col not in df.columns:
                    df[col] = None
            # Reorder columns consistently
            df = df[sorted(all_columns)]
            standardized_pois.append(df)
        
        combined_df = pd.concat(standardized_pois, ignore_index=True)
        
        # Add extraction timestamp
        extraction_timestamp = datetime.now().isoformat()
        combined_df['extraction_timestamp'] = extraction_timestamp
        combined_df['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
        combined_df['extraction_time'] = datetime.now().strftime('%H:%M:%S')
        
        # Ensure longitude and latitude are in the first columns for easy access
        priority_columns = ['poi_type', 'name', 'longitude', 'latitude', 'description', 
                           'osmid', 'element_type', 'extraction_timestamp', 'extraction_date', 'extraction_time']
        
        # Reorder columns: priority first, then all others
        existing_priority = [col for col in priority_columns if col in combined_df.columns]
        other_columns = [col for col in combined_df.columns if col not in existing_priority]
        final_column_order = existing_priority + sorted(other_columns)
        
        combined_df = combined_df[final_column_order]
        
        return combined_df
    
    # ============================================
    # EXPORT METHODS
    # ============================================
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = "london_pois.csv"):
        """
        Save POIs to CSV file with ALL attributes
        
        Ensures:
        - All columns are saved
        - Longitude and latitude are included
        - Description is included
        - All dates and timestamps are included
        - Geometry is saved as WKT
        """
        if df.empty:
            print("No data to save")
            return
        
        # Create a copy to avoid modifying original
        df_to_save = df.copy()
        
        # Ensure geometry column is handled (convert to string/WKT if present)
        if 'geometry' in df_to_save.columns:
            # Keep geometry as WKT string for CSV compatibility
            if 'geometry_wkt' not in df_to_save.columns:
                df_to_save['geometry_wkt'] = df_to_save['geometry'].apply(
                    lambda x: x.wkt if hasattr(x, 'wkt') else str(x) if pd.notna(x) else None
                )
            # Drop geometry object (can't serialize to CSV)
            df_to_save = df_to_save.drop(columns=['geometry'], errors='ignore')
        
        # Verify critical fields exist
        missing_critical = []
        for field in ['longitude', 'latitude']:
            if field not in df_to_save.columns:
                missing_critical.append(field)
                df_to_save[field] = None
        
        if missing_critical:
            print(f"Warning: Missing critical fields {missing_critical}, added as None")
        
        # Save with all columns
        try:
            df_to_save.to_csv(filename, index=False, encoding='utf-8')
            print(f"✓ Saved to {filename}")
            print(f"  Total records: {len(df_to_save):,}")
            print(f"  Total columns/attributes: {len(df_to_save.columns)}")
            print(f"  Key fields included: longitude, latitude, description, all timestamps")
        except Exception as e:
            print(f"Error saving CSV: {str(e)}")
            # Try saving without problematic columns
            print("Attempting to save with error handling...")
            # Replace any problematic values
            df_to_save = df_to_save.fillna('')
            df_to_save.to_csv(filename, index=False, encoding='utf-8', errors='replace')
            print(f"✓ Saved to {filename} (with error handling)")
    
    def save_to_json(self, df: pd.DataFrame, filename: str = "london_pois.json"):
        """Save POIs to JSON file"""
        if not df.empty:
            records = df.to_dict('records')
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2, default=str)
            print(f"✓ Saved to {filename}")
        else:
            print("No data to save")
    
    def print_poi_with_all_attributes(self, df: pd.DataFrame, poi_index: int = 0):
        """Print a single POI with all its attributes"""
        if df.empty:
            print("No POIs to display")
            return
        
        if poi_index >= len(df):
            print(f"Index {poi_index} out of range. Total POIs: {len(df)}")
            return
        
        poi = df.iloc[poi_index]
        
        print("\n" + "=" * 80)
        print(f"POI #{poi_index + 1} - ALL ATTRIBUTES")
        print("=" * 80)
        
        # Get all non-null attributes
        non_null_attrs = poi[poi.notna()].to_dict()
        
        print(f"\nTotal attributes available: {len(df.columns)}")
        print(f"Attributes with values: {len(non_null_attrs)}")
        print(f"Attributes with null/empty values: {len(df.columns) - len(non_null_attrs)}")
        
        # Group attributes by category for better readability
        basic_info = ['poi_type', 'name', 'osmid', 'element_type', 'latitude', 'longitude']
        address_info = [col for col in df.columns if col.startswith('addr:')]
        contact_info = [col for col in df.columns if col.startswith('contact:') or col in ['phone', 'email', 'website', 'fax']]
        amenity_info = ['amenity', 'tourism', 'leisure', 'shop', 'cuisine', 'opening_hours']
        other_attrs = [col for col in non_null_attrs.keys() 
                      if col not in basic_info + address_info + contact_info + amenity_info 
                      and col not in ['geometry']]
        
        # Print basic information
        print("\n" + "-" * 80)
        print("BASIC INFORMATION")
        print("-" * 80)
        for attr in basic_info:
            if attr in non_null_attrs:
                value = non_null_attrs[attr]
                if pd.notna(value) and str(value).strip():
                    print(f"  {attr:30s}: {value}")
        
        # Print address information
        if any(attr in non_null_attrs for attr in address_info):
            print("\n" + "-" * 80)
            print("ADDRESS INFORMATION")
            print("-" * 80)
            for attr in sorted(address_info):
                if attr in non_null_attrs:
                    value = non_null_attrs[attr]
                    if pd.notna(value) and str(value).strip():
                        print(f"  {attr:30s}: {value}")
        
        # Print contact information
        if any(attr in non_null_attrs for attr in contact_info):
            print("\n" + "-" * 80)
            print("CONTACT INFORMATION")
            print("-" * 80)
            for attr in sorted(contact_info):
                if attr in non_null_attrs:
                    value = non_null_attrs[attr]
                    if pd.notna(value) and str(value).strip():
                        print(f"  {attr:30s}: {value}")
        
        # Print amenity/tourism/leisure information
        amenity_found = [attr for attr in amenity_info if attr in non_null_attrs and pd.notna(non_null_attrs[attr])]
        if amenity_found:
            print("\n" + "-" * 80)
            print("AMENITY / TOURISM / LEISURE INFORMATION")
            print("-" * 80)
            for attr in amenity_found:
                value = non_null_attrs[attr]
                if pd.notna(value) and str(value).strip():
                    print(f"  {attr:30s}: {value}")
        
        # Print other attributes (first 50 to avoid overwhelming output)
        if other_attrs:
            print("\n" + "-" * 80)
            print(f"OTHER ATTRIBUTES (showing first 50 of {len(other_attrs)})")
            print("-" * 80)
            for attr in sorted(other_attrs)[:50]:
                value = non_null_attrs[attr]
                if pd.notna(value) and str(value).strip():
                    # Truncate very long values
                    str_value = str(value)
                    if len(str_value) > 100:
                        str_value = str_value[:100] + "..."
                    print(f"  {attr:30s}: {str_value}")
            
            if len(other_attrs) > 50:
                print(f"\n  ... and {len(other_attrs) - 50} more attributes")
        
        print("\n" + "=" * 80)
    
    def print_summary(self, df: pd.DataFrame):
        """Print a summary of extracted POIs"""
        if df.empty:
            print("No POIs to display")
            return
        
        print("\n" + "=" * 70)
        print("POI EXTRACTION SUMMARY")
        print("=" * 70)
        print(f"Total POIs extracted: {len(df):,}")
        
        if 'poi_type' in df.columns:
            print("\nBreakdown by type:")
            type_counts = df['poi_type'].value_counts()
            for poi_type, count in type_counts.items():
                print(f"  • {poi_type.capitalize()}: {count:,}")
        
        # Show sample with key information
        print("\n" + "-" * 70)
        print("Sample POIs (showing key information):")
        print("-" * 70)
        
        # Select columns to display
        display_cols = ['poi_type', 'name', 'latitude', 'longitude']
        if 'poi_type' not in df.columns:
            display_cols = ['name', 'latitude', 'longitude']
        
        available_cols = [col for col in display_cols if col in df.columns]
        sample_df = df[available_cols].head(10)
        
        # Format the display
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        
        print(sample_df.to_string(index=False))
        print()


def main():
    """Main function to extract and display POIs"""
    print("=" * 70)
    print("POINT OF INTEREST EXTRACTOR - LONDON")
    print("=" * 70)
    print(f"Extracting POIs from: London, UK\n")
    
    # Initialize extractor
    extractor = POIExtractor("London, UK")
    
    # Extract all POI types
    all_pois = extractor.extract_all_pois()
    
    if not all_pois.empty:
        # Print summary
        extractor.print_summary(all_pois)
        
        # Print detailed view of sample POIs from each type
        print("\n" + "=" * 80)
        print("DETAILED POI ATTRIBUTES - SAMPLE FROM EACH TYPE")
        print("=" * 80)
        
        if 'poi_type' in all_pois.columns:
            poi_types = all_pois['poi_type'].unique()
            for poi_type in poi_types[:3]:  # Show first 3 types
                type_df = all_pois[all_pois['poi_type'] == poi_type]
                if not type_df.empty:
                    print(f"\n\n{'='*80}")
                    print(f"Sample {poi_type.upper()} POI")
                    print(f"{'='*80}")
                    extractor.print_poi_with_all_attributes(type_df, 0)
        else:
            # If no poi_type column, just show first POI
            extractor.print_poi_with_all_attributes(all_pois, 0)
        
        # Verify all required fields are present
        print("\n" + "=" * 70)
        print("VERIFYING DATA COMPLETENESS")
        print("=" * 70)
        
        required_fields = ['longitude', 'latitude', 'description', 'extraction_timestamp']
        present_fields = [field for field in required_fields if field in all_pois.columns]
        missing_fields = [field for field in required_fields if field not in all_pois.columns]
        
        print(f"Required fields present: {len(present_fields)}/{len(required_fields)}")
        for field in present_fields:
            non_null_count = all_pois[field].notna().sum()
            print(f"  ✓ {field}: {non_null_count:,} non-null values")
        
        if missing_fields:
            print(f"  ⚠ Missing fields: {missing_fields}")
        
        # Count date/timestamp columns
        date_columns = [col for col in all_pois.columns if any(x in col.lower() for x in ['date', 'time', 'timestamp', 'created', 'modified', 'check_date'])]
        print(f"\nDate/Timestamp columns found: {len(date_columns)}")
        if date_columns:
            print(f"  Examples: {', '.join(date_columns[:10])}")
        
        # Count description columns
        desc_columns = [col for col in all_pois.columns if 'description' in col.lower()]
        print(f"\nDescription columns found: {len(desc_columns)}")
        if desc_columns:
            print(f"  Examples: {', '.join(desc_columns)}")
        
        print(f"\nTotal attributes to save: {len(all_pois.columns)}")
        
        # Save to files
        print("\n" + "=" * 70)
        print("SAVING RESULTS TO CSV")
        print("=" * 70)
        import os
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(data_dir, exist_ok=True)
        extractor.save_to_csv(all_pois, os.path.join(data_dir, "london_pois.csv"))
        
        print("\n" + "=" * 70)
        print("Extraction complete!")
        print("=" * 70)
    else:
        print("No POIs were extracted. Please check your connection and try again.")


if __name__ == "__main__":
    main()
