"""
Comprehensive OSM POI Catalog Generator
Creates an exhaustive list of ALL OpenStreetMap POI types and their attributes
"""

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def create_comprehensive_osm_poi_catalog():
    """
    Create comprehensive catalog of ALL OSM POI types and their attributes
    Based on OSM tag documentation and actual data
    """
    print("=" * 70)
    print("CREATING COMPREHENSIVE OSM POI CATALOG")
    print("=" * 70)
    
    # Load actual data to see what attributes exist
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    csv_file = os.path.join(data_dir, 'london_pois.csv')
    
    df = pd.read_csv(csv_file, low_memory=False, nrows=1000)  # Sample for attribute discovery
    all_attributes = set(df.columns)
    
    print(f"\nFound {len(all_attributes)} unique attributes in data")
    
    # Comprehensive list of OSM POI categories and types
    # Based on OSM tag documentation
    osm_poi_catalog = []
    
    # AMENITY POIs (most common)
    amenity_types = [
        'restaurant', 'cafe', 'fast_food', 'bar', 'pub', 'food_court',
        'ice_cream', 'biergarten', 'pharmacy', 'hospital', 'clinic', 'dentist',
        'veterinary', 'doctors', 'school', 'university', 'college', 'kindergarten',
        'library', 'theatre', 'cinema', 'community_centre', 'townhall',
        'courthouse', 'embassy', 'police', 'fire_station', 'post_office',
        'bank', 'atm', 'bureau_de_change', 'fuel', 'charging_station',
        'parking', 'parking_space', 'bicycle_parking', 'bicycle_rental',
        'car_rental', 'car_sharing', 'taxi', 'bus_station', 'ferry_terminal',
        'place_of_worship', 'grave_yard', 'marketplace', 'nightclub',
        'stripclub', 'brothel', 'casino', 'gambling', 'internet_cafe',
        'telephone', 'toilets', 'shower', 'drinking_water', 'fountain',
        'bench', 'waste_basket', 'recycling', 'vending_machine', 'bbq',
        'shelter', 'hunting_stand', 'public_bookcase', 'clock', 'emergency_phone'
    ]
    
    # TOURISM POIs
    tourism_types = [
        'hotel', 'motel', 'hostel', 'guest_house', 'apartment', 'camp_site',
        'caravan_site', 'chalet', 'alpine_hut', 'wilderness_hut',
        'museum', 'gallery', 'theme_park', 'zoo', 'aquarium', 'attraction',
        'artwork', 'viewpoint', 'information', 'map', 'guidepost'
    ]
    
    # LEISURE POIs
    leisure_types = [
        'park', 'playground', 'sports_centre', 'stadium', 'pitch', 'track',
        'swimming_pool', 'fitness_centre', 'golf_course', 'marina', 'beach_resort',
        'dance', 'hackerspace', 'ice_rink', 'bowling_alley', 'escape_game',
        'adult_gaming_centre', 'amusement_arcade', 'beach', 'firepit', 'garden',
        'nature_reserve', 'recreation_ground', 'sauna', 'slipway', 'summer_camp',
        'water_park', 'wildlife_hide'
    ]
    
    # SHOP POIs (extensive list)
    shop_types = [
        'supermarket', 'convenience', 'mall', 'department_store', 'kiosk',
        'bakery', 'butcher', 'seafood', 'cheese', 'confectionery', 'pastry',
        'alcohol', 'wine', 'beverages', 'organic', 'tea', 'coffee', 'farm',
        'health_food', 'pet', 'art', 'craft', 'frame', 'collector', 'gift',
        'stationery', 'book', 'newsagent', 'tobacco', 'toy', 'computer',
        'electronics', 'hifi', 'mobile_phone', 'bicycle', 'car', 'car_repair',
        'car_parts', 'motorcycle', 'tyres', 'furniture', 'kitchen', 'houseware',
        'carpet', 'curtain', 'interior_decoration', 'bed', 'bathroom_furnishing',
        'doityourself', 'hardware', 'trade', 'paint', 'florist', 'garden_centre',
        'hairdresser', 'beauty', 'cosmetics', 'optician', 'jewelry', 'watch',
        'clothes', 'shoes', 'fabric', 'tailor', 'bag', 'luggage', 'boutique',
        'sewing', 'erotic', 'fashion_accessories', 'charity', 'second_hand',
        'variety_store', 'general', 'music', 'musical_instrument', 'video',
        'video_games', 'anime', 'outdoor', 'sports', 'swimming_pool', 'copyshop',
        'dry_cleaning', 'laundry', 'pet_grooming', 'veterinary', 'travel_agency',
        'vacuum_cleaner', 'weapons', 'window_blind', 'massage', 'tattoo'
    ]
    
    # OFFICE POIs
    office_types = [
        'company', 'estate_agent', 'insurance', 'lawyer', 'notary', 'political_party',
        'religion', 'research', 'tax_advisor', 'accountant', 'advertising_agency',
        'architect', 'consulting', 'courier', 'diplomatic', 'educational_institution',
        'employment_agency', 'energy_supplier', 'financial', 'financial_advisor',
        'forestry', 'foundation', 'government', 'guide', 'it', 'logistics',
        'marketing', 'media', 'moving_company', 'ngo', 'newspaper', 'ngo',
        'parcel_locker', 'private_investigator', 'property_management', 'quarry',
        'real_estate_agent', 'realtor', 'recruiter', 'register_office', 'surveyor',
        'taxi', 'telecommunication', 'therapist', 'travel_agent', 'visa', 'water_utility'
    ]
    
    # HEALTHCARE POIs
    healthcare_types = [
        'hospital', 'clinic', 'doctors', 'dentist', 'pharmacy', 'veterinary',
        'physiotherapist', 'psychotherapist', 'optometrist', 'audiologist',
        'podiatrist', 'chiropractor', 'midwife', 'occupational_therapist',
        'speech_therapist', 'blood_donation', 'dialysis', 'laboratory', 'birthing_center'
    ]
    
    # Combine all POI types
    all_poi_types = []
    
    for poi_type in amenity_types:
        all_poi_types.append({
            'poi_category': 'amenity',
            'poi_type': poi_type,
            'osm_tag': f'amenity={poi_type}'
        })
    
    for poi_type in tourism_types:
        all_poi_types.append({
            'poi_category': 'tourism',
            'poi_type': poi_type,
            'osm_tag': f'tourism={poi_type}'
        })
    
    for poi_type in leisure_types:
        all_poi_types.append({
            'poi_category': 'leisure',
            'poi_type': poi_type,
            'osm_tag': f'leisure={poi_type}'
        })
    
    for poi_type in shop_types:
        all_poi_types.append({
            'poi_category': 'shop',
            'poi_type': poi_type,
            'osm_tag': f'shop={poi_type}'
        })
    
    for poi_type in office_types:
        all_poi_types.append({
            'poi_category': 'office',
            'poi_type': poi_type,
            'osm_tag': f'office={poi_type}'
        })
    
    for poi_type in healthcare_types:
        all_poi_types.append({
            'poi_category': 'healthcare',
            'poi_type': poi_type,
            'osm_tag': f'healthcare={poi_type}'
        })
    
    # Create catalog with attributes
    catalog_data = []
    
    # Common attributes that apply to most POIs
    common_attributes = [
        'name', 'name:*', 'alt_name', 'official_name', 'short_name',
        'latitude', 'longitude', 'addr:*', 'address', 'phone', 'email',
        'website', 'contact:*', 'opening_hours', 'description', 'description:*',
        'wheelchair', 'accessibility', 'parking', 'wifi', 'internet_access',
        'payment:*', 'capacity', 'seats', 'rooms', 'stars', 'rating', 'price',
        'fee', 'operator', 'brand', 'cuisine', 'diet:*', 'delivery', 'takeaway',
        'reservation', 'smoking', 'outdoor_seating', 'indoor_seating',
        'geometry_wkt', 'element_type', 'extraction_timestamp', 'extraction_date',
        'extraction_time', 'source:*', 'ref:*', 'check_date', 'check_date:*',
        'CREATEDATE', 'building:start_date', 'heritage:*', 'historic:*',
        'wikidata', 'wikipedia', 'image', 'image:*', 'facebook', 'twitter',
        'instagram', 'youtube', 'linkedin'
    ]
    
    # Category-specific attributes
    category_attributes = {
        'amenity': ['amenity', 'amenity:*', 'cuisine', 'diet:*', 'breakfast', 'lunch', 'dinner'],
        'tourism': ['tourism', 'tourism:*', 'stars', 'rooms', 'check_in', 'check_out'],
        'leisure': ['leisure', 'leisure:*', 'sport', 'surface', 'lit'],
        'shop': ['shop', 'shop:*', 'sells', 'second_hand', 'organic'],
        'office': ['office', 'office:*', 'company', 'employees'],
        'healthcare': ['healthcare', 'healthcare:*', 'speciality', 'emergency']
    }
    
    print(f"\nCreating catalog for {len(all_poi_types)} POI types...")
    
    for poi_info in all_poi_types:
        poi_category = poi_info['poi_category']
        poi_type = poi_info['poi_type']
        
        # Get attributes for this category
        attrs = common_attributes.copy()
        if poi_category in category_attributes:
            attrs.extend(category_attributes[poi_category])
        
        # Add all attributes from our data that might apply
        for attr in sorted(all_attributes):
            # Skip if already added
            if attr in attrs:
                continue
            # Add if it's a general attribute
            if not attr.startswith(('was:', 'old_', 'disused:', 'not:', 'dontimport:')):
                attrs.append(attr)
        
        # Create entry for each attribute
        for attr in attrs:
            catalog_data.append({
                'poi_category': poi_category,
                'poi_type': poi_type,
                'osm_tag': poi_info['osm_tag'],
                'attribute': attr,
                'attribute_category': categorize_attribute(attr),
                'is_common': attr in common_attributes,
                'is_category_specific': attr in category_attributes.get(poi_category, [])
            })
    
    catalog_df = pd.DataFrame(catalog_data)
    
    # Save comprehensive catalog
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    output_file = os.path.join(data_dir, 'osm_comprehensive_poi_catalog.csv')
    catalog_df.to_csv(output_file, index=False)
    
    print(f"\n✓ Saved comprehensive catalog to: {output_file}")
    print(f"  Total POI types: {len(all_poi_types)}")
    print(f"  Total attribute mappings: {len(catalog_df):,}")
    
    # Create summary
    summary_data = []
    for category in catalog_df['poi_category'].unique():
        cat_df = catalog_df[catalog_df['poi_category'] == category]
        summary_data.append({
            'poi_category': category,
            'poi_types_count': cat_df['poi_type'].nunique(),
            'total_attributes': cat_df['attribute'].nunique(),
            'common_attributes': len(cat_df[cat_df['is_common'] == True]['attribute'].unique())
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_file = os.path.join(data_dir, 'osm_poi_categories_summary.csv')
    summary_df.to_csv(summary_file, index=False)
    print(f"✓ Saved summary to: {summary_file}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("CATALOG SUMMARY")
    print("=" * 70)
    for _, row in summary_df.iterrows():
        print(f"\n{row['poi_category'].upper()}:")
        print(f"  POI types: {row['poi_types_count']}")
        print(f"  Total attributes: {row['total_attributes']}")
        print(f"  Common attributes: {row['common_attributes']}")
    
    return catalog_df, summary_df


def categorize_attribute(attr):
    """Categorize an attribute"""
    attr_lower = attr.lower()
    
    if any(x in attr_lower for x in ['name', 'alt_name', 'official']):
        return 'Naming'
    elif any(x in attr_lower for x in ['addr:', 'address', 'location']):
        return 'Location/Address'
    elif any(x in attr_lower for x in ['phone', 'email', 'website', 'contact:']):
        return 'Contact'
    elif any(x in attr_lower for x in ['opening_hours', 'hours']):
        return 'Hours'
    elif any(x in attr_lower for x in ['description', 'note']):
        return 'Description'
    elif any(x in attr_lower for x in ['payment:', 'price', 'fee', 'cost']):
        return 'Payment/Pricing'
    elif any(x in attr_lower for x in ['wheelchair', 'accessibility', 'access']):
        return 'Accessibility'
    elif any(x in attr_lower for x in ['wifi', 'internet', 'parking']):
        return 'Amenities'
    elif any(x in attr_lower for x in ['cuisine', 'diet:', 'food', 'drink']):
        return 'Food/Drink'
    elif any(x in attr_lower for x in ['source:', 'ref:', 'check_date']):
        return 'Metadata'
    elif any(x in attr_lower for x in ['extraction_', 'snapshot_']):
        return 'Extraction Metadata'
    elif any(x in attr_lower for x in ['geometry', 'latitude', 'longitude']):
        return 'Geometry'
    else:
        return 'Other'


if __name__ == "__main__":
    create_comprehensive_osm_poi_catalog()

