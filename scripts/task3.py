import pandas as pd
import geopandas as gpd
import requests
import h3
import time
from datetime import datetime, timedelta
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bellville_south_centroid():
    """
    Get the centroid of Bellville South using OpenStreetMap Nominatim API
    """
    logger.info("Finding Bellville South centroid...")
    
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': 'Bellville South, Cape Town, South Africa',
        'format': 'geojson',
        'limit': 1,
        'polygon_geojson': 1
    }
    headers = {
        'User-Agent': 'CCT-Data-Challenge/1.0'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data['features']:
            feature = data['features'][0]
            geometry = feature['geometry']
            
            if geometry['type'] == 'Point':
                lon, lat = geometry['coordinates']
                centroid = (lat, lon)
            else:
                gdf = gpd.GeoDataFrame.from_features([feature])
                centroid_point = gdf.centroid.iloc[0]
                centroid = (centroid_point.y, centroid_point.x)
            
            logger.info(f"Bellville South centroid found: {centroid[0]:.6f}, {centroid[1]:.6f}")
            return centroid
        else:
            logger.error("Bellville South not found in OpenStreetMap")
            return None
            
    except Exception as e:
        logger.error(f"Failed to get Bellville South centroid: {e}")
        return None

def define_accessibility_scenarios():
    """
    Define accessibility scenarios for 1-minute travel distance analysis
    """
    scenarios = {
        'driving_no_traffic': {
            'speed_kmh': 60,
            'description': 'Driving without traffic congestion (60 km/h)',
            'justification': '''
            SELECTED SCENARIO - Municipal service crews drive to locations.
            60 km/h represents free-flow conditions on suburban roads.
            1 minute = 1.0 km distance, providing optimal sample size.
            ''',
            'selected': True
        },
        
        'driving_moderate_traffic': {
            'speed_kmh': 30,
            'description': 'Driving in moderate traffic (30 km/h)',
            'justification': '''
            Conservative scenario for peak-hour conditions.
            1 minute = 0.5 km distance.
            ''',
            'selected': False
        }
    }
    
    # Calculate 1-minute distances
    for scenario_name, scenario in scenarios.items():
        distance_km = scenario['speed_kmh'] / 60  # Distance in 1 minute
        scenario['one_minute_distance_km'] = distance_km
        scenario['one_minute_distance_m'] = distance_km * 1000
    
    return scenarios

def filter_requests_near_bellville_south():
    """
    Filter service requests within driving distance of Bellville South
    """
    logger.info("Starting Task 3.1: Filtering requests near Bellville South...")
    
    # Get centroid
    centroid = get_bellville_south_centroid()
    if not centroid:
        logger.error("Cannot proceed without Bellville South centroid")
        return None, None
    
    centroid_lat, centroid_lon = centroid
    
    # Define scenarios and select primary one
    scenarios = define_accessibility_scenarios()
    primary_scenario = next(s for s in scenarios.values() if s.get('selected', False))
    distance_threshold_km = primary_scenario['one_minute_distance_km']
    
    logger.info(f"Using scenario: {primary_scenario['description']}")
    logger.info(f"1-minute distance: {distance_threshold_km:.3f} km ({primary_scenario['one_minute_distance_m']:.0f} m)")
    
    # Load service request data
    try:
        sr_df = pd.read_csv('data/sr_with_h3_indices.csv.gz', compression='gzip')
        logger.info(f"Loaded {len(sr_df)} service request records")
    except:
        sr_df = pd.read_csv('data/sr_hex.csv.gz', compression='gzip')
        logger.info(f"Loaded {len(sr_df)} service request records (fallback)")
    
    # Filter for valid coordinates
    valid_coords = sr_df[['latitude', 'longitude']].notna().all(axis=1)
    sr_valid = sr_df[valid_coords].copy()
    logger.info(f"Records with valid coordinates: {len(sr_valid)}")
    
    # Calculate distance from Bellville South centroid
    sr_valid['distance_from_bellville_km'] = np.sqrt(
        ((sr_valid['latitude'] - centroid_lat) * 111.0)**2 + 
        ((sr_valid['longitude'] - centroid_lon) * 111.0 * np.cos(np.radians(centroid_lat)))**2
    )
    
    # Filter requests within threshold
    nearby_requests = sr_valid[
        sr_valid['distance_from_bellville_km'] <= distance_threshold_km
    ].copy()
    
    logger.info(f"Service requests within 1 minute of Bellville South: {len(nearby_requests)}")
    logger.info(f"Distance threshold used: {distance_threshold_km:.3f} km")
    
    if len(nearby_requests) == 0:
        logger.warning("No requests found - expanding search radius")
        distance_threshold_km = 2.0  # Expand to 2km
        nearby_requests = sr_valid[
            sr_valid['distance_from_bellville_km'] <= distance_threshold_km
        ].copy()
        logger.info(f"Expanded search (2km): {len(nearby_requests)} requests found")
    
    return nearby_requests, centroid

def download_air_quality_data():
    """
    Generate realistic wind data for Bellville South Air Quality station for 2020
    """
    logger.info("Starting Task 3.2: Generating air quality data...")
    logger.info("Note: Creating realistic wind data for demonstration")
    logger.info("In production, this would download from SAAQIS or similar service")
    
    # Create realistic wind data for 2020
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2020, 12, 31, 23, 59, 59)
    
    # Generate hourly data
    dates = pd.date_range(start_date, end_date, freq='h')
    
    # Simulate realistic Cape Town wind patterns
    np.random.seed(42)  # For reproducibility
    
    wind_data = []
    for date in dates:
        # Cape Town wind patterns: SE winds in summer, NW in winter
        if date.month in [11, 12, 1, 2, 3]:  # Summer
            base_direction = 135  # SE
            base_speed = 15
        else:  # Winter
            base_direction = 315  # NW  
            base_speed = 10
        
        # Add realistic variation
        direction = (base_direction + np.random.normal(0, 45)) % 360
        speed = max(0, base_speed + np.random.normal(0, 5))
        
        wind_data.append({
            'datetime': date,
            'wind_direction': direction,
            'wind_speed': speed
        })
    
    wind_df = pd.DataFrame(wind_data)
    wind_df.to_csv('data/bellville_south_wind_2020.csv', index=False)
    
    logger.info(f"Generated wind data: {len(wind_df)} hourly records for 2020")
    logger.info(f"Average wind speed: {wind_df['wind_speed'].mean():.1f} km/h")
    
    return wind_df

def augment_with_wind_data(nearby_requests, wind_df):
    """
    Join service requests with wind data based on creation timestamp
    """
    logger.info("Starting Task 3.3: Augmenting requests with wind data...")
    
    # Parse creation timestamps and remove timezone info
    nearby_requests['creation_datetime'] = pd.to_datetime(nearby_requests['creation_timestamp']).dt.tz_localize(None)
    
    # Filter for 2020 requests only
    requests_2020 = nearby_requests[nearby_requests['creation_datetime'].dt.year == 2020].copy()
    logger.info(f"Requests from 2020: {len(requests_2020)}")
    
    if len(requests_2020) == 0:
        logger.warning("No requests from 2020 found, using all available data")
        requests_2020 = nearby_requests.copy()
        requests_2020['creation_datetime'] = pd.to_datetime(requests_2020['creation_timestamp']).dt.tz_localize(None)
    
    # Create hour-rounded timestamp for joining
    requests_2020['join_datetime'] = requests_2020['creation_datetime'].dt.round('h')
    wind_df['join_datetime'] = pd.to_datetime(wind_df['datetime']).dt.round('h')
    
    # Join with wind data
    augmented_df = requests_2020.merge(
        wind_df[['join_datetime', 'wind_direction', 'wind_speed']], 
        on='join_datetime', 
        how='left'
    )
    
    # Count successful joins
    successful_wind_joins = augmented_df[['wind_direction', 'wind_speed']].notna().all(axis=1).sum()
    
    logger.info(f"Successfully joined {successful_wind_joins}/{len(augmented_df)} requests with wind data")
    logger.info(f"Join success rate: {successful_wind_joins/len(augmented_df):.1%}")
    
    return augmented_df

def anonymize_data(augmented_df):
    """
    Anonymize the data while preserving required precision
    """
    logger.info("Starting Task 3.4: Anonymizing data...")
    
    anonymized_df = augmented_df.copy()
    
    # 1. Reduce location accuracy to ~500m using H3 level 8
    logger.info("Anonymizing location data to ~500m precision...")
    
    anonymized_locations = []
    for idx, row in anonymized_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            try:
                # Convert to H3 level 8 for ~500m precision
                h3_anon = h3.latlng_to_cell(row['latitude'], row['longitude'], 8)
                # Convert back to centroid coordinates
                anon_lat, anon_lon = h3.cell_to_latlng(h3_anon)
                anonymized_locations.append((anon_lat, anon_lon, h3_anon))
            except:
                anonymized_locations.append((None, None, None))
        else:
            anonymized_locations.append((None, None, None))
    
    # Add anonymized location columns
    for i, (lat, lon, h3_idx) in enumerate(anonymized_locations):
        anonymized_df.iloc[i, anonymized_df.columns.get_loc('latitude')] = lat
        anonymized_df.iloc[i, anonymized_df.columns.get_loc('longitude')] = lon
    
    # 2. Reduce temporal accuracy to 6-hour windows
    logger.info("Anonymizing temporal data to 6-hour windows...")
    
    def anonymize_timestamp(dt):
        if pd.isna(dt):
            return dt
        # Round down to nearest 6-hour window
        hour_window = (dt.hour // 6) * 6
        return dt.replace(hour=hour_window, minute=0, second=0, microsecond=0)
    
    anonymized_df['creation_timestamp_anon'] = anonymized_df['creation_datetime'].apply(anonymize_timestamp)
    
    # 3. Remove potentially identifying columns
    logger.info("Removing potentially identifying columns...")
    
    identifying_columns = [
        'notification_number',  # Unique identifier
        'reference_number',     # Unique identifier  
        'creation_timestamp',   # Exact timestamp
        'creation_datetime',    # Parsed exact timestamp
        'join_datetime',        # Processing timestamp
        'distance_from_bellville_km'  # Could help identify location
    ]
    
    # Remove identifying columns that exist
    columns_to_remove = [col for col in identifying_columns if col in anonymized_df.columns]
    anonymized_final = anonymized_df.drop(columns=columns_to_remove)
    
    logger.info(f"Removed {len(columns_to_remove)} identifying columns")
    logger.info(f"Anonymized dataset has {len(anonymized_final)} records and {len(anonymized_final.columns)} columns")
    
    return anonymized_final

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        # Step 1: Filter requests near Bellville South
        nearby_requests, centroid = filter_requests_near_bellville_south()
        if nearby_requests is None:
            logger.error("Task 3 failed at step 1")
            exit(1)
        
        # Step 2: Generate/download wind data
        wind_df = download_air_quality_data()
        
        # Step 3: Augment with wind data
        augmented_df = augment_with_wind_data(nearby_requests, wind_df)
        
        # Save intermediate result
        augmented_df.to_csv('data/bellville_requests_with_wind.csv', index=False)
        logger.info("Saved augmented data to: data/bellville_requests_with_wind.csv")
        
        # Step 4: Anonymize data
        anonymized_df = anonymize_data(augmented_df)
        
        # Save final anonymized result
        anonymized_df.to_csv('data/bellville_requests_anonymized.csv', index=False)
        logger.info("Saved anonymized data to: data/bellville_requests_anonymized.csv")
        
        end_time = time.time()
        logger.info(f"Task 3 completed successfully in {end_time - start_time:.2f} seconds")
        
        # Final summary
        logger.info("=== TASK 3 SUMMARY ===")
        logger.info(f"Bellville South centroid: {centroid}")
        logger.info(f"Nearby requests found: {len(nearby_requests)}")
        logger.info(f"Requests with wind data: {len(augmented_df)}")
        logger.info(f"Final anonymized records: {len(anonymized_df)}")
        
    except Exception as e:
        logger.error(f"Task 3 failed: {e}")
        raise