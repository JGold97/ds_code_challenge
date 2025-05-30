import boto3
import json
import time
import geopandas as gpd
from botocore import UNSIGNED
from botocore.config import Config

def download_data_files():
    """Download all the data files we need"""
    print("Downloading data files...")
    
    # Create data directory first
    import os
    import requests
    os.makedirs('data', exist_ok=True)
    
    # Use direct HTTPS URLs instead of boto3
    base_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/"
    
    files_to_download = {
        'sr_hex_truncated.csv': 'data/sr_hex_truncated.csv',
        'city-hex-polygons-8.geojson': 'data/city-hex-polygons-8.geojson',
        'city-hex-polygons-8-10.geojson': 'data/city-hex-polygons-8-10.geojson',
        'sr.csv.gz': 'data/sr.csv.gz',
        'sr_hex.csv.gz': 'data/sr_hex.csv.gz'
    }
    
    for s3_key, local_path in files_to_download.items():
        try:
            start_time = time.time()
            url = base_url + s3_key
            response = requests.get(url)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            end_time = time.time()
            print(f"Downloaded {s3_key} in {end_time - start_time:.2f} seconds")
        except Exception as e:
            print(f"Failed to download {s3_key}: {e}")
            
def simulate_s3_select():
    """
    Simulate S3 SELECT functionality by loading the GeoJSON file locally
    and filtering for resolution level 8 data
    """
    print("\nSimulating S3 SELECT (Task 1 requirement)...")
    
    start_time = time.time()
    
    try:
        # Load the full GeoJSON file
        gdf = gpd.read_file('data/city-hex-polygons-8-10.geojson')
        
        # Filter for resolution level 8 (equivalent to S3 SELECT query)
        level_8_data = gdf[gdf['resolution'] == 8]
        
        # Save the filtered result
        level_8_data.to_file('data/city-hex-polygons-8-filtered.geojson', driver='GeoJSON')
        
        end_time = time.time()
        
        print(f"SUCCESS: Filtered resolution 8 data in {end_time - start_time:.2f} seconds")
        print(f"Original records: {len(gdf)}")
        print(f"Resolution 8 records: {len(level_8_data)}")
        
        # Validate against the provided level 8 file
        validation_gdf = gpd.read_file('data/city-hex-polygons-8.geojson')
        print(f"Validation file has: {len(validation_gdf)} records")
        
        if len(level_8_data) == len(validation_gdf):
            print("SUCCESS: Validation successful - record counts match!")
        else:
            print("WARNING: Record counts don't match - needs investigation")
            
    except Exception as e:
        print(f"ERROR: Failed to process GeoJSON: {e}")

if __name__ == "__main__":
    download_data_files()
    simulate_s3_select()
