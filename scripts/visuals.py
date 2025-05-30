#!/usr/bin/env python3
"""
Create a heatmap visualization of service requests around Bellville South
This script can be run after the main pipeline to generate visualizations
"""

import pandas as pd
import geopandas as gpd
import h3
import folium
from folium.plugins import HeatMap
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_folium_heatmap():
    """Create an interactive heatmap using Folium"""
    logger.info("Creating interactive heatmap with Folium...")
    
    try:
        # Load the anonymized data
        df = pd.read_csv('data/bellville_requests_anonymized.csv')
        logger.info(f"Loaded {len(df)} anonymized records")
        
        # Filter for valid coordinates
        valid_coords = df[['latitude', 'longitude']].notna().all(axis=1)
        df_valid = df[valid_coords]
        logger.info(f"Records with valid coordinates: {len(df_valid)}")
        
        if len(df_valid) == 0:
            logger.error("No valid coordinates found for mapping")
            return None
        
        # Create base map centered on Bellville South
        center_lat = df_valid['latitude'].mean()
        center_lon = df_valid['longitude'].mean()
        
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=12,
            tiles='OpenStreetMap'
        )
        
        # Prepare data for heatmap
        heat_data = [[row['latitude'], row['longitude']] for idx, row in df_valid.iterrows()]
        
        # Add heatmap layer
        HeatMap(heat_data, radius=15, blur=10, gradient={
            0.2: 'blue',
            0.4: 'lime', 
            0.6: 'orange',
            1.0: 'red'
        }).add_to(m)
        
        # Add Bellville South marker
        bellville_center = (-33.919407, 18.637758)
        folium.Marker(
            bellville_center,
            popup="Bellville South Centroid",
            tooltip="Reference Point",
            icon=folium.Icon(color='green', icon='star')
        ).add_to(m)
        
        # Add circle showing 1km radius
        folium.Circle(
            bellville_center,
            radius=1000,  # 1km in meters
            popup="1-minute driving radius (1km)",
            color='green',
            fillColor='green',
            fillOpacity=0.1
        ).add_to(m)
        
        # Save the map
        m.save('data/bellville_service_requests_heatmap.html')
        logger.info("Interactive heatmap saved to: data/bellville_service_requests_heatmap.html")
        
        return m
        
    except Exception as e:
        logger.error(f"Failed to create Folium heatmap: {e}")
        return None

def create_h3_hexagon_map(resolution=9):
    """Create a choropleth map using H3 hexagons at specified resolution"""
    logger.info(f"Creating H3 hexagon choropleth map at resolution {resolution}...")
    
    try:
        # Load data
        df = pd.read_csv('data/bellville_requests_anonymized.csv')
        
        # Count requests per H3 hexagon
        h3_counts = Counter()
        for idx, row in df.iterrows():
            if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                try:
                    h3_index = h3.latlng_to_cell(row['latitude'], row['longitude'], resolution)
                    h3_counts[h3_index] += 1
                except:
                    continue
        
        logger.info(f"Found service requests in {len(h3_counts)} unique H3 hexagons")
        
        # Create GeoDataFrame with H3 hexagons
        hexagon_data = []
        for h3_index, count in h3_counts.items():
            try:
                # Get hexagon boundary (returns list of (lat, lon) tuples)
                boundary = h3.cell_to_boundary(h3_index)
                # Convert to proper polygon format (lon, lat for GeoJSON)
                coords = [[lon, lat] for lat, lon in boundary]
                coords.append(coords[0])  # Close the polygon
                
                from shapely.geometry import Polygon
                polygon = Polygon(coords)
                
                hexagon_data.append({
                    'h3_index': h3_index,
                    'request_count': count,
                    'geometry': polygon
                })
            except Exception as e:
                logger.warning(f"Failed to process H3 index {h3_index}: {e}")
                continue
        
        if not hexagon_data:
            logger.error("No valid hexagon data created")
            return None
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(hexagon_data, crs='EPSG:4326')
        
        # Create map
        center_lat = -33.919407
        center_lon = 18.637758
        
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=13,
            tiles='OpenStreetMap'
        )
        
        # Add choropleth layer
        folium.Choropleth(
            geo_data=gdf.to_json(),
            data=gdf,
            columns=['h3_index', 'request_count'],
            key_on='feature.properties.h3_index',
            fill_color='YlOrRd',
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name='Service Requests per Hexagon'
        ).add_to(m)
        
        # Add Bellville South marker
        folium.Marker(
            [center_lat, center_lon],
            popup="Bellville South Centroid",
            tooltip="Reference Point",
            icon=folium.Icon(color='blue', icon='star')
        ).add_to(m)
        
        # Save the map
        filename = f'data/bellville_h3_choropleth_res{resolution}_map.html'
        m.save(filename)
        logger.info(f"H3 choropleth map (resolution {resolution}) saved to: {filename}")
        
        return m
        
    except Exception as e:
        logger.error(f"Failed to create H3 choropleth map: {e}")
        return None

def create_static_plots():
    """Create static plots for analysis"""
    logger.info("Creating static analysis plots...")
    
    try:
        # Load data
        df = pd.read_csv('data/bellville_requests_anonymized.csv')
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Bellville South Service Request Analysis', fontsize=16, fontweight='bold')
        
        # 1. Requests by hour (6-hour windows)
        if 'creation_timestamp_anon' in df.columns:
            df['hour'] = pd.to_datetime(df['creation_timestamp_anon']).dt.hour
            hour_counts = df['hour'].value_counts().sort_index()
            
            axes[0,0].bar(hour_counts.index, hour_counts.values, color='skyblue', edgecolor='navy')
            axes[0,0].set_title('Service Requests by Time of Day\n(6-hour anonymized windows)')
            axes[0,0].set_xlabel('Hour of Day')
            axes[0,0].set_ylabel('Number of Requests')
            axes[0,0].set_xticks([0, 6, 12, 18])
            axes[0,0].set_xticklabels(['00:00', '06:00', '12:00', '18:00'])
        
        # 2. Wind speed distribution
        if 'wind_speed' in df.columns:
            df['wind_speed'].hist(bins=20, ax=axes[0,1], color='lightgreen', edgecolor='darkgreen')
            axes[0,1].set_title('Wind Speed Distribution')
            axes[0,1].set_xlabel('Wind Speed (km/h)')
            axes[0,1].set_ylabel('Frequency')
            axes[0,1].axvline(df['wind_speed'].mean(), color='red', linestyle='--', 
                            label=f'Mean: {df["wind_speed"].mean():.1f} km/h')
            axes[0,1].legend()
        
        # 3. Request type distribution (top 10)
        if 'request_type' in df.columns:
            type_counts = df['request_type'].value_counts().head(10)
            axes[1,0].barh(range(len(type_counts)), type_counts.values, color='coral')
            axes[1,0].set_yticks(range(len(type_counts)))
            axes[1,0].set_yticklabels(type_counts.index, fontsize=8)
            axes[1,0].set_title('Top 10 Request Types')
            axes[1,0].set_xlabel('Number of Requests')
        
        # 4. Spatial distribution (lat/lon scatter)
        valid_coords = df[['latitude', 'longitude']].notna().all(axis=1)
        df_valid = df[valid_coords]
        
        if len(df_valid) > 0:
            scatter = axes[1,1].scatter(df_valid['longitude'], df_valid['latitude'], 
                                     alpha=0.6, s=20, c='purple')
            axes[1,1].set_title('Spatial Distribution of Requests\n(Anonymized to ~500m precision)')
            axes[1,1].set_xlabel('Longitude')
            axes[1,1].set_ylabel('Latitude')
            
            # Add Bellville South center
            axes[1,1].scatter(-33.919407, 18.637758, color='red', s=100, marker='*', 
                            label='Bellville South', zorder=5)
            axes[1,1].legend()
        
        plt.tight_layout()
        plt.savefig('data/bellville_analysis_plots.png', dpi=300, bbox_inches='tight')
        logger.info("Static plots saved to: data/bellville_analysis_plots.png")
        
        plt.show()
        
    except Exception as e:
        logger.error(f"Failed to create static plots: {e}")

def main():
    """Main function to create all visualizations"""
    logger.info("Starting visualization creation...")
    
    # Check if data exists
    try:
        df = pd.read_csv('data/bellville_requests_anonymized.csv')
        logger.info(f"Found {len(df)} records to visualize")
    except FileNotFoundError:
        logger.error("Data file not found. Please run the main pipeline first.")
        return
    
    # Create visualizations
    heatmap = create_folium_heatmap()
    
    # Create choropleth maps at different resolutions
    resolutions = [8, 9, 10]  # You can add 11 for even more detail
    for res in resolutions:
        logger.info(f"Creating H3 map at resolution {res}...")
        choropleth = create_h3_hexagon_map(res)
    
    create_static_plots()
    
    logger.info("Visualization creation completed!")
    logger.info("Generated files:")
    logger.info("- data/bellville_service_requests_heatmap.html (interactive heatmap)")
    for res in resolutions:
        logger.info(f"- data/bellville_h3_choropleth_res{res}_map.html (H3 hexagon choropleth, resolution {res})")
    logger.info("- data/bellville_analysis_plots.png (static analysis plots)")

if __name__ == "__main__":
    main()