import geopandas as gpd
import pandas as pd
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient

def load_weather_collection():
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "data" 
    shp_path = DATA_DIR / "noaa_weather" / "noaa_weather" / "2019" / "jan" / "noaa_weather_jan2019_v2.shp"
    
    # 2. MongoDB Connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client['piraeus_ais_db']
    collection = db['weather']
    
    if not shp_path.exists():
        print(f"Error: File NOT found at: {shp_path}")
        return
        
    print(f"Reading Shapefile: {shp_path.name}")
    
    try:
        # load data with Geopandas
        gdf = gpd.read_file(shp_path)
        
        # Ensure coordinates are in WGS84
        if gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs(epsg=4326)
            
        print(f"File loaded. Processing {len(gdf):,} records...")
        
        # 3.map coordinates to Cell IDs
        gdf['coord_str'] = gdf.geometry.apply(lambda geom: f"{geom.x:.5f}_{geom.y:.5f}")
        unique_coords = sorted(gdf['coord_str'].unique())
        coord_map = {val: i for i, val in enumerate(unique_coords)}
        gdf['cell_id'] = gdf['coord_str'].map(coord_map)
        
        # 4. Prepare MongoDB Documents
        data = []
        for _, row in gdf.iterrows():
            ts_val = row.get('timestamp_')
            if pd.isna(ts_val):
                continue
                
            ts_val = int(ts_val)
            dt_object = datetime.fromtimestamp(ts_val, tz=timezone.utc)
            
            # Clean attributes and handle NaNs
            props = row.drop(['geometry', 'coord_str', 'cell_id']).to_dict()
            cleaned_props = {}
            for k, v in props.items():
                if pd.notna(v):
                    if isinstance(v, (pd.Timestamp, datetime)):
                        cleaned_props[k] = v
                    else:
                        try:
                            cleaned_props[k] = float(v)
                        except:
                            cleaned_props[k] = str(v)
            
            # unit conversion: Kelvin to Celsius
            if 'TMP' in cleaned_props:
                cleaned_props['temp_c'] = round(cleaned_props['TMP'] - 273.15, 2)
            
            # define GeoJSON point
            geom = {
                "type": "Point",
                "coordinates": [row.geometry.x, row.geometry.y]
            }
            
            doc = {
                "metadata": {
                    "cell_id": int(row['cell_id']),
                    "timestamp_unix": ts_val
                },
                "timestamp": dt_object,
                "location": geom,
                "weather_attributes": cleaned_props
            }
            data.append(doc)
            
        # 5. insert data into MongoDB
        if data:
            print("Dropping old weather collection...")
            collection.drop()
            
            print(f"Inserting {len(data):,} weather documents...")
            collection.insert_many(data)
            print("Insert complete.")
            
    except Exception as e:
        print(f"Error loading weather: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_weather_collection()