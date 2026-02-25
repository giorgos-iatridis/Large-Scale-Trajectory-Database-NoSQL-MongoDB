import geopandas as gpd
import os
import json
import pandas as pd
from pymongo import MongoClient, GEOSPHERE
from pathlib import Path

def load_geodata():
    BASE_DIR = Path(__file__).resolve().parent
    GEO_BASE = BASE_DIR / "data" / "geodata"
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['piraeus_ais_db']
    
    layers = [
        ("harbours", GEO_BASE / "harbours" / "harbours.shp"),
        ("islands", GEO_BASE / "islands" / "islands.shp"),
        ("piraeus_port", GEO_BASE / "piraeus_port" / "piraeus_port.shp"),
        ("regions", GEO_BASE / "regions" / "regions.shp"),
        ("territorial_waters", GEO_BASE / "territorial_waters" / "saronic_territorial_waters.shp")
    ]
    
    for col_name, file_path in layers:
        # Check if shapefile exists. If not, fallback to .dbf
        if not file_path.exists():
            fallback = file_path.with_suffix(".dbf")
            if fallback.exists():
                file_path = fallback
            else:
                print(f"! Skipping {col_name}: File not found at {file_path}")
                continue
                
        print(f"-> Processing {col_name}...")
        
        try:
            # read spatial data with Greek values
            # cp1253 is the standard for Greek Shapefiles
            gdf = gpd.read_file(file_path, encoding='cp1253')
            
            #convert to WGS84 (EPSG:4326) for MongoDB compatibility
            if gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs(epsg=4326)
                
            data = []
            for _, row in gdf.iterrows():
                #handle properties and convert NaNs to None (null)
                props = row.drop('geometry').to_dict()
                props = {k: (v if not pd.isna(v) else None) for k, v in props.items()}
                
                # now we onvert geometry to GeoJSON format
                geom = json.loads(json.dumps(row.geometry.__geo_interface__))
                
                doc = {
                    "properties": props,
                    "geometry": geom
                }
                data.append(doc)
            # drop old collection and insert new data 
            if data:
                db[col_name].drop() 
                db[col_name].insert_many(data)
                print(f"  Successfully inserted {len(data)} features into '{col_name}'.")
                
                # Create 2dsphere index for spatial queries (Intersect, Within, etc.)
                db[col_name].create_index([("geometry", GEOSPHERE)])
                print(f"  Spatial index created for '{col_name}'.")
                
        except Exception as e:
            print(f"  Error loading {col_name}: {e}")

if __name__ == "__main__":
    load_geodata()