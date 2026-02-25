import geopandas as gpd
import pandas as pd
import json
import os
import gc
import numpy as np
from pathlib import Path
from scipy.spatial import cKDTree


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WEATHER_PATH = DATA_DIR / "noaa_weather" / "noaa_weather" / "2019"
DYNAMIC_CSV = BASE_DIR / "dynamic.csv"
OUTPUT_CSV = BASE_DIR / "dynamic_with_weather.csv"
# lookup files
STATIONS_JSON = BASE_DIR / "stations_lookup.json"
WEATHER_JSON = BASE_DIR / "weather_data.json"
# list of shapefiles to load
SHP_FILES = [
    WEATHER_PATH / "jan" / "noaa_weather_jan2019_v2.shp",
    WEATHER_PATH / "feb" / "noaa_weather_feb2019_v2.shp",
    WEATHER_PATH / "mar" / "noaa_weather_mar2019_v2.shp"
]

def get_cardinal(degrees):
    #Convert degrees to compass cardinal (N, NE, ..
    if degrees is None or pd.isna(degrees): return None
    try:
        degrees = float(degrees) % 360
        bins = [0, 22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5, 360]
        labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N']
        return labels[np.digitize(degrees, bins) - 1]
    except: return None

def cleanup_files(files_list):
    #d elete existing files to start fresh
    for f in files_list:
        if f.exists():
            f.unlink()

def process_weather_shapes():
    #load shapefiles and create a dictionary for quick weather lookup
    cleanup_files([STATIONS_JSON, WEATHER_JSON])
    
    parts = []
    for path in SHP_FILES:
        if not path.exists():
            print(f"! Missing: {path.name}")
            continue
        print(f"-> Loading shapefile: {path.name}")
        gdf_tmp = gpd.read_file(path)
        if gdf_tmp.crs != "EPSG:4326":
            gdf_tmp = gdf_tmp.to_crs(epsg=4326)
        parts.append(gdf_tmp)

    if not parts:
        print("No weather data found!")
        return False

    gdf = pd.concat(parts, ignore_index=True)
    del parts
    gc.collect()

    # identify unique stations based on coordinates
    gdf['coord_key'] = gdf.geometry.apply(lambda g: f"{g.x:.5f}_{g.y:.5f}")
    unique_stations = gdf.drop_duplicates(subset=['coord_key']).copy()
    unique_stations['cell_id'] = range(len(unique_stations))
    
    # save station info for spatial joining later
    stations_list = [{"cell_id": int(r.cell_id), "lon": float(f"{r.geometry.x:.5f}"), "lat": float(f"{r.geometry.y:.5f}")} 
                     for r in unique_stations.itertuples()]
    
    with open(STATIONS_JSON, "w") as f: json.dump(stations_list, f)

    # build weather lookup dictionary
    coord_to_id = unique_stations.set_index('coord_key')['cell_id'].to_dict()
    gdf['cell_id'] = gdf['coord_key'].map(coord_to_id)

    weather_lookup = {}
    for row in gdf.itertuples():
        ts = getattr(row, 'timestamp_', None)
        if pd.isna(ts): continue
        
        # Round time to nearest 3-hour interval (NOAA standard)
        ts_aligned = int(np.round(ts / 10800) * 10800)
        key = f"{int(row.cell_id)}_{ts_aligned}"
        
        weather_lookup[key] = {"temp_c": round(float(row.TMP) - 273.15, 2) if pd.notnull(getattr(row, 'TMP', None)) else None,"wind_speed": round(float(row.WSPD), 2) if pd.notnull(getattr(row, 'WSPD', None)) else None,
            "wind_dir": round(float(row.WDIRMET), 2) if pd.notnull(getattr(row, 'WDIRMET', None)) else None,"visibility": round(float(row.VIS), 2) if pd.notnull(getattr(row, 'VIS', None)) else None,
            "pressure": round(float(row.PRMSL)/100, 2) if pd.notnull(getattr(row, 'PRMSL', None)) else None,
            "humidity": round(float(row.RH), 2) if pd.notnull(getattr(row, 'RH', None)) else None,"gust": round(float(row.GUST), 2) if pd.notnull(getattr(row, 'GUST', None)) else None}

    with open(WEATHER_JSON, "w") as f:json.dump(weather_lookup, f)
        
    print(f"Weather lookup ready ({len(weather_lookup):,} records)")
    return True

def merge_weather_with_dynamic():
    #enrich the dynamic AIS data with weather based on location and time
    cleanup_files([OUTPUT_CSV])
    
    with open(STATIONS_JSON, "r") as f: stations = json.load(f)
    with open(WEATHER_JSON, "r") as f: weather_data = json.load(f)

    # Use cKDTree for fast nearest-neighbor search (stations)
    tree = cKDTree(np.array([[s['lon'], s['lat']] for s in stations]))
    station_ids = np.array([s['cell_id'] for s in stations])

    chunk_size = 500000
    is_first = True
    for i, df in enumerate(pd.read_csv(DYNAMIC_CSV, chunksize=chunk_size)):
        # Calculate time and spatial keys
        t_sec = pd.to_datetime(df['t']).values.astype('datetime64[s]').astype('int64')
        _, indices = tree.query(df[['lon', 'lat']].values)
        
        ts_rounded = (np.round(t_sec / 10800) * 10800).astype('int64')
        lookup_keys = station_ids[indices].astype(str) + "_" + ts_rounded.astype(str)
        
        # Map weather data
        weather_df = pd.DataFrame(pd.Series(lookup_keys).map(weather_data).tolist(), index=df.index)
        
        # Add cardinals
        if 'wind_dir' in weather_df.columns:
            weather_df['wind_cardinal'] = weather_df['wind_dir'].apply(get_cardinal)
        if 'course' in df.columns:
            df['course_cardinal'] = df['course'].apply(get_cardinal)
        
        df['cell_id'] = station_ids[indices]
        final_df = pd.concat([df, weather_df], axis=1)

        # Strict string formatting for CSV export
        gps_cols = ['lat', 'lon']
        for col in gps_cols:
            final_df[col] = final_df[col].apply(lambda x: f"{float(x):.5f}" if pd.notnull(x) else "")
            
        float_cols = ['speed', 'course', 'temp_c', 'pressure', 'wind_speed', 'humidity', 'visibility', 'gust', 'wind_dir']
        for col in float_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].apply(lambda x: f"{float(x):.2f}" if pd.notnull(x) else "")

        # Save chunk
        mode, header = ('w', True) if is_first else ('a', False)
        final_df.to_csv(OUTPUT_CSV, index=False, mode=mode, header=header)
        
        is_first = False
        print(f"Chunk {i+1} processed...")
        gc.collect()

    print(f"\nSUCCESS: Data saved to {OUTPUT_CSV.name}")

if __name__ == "__main__":
    if process_weather_shapes():
        merge_weather_with_dynamic()