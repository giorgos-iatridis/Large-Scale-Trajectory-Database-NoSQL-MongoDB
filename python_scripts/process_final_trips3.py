import pandas as pd
import json
import os
import ast
from pathlib import Path

class RoundingEncoder(json.JSONEncoder):
    def iterencode(self, o, _one_shot=False):
        if isinstance(o, float):
            # Enforce 2 decimals for all float values (Metrics/Weather) 
            return format(o, '.2f')
        return super(RoundingEncoder, self).iterencode(o, _one_shot)

def clean_val(val, decimals):
    if val is None or pd.isna(val): 
        return None
    try:
        return float(f"{float(val):.{decimals}f}")
    except: 
        return None

def clean_annotations(val):
    if val is None: return []
    if isinstance(val, (list, pd.Series)):
        if len(val) == 0: return []
    try:
        if pd.isna(val): return []
    except: pass
    if val == "" or val == "[]": return []
    try:
        if isinstance(val, str):
            actual_list = ast.literal_eval(val)
            return actual_list if isinstance(actual_list, list) else [str(actual_list)]
        return [str(val)]
    except:
        return [str(val)]

def reconstruct_trips_enriched(input_csv="dynamic_with_weather.csv", output_json="trips_ready.json"):
    BASE_DIR = Path(__file__).resolve().parent
    INPUT_PATH = BASE_DIR / input_csv
    OUTPUT_PATH = BASE_DIR / output_json
    STATIC_PATH = BASE_DIR / "static.csv"
    if not INPUT_PATH.exists():
        print(f"Error: {input_csv} not found!")
        return

    # Static data lookup
    static_lookup = {}
    if STATIC_PATH.exists():
        static_lookup = pd.read_csv(STATIC_PATH).set_index('vessel_id').to_dict('index')

    # Load and sort data
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    df['t'] = pd.to_datetime(df['t'])
    df = df.sort_values(by=['vessel_id', 't']).reset_index(drop=True)
    
    # trip Splitting logic (120 minute gap), our modeling base
    df['time_diff'] = df.groupby('vessel_id')['t'].diff().dt.total_seconds() / 60
    df['trip_id'] = ((df['vessel_id'] != df['vessel_id'].shift()) | (df['time_diff'] > 120)).cumsum()

    print(f"Processing {len(df):,} rows...")
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        for trip_id, group in df.groupby('trip_id'):
            points = []
            v_id = group.iloc[0]['vessel_id']
            v_static = static_lookup.get(v_id, {})

            for row in group.itertuples():
                # speed Filter (0-60 knots), because the dataset does not contain army vessels or speedboats
                raw_speed = getattr(row, 'speed', 0.0)
                clean_speed = clean_val(raw_speed, 2)
                if clean_speed and clean_speed > 60:
                    clean_speed = 0.0

                # wather Data (enforce 2 decimals)
                weather = {}
                weather_fields = ["temp_c", "wind_speed", "wind_dir", "humidity", "pressure", "visibility", "gust"]
                for field in weather_fields:
                    val = getattr(row, field, None)
                    if pd.notnull(val):
                        weather[field] = clean_val(val, 2)
                
                if hasattr(row, 'wind_cardinal') and pd.notnull(row.wind_cardinal):
                    weather["wind_cardinal"] = row.wind_cardinal

                # build  each point with 5 decimals for GPS coordinates
                p = {
                    "t": row.t.isoformat(),
                    "loc": {
                        "type": "Point",
                        "coordinates": [
                            float(f"{float(row.lon):.5f}"), 
                            float(f"{float(row.lat):.5f}")
                        ],
                        "cell_id": int(row.cell_id) if pd.notnull(row.cell_id) else None
                    },
                    "metrics": {
                        "speed": clean_speed,
                        "course": clean_val(row.course, 2),
                        "heading": int(row.heading) if pd.notnull(row.heading) and row.heading <= 360 else None,
                        "course_cardinal": getattr(row, 'course_cardinal', None)
                    },
                    "weather_data": weather,
                    "annotations": clean_annotations(getattr(row, 'annotations', []))
                }
                points.append(p)

            # we save trip only if it contains multiple points (our rule)
            if len(points) > 1:
                doc = {
                    "trip_id": int(trip_id),
                    "vessel_id": str(v_id),
                    "country": v_static.get('country', 'Unknown'),
                    "shiptype": int(v_static.get('shiptype', 0)) if pd.notnull(v_static.get('shiptype', 0)) else 0,
                    "vessel_type_description": v_static.get('Description', 'N/A'),
                    "start_time": points[0]['t'],
                    "end_time": points[-1]['t'],
                    "point_count": len(points),
                    "trajectory": points
                }
                f.write(json.dumps(doc, ensure_ascii=False, cls=RoundingEncoder) + '\n')

    print(f"SUCCESS: Trips reconstructed in {output_json}")

if __name__ == "__main__":
    reconstruct_trips_enriched()