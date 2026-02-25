import pandas as pd
import json
import os

def process_static_data(output_file="vessels_ready.json"):
    input_file = "static.csv"
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return

    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} static records.")

    vessels_list = []
    
    for _, row in df.iterrows():
        # create document for final collection  "vessels" to load after into mongodb
        vessel_doc = {
            "vessel_id": str(row['vessel_id']).strip(),
            "country": str(row['country']) if pd.notnull(row['country']) else "Unknown", 
            "type_info": {
                "shiptype_code": int(row['shiptype']) if pd.notnull(row['shiptype']) else 0,
                "description": str(row['Description']).strip() if pd.notnull(row['Description']) else "Unknown"
            }
        }
        vessels_list.append(vessel_doc)

    with open(output_file, 'w', encoding='utf-8') as f:
        for doc in vessels_list:
            f.write(json.dumps(doc) + '\n')
            
    print(f"Successfully saved {len(vessels_list)} vessels to {output_file}")

if __name__ == "__main__":
    process_static_data()