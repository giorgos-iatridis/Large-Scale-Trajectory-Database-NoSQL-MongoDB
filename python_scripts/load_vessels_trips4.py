import json
import os
import time
import sys
from pymongo import MongoClient

def load_data():
    try:
        # connect to local MongoDB server
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db_name = 'piraeus_ais_db'
        db = client[db_name]
        
        print(f"\n[!] Dropping database '{db_name}'...")
        client.drop_database(db_name)
        # we wait on purpose for MongoDB to complete file system sync
        time.sleep(2) 
        # 2. PRE-UPLOAD CHECK
        collections = db.list_collection_names()
        if not collections:
            print("Status: Database is COMPLETELY EMPTY. Ready to upload.")
        else:
            print(f"Warning: Found {len(collections)} collections remaining. Aborting.")
            sys.exit(1)

    except Exception as e:
        print(f"Connection or Reset failed: {e}")
        return

    # 3. LOADING VESSELS (Chunked for Memory Safety)
    if os.path.exists('vessels_ready.json'):
        v_count = 0
        v_chunk = []
        with open('vessels_ready.json', 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        doc = json.loads(line)
                        doc.pop('_id', None) 
                        v_chunk.append(doc)
                        if len(v_chunk) >= 1000:
                            db.vessels.insert_many(v_chunk, ordered=False)
                            v_count += len(v_chunk)
                            v_chunk = []
                    except: continue
            if v_chunk:
                db.vessels.insert_many(v_chunk, ordered=False)
                v_count += len(v_chunk)
        print(f"Successfully inserted {v_count:,} vessels.")

    # 4. LOADING TRIPS (Chunked Loading)
    if os.path.exists('trips_ready.json'):
        print("\nLoading Trips...")
        t_count = 0
        rejected_count = 0
        t_chunk = []
        chunk_size = 1000
        
        with open('trips_ready.json', 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    doc = json.loads(line)
                    doc.pop('_id', None) 
                    t_chunk.append(doc)
                    
                    if len(t_chunk) >= chunk_size:
                        try:
                            # Bulk insertion for speed
                            db.trips.insert_many(t_chunk, ordered=False)
                            t_count += len(t_chunk)
                        except Exception:
                            # Fallback if chunk fails
                            for d in t_chunk:
                                try:
                                    db.trips.insert_one(d)
                                    t_count += 1
                                except: rejected_count += 1
                        
                        print(f"Progress: {t_count:,} trips inserted...")
                        t_chunk = []
                except Exception as e:
                    print(f"Skip line error: {e}")

            # Final chunk processing
            if t_chunk:
                for d in t_chunk:
                    try:
                        db.trips.insert_one(d)
                        t_count += 1
                    except: rejected_count += 1

        print(f"FINISH: {t_count:,} inserted, {rejected_count:,} rejected.")

    print(f"  -> Vessels: {db.vessels.count_documents({}):,}")
    print(f"  -> Trips:   {db.trips.count_documents({}):,}")

if __name__ == "__main__":
    load_data()