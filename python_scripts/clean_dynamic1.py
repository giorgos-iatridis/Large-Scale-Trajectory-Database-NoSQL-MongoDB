import gc
import os
import glob
from pathlib import Path
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_PATH = DATA_DIR / "unipi_ais_dynamic_2019"
OUTPUT_FILE = "dynamic.csv"
MONTHS = ("jan", "feb", "mar")

if not INPUT_PATH.exists():
    raise FileNotFoundError(f"Folder not found: {INPUT_PATH}")


# we look for CSV files in the folder
pattern = str(INPUT_PATH / "*.csv")
all_files = glob.glob(pattern)

# filter for Jan, Feb, Mar months
files = [f for f in all_files if any(m in os.path.basename(f).lower() for m in MONTHS)]
files = sorted(files)

if not files:
    raise FileNotFoundError(f"No matching CSV files found in: {INPUT_PATH}")

print("Files to process:", len(files))
print("Processing:", ", ".join(os.path.basename(f) for f in files))

# LOAD DATA
chunks = []
for f in files:
    print(f"-> Loading: {os.path.basename(f)}")
    chunks.append(pd.read_csv(f, low_memory=False))

df = pd.concat(chunks, ignore_index=True)
del chunks
gc.collect()

print("Initial rows:", f"{len(df):,}")

#  CLEANING 
# 1. Timestamps
df["t"] = pd.to_datetime(df["t"], unit="ms", errors="coerce")
df = df[df["t"].notna()].copy()

# Keep only 2019 Q1
df = df[(df["t"].dt.year == 2019) & (df["t"].dt.month.isin([1, 2, 3]))].copy()

# 2. Basic Cleanup
df.drop_duplicates(inplace=True)

# 3. Handle Special Values
# Heading 511 is N/A in AIS, set to null
if "heading" in df.columns:
    df.loc[df["heading"] == 511, "heading"] = None

# 4. Filter Outliers (Speed & Course)
# Rules: Speed 0-60 knots, Course 0-360 degrees
if "speed" in df.columns:
    df = df[(df["speed"] >= 0.0) & (df["speed"] <= 60.0)].copy()

if "course" in df.columns:
    df = df[(df["course"] >= 0.0) & (df["course"] <= 360.0)].copy()

# 5. Sort and Resolve Conflicts
df.sort_values(by=["vessel_id", "t"], inplace=True)
df.drop_duplicates(subset=["vessel_id", "t"], keep="first", inplace=True)

#  FORMATTING 
# Force specific decimals using string formatting
# GPS to 5 decimals
for col in ("lat", "lon"):
    if col in df.columns:
        df[col] = df[col].apply(lambda x: f"{x:.5f}" if pd.notnull(x) else "")

# Metrics to 2 decimals
for col in ("speed", "course", "heading"):
    if col in df.columns:
        df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

# --- SAVE ---
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

df.to_csv(OUTPUT_FILE, index=False)

print("\n--- DONE ---")
print("Saved to:", OUTPUT_FILE)
print("Final rows:", f"{len(df):,}")