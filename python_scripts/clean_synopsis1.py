import gc
import os
import glob
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

BASE_PATH = DATA_DIR / "unipi_ais_dynamic_synopses" / "ais_synopses" / "2019"
OUTPUT_PATH = "dynamic_synopsis.csv"
MONTH_FILTER = ("jan", "feb", "mar")


if not BASE_PATH.exists():
    raise FileNotFoundError(f"Missing folder: {BASE_PATH}")

pattern = str(BASE_PATH / "unipi_ais_synopses_*.csv")
files = glob.glob(pattern)

# Keep only Q1 files 
files = [f for f in files if any(m in os.path.basename(f).lower() for m in MONTH_FILTER)]
files = sorted(files)

if not files:
    raise FileNotFoundError(f"No synopsis files found under: {BASE_PATH}")

print("Synopsis files:", len(files))
print("Sample:", ", ".join(os.path.basename(f) for f in files[:3]))

# Load + concat 
parts = []
for f in files:
    parts.append(pd.read_csv(f, low_memory=False))

df = pd.concat(parts, ignore_index=True)
del parts
gc.collect()

print("Raw rows:", f"{len(df):,}")

# Timestamp cleanup
df["t"] = pd.to_datetime(df["t"], unit="ms", errors="coerce")
bad_ts = df["t"].isna().sum()
if bad_ts:
    df = df[df["t"].notna()].copy()
    print("Dropped bad timestamps:", f"{bad_ts:,}")

# Keep 2019 Jan–Mar (even if filenames already filter months)
df = df[(df["t"].dt.year == 2019) & (df["t"].dt.month.isin([1, 2, 3]))].copy()

# Remove exact duplicates, then resolve conflicts on (vessel_id, t)
before = len(df)
df.drop_duplicates(inplace=True)
print("Exact dups removed:", f"{before - len(df):,}")

if "vessel_id" not in df.columns:
    raise KeyError("Column 'vessel_id' not found")

df.sort_values(by=["vessel_id", "t"], inplace=True)
before = len(df)
df.drop_duplicates(subset=["vessel_id", "t"], keep="first", inplace=True)
print("Conflicts removed (vessel_id,t):", f"{before - len(df):,}")

# string formatting on purpose
for c in ("lat", "lon"):
    if c in df.columns:
        df[c] = df[c].apply(lambda x: f"{x:.5f}" if pd.notnull(x) else "")

for c in ("speed", "course", "heading"):
    if c in df.columns:
        df[c] = df[c].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

if os.path.exists(OUTPUT_PATH):
    os.remove(OUTPUT_PATH)

df.to_csv(OUTPUT_PATH, index=False)

print("Saved:", OUTPUT_PATH, "| rows:", f"{len(df):,}")
