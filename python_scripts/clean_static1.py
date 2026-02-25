import os
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

PATH_STATIC = DATA_DIR / "ais_static" / "ais_static" / "unipi_ais_static.csv"
PATH_DESCRIPTION = DATA_DIR / "ais_static" / "ais_static" / "ais_codes_descriptions.csv"

OUTPUT_PATH = "static.csv"
df_static = pd.read_csv(PATH_STATIC)
df_description = pd.read_csv(PATH_DESCRIPTION)

print("First rows of static:")
print(df_static.head())

print("Total static number:", f"{len(df_static):,} rows")


print("First rows of descriptions:")
print(df_description.head())
print("Total rows of descriptions: ", f"{len(df_description):,} rows")

# Duplicate & missing value checks
print("Duplicates of static file:", f"{df_static.duplicated().sum():,}")
print("Missing values of static file:\n", df_static.isnull().sum())

# - country: keep a simple placeholder instead of NaN
# - shiptype: fill with 0 so it can be matched to the code dictionary
df_static["country"] = df_static["country"].fillna("Unknown")
df_static["shiptype"] = df_static["shiptype"].fillna(0).astype(int)

print("Missing valuesof static file:", df_static.isnull().sum())

print("Duplicates for descriptions:", f"{df_description.duplicated().sum():,}")
print("Description missing values:\n", df_description.isnull().sum())


# Left join keeps every vessel row, even if a shiptype code has no match.
vessels_df = pd.merge(df_static, df_description, left_on="shiptype", right_on="Type Code",how="left",)

# If a shiptype code is not found in the dictionary, Description becomes NaN.
missing_desc = vessels_df["Description"].isna().sum()
print("After Join: missing descriptions:", f"{missing_desc:,}")

if missing_desc:
    bad_codes = vessels_df.loc[vessels_df["Description"].isna(), "shiptype"].unique()
    print("Unmatched shiptype codes (sample):", bad_codes[:20])

# Standard placeholder for unknown codes.
vessels_df["Description"] = vessels_df["Description"].fillna("Unknown Type")

# Exact duplicates (fully identical rows)
print("Joined exact duplicate rows:", f"{vessels_df.duplicated().sum():,}")

# Logical duplicates: same vessel_id appears multiple times with different info.
# We keep one record per vessel_id and prefer the one with a real description.
vessel_counts = vessels_df["vessel_id"].value_counts()
dup_ids = vessel_counts[vessel_counts > 1]
print("vessel_id appearing multiple times:", f"{len(dup_ids):,}")

#helper flag used only for sorting preference
vessels_df["is_unknown"] = vessels_df["Description"].apply(lambda x: 1 if x in ("Unknown Type", "Not available (default)") else 0)
# Sort so the preferred row per vessel_id comes first, then keep the first
vessels_df = vessels_df.sort_values(by=["vessel_id", "is_unknown"])
vessels_df_clean = vessels_df.drop_duplicates(subset=["vessel_id"], keep="first")

#drop helper/join columns that we don't want in the output
drop_cols = [c for c in ["is_unknown", "Type Code"] if c in vessels_df_clean.columns]
vessels_df_clean = vessels_df_clean.drop(columns=drop_cols)


if os.path.exists(OUTPUT_PATH):
    os.remove(OUTPUT_PATH)

vessels_df_clean.to_csv(OUTPUT_PATH, index=False)

print("Export finished.")
print("  joined rows: ", f"{len(vessels_df):,}")
print("  unique rows: ", f"{len(vessels_df_clean):,}")
print("  file:       ", OUTPUT_PATH)
