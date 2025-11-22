# bronze_layer.py
# Raw ingestion: copy uploaded CSVs to Bronze folder and save a few preview graphs.

import os
import pandas as pd
import matplotlib.pyplot as plt

# Paths (adjust if you put CSVs somewhere else)
LOCAL_DEMAND = "/dbfs/mnt/bronze/api-data-demand/85504c2a-18b7-4fb8-b80c-2b8e3b4a197d.csv"
LOCAL_EXTERNAL = "/dbfs/mnt/bronze/Snowflake-data-external/data_0_0_0.csv"
LOCAL_FEATENG = "/dbfs/mnt/bronze/aws-data-feature/feature_engineering_data.csv"

BRONZE_PATH = "/dbfs/mnt/bronze/"
os.makedirs(BRONZE_PATH, exist_ok=True)

def safe_read(path):
    try:
        df = pd.read_csv(path)
        print(f"Loaded {path} shape={df.shape}")
        return df
    except Exception as e:
        raise FileNotFoundError(f"Could not load {path}: {e}")

demand = safe_read(LOCAL_DEMAND)
external = safe_read(LOCAL_EXTERNAL)
feateng = safe_read(LOCAL_FEATENG)

# Save raw copies
demand.to_csv(os.path.join(BRONZE_PATH, "demand_bronze.csv"), index=False)
external.to_csv(os.path.join(BRONZE_PATH, "external_bronze.csv"), index=False)
feateng.to_csv(os.path.join(BRONZE_PATH, "feateng_bronze.csv"), index=False)
print("Saved bronze CSVs to", BRONZE_PATH)

# Quick visualizations (save as PNGs)
# Demand preview - if numeric columns exist, plot first numeric column
plt.tight_layout()
try:
    num_cols = demand.select_dtypes(include=["number"]).columns
    if len(num_cols) > 0:
        plt.figure(figsize=(10,4))
        demand[num_cols[0]].head(200).plot(title="Bronze: Demand - first numeric column (preview)")
        plt.savefig(os.path.join(BRONZE_PATH, "bronze_demand_preview.png"))
        plt.close()
except Exception as e:
    print("Bronze demand plot skipped:", e)

# External preview
try:
    num_cols = external.select_dtypes(include=["number"]).columns
    if len(num_cols) > 0:
        plt.figure(figsize=(10,4))
        external[num_cols[0]].head(200).plot(title="Bronze: External - first numeric column (preview)")
        plt.savefig(os.path.join(BRONZE_PATH, "bronze_external_preview.png"))
        plt.close()
except Exception as e:
    print("Bronze external plot skipped:", e)

# Feateng preview
try:
    num_cols = feateng.select_dtypes(include=["number"]).columns
    if len(num_cols) > 0:
        plt.figure(figsize=(10,4))
        feateng[num_cols[0]].head(200).plot(title="Bronze: FeatEng - first numeric column (preview)")
        plt.savefig(os.path.join(BRONZE_PATH, "bronze_feateng_preview.png"))
        plt.close()
except Exception as e:
    print("Bronze feateng plot skipped:", e)

print("Bronze layer complete. Files and plots are in:", BRONZE_PATH)
