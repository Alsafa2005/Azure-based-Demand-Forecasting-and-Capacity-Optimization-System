# silver_layer.py
# Clean + merge Bronze CSVs into Silver layer and produce a couple of summary graphs.

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

BRONZE_PATH = "/dbfs/mnt/bronze/"
SILVER_PATH = "/dbfs/mnt/silver/"
os.makedirs(SILVER_PATH, exist_ok=True)

# Read bronze files
demand = pd.read_csv(os.path.join(BRONZE_PATH, "demand_bronze.csv"))
external = pd.read_csv(os.path.join(BRONZE_PATH, "external_bronze.csv"))
feateng = pd.read_csv(os.path.join(BRONZE_PATH, "feateng_bronze.csv"))

# Find merge key heuristic
possible_keys = ['date','Date','day','Day','id','ID','product_id','sku']
merge_key = None
for k in possible_keys:
    if k in demand.columns and (k in external.columns or k in feateng.columns):
        merge_key = k
        break

print("Selected merge key:", merge_key)

silver = demand.copy()

# Merge external
try:
    if merge_key and merge_key in external.columns:
        silver = pd.merge(silver, external, on=merge_key, how='left')
    else:
        silver = pd.concat([silver.reset_index(drop=True), external.reset_index(drop=True)], axis=1)
except Exception as e:
    print("External merge/concat failed:", e)

# Merge feateng
try:
    if merge_key and merge_key in feateng.columns:
        silver = pd.merge(silver, feateng, on=merge_key, how='left')
    else:
        silver = pd.concat([silver.reset_index(drop=True), feateng.reset_index(drop=True)], axis=1)
except Exception as e:
    print("Feateng merge/concat failed:", e)

# Basic cleaning
silver.columns = silver.columns.str.strip()
# Fill obvious missing (light cleaning only)
for c in silver.select_dtypes(include=[np.number]).columns:
    silver[c] = silver[c].fillna(silver[c].median())
for c in silver.select_dtypes(include=['object','category']).columns:
    silver[c] = silver[c].fillna('UNKNOWN')

# Save silver
silver.to_csv(os.path.join(SILVER_PATH, "silver_merged.csv"), index=False)
print("Saved silver to", SILVER_PATH)

# Silver visualizations
plt.figure(figsize=(10,4))
num_cols = silver.select_dtypes(include=["number"]).columns
if len(num_cols) > 0:
    # plot first 2 numeric columns for quick trend check (if exist)
    cols_to_plot = num_cols[:2].tolist()
    silver[cols_to_plot].head(200).plot(title="Silver - first numeric columns (preview)")
    plt.savefig(os.path.join(SILVER_PATH, "silver_numeric_preview.png"))
    plt.close()
else:
    print("No numeric columns to plot in silver.")

# Simple correlation heatmap-like (uses numeric only)
try:
    corr = silver.select_dtypes(include=["number"]).corr()
    if corr.shape[0] > 1:
        plt.figure(figsize=(8,6))
        plt.imshow(corr, aspect='auto')
        plt.colorbar()
        plt.title("Silver - Numeric Correlation (visual)")
        plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
        plt.yticks(range(len(corr.columns)), corr.columns)
        plt.tight_layout()
        plt.savefig(os.path.join(SILVER_PATH, "silver_correlation_visual.png"))
        plt.close()
except Exception as e:
    print("Silver correlation plot skipped:", e)

print("Silver layer complete. Files and plots are in:", SILVER_PATH)
