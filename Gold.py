# gold_layer.py
# Feature engineering and richer visualizations saved to Gold folder.

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

SILVER_PATH = "/dbfs/mnt/silver/"
GOLD_PATH = "/dbfs/mnt/gold/"
os.makedirs(GOLD_PATH, exist_ok=True)

silver = pd.read_csv(os.path.join(SILVER_PATH, "silver_merged.csv"))
gold = silver.copy()

# Detect date column
date_col = None
for c in gold.columns:
    if 'date' in c.lower() or 'day' in c.lower():
        date_col = c
        try:
            gold[date_col] = pd.to_datetime(gold[date_col], errors='coerce')
        except Exception:
            pass
        break

# Detect target column (heuristic)
target_candidates = [c for c in gold.columns if any(w in c.lower() for w in ['demand','sales','qty','quantity','target'])]
if len(target_candidates) > 0:
    target_col = target_candidates[0]
else:
    # fallback to first numeric column
    nums = gold.select_dtypes(include=[np.number]).columns.tolist()
    target_col = nums[0] if nums else None

print("Gold: date_col =", date_col, "target_col =", target_col)

# Basic imputation
for c in gold.select_dtypes(include=[np.number]).columns:
    gold[c] = gold[c].fillna(gold[c].median())
for c in gold.select_dtypes(include=['object','category']).columns:
    gold[c] = gold[c].fillna('UNKNOWN')

# Time-based features
if date_col:
    gold = gold.sort_values(date_col)
    gold['year'] = gold[date_col].dt.year
    gold['month'] = gold[date_col].dt.month
    gold['day'] = gold[date_col].dt.day
    gold['weekday'] = gold[date_col].dt.weekday

if target_col:
    gold['lag_1'] = gold[target_col].shift(1).fillna(method='bfill')
    gold['rolling_7'] = gold[target_col].rolling(window=7, min_periods=1).mean().fillna(method='bfill')

# Save gold
gold.to_csv(os.path.join(GOLD_PATH, "gold_features.csv"), index=False)
print("Saved gold_features.csv to", GOLD_PATH)

# Gold visualizations:
# 1) Target trend
if target_col:
    plt.figure(figsize=(12,4))
    gold[target_col].head(500).plot(title=f"Gold - {target_col} trend (first 500 rows)")
    plt.ylabel(target_col)
    plt.tight_layout()
    plt.savefig(os.path.join(GOLD_PATH, "gold_target_trend.png"))
    plt.close()

# 2) Rolling mean
if 'rolling_7' in gold.columns:
    plt.figure(figsize=(12,4))
    gold['rolling_7'].head(500).plot(title="Gold - rolling_7 (first 500 rows)")
    plt.ylabel("rolling_7")
    plt.tight_layout()
    plt.savefig(os.path.join(GOLD_PATH, "gold_rolling7.png"))
    plt.close()

# 3) Histogram of target
try:
    if target_col:
        plt.figure(figsize=(8,4))
        gold[target_col].hist(bins=50)
        plt.title(f"Gold - {target_col} distribution")
        plt.tight_layout()
        plt.savefig(os.path.join(GOLD_PATH, "gold_target_hist.png"))
        plt.close()
except Exception as e:
    print("Gold histogram skipped:", e)

print("Gold layer complete. Files and plots are in:", GOLD_PATH)
