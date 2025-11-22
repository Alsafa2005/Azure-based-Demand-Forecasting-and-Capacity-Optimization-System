# outputs_layer.py
# Models: LinearRegression, Ridge, ElasticNet, RandomForest, GradientBoosting
# Saves models, predictions, evaluation CSV, and plots (Actual vs Pred, Residual, Feature importances)

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression, Ridge, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

GOLD_PATH = "/dbfs/mnt/gold/"
OUTPUTS_PATH = "/dbfs/mnt/outputs/"
os.makedirs(OUTPUTS_PATH, exist_ok=True)

gold = pd.read_csv(os.path.join(GOLD_PATH, "gold_features.csv"))

# Detect target again
target_candidates = [c for c in gold.columns if any(w in c.lower() for w in ['demand','sales','qty','quantity','target'])]
if len(target_candidates) > 0:
    target_col = target_candidates[0]
else:
    nums = gold.select_dtypes(include=[np.number]).columns.tolist()
    if len(nums) == 0:
        raise ValueError("No numeric column found to act as target.")
    target_col = nums[0]

print("Modeling target:", target_col)

# Prepare X, y
X = gold.drop(columns=[target_col])
y = gold[target_col].astype(float)

# One-hot encode categorical features
X = pd.get_dummies(X, drop_first=True)

# Show missing value summary before filtering
display(gold[[target_col]].isnull().value_counts())
print("Total rows before filtering:", len(gold))

# Only drop rows where the target is missing
mask = ~y.isnull()
X = X.loc[mask].copy()
y = y.loc[mask].copy()

if X.shape[0] == 0 or y.shape[0] == 0:
    raise ValueError(
        "No data left after filtering for NA values in target. "
        "Check the missing value summary above and your input data."
    )

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

numeric_features = X.select_dtypes(include=[np.number]).columns.tolist()

preprocessor = ColumnTransformer(transformers=[
    ("num", Pipeline([("imputer", SimpleImputer(strategy="median")),
                      ("scaler", StandardScaler())]), numeric_features)
], remainder="passthrough")

models = {
    "LinearRegression": LinearRegression(),
    "Ridge": Ridge(alpha=1.0),
    "ElasticNet": ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=2000),
    "RandomForest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "GradientBoosting": GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
}

metrics = []
for name, mdl in models.items():
    print("Training", name)
    pipe = Pipeline([("preproc", preprocessor), ("model", mdl)])
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)

    rmse = mean_squared_error(y_test, y_pred, squared=False)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    metrics.append({"model": name, "rmse": float(rmse), "mae": float(mae), "r2": float(r2)})

    # Save model
    joblib.dump(pipe, os.path.join(OUTPUTS_PATH, f"model_{name}.joblib"))

    # Save predictions
    preds_df = pd.DataFrame({"y_true": y_test.values, "y_pred": y_pred})
    preds_df.to_csv(os.path.join(OUTPUTS_PATH, f"predictions_{name}.csv"), index=False)

    # Plot: Actual vs Predicted
    plt.figure(figsize=(6,4))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.xlabel("Actual")
    plt.ylabel("Predicted")
    plt.title(f"Actual vs Predicted - {name}")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUTS_PATH, f"actual_vs_pred_{name}.png"))
    plt.close()

    # Plot: Residuals
    resid = y_test - y_pred
    plt.figure(figsize=(6,4))
    plt.hist(resid, bins=30)
    plt.xlabel("Residual")
    plt.ylabel("Count")
    plt.title(f"Residuals - {name}")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUTS_PATH, f"residuals_{name}.png"))
    plt.close()

    # Feature importances for tree models
    if name in ["RandomForest", "GradientBoosting"]:
        try:
            model_obj = pipe.named_steps["model"]
            fi = model_obj.feature_importances_
            # Map feature importances to numeric_features order after preprocessor transformation:
            # Note: Because we used a simple preprocessor (num-> scaled) and remainder passthrough, the numeric_features
            # order aligns to X.columns for the numeric portion. We'll match to numeric_features length.
            fi_series = pd.Series(fi, index=numeric_features).sort_values(ascending=False)
            topk = fi_series.head(40)
            topk.to_csv(os.path.join(OUTPUTS_PATH, f"feature_importances_{name}.csv"))

            plt.figure(figsize=(10,6))
            topk.plot(kind="bar")
            plt.title(f"Feature importances - {name}")
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUTS_PATH, f"feature_importances_{name}.png"))
            plt.close()
        except Exception as e:
            print("Could not compute/save feature importances for", name, ":", e)

# Save metrics summary
metrics_df = pd.DataFrame(metrics).sort_values("rmse")
metrics_df.to_csv(os.path.join(OUTPUTS_PATH, "model_evaluation_summary.csv"), index=False)

print("Modeling complete. Outputs (models, predictions, plots, metrics) saved to:", OUTPUTS_PATH)
