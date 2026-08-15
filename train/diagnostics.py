"""Phase 5 / Step 5 — feature diagnostics & error profiling.

Gain importances plus cohort means for FN (missed fraud), TP (caught fraud)
and FP (false alarms) at the tuned threshold (0.8046), to profile why some
fraud bypasses detection.
"""

import pandas as pd
from xgboost import XGBClassifier

THRESHOLD = 0.8046

# --- Reproduce baseline predictions (same config as train_baseline.py) ---
train_df = pd.read_parquet("data/gold_train.parquet")
test_df = pd.read_parquet("data/gold_test.parquet")

feature_cols = [c for c in train_df.columns if c not in ["step", "is_fraud"]]
X_train, y_train = train_df[feature_cols], train_df["is_fraud"]
X_test, y_test = test_df[feature_cols], test_df["is_fraud"]

scale_pos_weight = (y_train == 0).sum() / y_train.sum()

model = XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    scale_pos_weight=scale_pos_weight,
    eval_metric="aucpr",
    early_stopping_rounds=15,
    random_state=42,
    tree_method="hist",
    n_jobs=-1,
)
model.fit(
    X_train,
    y_train,
    eval_set=[(X_train, y_train), (X_test, y_test)],
    verbose=False,
)
y_pred_proba = model.predict_proba(X_test)[:, 1]

# 1. Feature importance (gain)
importance_df = pd.DataFrame(
    {"Feature": feature_cols, "Gain_Importance": model.feature_importances_}
).sort_values(by="Gain_Importance", ascending=False)

print("--- Feature Importance (Gain) ---")
print(importance_df.to_string(index=False))

# 2. Cohort profiles
y_pred = (y_pred_proba >= THRESHOLD).astype(int)

analysis_df = X_test.copy()
analysis_df["is_fraud"] = y_test
analysis_df["pred_prob"] = y_pred_proba
analysis_df["pred_label"] = y_pred

fn_mask = (analysis_df["is_fraud"] == 1) & (analysis_df["pred_label"] == 0)
tp_mask = (analysis_df["is_fraud"] == 1) & (analysis_df["pred_label"] == 1)
fp_mask = (analysis_df["is_fraud"] == 0) & (analysis_df["pred_label"] == 1)

print("\n--- Feature Means by Prediction Cohort ---")
cohort_means = pd.DataFrame(
    {
        "FN (Missed Fraud)": analysis_df.loc[fn_mask, feature_cols].mean(),
        "TP (Caught Fraud)": analysis_df.loc[tp_mask, feature_cols].mean(),
        "FP (False Alarms)": analysis_df.loc[fp_mask, feature_cols].mean(),
    }
)
print(cohort_means.round(2).to_string())

print(f"\nMax Probability Output: {y_pred_proba.max():.4f}")
print(f"Min Probability Output: {y_pred_proba.min():.4f}")
