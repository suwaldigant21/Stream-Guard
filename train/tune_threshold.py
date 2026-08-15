"""Phase 5 / Step 4 — threshold tuning via Precision-Recall curve.

scale_pos_weight pushes scores toward 1.0, so the 0.5 cutoff is too low.
Reconstructs the identical baseline (same seed/hyperparams) to reproduce
y_pred_proba, then evaluates F1 across all thresholds and prints an
operational trade-off matrix across key cutoffs.
"""

import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix, precision_recall_curve
from xgboost import XGBClassifier

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

# 1. Precision-Recall curve
precisions, recalls, thresholds = precision_recall_curve(y_test, y_pred_proba)

# 2. F1 across all thresholds
f1_scores = (2 * precisions[:-1] * recalls[:-1]) / (
    precisions[:-1] + recalls[:-1] + 1e-10
)
best_idx = np.argmax(f1_scores)
best_threshold = thresholds[best_idx]

print("--- Threshold Optimization Results ---")
print(f"Max F1 Threshold:     {best_threshold:.4f}")
print(f"Max F1 Score:         {f1_scores[best_idx]:.4f}")
print(f"Precision at Max F1:  {precisions[best_idx]:.4f}")
print(f"Recall at Max F1:     {recalls[best_idx]:.4f}")

# 3. Operational threshold breakdown
target_thresholds = sorted(
    {0.50, float(best_threshold), 0.90, 0.95, 0.99, 0.999}
)

print("\n--- Operational Trade-off Matrix ---")
print(
    f"{'Threshold':<10} | {'Precision':<10} | {'Recall':<10} | "
    f"{'F1-Score':<10} | {'FP Count':<10} | {'FN Count':<10}"
)
print("-" * 72)

for th in target_thresholds:
    y_pred = (y_pred_proba >= th).astype(int)
    cm = confusion_matrix(y_test, y_pred)
    tn, fp, fn, tp = cm.ravel()

    prec = tp / (tp + fp) if (tp + fp) > 0 else 0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (prec * rec) / (prec + rec) if (prec + rec) > 0 else 0

    print(f"{th:<10.4f} | {prec:<10.4f} | {rec:<10.4f} | {f1:<10.4f} | {fp:<10,} | {fn:<10,}")
