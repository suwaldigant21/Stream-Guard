"""Phase 5b / Step 4 — retrain XGBoost on the 13-feature Gold dataset.

Adds the Phase 5b entity-level window aggregates (velocity_orig_count_24h,
velocity_orig_amt_24h, fan_in_dest_count_24h) and measures whether
fan_in_dest_count_24h breaks the single-feature reliance on
error_balance_orig and carves down the 1,150 false negatives from Phase 5a.
"""

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)
from xgboost import XGBClassifier

# 1. Load updated splits
train_df = pd.read_parquet("data/gold_train.parquet")
test_df = pd.read_parquet("data/gold_test.parquet")

feature_cols = [c for c in train_df.columns if c not in ["step", "is_fraud"]]
X_train, y_train = train_df[feature_cols], train_df["is_fraud"]
X_test, y_test = test_df[feature_cols], test_df["is_fraud"]

# 2. Dynamic train scale_pos_weight
scale_pos_weight = (y_train == 0).sum() / y_train.sum()

# 3. Fit XGBoost with early stopping
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
    verbose=25,
)

# 4. Predict & tune threshold for max F1
y_pred_proba = model.predict_proba(X_test)[:, 1]
precisions, recalls, thresholds = precision_recall_curve(y_test, y_pred_proba)
f1_scores = (2 * precisions[:-1] * recalls[:-1]) / (
    precisions[:-1] + recalls[:-1] + 1e-10
)

best_idx = np.argmax(f1_scores)
best_threshold = thresholds[best_idx]

y_pred = (y_pred_proba >= best_threshold).astype(int)
tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()

# 5. Output results
print("\n--- Phase 5b Enriched Model Performance ---")
print(f"Best Iteration:    {model.best_iteration}")
print(f"Optimal Threshold: {best_threshold:.6f}")
print(f"PR-AUC:            {average_precision_score(y_test, y_pred_proba):.4f}")
print(f"ROC-AUC:           {roc_auc_score(y_test, y_pred_proba):.4f}")
print(f"Precision:         {tp / (tp + fp):.4f}")
print(f"Recall:            {tp / (tp + fn):.4f}")
print(f"False Positives:   {fp:,}")
print(f"False Negatives:   {fn:,}")

print("\n--- Feature Gain Importances ---")
imp_df = pd.DataFrame(
    {"Feature": feature_cols, "Gain": model.feature_importances_}
).sort_values("Gain", ascending=False)
print(imp_df.to_string(index=False))
