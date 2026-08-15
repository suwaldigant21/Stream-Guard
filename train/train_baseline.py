"""Phase 5 / Step 3 — baseline XGBoost with train-derived scale_pos_weight.

Evaluates on the temporal test split with PR-AUC as the primary metric.
scale_pos_weight is computed strictly from the training set (using test data to
tune hyperparameters would be leakage).
"""

import numpy as np  # noqa: F401  (kept for parity with review script)
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from xgboost import XGBClassifier

# 1. Load splits
train_df = pd.read_parquet("data/gold_train.parquet")
test_df = pd.read_parquet("data/gold_test.parquet")

feature_cols = [c for c in train_df.columns if c not in ["step", "is_fraud"]]

X_train, y_train = train_df[feature_cols], train_df["is_fraud"]
X_test, y_test = test_df[feature_cols], test_df["is_fraud"]

# 2. Compute train-only scale_pos_weight
train_neg = (y_train == 0).sum()
train_pos = y_train.sum()
scale_pos_weight = train_neg / train_pos

print(f"Train scale_pos_weight: {scale_pos_weight:.2f}")

# 3. Train baseline XGBoost model
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

# 4. Predict probabilities
y_pred_proba = model.predict_proba(X_test)[:, 1]
y_pred_default = (y_pred_proba >= 0.5).astype(int)

# 5. Evaluate
pr_auc = average_precision_score(y_test, y_pred_proba)
roc_auc = roc_auc_score(y_test, y_pred_proba)

print("\n--- Baseline Model Performance ---")
print(f"PR-AUC (Primary Metric): {pr_auc:.4f}")
print(f"ROC-AUC:                {roc_auc:.4f}")
print("\nClassification Report (Default 0.5 Threshold):")
print(classification_report(y_test, y_pred_default, digits=4))

cm = confusion_matrix(y_test, y_pred_default)
print(f"Confusion Matrix:\nTN: {cm[0][0]:,} | FP: {cm[0][1]:,}\nFN: {cm[1][0]:,} | TP: {cm[1][1]:,}")
print(f"\nBest iteration: {model.best_iteration}")
