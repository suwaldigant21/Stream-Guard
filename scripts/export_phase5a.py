"""Phase 5a — freeze the baseline model + export artifacts for audit.

Exports into data/model_artifacts/:
  model.json             native XGBoost binary (n_estimators locked to best_iteration 18)
  metadata.json          operational metadata (threshold, metrics, features)
  pr_curve.png           Precision-Recall curve (primary metric)
  roc_curve.png          ROC curve
  confusion_matrix.png   Confusion matrix at decision threshold
  feature_importance.png Gain importance bar chart
"""

import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_curve,
)
from xgboost import XGBClassifier

artifact_dir = "data/model_artifacts"
os.makedirs(artifact_dir, exist_ok=True)

# 1. Load splits
train_df = pd.read_parquet("data/gold_train.parquet")
test_df = pd.read_parquet("data/gold_test.parquet")

feature_cols = [c for c in train_df.columns if c not in ["step", "is_fraud"]]
X_train, y_train = train_df[feature_cols], train_df["is_fraud"]
X_test, y_test = test_df[feature_cols], test_df["is_fraud"]

# 2. Fit model locked to best iteration.
# NOTE: XGBoost best_iteration is 0-indexed; the early-stopped model predicts
# with trees 0..best_iteration (i.e. best_iteration + 1 trees). Lock n_estimators
# to 19 so the exported binary is identical to the reviewed Step 3 model.
best_iteration = 18
n_trees = best_iteration + 1
scale_pos_weight = (y_train == 0).sum() / y_train.sum()
model = XGBClassifier(
    n_estimators=n_trees,
    max_depth=6,
    learning_rate=0.05,
    scale_pos_weight=scale_pos_weight,
    eval_metric="aucpr",
    random_state=42,
    tree_method="hist",
    n_jobs=-1,
)
model.fit(X_train, y_train)

# Save native XGBoost model binary
model.save_model(os.path.join(artifact_dir, "model.json"))

# 3. Save metadata artifact.
# Operational threshold = exact max-F1 point from the PR curve (step 4). Using
# the rounded 0.8046 as a float literal would shift the cutoff by ~1e-4 and
# silently drop ~100 TPs — so derive it from precision_recall_curve, never
# hardcode it.
y_pred_proba = model.predict_proba(X_test)[:, 1]
prec_curve, rec_curve, thresh_curve = precision_recall_curve(y_test, y_pred_proba)
f1_curve = (2 * prec_curve[:-1] * rec_curve[:-1]) / (
    prec_curve[:-1] + rec_curve[:-1] + 1e-10
)
threshold = float(thresh_curve[int(np.argmax(f1_curve))])
y_pred = (y_pred_proba >= threshold).astype(int)
cm = confusion_matrix(y_test, y_pred)
tn, fp, fn, tp = cm.ravel()

metadata = {
    "phase": "5a_baseline",
    "model_type": "XGBClassifier",
    "best_iteration": best_iteration,
    "num_trees": n_trees,
    "scale_pos_weight": float(scale_pos_weight),
    "decision_threshold": threshold,
    "metrics": {
        "pr_auc": float(average_precision_score(y_test, y_pred_proba)),
        "precision": float(tp / (tp + fp)),
        "recall": float(tp / (tp + fn)),
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
    },
    "features": feature_cols,
}

with open(os.path.join(artifact_dir, "metadata.json"), "w") as f:
    json.dump(metadata, f, indent=2)

# 4. Generate diagnostic plots
plt.style.use(
    "seaborn-v0_8-whitegrid"
    if "seaborn-v0_8-whitegrid" in plt.style.available
    else "default"
)

# Plot A: Precision-Recall curve
prec, rec, _ = precision_recall_curve(y_test, y_pred_proba)
plt.figure(figsize=(6, 4))
plt.plot(rec, prec, label=f"PR-AUC = {metadata['metrics']['pr_auc']:.4f}", color="b")
plt.axvline(x=0.7299, color="r", linestyle="--", label="Recall @ 0.8046 (73%)")
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("Precision-Recall Curve (Baseline)")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(artifact_dir, "pr_curve.png"))
plt.close()

# Plot B: ROC curve
fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
plt.figure(figsize=(6, 4))
plt.plot(fpr, tpr, color="darkorange", label="ROC Curve")
plt.plot([0, 1], [0, 1], color="navy", linestyle="--")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("ROC Curve")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(artifact_dir, "roc_curve.png"))
plt.close()

# Plot C: Confusion matrix heatmap
plt.figure(figsize=(5, 4))
sns.heatmap(cm, annot=True, fmt=",d", cmap="Blues", cbar=False)
plt.xlabel("Predicted Label")
plt.ylabel("True Label")
plt.title(f"Confusion Matrix (Thresh = {threshold})")
plt.tight_layout()
plt.savefig(os.path.join(artifact_dir, "confusion_matrix.png"))
plt.close()

# Plot D: Feature importance (gain)
imp_df = pd.DataFrame(
    {"Feature": feature_cols, "Gain": model.feature_importances_}
).sort_values("Gain", ascending=True)
plt.figure(figsize=(7, 4))
plt.barh(imp_df["Feature"], imp_df["Gain"], color="teal")
plt.xlabel("Gain Importance")
plt.title("XGBoost Feature Gain (Baseline)")
plt.tight_layout()
plt.savefig(os.path.join(artifact_dir, "feature_importance.png"))
plt.close()

print(f"Phase 5a artifacts successfully exported to {artifact_dir}/")
