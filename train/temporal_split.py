"""Phase 5 / Step 2 — temporal (not random) train/test split by `step`.

Each `step` = 1 hour. Split at the 80th percentile of time so the model never
sees future patterns during training (avoids temporal data leakage from a
random 80/20 split). Caches the Step 1 Athena extract locally on first run.
"""

import os

import pandas as pd
from extract_gold import main as extract_gold

CACHE_PATH = "data/gold_training.parquet"
SPLIT_QUANTILE = 0.80
FEATURE_DROPS = ["step", "is_fraud"]


def load_gold() -> pd.DataFrame:
    if not os.path.exists(CACHE_PATH):
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        df = extract_gold()
        df.to_parquet(CACHE_PATH, index=False)
        print(f"Cached dataset to {CACHE_PATH}")
    else:
        df = pd.read_parquet(CACHE_PATH)
        print(f"Loaded dataset from {CACHE_PATH}")
    return df


def main() -> None:
    df = load_gold()

    split_step = df["step"].quantile(SPLIT_QUANTILE)

    train_mask = df["step"] <= split_step
    test_mask = df["step"] > split_step

    feature_cols = [c for c in df.columns if c not in FEATURE_DROPS]

    X_train = df.loc[train_mask, feature_cols]
    y_train = df.loc[train_mask, "is_fraud"]
    X_test = df.loc[test_mask, feature_cols]
    y_test = df.loc[test_mask, "is_fraud"]

    print(
        f"Temporal Split Step Cutoff: <= {split_step:.0f} (Train) | "
        f"> {split_step:.0f} (Test)"
    )
    print(f"\nTrain Set: {X_train.shape[0]:,} rows")
    print(f"  - Fraud: {int(y_train.sum()):,} ({y_train.mean():.4%})")
    print(f"  - Legit: {int((y_train == 0).sum()):,}")
    print(f"\nTest Set: {X_test.shape[0]:,} rows")
    print(f"  - Fraud: {int(y_test.sum()):,} ({y_test.mean():.4%})")
    print(f"  - Legit: {int((y_test == 0).sum()):,}")

    pd.concat([X_train, y_train], axis=1).to_parquet(
        "data/gold_train.parquet", index=False
    )
    pd.concat([X_test, y_test], axis=1).to_parquet(
        "data/gold_test.parquet", index=False
    )
    print("\nSaved splits to data/gold_train.parquet + data/gold_test.parquet")


if __name__ == "__main__":
    main()
