"""Phase 5 / Step 1 — extract Gold training vectors for fraud-bearing classes.

Loads only the CASH_OUT + TRANSFER partitions from gold_transactions
(2,770,409 rows expected; 8,213 fraud positives) via Athena streaming
through awswrangler. Excludes PAYMENT / DEBIT / CASH_IN (zero fraud signal).
"""

import awswrangler as wr
import pandas as pd

QUERY = """
SELECT
    amount,
    has_balance_orig,
    balance_delta_orig,
    error_balance_orig,
    has_balance_dest,
    balance_delta_dest,
    error_balance_dest,
    is_flagged_fraud,
    fan_in_dest_count_24h,
    step,
    is_fraud
FROM streamguard_db.gold_transactions
WHERE type IN ('CASH_OUT', 'TRANSFER')
"""


def main() -> pd.DataFrame:
    df = wr.athena.read_sql_query(
        sql=QUERY,
        database="streamguard_db",
        workgroup="streamguard-dev",
    )
    print(f"Dataset Shape: {df.shape}")
    print(f"Fraud Distribution:\n{df['is_fraud'].value_counts()}")
    return df


if __name__ == "__main__":
    main()
