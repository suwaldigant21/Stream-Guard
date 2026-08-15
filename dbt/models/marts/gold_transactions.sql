-- Gold layer (mart): feature set ready for XGBoost training.
-- Partitioned by type so CASH_OUT/TRANSFER (the only fraud-bearing classes)
-- can be scanned without touching the zero-signal partitions.
-- `step` is carried for the Phase 5 TEMPORAL train/test split only — it is not
-- a model feature.
-- Account identifiers are excluded as RAW columns: high-cardinality strings
-- cannot be split by tree models without severe overfitting, and they are
-- account PII (Phase 6 GDPR scope). The ONE engineered aggregate derived from
-- the account IDs that survived the Phase 5b ablation (destination 24h fan-in
-- on nameDest) IS included — the raw IDs are simply never fed in directly.
-- (Originator-velocity features were dropped: zero gain — PaySim originators
-- are single-use.)
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
    is_fraud,
    type
FROM {{ ref('stg_transactions') }}
