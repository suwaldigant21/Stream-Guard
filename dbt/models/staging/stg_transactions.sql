-- Silver layer (staging): snake_case names + balance-transition features.
-- Reads the Bronze lakehouse READ-ONLY via source() — never materialized
-- over the raw transactions/ S3 location (see sources.yml).
--
-- P1-2 GDPR masking: LEFT JOIN the append-only gdpr_requests registry (raw
-- account_id -> salted-HMAC alias). An erased account's name_orig / name_dest
-- are projected as its ALIAS at query time — the raw id never leaves Bronze,
-- no rewrite, no DELETE. Grouping key for the fan-in window uses the masked
-- destination, which is 1:1 with the raw id, so feature values are unchanged.
SELECT
    step,
    type,
    amount,
    COALESCE(g1.account_alias, nameOrig)   AS name_orig,
    oldbalanceOrg   AS old_balance_orig,
    newbalanceOrig  AS new_balance_orig,
    COALESCE(g2.account_alias, nameDest)   AS name_dest,
    oldbalanceDest  AS old_balance_dest,
    newbalanceDest  AS new_balance_dest,
    isFraud    AS is_fraud,
    isFlaggedFraud  AS is_flagged_fraud,
    ingested_at,
    -- balance transition features
    oldbalanceOrg - newbalanceOrig              AS balance_delta_orig,
    newbalanceDest - oldbalanceDest             AS balance_delta_dest,
    oldbalanceOrg - newbalanceOrig - amount     AS error_balance_orig,
    oldbalanceDest + amount - newbalanceDest    AS error_balance_dest,
    CAST(oldbalanceOrg > 0 AS INT)              AS has_balance_orig,
    CAST(oldbalanceDest > 0 AS INT)             AS has_balance_dest,
    -- Phase 5b: entity-level window aggregate (1 step = 1 hour), past-only.
    -- -1 excludes the CURRENT row so the feature describes prior behavior.
    -- (Originator-velocity variants were dropped: PaySim originators are
    -- single-use, giving zero gain in the Phase 5b ablation.)
    COUNT(*) OVER (
        PARTITION BY COALESCE(g2.account_alias, nameDest)
        ORDER BY step
        RANGE BETWEEN 24 PRECEDING AND CURRENT ROW
    ) - 1 AS fan_in_dest_count_24h
FROM {{ source('streamguard_db', 'bronze_transactions') }} AS t
LEFT JOIN {{ source('streamguard_db', 'gdpr_requests') }} AS g1
    ON t.nameOrig = g1.account_id
LEFT JOIN {{ source('streamguard_db', 'gdpr_requests') }} AS g2
    ON t.nameDest = g2.account_id
