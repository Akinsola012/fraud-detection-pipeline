-- Dimension model: one row per user
-- Summarises each user's transaction behaviour
-- Used by the dashboard to show user risk profiles

SELECT
    user_id,

    -- Transaction volume
    COUNT(*)                                AS total_transactions,
    SUM(amount)                             AS total_spend,
    ROUND(AVG(amount), 2)                   AS avg_transaction_amount,
    MAX(amount)                             AS max_transaction_amount,
    MIN(amount)                             AS min_transaction_amount,

    -- Fraud metrics
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
                                            AS total_fraud_flags,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    )                                       AS fraud_rate_pct,

    -- Activity patterns
    COUNT(DISTINCT city)                    AS cities_transacted_in,
    COUNT(DISTINCT merchant_category)       AS merchant_categories_used,
    COUNT(DISTINCT device_id)               AS devices_used,

    -- Time metrics
    MIN(transaction_timestamp)              AS first_transaction_at,
    MAX(transaction_timestamp)              AS last_transaction_at,

    -- Risk classification
    -- Based on fraud rate: LOW < 10%, MEDIUM 10-30%, HIGH > 30%
    CASE
        WHEN 100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0) > 30 THEN 'HIGH'
        WHEN 100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0) > 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                     AS risk_category,

    -- When this row was last refreshed
    NOW()                                   AS dbt_updated_at

FROM {{ ref('stg_transactions') }}

GROUP BY user_id