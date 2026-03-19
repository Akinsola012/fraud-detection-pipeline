-- Dimension model: one row per merchant
-- Shows which merchants have the most fraud activity

SELECT
    merchant,
    merchant_category,

    COUNT(*)                                AS total_transactions,
    SUM(amount)                             AS total_revenue,
    ROUND(AVG(amount), 2)                   AS avg_transaction_amount,

    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
                                            AS fraud_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    )                                       AS fraud_rate_pct,

    COUNT(DISTINCT user_id)                 AS unique_customers,
    COUNT(DISTINCT city)                    AS cities_active_in,

    NOW()                                   AS dbt_updated_at

FROM {{ ref('stg_transactions') }}

GROUP BY merchant, merchant_category