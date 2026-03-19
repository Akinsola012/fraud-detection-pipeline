-- Fact model: daily fraud summary
-- This is what the dashboard's main KPI cards will read from
-- One row per day showing fraud activity

SELECT
    DATE(transaction_timestamp)             AS transaction_date,

    -- Volume
    COUNT(*)                                AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
                                            AS fraud_count,
    COUNT(*) - SUM(CASE WHEN is_fraud
                        THEN 1 ELSE 0 END) AS clean_count,

    -- Rates
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    )                                       AS fraud_rate_pct,

    -- Amounts
    SUM(amount)                             AS total_transaction_value,
    SUM(CASE WHEN is_fraud
             THEN amount ELSE 0 END)        AS fraudulent_value,
    ROUND(AVG(CASE WHEN is_fraud
                   THEN amount END), 2)     AS avg_fraud_amount,

    -- Top fraud reason of the day
    MODE() WITHIN GROUP (
        ORDER BY fraud_reason
    )                                       AS most_common_fraud_reason,

    -- Cities with most fraud
    COUNT(DISTINCT CASE WHEN is_fraud
                        THEN city END)      AS cities_with_fraud,

    NOW()                                   AS dbt_updated_at

FROM {{ ref('stg_transactions') }}

GROUP BY DATE(transaction_timestamp)

ORDER BY transaction_date DESC