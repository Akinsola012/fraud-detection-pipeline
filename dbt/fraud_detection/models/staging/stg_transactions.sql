-- Staging model: clean and standardise raw transactions
-- Think of this as the "wash the vegetables before cooking" step
-- We cast types, handle nulls, and rename columns consistently

SELECT
    transaction_id,
    user_id,
    CAST(amount AS DECIMAL(12,2))           AS amount,
    merchant,
    merchant_category,
    CAST(location_lat AS DECIMAL(10,8))     AS location_lat,
    CAST(location_lon AS DECIMAL(10,8))     AS location_lon,
    city,
    device_id,
    device_type,
    CAST(timestamp AS TIMESTAMP)            AS transaction_timestamp,
    CAST(is_fraud AS BOOLEAN)               AS is_fraud,
    COALESCE(fraud_reason, 'none')          AS fraud_reason,
    created_at

FROM {{ source('public', 'transactions') }}

WHERE transaction_id IS NOT NULL
  AND user_id IS NOT NULL
  AND amount > 0