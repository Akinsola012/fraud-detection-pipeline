-- Staging model: clean fraud alerts

SELECT
    alert_id,
    transaction_id,
    CAST(risk_score AS DECIMAL(5,2))        AS risk_score,
    alert_reason,
    CAST(timestamp AS TIMESTAMP)            AS alert_timestamp,
    CAST(is_reviewed AS BOOLEAN)            AS is_reviewed

FROM {{ source('public', 'fraud_alerts') }}

WHERE alert_id IS NOT NULL