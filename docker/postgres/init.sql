-- Create database tables for fraud detection
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    amount DECIMAL(10,2),
    merchant VARCHAR(100),
    merchant_category VARCHAR(50),
    location_lat DECIMAL(10,8),
    location_lon DECIMAL(10,8),
    city VARCHAR(50),
    device_id VARCHAR(50),
    device_type VARCHAR(20),
    timestamp TIMESTAMP,
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_reason VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) REFERENCES transactions(transaction_id),
    risk_score DECIMAL(5,2),
    alert_reason VARCHAR(200),
    timestamp TIMESTAMP,
    is_reviewed BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);
