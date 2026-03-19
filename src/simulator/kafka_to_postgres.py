import json
import psycopg2 # type: ignore
from kafka import KafkaConsumer # type: ignore
import time

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="fraud_db",
    user="fraud_user",
    password="fraud_pass"
)
cur = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'raw_transactions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Listening for transactions... Press Ctrl+C to stop")
message_count = 0

try:
    for message in consumer:
        tx = message.value
        
        # Insert into PostgreSQL
        cur.execute("""
            INSERT INTO transactions 
            (transaction_id, user_id, amount, merchant, merchant_category, 
             location_lat, location_lon, city, device_id, device_type, 
             timestamp, is_fraud, fraud_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            tx['transaction_id'], tx['user_id'], tx['amount'], 
            tx['merchant'], tx['merchant_category'],
            tx['location_lat'], tx['location_lon'], tx['city'],
            tx['device_id'], tx['device_type'],
            tx['timestamp'], tx['is_fraud'], tx['fraud_reason']
        ))
        
        conn.commit()
        message_count += 1
        if message_count % 10 == 0:
            print(f"Saved {message_count} transactions to PostgreSQL")
            
except KeyboardInterrupt:
    print(f"\nStopped. Total messages saved: {message_count}")
finally:
    consumer.close()
    cur.close()
    conn.close()