import psycopg2 # type: ignore

# Use connection string format
try:
    conn = psycopg2.connect(
        "postgresql://fraud_user:fraud_pass@localhost:5432/fraud_db"
    )
    print("✅ Connected with connection string")
    conn.close()
except Exception as e:
    print(f"❌ Failed: {e}")