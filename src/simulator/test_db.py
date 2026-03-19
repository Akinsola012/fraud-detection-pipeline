import psycopg2 # type: ignore

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="fraud_db",
        user="fraud_user",
        password="fraud_pass"
    )
    print("✅ Connected successfully!")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")