import psycopg2

# Use the container's IP address directly
try:
    conn = psycopg2.connect(
        host="172.21.0.2",  # Direct container IP
        port=5432,
        dbname="fraud_db",
        user="fraud_user",
        password="fraud_pass",
        connect_timeout=5
    )
    print("✅ Connected using container IP!")
    
    # Test a simple query
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"✅ Query result: {result[0]}")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"❌ Failed: {e}")