import psycopg2
import socket

print("=" * 50)
print("POSTGRESQL CONNECTION TEST")
print("=" * 50)

print("\n1. Testing network connectivity...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(('127.0.0.1', 5432))
print(f"   Port 5432 reachable: {result == 0}")
sock.close()

print("\n2. Trying connection to 127.0.0.1...")
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        dbname="fraud_db",
        user="fraud_user",
        password="fraud_pass",  # keep password
        connect_timeout=5
    )
    print("   ✅ SUCCESS! Connected to PostgreSQL")
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print(f"   ✅ {cur.fetchone()[0][:50]}")
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public';")
    tables = cur.fetchall()
    print(f"   ✅ Tables found: {[t[0] for t in tables]}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"   ❌ Failed: {e}")

print("\n" + "=" * 50)