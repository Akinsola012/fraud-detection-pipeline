import psycopg2
import socket
import sys

print("=" * 50)
print("CONNECTION DIAGNOSTIC")
print("=" * 50)

# Check Python version
print(f"Python version: {sys.version}")
print(f"psycopg2 version: {psycopg2.__version__}")

# Test basic network connectivity
print(f"\n--- Network Test ---")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(('127.0.0.1', 5432))
print(f"Port 5432 reachable: {result == 0}")
sock.close()

# Test with different combinations
print(f"\n--- Connection Attempts ---")

# 1. No password, trust should work
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="fraud_db",
        user="fraud_user",
        password=None,  # Explicitly no password
        connect_timeout=5
    )
    print("✅ Connected with NO password")
    conn.close()
except Exception as e:
    print(f"❌ No password: {e}")

# 2. Empty string password
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="fraud_db",
        user="fraud_user",
        password="",  # Empty string
        connect_timeout=5
    )
    print("✅ Connected with EMPTY password")
    conn.close()
except Exception as e:
    print(f"❌ Empty password: {e}")

# 3. With password
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="fraud_db",
        user="fraud_user",
        password="fraud_pass",
        connect_timeout=5
    )
    print("✅ Connected with fraud_pass")
    conn.close()
except Exception as e:
    print(f"❌ fraud_pass: {e}")

# 4. With simple password
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="fraud_db",
        user="fraud_user",
        password="fraud",
        connect_timeout=5
    )
    print("✅ Connected with fraud")
    conn.close()
except Exception as e:
    print(f"❌ fraud: {e}")