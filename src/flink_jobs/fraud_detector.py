# ─────────────────────────────────────────────────────────────
# FRAUD DETECTION PIPELINE
# Layer 3: Real-Time Fraud Detector
#
# What this file does:
# - Reads every transaction from Kafka as it arrives
# - Applies fraud detection rules in real time
# - Writes every transaction to PostgreSQL
# - Creates a fraud alert for suspicious transactions
#
# Think of this as the fraud detective sitting in the post
# office, reading every letter as it arrives and flagging
# anything suspicious before it gets filed away.
# ─────────────────────────────────────────────────────────────

import json
import psycopg2
import psycopg2.extras    # helps insert Python dicts into PostgreSQL
from kafka import KafkaConsumer
from datetime import datetime
import math               # for distance calculation between GPS points


# ─────────────────────────────────────────────────────────────
# SECTION 1: DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────

def get_db_connection():
    """
    Opens a connection to PostgreSQL.
    Returns the connection object.
    Same credentials as before — port 5433 because
    Windows has its own Postgres on 5432.
    """
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        dbname="fraud_db",
        user="fraud_user",
        password="fraud_pass"
    )


# ─────────────────────────────────────────────────────────────
# SECTION 2: FRAUD DETECTION RULES
# Each function checks one specific fraud pattern.
# Returns (is_fraud, reason, risk_score)
# risk_score is 0-100. Higher = more suspicious.
# ─────────────────────────────────────────────────────────────

def check_high_amount(transaction):
    """
    Rule 1: Is the amount unusually high?
    Thresholds by merchant category.
    A ₦5,000 Uber ride is normal.
    A ₦5,000,000 Uber ride is not.
    """
    # Maximum normal amounts per category (in Naira)
    thresholds = {
        "supermarket":      300000,    # was 500000
        "restaurant":        50000,    # was 80000
        "fuel_station":     200000,    # was 300000
        "electronics":     1000000,    # was 1500000
        "clothing":         250000,    # was 400000
        "hospital":         800000,    # was 1000000
        "hotel":            600000,    # was 800000
        "transport":         30000,    # was 50000
        "atm_withdrawal":   300000,    # was 500000
        "online_transfer": 2000000,    # was 3000000
}
    category  = transaction.get("merchant_category", "online_transfer")
    amount    = float(transaction.get("amount", 0))
    threshold = thresholds.get(category, 500000)

    if amount > threshold:
        # Calculate how far over the limit they are
        # e.g. 2x the limit = score 60, 5x the limit = score 90
        ratio      = amount / threshold
        risk_score = min(95, 50 + (ratio * 10))
        return True, "high_amount_for_category", round(risk_score, 2)

    return False, None, 0


def check_impossible_travel(transaction, last_transaction):
    """
    Rule 2: Did this user travel impossibly fast?
    If someone transacted in Lagos 5 minutes ago
    and now they're in London — that's physically impossible.

    Uses the Haversine formula to calculate distance
    between two GPS coordinates.
    """
    # If no previous transaction to compare, skip
    if not last_transaction:
        return False, None, 0

    # Get coordinates of both transactions
    lat1 = float(transaction.get("location_lat", 0))
    lon1 = float(transaction.get("location_lon", 0))
    lat2 = float(last_transaction.get("location_lat", 0))
    lon2 = float(last_transaction.get("location_lon", 0))

    # Haversine formula — calculates distance between
    # two points on a sphere (the Earth)
    # Result is in kilometres
    R    = 6371  # Earth's radius in kilometres
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a    = (math.sin(dlat/2)**2 +
            math.cos(math.radians(lat1)) *
            math.cos(math.radians(lat2)) *
            math.sin(dlon/2)**2)
    distance_km = 2 * R * math.asin(math.sqrt(a))

    # Get time difference between the two transactions
    try:
        time1 = datetime.fromisoformat(
            transaction.get("timestamp", datetime.now().isoformat())
        )
        time2 = datetime.fromisoformat(
            last_transaction.get("timestamp", datetime.now().isoformat())
        )
        # Time difference in hours
        time_diff_hours = abs((time1 - time2).total_seconds()) / 3600
    except Exception:
        return False, None, 0

    # Avoid division by zero
    if time_diff_hours < 0.01:
        time_diff_hours = 0.01

    # Speed = distance / time
    # If speed required is faster than a commercial plane
    # (approx 900 km/h) — flag it
    required_speed = distance_km / time_diff_hours

    if required_speed > 500 and distance_km > 500:    # was 900 and 100
        risk_score = min(95, 60 + (required_speed / 100))
        return (True,
        f"impossible_travel_detected",    # simplified
        round(risk_score, 2))

    return False, None, 0


def check_rapid_transactions(transaction, recent_transactions):
    """
    Rule 3: Are many small transactions happening rapidly?
    Fraudsters test a stolen card with tiny amounts
    before making a big purchase.
    5+ transactions under ₦1000 in 5 minutes = suspicious.
    """
    amount = float(transaction.get("amount", 0))

    # Only check small amounts
    if amount > 1000:
        return False, None, 0

    user_id = transaction.get("user_id")

    # Count how many small transactions this user
    # has made recently (in our recent_transactions memory)
    small_count = sum(
        1 for t in recent_transactions
        if (t.get("user_id") == user_id and
            float(t.get("amount", 0)) < 1000)
    )

    if small_count >= 4:   # this one + 4 previous = 5 total
        risk_score = min(90, 50 + (small_count * 8))
        return (True,
                f"rapid_small_transactions_{small_count+1}_in_window",
                round(risk_score, 2))

    return False, None, 0


def check_unusual_hour(transaction):
    """
    Rule 4: Is this transaction happening at an unusual hour?
    Transactions between 1AM and 4AM combined with
    high amounts are suspicious.
    """
    try:
        timestamp = datetime.fromisoformat(
            transaction.get("timestamp", datetime.now().isoformat())
        )
        hour   = timestamp.hour
        amount = float(transaction.get("amount", 0))

        # Between 1AM and 4AM AND high value
        if 1 <= hour <= 4 and amount > 100000:
            risk_score = 65 + (amount / 100000)
            return (True,
                    f"unusual_hour_{hour}am_high_value",
                    round(min(90, risk_score), 2))
    except Exception:
        pass

    return False, None, 0


def check_new_device(transaction, known_devices):
    """
    Rule 5: Is this transaction from a device we've never seen?
    A user's 10th transaction on a brand new device
    with a high amount is suspicious.
    """
    user_id   = transaction.get("user_id")
    device_id = transaction.get("device_id")
    amount    = float(transaction.get("amount", 0))

    # Only flag if it's a high-value transaction
    if amount < 500000:    # was 150000
       return False, None, 0

    # Get all known devices for this user
    user_devices = known_devices.get(user_id, set())

    if device_id not in user_devices and len(user_devices) > 0:
        return (True,
                f"new_device_high_value_{device_id}",
                75.0)

    return False, None, 0


# ─────────────────────────────────────────────────────────────
# SECTION 3: APPLY ALL RULES
# Runs every rule against a transaction.
# Returns the highest-risk fraud finding.
# ─────────────────────────────────────────────────────────────

def detect_fraud(transaction, last_transaction,
                 recent_transactions, known_devices):
    """
    Runs all fraud rules against a transaction.
    Returns (is_fraud, reason, risk_score).
    """
    # Run all rules
    checks = [
        check_high_amount(transaction),
        check_impossible_travel(transaction, last_transaction),
        check_rapid_transactions(transaction, recent_transactions),
        check_unusual_hour(transaction),
        check_new_device(transaction, known_devices),
    ]

    # Find the highest risk score among all checks
    fraud_checks = [(f, r, s) for f, r, s in checks if f]

    if fraud_checks:
        # Return the most severe finding
        most_severe = max(fraud_checks, key=lambda x: x[2])
        return most_severe

    return False, None, 0


# ─────────────────────────────────────────────────────────────
# SECTION 4: DATABASE WRITERS
# Two functions — one saves the transaction,
# one saves a fraud alert
# ─────────────────────────────────────────────────────────────

def save_transaction(cursor, transaction, is_fraud,
                     fraud_reason):
    """
    Saves a transaction to the transactions table.
    Uses INSERT ... ON CONFLICT DO NOTHING to handle
    duplicate transaction IDs safely.
    """
    cursor.execute("""
        INSERT INTO transactions (
            transaction_id, user_id, amount, merchant,
            merchant_category, location_lat, location_lon,
            city, device_id, device_type, timestamp,
            is_fraud, fraud_reason
        ) VALUES (
            %(transaction_id)s, %(user_id)s, %(amount)s,
            %(merchant)s, %(merchant_category)s,
            %(location_lat)s, %(location_lon)s,
            %(city)s, %(device_id)s, %(device_type)s,
            %(timestamp)s, %(is_fraud)s, %(fraud_reason)s
        )
        ON CONFLICT (transaction_id) DO NOTHING
    """, {
        "transaction_id":    transaction["transaction_id"],
        "user_id":           transaction["user_id"],
        "amount":            transaction["amount"],
        "merchant":          transaction["merchant"],
        "merchant_category": transaction["merchant_category"],
        "location_lat":      transaction["location_lat"],
        "location_lon":      transaction["location_lon"],
        "city":              transaction["city"],
        "device_id":         transaction["device_id"],
        "device_type":       transaction["device_type"],
        "timestamp":         transaction["timestamp"],
        "is_fraud":          is_fraud,
        "fraud_reason":      fraud_reason
    })


def save_fraud_alert(cursor, transaction, risk_score, reason):
    """
    Creates a fraud alert record.
    Only called when a transaction is flagged as fraud.
    """
    cursor.execute("""
        INSERT INTO fraud_alerts (
            transaction_id, risk_score,
            alert_reason, timestamp
        ) VALUES (%s, %s, %s, %s)
    """, (
        transaction["transaction_id"],
        risk_score,
        reason,
        transaction["timestamp"]
    ))


# ─────────────────────────────────────────────────────────────
# SECTION 5: MAIN CONSUMER LOOP
# ─────────────────────────────────────────────────────────────

def run_fraud_detector():
    """
    Main function. Reads from Kafka, detects fraud,
    writes to PostgreSQL.
    """
    print("=" * 55)
    print("  FRAUD DETECTION PIPELINE — REAL-TIME DETECTOR")
    print("=" * 55)

    # Connect to PostgreSQL
    print("\nConnecting to PostgreSQL...")
    conn   = get_db_connection()
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL")

    # Connect to Kafka as a consumer
    # A consumer is like a mailman who picks up messages
    # from a specific mailbox (topic)
    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        "transactions",              # which topic to read from
        bootstrap_servers=["localhost:9092"],
        # How to convert bytes back to Python dict
        # This is the reverse of what the simulator does
        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        ),
        # Consumer group ID — Kafka remembers where
        # this consumer left off, so if it restarts
        # it picks up from where it stopped
        group_id="fraud-detector-group",
        # Start from the earliest unread message
        auto_offset_reset="earliest",
        api_version=(2, 5, 0)
    )
    print("✅ Connected to Kafka")
    print("\nListening for transactions...")
    print("Press Ctrl+C to stop\n")
    print(f"{'#':<6} {'Status':<12} {'User':<12} "
          f"{'Amount':>14} {'City':<16} {'Risk'}")
    print("-" * 72)

    # Memory stores — we keep recent data in memory
    # to detect patterns across transactions
    # In a real Flink job these would be Flink state stores
    last_transaction_per_user = {}   # user_id → last transaction
    recent_transactions       = []   # last 50 transactions
    known_devices_per_user    = {}   # user_id → set of device IDs

    processed = 0
    flagged   = 0

    # This loop runs forever, reading one message at a time
    for message in consumer:
        try:
            # message.value is already a Python dict
            # because of value_deserializer above
            transaction = message.value
            user_id     = transaction.get("user_id")
            device_id   = transaction.get("device_id")

            # Get this user's last transaction (for travel check)
            last_txn = last_transaction_per_user.get(user_id)

            # Run all fraud detection rules
            is_fraud, reason, risk_score = detect_fraud(
                transaction,
                last_txn,
                recent_transactions,
                known_devices_per_user
            )

            # Save transaction to PostgreSQL
            save_transaction(cursor, transaction,
                             is_fraud, reason)

            # If fraud detected — also save an alert
            if is_fraud:
                save_fraud_alert(cursor, transaction,
                                 risk_score, reason)
                flagged += 1

            # Commit the database write
            # Like pressing Save after typing a document
            conn.commit()

            # Update our memory stores
            last_transaction_per_user[user_id] = transaction

            # Keep only last 50 transactions in memory
            recent_transactions.append(transaction)
            if len(recent_transactions) > 50:
                recent_transactions.pop(0)

            # Track known devices per user
            if user_id not in known_devices_per_user:
                known_devices_per_user[user_id] = set()
            known_devices_per_user[user_id].add(device_id)

            processed += 1

            # Display
            status       = "⚠ FRAUD" if is_fraud else "✅ clean"
            amount_disp  = f"₦{float(transaction['amount']):>12,.2f}"
            risk_disp    = f"{risk_score:.1f}" if is_fraud else "-"
            print(
                f"{processed:<6} "
                f"{status:<12} "
                f"{user_id:<12} "
                f"{amount_disp} "
                f"{transaction.get('city',''):<16} "
                f"{risk_disp}",
                flush=True
            )

            # Every 50 transactions show a summary
            if processed % 50 == 0:
                print(f"\n--- {processed} processed | "
                      f"{flagged} flagged | "
                      f"{flagged/processed*100:.1f}% fraud rate ---\n")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error processing transaction: {e}")
            conn.rollback()   # undo any partial write on error

    # Clean up
    print(f"\n\nStopped. Processed: {processed} | Flagged: {flagged}")
    cursor.close()
    conn.close()
    consumer.close()


if __name__ == "__main__":
    run_fraud_detector()