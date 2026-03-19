# ─────────────────────────────────────────────────────────────
# FRAUD DETECTION PIPELINE
# Layer 2: Transaction Simulator
#
# What this file does:
# Generates realistic fake Nigerian fintech transactions
# and sends them to Kafka every second.
# Some transactions are normal. Some are deliberately fraudulent.
# ─────────────────────────────────────────────────────────────

import json          # converts Python dictionaries to JSON strings
import time          # lets us pause between transactions
import random        # generates random values
import uuid          # generates unique IDs
from datetime import datetime          # gets current date and time
from kafka import KafkaProducer        # sends messages to Kafka


# ─────────────────────────────────────────────────────────────
# SECTION 1: REALISTIC DATA
# These are the building blocks for generating
# realistic Nigerian fintech transactions
# ─────────────────────────────────────────────────────────────

# Nigerian cities with their GPS coordinates
# Format: (city_name, latitude, longitude)
# We need GPS to detect "impossible travel" fraud later
# e.g. someone transacts in Lagos then Abuja 10 minutes later
NIGERIAN_CITIES = [
    ("Lagos",       6.5244,  3.3792),
    ("Abuja",       9.0765,  7.3986),
    ("Kano",       12.0022,  8.5920),
    ("Ibadan",      7.3775,  3.9470),
    ("Port Harcourt",4.8156, 7.0498),
    ("Enugu",       6.4584,  7.5464),
    ("Kaduna",     10.5222,  7.4383),
    ("Benin City",  6.3350,  5.6270),
    ("Owerri",      5.4836,  7.0343),
    ("Warri",       5.5167,  5.7500),
]

# Merchant categories — what kind of shop the transaction is at
# Each category has a typical price range (min, max) in Naira
MERCHANT_CATEGORIES = {
    "supermarket":      (2000,   85000),
    "restaurant":       (1500,   25000),
    "fuel_station":     (5000,  150000),
    "electronics":      (15000, 800000),
    "clothing":         (3000,  120000),
    "hospital":         (5000,  500000),
    "hotel":            (20000, 450000),
    "transport":        (500,    15000),
    "atm_withdrawal":   (5000,  300000),
    "online_transfer":  (1000, 2000000),
}

# Specific merchant names per category
# Makes the data look realistic
MERCHANTS = {
    "supermarket":     ["Shoprite", "Spar", "Justrite", "Prince Ebeano"],
    "restaurant":      ["Chicken Republic", "Dominos", "Mr Biggs", "Tantalizers"],
    "fuel_station":    ["Total", "Ardova", "Conoil", "MRS"],
    "electronics":     ["Slot", "Fouani", "Pointek", "3C Hub"],
    "clothing":        ["Zara Lagos", "H&M", "Polo Avenue", "NoLimit"],
    "hospital":        ["Reddington Hospital", "Eko Hospital", "LASUTH", "UCH"],
    "hotel":           ["Transcorp Hilton", "Radisson Blu", "Eko Hotel", "Federal Palace"],
    "transport":       ["Uber", "Bolt", "Lagbus", "Gigm"],
    "atm_withdrawal":  ["GTBank ATM", "Access ATM", "Zenith ATM", "UBA ATM"],
    "online_transfer": ["GTBank Transfer", "Opay", "Palmpay", "Kuda"],
}

# Device types — what device was used for the transaction
DEVICE_TYPES = ["mobile_ios", "mobile_android", "web_desktop", "web_mobile", "pos_terminal"]

# 50 fake user IDs — our "customers"
# In a real system these would come from a users database
USER_IDS = [f"USR_{str(i).zfill(4)}" for i in range(1, 51)]
# This creates: USR_0001, USR_0002, ... USR_0050


# ─────────────────────────────────────────────────────────────
# SECTION 2: TRANSACTION GENERATORS
# Two functions — one for normal transactions,
# one for fraudulent ones
# ─────────────────────────────────────────────────────────────

def generate_normal_transaction():
    """
    Creates a realistic, innocent transaction.
    A normal customer buying something normal.
    """
    # Pick a random user
    user_id = random.choice(USER_IDS)

    # Pick a random city for this transaction
    city_name, lat, lon = random.choice(NIGERIAN_CITIES)

    # Add tiny GPS variation so not every transaction
    # in Lagos is at the exact same coordinate
    # random.uniform(-0.05, 0.05) gives a small random offset
    lat  += random.uniform(-0.05, 0.05)
    lon  += random.uniform(-0.05, 0.05)

    # Pick a random merchant category
    category = random.choice(list(MERCHANT_CATEGORIES.keys()))

    # Get the price range for that category
    min_amount, max_amount = MERCHANT_CATEGORIES[category]

    # Pick a random amount within that range
    # round(..., 2) keeps it to 2 decimal places like real money
    amount = round(random.uniform(min_amount, max_amount), 2)

    # Pick a specific merchant from that category
    merchant = random.choice(MERCHANTS[category])

    # Build the transaction dictionary
    # This is what gets saved to Kafka and PostgreSQL
    transaction = {
        "transaction_id":    str(uuid.uuid4()),   # unique ID e.g. "a3f4-..."
        "user_id":           user_id,
        "amount":            amount,
        "merchant":          merchant,
        "merchant_category": category,
        "location_lat":      round(lat, 8),
        "location_lon":      round(lon, 8),
        "city":              city_name,
        "device_id":         f"DEV_{user_id}_{random.randint(1,3)}",
        "device_type":       random.choice(DEVICE_TYPES),
        "timestamp":         datetime.now().isoformat(),
        "is_fraud":          False,
        "fraud_reason":      None
    }

    return transaction


def generate_fraudulent_transaction():
    """
    Creates a suspicious transaction.
    One of several fraud patterns that real fraud teams look for.
    """
    user_id = random.choice(USER_IDS)

    # Pick a random fraud pattern to simulate
    fraud_type = random.choice([
        "high_amount",        # unusually large transaction
        "impossible_travel",  # two cities too far apart too quickly
        "rapid_fire",         # many transactions in seconds
        "unusual_hour",       # transaction at 3AM
        "new_device",         # transaction from a brand new device
    ])

    # Start with a base normal transaction
    transaction = generate_normal_transaction()

    # Override the user_id so we track the same user
    transaction["user_id"] = user_id

    # Now modify it based on the fraud type
    if fraud_type == "high_amount":
        # 10x to 50x the normal maximum for this category
        transaction["amount"] = round(
            random.uniform(500000, 5000000), 2
        )
        transaction["fraud_reason"] = "amount_exceeds_threshold"

    elif fraud_type == "impossible_travel":
        # Put the transaction in a city far from the user's last known location
        # We pick London coordinates — impossible if they just transacted in Lagos
        transaction["location_lat"] = 51.5074   # London latitude
        transaction["location_lon"] = -0.1278   # London longitude
        transaction["city"]         = "London"
        transaction["fraud_reason"] = "impossible_travel_detected"

    elif fraud_type == "rapid_fire":
        # Same device, very small amounts — testing if card works
        # Fraudsters do this before making a big purchase
        transaction["amount"]      = round(random.uniform(50, 500), 2)
        transaction["device_id"]   = f"DEV_{user_id}_RAPID"
        transaction["fraud_reason"] = "rapid_small_transactions"

    elif fraud_type == "unusual_hour":
        # Transaction at 3AM — unusual for most people
        suspicious_time = datetime.now().replace(
            hour=3,
            minute=random.randint(0, 59)
        )
        transaction["timestamp"]    = suspicious_time.isoformat()
        transaction["amount"]       = round(random.uniform(200000, 1000000), 2)
        transaction["fraud_reason"] = "unusual_hour_high_amount"

    elif fraud_type == "new_device":
        # Transaction from a device ID never seen before for this user
        transaction["device_id"]   = f"DEV_UNKNOWN_{uuid.uuid4().hex[:8]}"
        transaction["amount"]      = round(random.uniform(100000, 800000), 2)
        transaction["fraud_reason"] = "new_device_high_value_transaction"

    # Mark it as fraud so we can measure how well Flink catches it
    transaction["is_fraud"] = True

    return transaction


# ─────────────────────────────────────────────────────────────
# SECTION 3: KAFKA PRODUCER
# This is what sends the transaction to Kafka
# ─────────────────────────────────────────────────────────────

def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        request_timeout_ms=10000,
        retries=3,
        api_version=(2, 5, 0)   # ← add this line
    )
    return producer


# ─────────────────────────────────────────────────────────────
# SECTION 4: MAIN LOOP
# This runs forever, generating and sending transactions
# ─────────────────────────────────────────────────────────────

def run_simulator():
    """
    The main function that runs the simulator.
    Generates transactions and sends them to Kafka in a loop.
    """
    print("=" * 55)
    print("  FRAUD DETECTION PIPELINE — TRANSACTION SIMULATOR")
    print("=" * 55)
    print("Connecting to Kafka at localhost:9092...")

    # Create the Kafka producer (our delivery person)
    producer = create_kafka_producer()
    print("✅ Connected to Kafka\n")

    # Counters for our progress display
    total_sent    = 0
    fraud_sent    = 0
    normal_sent   = 0

    print("Starting transaction stream...")
    print("Press Ctrl+C to stop\n")
    print(f"{'#':<6} {'Type':<10} {'User':<12} {'Amount':>12} {'City':<16} {'Merchant'}")
    print("-" * 75)

    # This loop runs forever until you press Ctrl+C
    while True:
        try:
            # 90% chance of normal transaction
            # 10% chance of fraudulent transaction
            # This is realistic — most transactions are legitimate
            if random.random() < 0.90:
                transaction = generate_normal_transaction()
                tx_type     = "NORMAL"
                normal_sent += 1
            else:
                transaction = generate_fraudulent_transaction()
                tx_type     = "⚠ FRAUD"
                fraud_sent  += 1

            total_sent += 1

            # Send to Kafka topic called "transactions"
            # If this topic doesn't exist yet, Kafka creates it automatically
            # (because we set KAFKA_AUTO_CREATE_TOPICS_ENABLE: true)
            producer.send("transactions", value=transaction)

            # Print a summary line so we can watch it work
            amount_display = f"₦{transaction['amount']:>12,.2f}"
            reason_display = transaction.get("fraud_reason") or ""
            print(
                f"{total_sent:<6} "
                f"{tx_type:<10} "
                f"{transaction['user_id']:<12} "
                f"{amount_display} "
                f"{transaction['city']:<16} "
                f"{transaction['merchant']:<20} "
                f"{reason_display}",
                flush=True
            )

            # Every 100 transactions print a summary
            if total_sent % 100 == 0:
                print(f"\n--- SUMMARY: {total_sent} sent | "
                      f"{normal_sent} normal | "
                      f"{fraud_sent} flagged ---\n")

            # Wait 1 second before the next transaction
            # Change this to 0.1 for faster testing
            time.sleep(1)

        except KeyboardInterrupt:
            # This runs when you press Ctrl+C
            print(f"\n\nSimulator stopped.")
            print(f"Total sent:    {total_sent}")
            print(f"Normal:        {normal_sent}")
            print(f"Fraudulent:    {fraud_sent}")
            producer.close()
            break

        except Exception as e:
            # If anything goes wrong, print the error but keep running
            print(f"Error sending transaction: {e}")
            time.sleep(2)


# ─────────────────────────────────────────────────────────────
# This runs the simulator when you execute this file directly
# "if __name__ == '__main__'" means:
# "only run this if I ran THIS file, not if someone imported it"
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_simulator()