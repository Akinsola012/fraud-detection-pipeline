# ─────────────────────────────────────────────────────────────
# FRAUD DETECTION PIPELINE
# Layer 5: Airflow DAG — Daily Pipeline Orchestration
#
# What this file does:
# Runs automatically every day at midnight.
# It refreshes all DBT analytics models and updates
# the user risk scores table in PostgreSQL.
#
# DAG = Directed Acyclic Graph
# In plain English: a set of tasks that run in order,
# where each task only starts after the previous one finishes.
#
# Our DAG has 3 tasks:
# 1. Check PostgreSQL is healthy
# 2. Run DBT to refresh analytics tables
# 3. Update user risk scores from DBT output
# ─────────────────────────────────────────────────────────────

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2
import logging

# ─────────────────────────────────────────────────────────────
# DAG CONFIGURATION
# These settings apply to the whole DAG
# ─────────────────────────────────────────────────────────────

# Default arguments inherited by every task in this DAG
default_args = {
    "owner":            "fraud_pipeline",

    # If a run fails, retry once after 5 minutes
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),

    # Send email on failure (we won't configure SMTP
    # but this is how you'd do it in production)
    "email_on_failure": False,
    "email_on_retry":   False,
}

# Create the DAG object
# This is the container that holds all our tasks
dag = DAG(
    # Unique name for this DAG — shows in Airflow UI
    dag_id="fraud_detection_daily_pipeline",

    # Use the default args we defined above
    default_args=default_args,

    # Human-readable description — shows in Airflow UI
    description="Daily refresh of fraud detection analytics",

    # When to run: every day at midnight
    # Cron format: minute hour day month weekday
    # "0 0 * * *" = minute 0, hour 0, every day
    schedule_interval="0 0 * * *",

    # Start date — Airflow won't run for dates before this
    start_date=datetime(2026, 1, 1),

    # Don't run catch-up jobs for all the days
    # between start_date and today
    catchup=False,

    # Tags help organise DAGs in the UI
    tags=["fraud", "dbt", "daily"],
)


# ─────────────────────────────────────────────────────────────
# TASK 1: HEALTH CHECK
# Verify PostgreSQL is reachable before doing anything else
# If this fails, the whole DAG stops — no point refreshing
# models if the database is down
# ─────────────────────────────────────────────────────────────

def check_postgres_health():
    """
    Connects to PostgreSQL and runs a simple query.
    If it fails, raises an exception which marks
    the task as FAILED in Airflow.
    """
    logging.info("Checking PostgreSQL health...")

    try:
        # Inside Docker, Airflow connects to Postgres
        # using the container name, not localhost
        # Because Airflow IS inside Docker
        conn = psycopg2.connect(
            host="postgres",      # container name on fraud-network
            port=5432,            # internal port (not 5433)
            dbname="fraud_db",
            user="fraud_user",
            password="fraud_pass"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions;")
        count = cursor.fetchone()[0]
        logging.info(f"✅ PostgreSQL healthy. "
                     f"Transactions in DB: {count}")
        cursor.close()
        conn.close()
        return count

    except Exception as e:
        logging.error(f"❌ PostgreSQL health check failed: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# TASK 3: UPDATE RISK SCORES
# After DBT runs, copy the latest risk scores from
# analytics.dim_users into the user_risk_scores table
# This table is what the dashboard reads for real-time
# risk scores per user
# ─────────────────────────────────────────────────────────────

def update_user_risk_scores():
    """
    Copies risk scores from analytics.dim_users
    into the user_risk_scores operational table.

    Uses INSERT ... ON CONFLICT DO UPDATE (UPSERT)
    which means:
    - If user already has a score → update it
    - If user is new → insert a new row
    """
    logging.info("Updating user risk scores...")

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="fraud_db",
        user="fraud_user",
        password="fraud_pass"
    )
    cursor = conn.cursor()

    # This SQL reads from dim_users (built by DBT)
    # and writes into user_risk_scores
    cursor.execute("""
        INSERT INTO user_risk_scores (
            user_id,
            risk_score,
            avg_txn_amount,
            txn_count_7d,
            txn_count_30d,
            flagged_count_30d,
            last_updated
        )
        SELECT
            user_id,
            -- Normalise fraud_rate_pct to 0-100 risk score
            LEAST(100, fraud_rate_pct)      AS risk_score,
            avg_transaction_amount          AS avg_txn_amount,
            -- For now use total as approximation
            -- In production you'd filter by date
            total_transactions              AS txn_count_7d,
            total_transactions              AS txn_count_30d,
            total_fraud_flags               AS flagged_count_30d,
            NOW()                           AS last_updated
        FROM analytics.dim_users

        -- ON CONFLICT means: if this user_id already exists,
        -- update the existing row instead of failing
        ON CONFLICT (user_id) DO UPDATE SET
            risk_score        = EXCLUDED.risk_score,
            avg_txn_amount    = EXCLUDED.avg_txn_amount,
            txn_count_7d      = EXCLUDED.txn_count_7d,
            txn_count_30d     = EXCLUDED.txn_count_30d,
            flagged_count_30d = EXCLUDED.flagged_count_30d,
            last_updated      = EXCLUDED.last_updated
    """)

    updated = cursor.rowcount
    conn.commit()

    logging.info(f"✅ Updated risk scores for {updated} users")
    cursor.close()
    conn.close()
    return updated


# ─────────────────────────────────────────────────────────────
# WIRE UP THE TASKS
# Define the tasks and set their execution order
# ─────────────────────────────────────────────────────────────

# Task 1: Health check (Python function)
task_health_check = PythonOperator(
    task_id="check_postgres_health",
    python_callable=check_postgres_health,
    dag=dag,
)

# Task 2: Run DBT (Bash command)
# BashOperator runs a shell command
# We run dbt from its project directory
task_dbt_run = BashOperator(
    task_id="run_dbt_models",
    bash_command="/home/airflow/.local/bin/dbt run --project-dir /opt/airflow/dbt/fraud_detection --profiles-dir /home/airflow/.dbt",
    dag=dag,
)

# Task 3: Update risk scores (Python function)
task_update_scores = PythonOperator(
    task_id="update_user_risk_scores",
    python_callable=update_user_risk_scores,
    dag=dag,
)

# Set execution order using >> operator
# >> means "run this task after the previous one"
# health check → dbt run → update scores
task_health_check >> task_dbt_run >> task_update_scores