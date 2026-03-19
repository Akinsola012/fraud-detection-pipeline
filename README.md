# Real-Time Fraud Detection Pipeline

A production-style data engineering pipeline that streams Nigerian fintech transactions through Kafka, detects fraud in real time using 5 detection rules, models the data with DBT, orchestrates daily refreshes with Airflow, and visualises everything on a live Power BI dashboard — all containerised with Docker.

---

## Dashboard Preview

> Real-Time Fraud Detection Dashboard — 20K+ transactions · ₦3.93B flagged · 50 users risk-scored

---

## Architecture

```
Python Simulator
      │
      ▼
  Apache Kafka          ← real-time transaction stream
      │
      ▼
 Fraud Detector         ← 5 fraud rules, risk scoring
 (Python consumer)
      │
      ▼
  PostgreSQL            ← raw transactions + fraud alerts
      │
      ├──────────────────────────────┐
      ▼                              ▼
    DBT                          Airflow
  (dim_users,               (daily pipeline
  dim_merchants,             orchestration)
  fact_fraud_summary)
      │
      ▼
  Power BI              ← live fraud dashboard
```

Everything runs locally with **Docker Compose** — one command starts the full stack.

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Streaming | Apache Kafka | Real-time transaction ingestion |
| Detection | Python | Fraud rules engine |
| Storage | PostgreSQL | Raw data + enriched analytics |
| Modelling | DBT | Dimensional models |
| Batch | Apache Spark | Historical analysis |
| Orchestration | Apache Airflow | Daily pipeline scheduling |
| Visualisation | Power BI | Live fraud dashboard |
| Infrastructure | Docker Compose | Full stack containerisation |

---

## Fraud Detection Rules

Five rules fire in real time as each transaction arrives:

| Rule | Pattern | Risk Score |
|---|---|---|
| High amount | Transaction exceeds category threshold | 50–95 |
| Impossible travel | User transacts in two cities too far apart in too little time | 60–95 |
| Rapid small transactions | 5+ transactions under ₦1,000 in quick succession (card testing) | 50–90 |
| Unusual hour | High-value transaction between 1AM–4AM | 65–90 |
| New device | High-value transaction from an unrecognised device | 75 |

---

## DBT Models

```
sources (raw PostgreSQL)
    └── staging/
    │       stg_transactions.sql     ← cleaned, type-cast transactions
    │       stg_fraud_alerts.sql     ← cleaned fraud alerts
    └── marts/
            dim_users.sql            ← one row per user, risk profile
            dim_merchants.sql        ← one row per merchant, fraud stats
            fact_fraud_summary.sql   ← daily fraud KPIs
```

---

## Project Structure

```
fraud-detection-pipeline/
├── docker-compose.yml           ← full stack definition
├── docker/
│   └── postgres/
│       └── init.sql             ← database schema
├── src/
│   ├── simulator/
│   │   └── transaction_simulator.py   ← generates fake transactions
│   └── flink_jobs/
│       └── fraud_detector.py          ← real-time fraud detection
├── dbt/
│   └── fraud_detection/
│       └── models/
│           ├── staging/         ← stg_transactions, stg_fraud_alerts
│           └── marts/           ← dim_users, dim_merchants, fact_fraud_summary
└── airflow/
    └── dags/
        └── fraud_pipeline_dag.py      ← daily orchestration DAG
```

---

## How to Run

### Prerequisites
- Docker Desktop
- Python 3.11
- Power BI Desktop (for dashboard)

### 1. Start the full stack
```bash
docker compose up -d
```

Verify all services are running:
```bash
docker ps
```

### 2. Start the transaction simulator
```bash
py -3.11 src/simulator/transaction_simulator.py
```

Generates 1 transaction per second — 90% normal, 10% fraudulent.

### 3. Start the fraud detector
```bash
py -3.11 src/flink_jobs/fraud_detector.py
```

Reads from Kafka, applies fraud rules, writes to PostgreSQL in real time.

### 4. Run DBT models
```bash
cd dbt/fraud_detection
dbt run
```

Builds dimensional models in the `analytics` schema.

### 5. Open Airflow UI
```
http://localhost:8082
Login: admin / admin
```

Trigger the `fraud_detection_daily_pipeline` DAG manually or let it run on its daily schedule.

### 6. Connect Power BI
```
Server:   localhost:5433
Database: fraud_db
User:     fraud_user
Password: fraud_pass
```

Load tables from the `analytics` schema.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8082 | admin / admin |
| Kafka UI | http://localhost:8083 | — |
| Flink Dashboard | http://localhost:8081 | — |
| Spark Dashboard | http://localhost:8080 | — |
| PostgreSQL | localhost:5433 | fraud_user / fraud_pass |

---

## Key Results

- **20,000+** transactions processed
- **5 fraud patterns** detected in real time
- **₦3.93B** in fraudulent value identified
- **50 users** risk-scored and ranked
- **40 merchants** analysed by fraud rate
- **Online transfers** and **electronics** are highest-fraud categories

---

## Database Schema

```sql
-- Raw data (public schema)
transactions        -- every transaction with fraud flag
fraud_alerts        -- one row per flagged transaction
user_risk_scores    -- daily risk scores per user

-- Analytics (analytics schema — built by DBT)
dim_users           -- user risk profiles
dim_merchants       -- merchant fraud statistics
fact_fraud_summary  -- daily fraud KPIs
```

---

## Author

**Oluwaseye Akinsola**
Data Engineer · Product Manager · Business Analyst

📧 akinsolaoluwaseye1@gmail.com
🔗 [LinkedIn](https://www.linkedin.com/in/oluwaseye-akinsola/)
