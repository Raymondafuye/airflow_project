# BeejanRide ELT Orchestration — Airflow Project

Orchestration layer for the BeejanRide data platform. Manages the full ELT pipeline from Airbyte ingestion through dbt transformation to BigQuery, running daily at 06:00.

> **Related repo:** [modeling_airbyte_dbt_bigquery](https://github.com/raymondafuye/modeling_airbyte_dbt_bigquery) contains the dbt project, sources, models, and snapshots.

---

## Architecture

 <img width="543" height="294" alt="image" src="https://github.com/user-attachments/assets/e83d4521-40d8-4c5a-aae4-af054a63d148" />

### Infrastructure
| Component | Technology |
|-----------|-----------|
| Orchestrator | Apache Airflow 3.0.6 (CeleryExecutor) |
| Ingestion | Airbyte Cloud |
| Transformation | dbt-bigquery 1.9.0 |
| Data Warehouse | Google BigQuery |
| Message Broker | Redis 7.2 |
| Metadata DB | PostgreSQL 13 |
| Containerisation | Docker Compose |

---

## DAG: `beejanride_elt_pipeline`
 
**Catchup:** enabled backfills from `2025-01-01`  
**Max active runs:** 1

### Task Graph

```
airbyte_trigger_sync
        │
airbyte_wait_sync
        │
dbt_source_freshness
        │
dbt_run_staging ──► dbt_test_staging
                            │
                   dbt_run_intermediate
                            │
                     dbt_run_marts ──► dbt_test_marts
                                               │
                                         dbt_snapshot
```

### Task Descriptions

| Task | Operator | Description |
|------|----------|-------------|
| `airbyte_trigger_sync` | `AirbyteTriggerSyncOperator` | Triggers Airbyte sync pulls all 6 tables (cities, drivers, driver_status_events, payments, riders, trips) from Postgres into BigQuery |
| `airbyte_wait_sync` | `AirbyteJobSensor` | Polls Airbyte every 30s until sync succeeds. Timeout: 1 hour |
| `dbt_source_freshness` | `BashOperator` | Validates raw BigQuery tables were updated within freshness thresholds |
| `dbt_run_staging` | `BashOperator` | Runs dbt models tagged `staging` cleans and standardises raw data |
| `dbt_test_staging` | `BashOperator` | Runs dbt tests on staging models (not_null, unique, accepted_values) |
| `dbt_run_intermediate` | `BashOperator` | Runs dbt models tagged `intermediate` — joins and enriches staging data |
| `dbt_run_marts` | `BashOperator` | Runs dbt models tagged `marts` produces business-facing fact and dimension tables |
| `dbt_test_marts` | `BashOperator` | Runs dbt tests on mart models to validate business logic |
| `dbt_snapshot` | `BashOperator` | Runs dbt snapshots to track slowly changing dimensions (SCD Type 2) |




### Why one Airbyte connection for all tables?
BeejanRide uses a single Airbyte connection (Postgres → BigQuery) that syncs all tables as streams. Triggering one sync job loads all tables atomically no partial loads, no coordination overhead between per-table triggers.

### Idempotency
Idempotency ensures re-running a DAG for the same date produces the same result without duplicating data:

- **Airbyte:** configured with incremental + deduplication sync mode. Re-triggering the same sync upserts records by primary key  no duplicates in BigQuery raw tables.
- **dbt staging/intermediate/marts:** all models use `materialized = 'table'` or `materialized = 'incremental'` with `unique_key`. A re-run fully replaces or upserts never appends blindly.
- **dbt snapshots:** use `strategy = 'timestamp'` with `updated_at` as the check column. Re-running a snapshot for the same logical date does not create duplicate history rows.
- **Airflow:** `max_active_runs = 1` prevents concurrent runs for the same DAG. `depends_on_past = False` allows individual task retries without blocking the whole pipeline.

### Backfill
Because `catchup = True` and `start_date = 2025-01-01`, Airflow automatically creates DAG runs for every day from the start date to today when first deployed. You can also trigger a manual backfill via CLI:

```bash
docker exec airflow_project-airflow-scheduler-1 \
  airflow dags backfill beejanride_elt_pipeline \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```

### Failure Handling
- Tasks retry **2 times** with a **5-minute delay** before marking as failed
- On failure: email alert sent via Gmail SMTP with DAG name, task name, run ID, and error message
- On DAG success: email notification sent with run summary


## Setup

### Prerequisites
- Docker Desktop
- A Google Cloud service account JSON key with BigQuery access
- An Airbyte Cloud account with a Postgres → BigQuery connection

### 1. Clone the repo
```bash
git clone https://github.com/raymondafuye/airflow_project.git
cd airflow_project
```

### 2. Configure `.env`
```env
AIRFLOW_UID=50000
DBT_PROJECT_DIR=../modeling_airbyte_dbt_bigquery/myanalystdata
DBT_KEYFILE=../modeling_airbyte_dbt_bigquery/myanalystdata/your-keyfile.json
```

### 3. Build and start
```bash
docker compose build
docker compose up -d
```

### 4. Access the UI
Open [http://localhost:8080](http://localhost:8080)

Get your login password:
```bash
docker exec airflow_project-airflow-apiserver-1 \
  cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### 5. Add Airflow connections

**Airbyte** (`Admin → Connections`):
| Field | Value |
|-------|-------|
| Connection Id | `airbyte_default` |
| Connection Type | `Airbyte` |
| Server URL | `https://api.airbyte.com/v1` |
| Client ID | your Airbyte Client ID |
| Client Secret | your Airbyte Client Secret |
| Token URL | `https://api.airbyte.com/v1/applications/token` |

**SMTP** (`Admin → Connections`):
| Field | Value |
|-------|-------|
| Connection Id | `smtp_conn` |
| Connection Type | `Email` |
| Host | `smtp.gmail.com` |
| Login | your Gmail address |
| Password | your Gmail app password |
| Port | `587` |
| Extra | `{"use_starttls": true}` |

---

## Dependencies

Installed via `Dockerfile`:
```
apache/airflow:3.0.6
dbt-bigquery==1.9.0
apache-airflow-providers-airbyte
apache-airflow-providers-http
```
