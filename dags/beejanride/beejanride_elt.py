"""
BeejanRide ELT Orchestration DAG
Pipeline: Airbyte Sync -> dbt Staging -> dbt Intermediate -> dbt Marts -> dbt Tests -> Snapshot
Schedule: Daily at 06:00 UTC

One Airbyte connection syncs all tables (cities, drivers, driver_status_events,
payments, riders, trips) as streams from Postgres -> BigQuery.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

from beejanride.callbacks import on_failure, on_success

# One Airbyte connection syncs all tables as streams (Postgres -> BigQuery).
AIRBYTE_CONN_ID = "airbyte_default"
AIRBYTE_CONNECTION_UUID = "88cc0c20-ac95-4806-b1c7-0cceda7570bf"

DBT_DIR = "/opt/dbt/myanalystdata"
DBT_CMD = "/home/airflow/.local/bin/dbt --no-use-colors"

default_args = {
    "owner": "beejanride",
    "depends_on_past": False,
    "email": ["raymondafuye@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="beejanride_elt_pipeline",
    description="BeejanRide: Airbyte ingestion -> dbt transformation -> tests -> snapshot",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["beejanride", "elt", "production"],
    on_success_callback=on_success,
) as dag:

    # 1. INGESTION: Trigger Airbyte sync (all tables as streams) and wait for completion.
    airbyte_trigger = AirbyteTriggerSyncOperator(
        task_id="airbyte_trigger_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTION_UUID,
        asynchronous=True,
    )

    airbyte_wait = AirbyteJobSensor(
        task_id="airbyte_wait_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        airbyte_job_id="{{ task_instance.xcom_pull('airbyte_trigger_sync', key='return_value') }}",
        poke_interval=30,
        timeout=3600,
    )

    airbyte_trigger >> airbyte_wait
    all_syncs_done = [airbyte_wait]

    # 2. SOURCE FRESHNESS CHECK
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CMD} source freshness --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    # 3. TRANSFORMATION: dbt staging layer
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select tag:staging --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_CMD} test --select tag:staging --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    # 4. TRANSFORMATION: dbt intermediate layer
    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT_CMD} run --select tag:intermediate --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    # 5. TRANSFORMATION: dbt marts layer
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select tag:marts --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT_CMD} test --select tag:marts --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    # 6. SNAPSHOT: track slowly changing dimensions
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={"GOOGLE_APPLICATION_CREDENTIALS": "/opt/dbt/keyfile.json"},
    )

    # Task dependency chain:
    # All Airbyte syncs (parallel) -> freshness check -> staging -> intermediate -> marts -> snapshot
    all_syncs_done >> dbt_source_freshness
    dbt_source_freshness >> dbt_staging >> dbt_test_staging
    dbt_test_staging >> dbt_intermediate >> dbt_marts >> dbt_test_marts >> dbt_snapshot
