import os

from airflow.utils.email import send_email_smtp


def on_failure(context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = getattr(ti, "run_id", "unknown")
    execution_time = getattr(ti, "start_date", "unknown")
    error = str(context.get("exception", "No exception captured"))

    base_url = os.getenv("AIRFLOW__API__BASE_URL", "http://localhost:8080")
    log_url = f"{base_url}/dags/{dag_id}/runs/{run_id}"

    subject = f"Airflow Task Failed: {dag_id}.{task_id}"
    html_content = f"""
    <h3>BeejanRide Pipeline - Task Failed</h3>
    <ul>
      <li><b>DAG:</b> {dag_id}</li>
      <li><b>Task:</b> {task_id}</li>
      <li><b>Run ID:</b> {run_id}</li>
      <li><b>Execution Time:</b> {execution_time}</li>
      <li><b>Error:</b> {error}</li>
    </ul>
    <p><a href="{log_url}">View Logs</a></p>
    """
    try:
        send_email_smtp(
            to="raymondafuye@gmail.com",
            subject=subject,
            html_content=html_content,
            conn_id="smtp_conn",
        )
    except Exception:
        pass


def on_success(context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    run_id = getattr(ti, "run_id", "unknown")

    base_url = os.getenv("AIRFLOW__API__BASE_URL", "http://localhost:8080")
    log_url = f"{base_url}/dags/{dag_id}/runs/{run_id}"

    subject = f"BeejanRide Pipeline Succeeded: {dag_id}"
    html_content = f"""
    <h3>BeejanRide Pipeline - DAG Succeeded</h3>
    <ul>
      <li><b>DAG:</b> {dag_id}</li>
      <li><b>Run ID:</b> {run_id}</li>
    </ul>
    <p><a href="{log_url}">View Run</a></p>
    """
    try:
        send_email_smtp(
            to="raymondafuye@gmail.com",
            subject=subject,
            html_content=html_content,
            conn_id="smtp_conn",
        )
    except Exception:
        pass
