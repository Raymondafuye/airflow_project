FROM apache/airflow:3.0.6

RUN pip install --no-cache-dir \
    dbt-bigquery==1.9.0 \
    apache-airflow-providers-airbyte \
    apache-airflow-providers-http
