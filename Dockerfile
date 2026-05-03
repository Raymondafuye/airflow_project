FROM apache/airflow:3.0.0

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-airbyte==4.0.0 \
    apache-airflow-providers-http

RUN pip install --no-cache-dir \
    dbt-bigquery==1.9.0 \
    "protobuf<5.0"