ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

USER root
# Java is only needed if you run the optional PySpark job from inside Airflow
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# Pipeline libraries, installed into Airflow's own environment
COPY --chown=airflow:root requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# dbt lives in its own virtualenv so its pins never fight Airflow's
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.11,<2" "dbt-duckdb>=1.9.4,<2" "dbt-snowflake>=1.10,<2"
ENV DBT_BIN=/opt/airflow/dbt-venv/bin/dbt
