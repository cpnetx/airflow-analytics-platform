ARG AIRFLOW_VERSION=3.0.2
FROM apache/airflow:${AIRFLOW_VERSION}

# Grab the matching constraints list
ARG AIRFLOW_CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

USER root

# Create directory for Kedro project
RUN mkdir -p /opt/kedro_project && chown -R airflow:root /opt/kedro_project

USER airflow

# Install Python dependencies
COPY --chown=airflow:root requirements.txt /requirements.txt

# Install non-conflicting packages with constraints
RUN pip install --no-cache-dir \
    --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}" \
    influxdb-client>=1.36.0 \
    clickhouse-driver>=0.2.5 \
    clickhouse-connect>=0.6.0 \
    sqlalchemy-clickhouse>=0.1.5 \
    azure-storage-blob>=12.14.0 \
    openai>=1.0.0 \
    pgvector>=0.2.0 \
    openpyxl>=3.0.0 \
    scikit-learn>=1.3.0

# Install Kedro packages without constraints to avoid conflicts
RUN pip install --no-cache-dir \
    kedro==0.19.5 \
    kedro-airflow==0.10.0 \
    kedro-datasets==3.0.0

# Copy Kedro project (will be mounted as volume in docker-compose)
# This is optional - you can mount it as volume instead
# COPY --chown=airflow:root ../kedro_dags/maintenance_kedro /opt/kedro_project/

# Add Kedro project to Python path
ENV PYTHONPATH="/opt/kedro_project/src:${PYTHONPATH}"