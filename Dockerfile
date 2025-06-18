ARG AIRFLOW_VERSION=3.0.2
FROM apache/airflow:${AIRFLOW_VERSION}

# Grab the matching constraints list
ARG AIRFLOW_CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

USER airflow
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir               \
        --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}" \
        -r /requirements.txt