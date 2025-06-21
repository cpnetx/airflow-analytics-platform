#!/bin/bash

# Script to set up Kedro credentials as Airflow Variables
# This keeps sensitive data out of the codebase

echo "Setting up Kedro credentials in Airflow..."

# Database credentials
docker-compose exec -T airflow-scheduler airflow variables set KEDRO_DB_CONNECTION_STRING \
    "postgresql://maintenance_user:maintenance_dev_pass@host.docker.internal:5432/maintenance_dev"

# Azure credentials (update with real values)
docker-compose exec -T airflow-scheduler airflow variables set KEDRO_AZURE_CONNECTION_STRING \
    "dummy-not-used"

# OpenAI credentials (update with real values)
docker-compose exec -T airflow-scheduler airflow variables set KEDRO_OPENAI_API_KEY \
    "your-openai-api-key-here"

echo "Credentials set up successfully!"
echo "Note: Update the script with real credentials before running in production"