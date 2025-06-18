#!/usr/bin/env bash
set -euo pipefail

# This script applies Airflow Variables to the cloud deployment
# Based on values from globals.yml

NAMESPACE="airflow"

echo "Setting up Airflow Variables in cloud deployment..."
echo "Using KUBECONFIG: ${KUBECONFIG:-not set}"

# Check if we can access the cluster
if ! kubectl get ns $NAMESPACE &>/dev/null; then
    echo "ERROR: Cannot access namespace $NAMESPACE. Check your KUBECONFIG."
    exit 1
fi

# Find a running scheduler pod
SCHEDULER_POD=$(kubectl get pods -n $NAMESPACE -l component=scheduler -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "$SCHEDULER_POD" ]; then
    echo "ERROR: No scheduler pod found"
    exit 1
fi

echo "Using scheduler pod: $SCHEDULER_POD"
echo ""

# Function to set a variable
set_airflow_var() {
    local var_name=$1
    local var_value=$2
    echo -n "Setting $var_name... "
    if kubectl exec -n $NAMESPACE $SCHEDULER_POD -- airflow variables set "$var_name" "$var_value" &>/dev/null; then
        echo "✓"
    else
        echo "✗ (failed)"
    fi
}

echo "Setting InfluxDB tokens..."
set_airflow_var "INFLUXDB_TOKEN_SCH" "<REPLACE_WITH_SCH_TOKEN>"
set_airflow_var "INFLUXDB_TOKEN_SV" "<REPLACE_WITH_SV_TOKEN>"

echo ""
echo "Setting ClickHouse configuration..."
set_airflow_var "IQS_DB_CLICKHOUSE" "clickhouse://default:@10.0.0.6:8223/wpp"

echo ""
echo "Setting AlertManager API keys..."
set_airflow_var "ALERTMAN_API_KEY_PROD" "<REPLACE_WITH_PROD_API_KEY>"
set_airflow_var "ALERTMAN_API_KEY_DEV" "<REPLACE_WITH_DEV_API_KEY>"

echo ""
echo "Setting OAuth configuration..."
set_airflow_var "OAUTH_CLIENT_ID" "75aa9d86-2803-48db-8d2b-c5355acb6bac"
set_airflow_var "OAUTH_CLIENT_SECRET" "<REPLACE_WITH_ACTUAL_SECRET>"
set_airflow_var "OAUTH_METADATA_URL" "https://login.microsoftonline.com/665fd09e-d93c-4030-9f0a-b00de5de93b8/v2.0/.well-known/openid-configuration"

echo ""
echo "Setting Azure Storage configuration..."
# Using the SCH storage account as default
AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=tfwppschdatastore;AccountKey=<need-actual-key>;EndpointSuffix=core.windows.net"
set_airflow_var "AZURE_STORAGE_CONNECTION_STRING" "$AZURE_CONNECTION_STRING"
set_airflow_var "AZURE_SAS_TOKEN" "<REPLACE_WITH_SAS_TOKEN>"

# Additional storage variables for report uploads
set_airflow_var "AZURE_STORAGE_ACCOUNT_NAME" "timefabricus"
set_airflow_var "PROCESS_REPORT_CONTAINER_NAME" "process-report-dev"
set_airflow_var "AZURE_STORAGE_SAS_TOKEN" "<REPLACE_WITH_STORAGE_SAS_TOKEN>"

echo ""
echo "Setting Process Report API configuration..."
set_airflow_var "process_report_api_url" "https://know.dev.cpnet.ai"
set_airflow_var "AIRFLOW_SERVICE_TOKEN" "<REPLACE_WITH_SERVICE_TOKEN>"

echo ""
echo "Verifying variables were set..."
echo "Listing all variables:"
kubectl exec -n $NAMESPACE $SCHEDULER_POD -- airflow variables list

echo ""
echo "✅ Airflow Variables setup complete!"
echo ""
echo "Note: The ClickHouse URL is set to 'host.docker.internal:18123' which may need adjustment"
echo "for cloud deployment. Update it if your ClickHouse instance has a different endpoint."
echo ""
echo "To update a variable manually:"
echo "kubectl exec -n $NAMESPACE $SCHEDULER_POD -- airflow variables set <KEY> '<VALUE>'"