#!/usr/bin/env bash
set -euo pipefail

# This script helps set up Airflow Variables in the cloud deployment
# It can either create a Kubernetes secret or generate Airflow CLI commands

NAMESPACE="airflow"

echo "Airflow Variables Setup"
echo "======================"
echo ""
echo "This script will help you set up the required Airflow Variables."
echo "Please have your configuration values ready."
echo ""

# Function to prompt for value with optional default
prompt_value() {
    local var_name=$1
    local description=$2
    local default=${3:-}
    
    if [ -n "$default" ]; then
        read -p "$description [$default]: " value
        value=${value:-$default}
    else
        read -p "$description: " value
    fi
    
    echo "$value"
}

# Collect all variables
echo "Please provide the following configuration values:"
echo ""

INFLUXDB_TOKEN_SCH=$(prompt_value "INFLUXDB_TOKEN_SCH" "InfluxDB token for SCH site")
INFLUXDB_TOKEN_SV=$(prompt_value "INFLUXDB_TOKEN_SV" "InfluxDB token for SV site")
IQS_DB_CLICKHOUSE=$(prompt_value "IQS_DB_CLICKHOUSE" "ClickHouse URL" "clickhouse://default:@host.docker.internal:18123/wpp")
ALERTMAN_API_KEY_PROD=$(prompt_value "ALERTMAN_API_KEY_PROD" "AlertManager API key (production)")
ALERTMAN_API_KEY_DEV=$(prompt_value "ALERTMAN_API_KEY_DEV" "AlertManager API key (development)")
OAUTH_CLIENT_ID=$(prompt_value "OAUTH_CLIENT_ID" "OAuth Client ID for email")
OAUTH_CLIENT_SECRET=$(prompt_value "OAUTH_CLIENT_SECRET" "OAuth Client Secret")
OAUTH_METADATA_URL=$(prompt_value "OAUTH_METADATA_URL" "OAuth Metadata URL" "https://login.microsoftonline.com/665fd09e-d93c-4030-9f0a-b00de5de93b8/v2.0/.well-known/openid-configuration")
AZURE_STORAGE_CONNECTION_STRING=$(prompt_value "AZURE_STORAGE_CONNECTION_STRING" "Azure Storage Connection String")
AZURE_SAS_TOKEN=$(prompt_value "AZURE_SAS_TOKEN" "Azure SAS Token")

echo ""
echo "Choose how to apply these variables:"
echo "1) Create Kubernetes Secret (recommended)"
echo "2) Generate Airflow CLI commands"
echo "3) Generate kubectl exec commands"
read -p "Enter choice (1-3): " choice

case $choice in
    1)
        echo ""
        echo "Creating Kubernetes secret..."
        echo "Make sure KUBECONFIG is set correctly."
        echo ""
        
        kubectl create secret generic airflow-variables -n $NAMESPACE \
            --from-literal=INFLUXDB_TOKEN_SCH="$INFLUXDB_TOKEN_SCH" \
            --from-literal=INFLUXDB_TOKEN_SV="$INFLUXDB_TOKEN_SV" \
            --from-literal=IQS_DB_CLICKHOUSE="$IQS_DB_CLICKHOUSE" \
            --from-literal=ALERTMAN_API_KEY_PROD="$ALERTMAN_API_KEY_PROD" \
            --from-literal=ALERTMAN_API_KEY_DEV="$ALERTMAN_API_KEY_DEV" \
            --from-literal=OAUTH_CLIENT_ID="$OAUTH_CLIENT_ID" \
            --from-literal=OAUTH_CLIENT_SECRET="$OAUTH_CLIENT_SECRET" \
            --from-literal=OAUTH_METADATA_URL="$OAUTH_METADATA_URL" \
            --from-literal=AZURE_STORAGE_CONNECTION_STRING="$AZURE_STORAGE_CONNECTION_STRING" \
            --from-literal=AZURE_SAS_TOKEN="$AZURE_SAS_TOKEN" \
            --dry-run=client -o yaml | kubectl apply -f -
            
        echo "✅ Secret created successfully!"
        echo ""
        echo "Remember to update your Helm values to use this secret:"
        echo "extraEnvFrom: |"
        echo "  - secretRef:"
        echo "      name: airflow-variables"
        ;;
        
    2)
        echo ""
        echo "# Airflow CLI commands to set variables:"
        echo "# Run these from inside the scheduler pod or via kubectl exec"
        echo ""
        echo "airflow variables set INFLUXDB_TOKEN_SCH '$INFLUXDB_TOKEN_SCH'"
        echo "airflow variables set INFLUXDB_TOKEN_SV '$INFLUXDB_TOKEN_SV'"
        echo "airflow variables set IQS_DB_CLICKHOUSE '$IQS_DB_CLICKHOUSE'"
        echo "airflow variables set ALERTMAN_API_KEY_PROD '$ALERTMAN_API_KEY_PROD'"
        echo "airflow variables set ALERTMAN_API_KEY_DEV '$ALERTMAN_API_KEY_DEV'"
        echo "airflow variables set OAUTH_CLIENT_ID '$OAUTH_CLIENT_ID'"
        echo "airflow variables set OAUTH_CLIENT_SECRET '$OAUTH_CLIENT_SECRET'"
        echo "airflow variables set OAUTH_METADATA_URL '$OAUTH_METADATA_URL'"
        echo "airflow variables set AZURE_STORAGE_CONNECTION_STRING '$AZURE_STORAGE_CONNECTION_STRING'"
        echo "airflow variables set AZURE_SAS_TOKEN '$AZURE_SAS_TOKEN'"
        ;;
        
    3)
        echo ""
        echo "# kubectl exec commands to set variables:"
        echo "# Run these from your local machine with KUBECONFIG set"
        echo ""
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set INFLUXDB_TOKEN_SCH '$INFLUXDB_TOKEN_SCH'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set INFLUXDB_TOKEN_SV '$INFLUXDB_TOKEN_SV'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set IQS_DB_CLICKHOUSE '$IQS_DB_CLICKHOUSE'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set ALERTMAN_API_KEY_PROD '$ALERTMAN_API_KEY_PROD'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set ALERTMAN_API_KEY_DEV '$ALERTMAN_API_KEY_DEV'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set OAUTH_CLIENT_ID '$OAUTH_CLIENT_ID'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set OAUTH_CLIENT_SECRET '$OAUTH_CLIENT_SECRET'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set OAUTH_METADATA_URL '$OAUTH_METADATA_URL'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set AZURE_STORAGE_CONNECTION_STRING '$AZURE_STORAGE_CONNECTION_STRING'"
        echo "kubectl exec -n $NAMESPACE deployment/airflow-scheduler -- airflow variables set AZURE_SAS_TOKEN '$AZURE_SAS_TOKEN'"
        ;;
esac

echo ""
echo "Done! Variables are ready to be applied."