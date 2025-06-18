#!/usr/bin/env bash
set -euo pipefail

# This script prepares DAGs for cloud deployment by:
# 1. Creating a clean copy without sensitive files
# 2. Updating configurations to use Airflow Variables

echo "Preparing DAGs for cloud deployment..."

# Create a temporary staging directory
STAGING_DIR="./dags-staging"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"

# Copy DAGs excluding sensitive files
echo "Copying DAG files..."
cp -r dags/* "$STAGING_DIR/" 2>/dev/null || true

# Remove sensitive files
echo "Removing sensitive configuration files..."
rm -f "$STAGING_DIR/wpp/globals.yml"

# Create a placeholder file explaining the configuration
cat > "$STAGING_DIR/wpp/README.md" << 'EOF'
# Configuration Notice

The `globals.yml` file has been removed from this deployment for security reasons.

All configuration values are now managed through Airflow Variables:

## Required Airflow Variables

Set these in the Airflow UI (Admin -> Variables):

- `INFLUXDB_TOKEN_SCH` - InfluxDB token for SCH site
- `INFLUXDB_TOKEN_SV` - InfluxDB token for SV site  
- `IQS_DB_CLICKHOUSE` - ClickHouse connection URL
- `ALERTMAN_API_KEY_PROD` - AlertManager API key for production
- `ALERTMAN_API_KEY_DEV` - AlertManager API key for development
- `OAUTH_CLIENT_ID` - OAuth client ID for email
- `OAUTH_CLIENT_SECRET` - OAuth client secret
- `OAUTH_METADATA_URL` - OAuth metadata URL
- `AZURE_STORAGE_CONNECTION_STRING` - Azure storage connection
- `AZURE_SAS_TOKEN` - Azure SAS token for report uploads

The DAGs are configured to fall back to Airflow Variables when globals.yml is not found.
EOF

echo "✅ DAGs prepared for cloud deployment in: $STAGING_DIR"
echo ""
echo "Note: Remember to set all required Airflow Variables in the cloud environment!"