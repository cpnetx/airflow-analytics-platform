# Cloud Deployment Guide for Airflow DAGs

This guide walks through deploying your DAGs to the cloud Airflow instance on AKS.

## Prerequisites

1. Azure CLI installed and logged in
2. Access to the Azure Container Registry (ACR)
3. Access to the AKS cluster
4. PostgreSQL password from infrastructure deployment

## Step 1: Prepare DAGs for Cloud

Remove sensitive files from DAGs:

```bash
cd /Users/bichengchen/airflow3-local
./scripts/prepare-dags-for-cloud.sh
```

## Step 2: Build and Push Custom Image

```bash
# Set your ACR name
export ACR_NAME="<your-acr-name>"  # e.g., cpnetacr

# Build and push image
./scripts/build-and-push-cloud-image.sh
```

## Step 3: Update Helm Values

1. Copy the custom values file to the infra-one repo:
   ```bash
   cp cloud-deployment/airflow-values-custom-image.yaml \
      /Users/bichengchen/codes/infra-one/airflow/helm/
   ```

2. Edit the file to replace `<ACR_NAME>` with your actual ACR name

3. Merge with existing values or use as override:
   ```bash
   # Option A: Use as additional values file
   helm upgrade ... -f airflow-values.yaml -f airflow-values-custom-image.yaml
   
   # Option B: Update the main values file directly
   ```

## Step 4: Create Airflow Variables Secret

```bash
export KUBECONFIG=/Users/bichengchen/codes/infra-one/infra-cpnet-aks/scripts/kubeconfig

# Create secret with all variables
kubectl create secret generic airflow-variables -n airflow \
  --from-literal=INFLUXDB_TOKEN_SCH="<token>" \
  --from-literal=INFLUXDB_TOKEN_SV="<token>" \
  --from-literal=IQS_DB_CLICKHOUSE="clickhouse://user:pass@host:port/db" \
  --from-literal=OAUTH_CLIENT_ID="<client-id>" \
  --from-literal=OAUTH_CLIENT_SECRET="<secret>" \
  --from-literal=AZURE_STORAGE_CONNECTION_STRING="<connection-string>" \
  --from-literal=AZURE_SAS_TOKEN="<sas-token>" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Step 5: Deploy Airflow

```bash
cd /Users/bichengchen/codes/infra-one
export PG_PASS="<your-postgres-password>"

# If using custom values as override
./airflow/scripts/deploy_airflow.sh

# Or modify the script to include both values files
```

## Step 6: Set Airflow Variables via UI

Alternative to Step 4 - Set variables through Airflow UI:

1. Port-forward to access UI:
   ```bash
   kubectl port-forward -n airflow svc/airflow-api-server 8080:8080
   ```

2. Access http://localhost:8080

3. Navigate to Admin → Variables

4. Add each variable:
   - INFLUXDB_TOKEN_SCH
   - INFLUXDB_TOKEN_SV
   - IQS_DB_CLICKHOUSE
   - ALERTMAN_API_KEY_PROD
   - ALERTMAN_API_KEY_DEV
   - OAUTH_CLIENT_ID
   - OAUTH_CLIENT_SECRET
   - OAUTH_METADATA_URL
   - AZURE_STORAGE_CONNECTION_STRING
   - AZURE_SAS_TOKEN

## Step 7: Verify Deployment

1. Check pods are running:
   ```bash
   kubectl get pods -n airflow
   ```

2. Check DAGs are loaded:
   ```bash
   kubectl exec -n airflow deployment/airflow-scheduler -- airflow dags list
   ```

3. Check for import errors:
   ```bash
   kubectl logs -n airflow deployment/airflow-scheduler | grep -i error
   ```

## Updating DAGs

When you need to update DAGs:

1. Make changes in your local repository
2. Run `./scripts/prepare-dags-for-cloud.sh`
3. Build new image: `./scripts/build-and-push-cloud-image.sh`
4. Update image tag in Helm values if changed
5. Redeploy: `./airflow/scripts/redeploy_airflow.sh`

## CI/CD Pipeline (Future Enhancement)

Consider setting up a GitHub Actions workflow:

```yaml
name: Deploy DAGs
on:
  push:
    branches: [main]
    paths:
      - 'dags/**'
      - 'requirements.txt'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}
      - name: Build and push
        run: |
          ./scripts/prepare-dags-for-cloud.sh
          ACR_NAME=${{ secrets.ACR_NAME }} ./scripts/build-and-push-cloud-image.sh
      - name: Deploy to AKS
        run: |
          # Add kubectl setup and helm deployment
```

## Troubleshooting

### DAGs not showing up
- Check scheduler logs: `kubectl logs -n airflow deployment/airflow-scheduler`
- Exec into pod: `kubectl exec -it -n airflow deployment/airflow-scheduler -- bash`
- List DAGs: `airflow dags list`

### Import errors
- Check if all requirements are in the image
- Verify Python version compatibility
- Check for missing Airflow Variables

### Connection issues
- Verify ClickHouse/InfluxDB endpoints are accessible from AKS
- Check network policies and firewall rules
- Ensure credentials in Airflow Variables are correct