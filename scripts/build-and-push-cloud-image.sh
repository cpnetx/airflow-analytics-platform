#!/usr/bin/env bash
set -euo pipefail

# Configuration
ACR_NAME="${ACR_NAME:-}"
IMAGE_NAME="${IMAGE_NAME:-airflow-v3-custom}"
IMAGE_TAG="${IMAGE_TAG:-3.0.2}"
DOCKERFILE="Dockerfile.cloud"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Airflow custom image build and push process...${NC}"

# Validate ACR_NAME
if [ -z "$ACR_NAME" ]; then
    echo -e "${RED}ERROR: ACR_NAME environment variable is not set${NC}"
    echo "Usage: ACR_NAME=<your-acr-name> ./scripts/build-and-push-cloud-image.sh"
    echo "Example: ACR_NAME=cpnetacr ./scripts/build-and-push-cloud-image.sh"
    exit 1
fi

# Check if Dockerfile exists
if [ ! -f "$DOCKERFILE" ]; then
    echo -e "${RED}ERROR: $DOCKERFILE not found${NC}"
    exit 1
fi

# Login to Azure (if not already logged in)
echo -e "${YELLOW}Checking Azure login status...${NC}"
if ! az account show &>/dev/null; then
    echo -e "${YELLOW}Not logged in to Azure. Please login...${NC}"
    az login
fi

# Get ACR login server
ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.io"
FULL_IMAGE_NAME="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${IMAGE_TAG}"

echo -e "${GREEN}Building image: ${FULL_IMAGE_NAME}${NC}"

# Option 1: Build locally and push (for smaller images or local testing)
# docker build -f "$DOCKERFILE" -t "$FULL_IMAGE_NAME" .
# az acr login --name "$ACR_NAME"
# docker push "$FULL_IMAGE_NAME"

# Option 2: Build directly in ACR (recommended - no local Docker required)
echo -e "${YELLOW}Building image in Azure Container Registry...${NC}"
az acr build \
    --registry "$ACR_NAME" \
    --image "${IMAGE_NAME}:${IMAGE_TAG}" \
    --file "$DOCKERFILE" \
    .

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Image built and pushed successfully!${NC}"
    echo -e "${GREEN}Image: ${FULL_IMAGE_NAME}${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Update the Helm values file to use this image:"
    echo "   images:"
    echo "     airflow:"
    echo "       repository: ${ACR_LOGIN_SERVER}/${IMAGE_NAME}"
    echo "       tag: ${IMAGE_TAG}"
    echo "       pullPolicy: Always"
    echo ""
    echo "2. Redeploy Airflow:"
    echo "   cd /Users/bichengchen/codes/infra-one"
    echo "   export PG_PASS='<your-postgres-password>'"
    echo "   ./airflow/scripts/redeploy_airflow.sh"
else
    echo -e "${RED}❌ Image build failed!${NC}"
    exit 1
fi