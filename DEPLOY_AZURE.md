
# Deploying to Azure App Service

## Prerequisites
- Azure CLI installed: `brew install azure-cli` (macOS) or download from https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
- Azure account with active subscription

## Method 1: Deploy via Azure CLI

### 1. Login to Azure
```bash
az login
```

### 2. Create Resource Group (if needed)
```bash
az group create --name photo-log-map-rg --location eastus
```

### 3. Create App Service Plan
```bash
az appservice plan create \
  --name photo-log-map-plan \
  --resource-group photo-log-map-rg \
  --sku B1 \
  --is-linux
```

### 4. Create Web App
```bash
az webapp create \
  --resource-group photo-log-map-rg \
  --plan photo-log-map-plan \
  --name photo-log-map-api \
  --runtime "PYTHON:3.9"
```

### 5. Configure Environment Variables
```bash
az webapp config appsettings set \
  --name photo-log-map-api \
  --resource-group photo-log-map-rg \
  --settings \
  MONGO_CONNECTION_STRING="<YOUR_MONGO_CONNECTION_STRING>" \
  MONGO_DATABASE_NAME="photo_mapper" \
  AZURE_STORAGE_CONNECTION_STRING="<YOUR_AZURE_STORAGE_CONNECTION_STRING>" \
  AZURE_STORAGE_CONTAINER="photo-log-map" \
  MAP_TILER_KEY="<YOUR_MAP_TILER_KEY>" \
  API_VERSION="1.0.0"
```

**Note**: Replace the placeholders with your actual values from the `.env` file.

### 6. Configure Startup Command
```bash
az webapp config set \
  --name photo-log-map-api \
  --resource-group photo-log-map-rg \
  --startup-file "python -m uvicorn api.main:app --host 0.0.0.0 --port 8000"
```

### 7. Deploy from GitHub
```bash
az webapp deployment source config \
  --name photo-log-map-api \
  --resource-group photo-log-map-rg \
  --repo-url https://github.com/MaFalana/20250922-API \
  --branch main \
  --manual-integration
```

## Method 2: Deploy via GitHub Actions

See `.github/workflows/azure-deploy.yml` for automated deployment on push.

## Testing Your Deployment

Once deployed, your API will be available at:
- https://photo-log-map-api.azurewebsites.net/
- https://photo-log-map-api.azurewebsites.net/docs (Swagger UI)
- https://photo-log-map-api.azurewebsites.net/health

## Monitoring

View logs:
```bash
az webapp log tail \
  --name photo-log-map-api \
  --resource-group photo-log-map-rg
```

Enable application insights:
```bash
az monitor app-insights component create \
  --app photo-log-map-insights \
  --location eastus \
  --resource-group photo-log-map-rg
```

## Scaling

To scale up/down:
```bash
# Change plan
az appservice plan update \
  --name photo-log-map-plan \
  --resource-group photo-log-map-rg \
  --sku S1  # or P1V2 for production
```