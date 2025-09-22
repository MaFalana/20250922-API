# Deploying to Vercel

## Prerequisites
1. Install Vercel CLI: `npm install -g vercel`
2. Create a Vercel account at https://vercel.com

## Setup Environment Variables
Before deploying, you need to add your environment variables to Vercel:

1. Go to your Vercel dashboard
2. Select your project (or create it during first deploy)
3. Go to Settings → Environment Variables
4. Add these variables:
   - `MONGO_CONNECTION_STRING`
   - `MONGO_DATABASE_NAME`
   - `AZURE_STORAGE_CONNECTION_STRING`
   - `AZURE_STORAGE_CONTAINER`
   - `MAP_TILER_KEY`
   - `API_VERSION`

## Deploy Steps

1. From the project root directory, run:
   ```bash
   vercel
   ```

2. Follow the prompts:
   - Link to existing project or create new
   - Confirm project settings

3. For production deployment:
   ```bash
   vercel --prod
   ```

## Important Notes

- Vercel has a 10-second timeout for serverless functions (60 seconds on Pro plan)
- Your photo processing might need adjustment for serverless environment
- Background tasks won't work as expected in serverless - consider using Vercel Cron Jobs
- File uploads are limited to 4.5MB on free plan

## Testing Your Deployment
Once deployed, test your API endpoints:
- `https://your-app.vercel.app/`
- `https://your-app.vercel.app/health`
- `https://your-app.vercel.app/docs`