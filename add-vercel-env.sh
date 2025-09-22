#!/bin/bash

# Read .env file and add each variable to Vercel
while IFS='=' read -r key value; do
  # Skip empty lines and comments
  if [[ -n "$key" && ! "$key" =~ ^# ]]; then
    # Remove quotes from value if present
    value="${value%\"}"
    value="${value#\"}"
    echo "Adding $key to Vercel..."
    echo "$value" | vercel env add "$key" production
  fi
done < api/.env

echo "All environment variables added!"