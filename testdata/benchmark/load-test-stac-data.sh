#!/bin/bash

# Script to load the test STAC collection and item data into the STAC API
# Run this script after starting the local services with docker-compose-local.yml

set -e

STAC_API_URL="http://localhost:8888"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Loading STAC collection..."
if curl -X POST "${STAC_API_URL}/collections" \
  -H 'Content-Type: application/json' \
  -d @"${SCRIPT_DIR}/collection.json" \
  -f -v; then
  echo "Collection loaded successfully"
else
  if [ $? -eq 22 ]; then
    echo "Collection already exists (409 Conflict) - skipping"
  else
    echo "Failed to load collection"
    exit 1
  fi
fi

echo "Loading STAC item..."
if curl -X POST "${STAC_API_URL}/collections/usgs-fim-collection/items" \
  -H 'Content-Type: application/json' \
  -d @"${SCRIPT_DIR}/01080203-shvm3-usgs.json" \
  -f -v; then
  echo "Item loaded successfully"
else
  if [ $? -eq 22 ]; then
    echo "Item already exists (409 Conflict) - skipping"
  else
    echo "Failed to load item"
    exit 1
  fi
fi

echo "Data loading complete"
