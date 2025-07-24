#!/bin/bash

set -e

# Get the repo root directory
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "Refreshing local Nomad server..."

echo "Stopping nomad-server container..."
docker compose -f docker-compose-local.yml down --remove-orphans nomad-server

echo "Cleaning up old data..."
rm -rf "$REPO_ROOT/.data"
mkdir -p "$REPO_ROOT/.data/nomad/data"

echo "Starting nomad-server container..."
docker compose -f docker-compose-local.yml up --remove-orphans nomad-server -d

echo "Waiting for Nomad server to be ready..."
sleep 3

echo "Registering jobs..."
docker run --rm --name nomad-job-registrar \
  -v "$REPO_ROOT/local-nomad/register-jobs.sh:/usr/local/bin/register-jobs.sh" \
  -v "$REPO_ROOT/job_defs/local:/nomad/jobs" \
  -v "$REPO_ROOT:/repo:ro" \
  -e "NOMAD_ADDR=http://nomad-server:4646" \
  -e "REPO_ROOT=$REPO_ROOT" \
  --network autoeval-coordinator_autoeval-net \
  --entrypoint /bin/sh \
  $(docker build -q -f "$REPO_ROOT/local-nomad/Dockerfile" "$REPO_ROOT/local-nomad") \
  -c "/usr/local/bin/register-jobs.sh"

echo "Local Nomad server refreshed successfully!"
echo "Access the UI at: http://localhost:4646"