#!/bin/bash
docker compose -f docker-compose-local.yml down --remove-orphans nomad-server
rm -rf .data
mkdir -p ./.data/nomad/data
docker compose -f docker-compose-local.yml up --remove-orphans nomad-server -d
docker run --rm --name nomad-job-registrar   -v "$(pwd)/local-nomad/register-jobs.sh:/usr/local/bin/register-jobs.sh"   -v "$(pwd)/job_defs/local:/nomad/jobs"   -v "$(pwd):/repo:ro"   -e "NOMAD_ADDR=http://nomad-server:4646"   -e "REPO_ROOT=$(pwd)"   --network autoeval-coordinator_autoeval-net   --entrypoint /bin/sh   $(docker build -q -f ./local-nomad/Dockerfile ./local-nomad)   -c "/usr/local/bin/register-jobs.sh"
