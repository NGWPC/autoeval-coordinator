After the `nomad-server` is up and running. From the root of the repository execute following command to register jobs. This step is required only one time.

```sh
docker run --rm --name nomad-job-registrar \
  -v "$(pwd)/local-nomad/register-jobs.sh:/usr/local/bin/register-jobs.sh" \
  -v "$(pwd)/job_defs/local:/nomad/jobs" \
  -v "$(pwd):/repo:ro" \
  -e "NOMAD_ADDR=http://nomad-server:4646" \
  -e "REPO_ROOT=$(pwd)" \
  --network autoeval-coordinator_autoeval-net \
  --entrypoint /bin/sh \
  $(docker build -q -f ./local-nomad/Dockerfile ./local-nomad) \
  -c "/usr/local/bin/register-jobs.sh"
```