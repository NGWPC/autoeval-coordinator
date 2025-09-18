This document contains instructions for running a batch of autoeval pipelines in Parallel Works on a single instance using a local Nomad cluster.

# Running a pipeline batch in Parallel Works

## Start the `fimsinglenode` cluster and attach a desktop

Eval pipeline batches are not designed to be run on a Parallel Works cluster being used by other workloads. If another user is running a large workload on `fimsinglenode` it is recommended to create a clone of the cluster on which to execute the batch.

## Start a terminal and navigate to the repo root

Unless otherwise noted all the commands below should be run from the root of the Parallel Works clone of the `autoeval-coordinator` repostitory. Currently this clone is located at: `/efs/demonstrations/pi7/autoeval-coordinator` when using the `fimsinglenode` cluster.

## Export your NOMAD_ADDR and AWS credentials

These environment variables are used by the batch submission code to determine which Nomad API to send requests to and authenticate to the S3 bucket that outputs will be written to.

The NOMAD_ADDR can be set with:

```
export NOMAD_ADDR="http://localhost:4646"
```

The domain is localhost since we are using a local Nomad cluster.

The AWS creds for the NGWPC Data account also need to be exported. They can be obtained from the NGWPC AWS Access Portal. When you sign into that portal then if you have a role with access to the Data account then the appropriate credentials can be copied from the popup that shows up when you click "Access Keys". The credentials should look something like:

```
export AWS_ACCESS_KEY_ID="<access_key_id_string>"
export AWS_SECRET_ACCESS_KEY="<secret_access_key_string"
export AWS_SESSION_TOKEN="<token_string>"
```

These variables will be read from your environment when you start the docker container that will execute and inspect the batch runs. The AWS credentials expire every 8 hrs so should be refreshed if you resubmit a batch after that time period has elapsed.

## Configure Nomad Job definitions

Most of the environment variables in ./job_defs/test/ should already be configured but if you are using a different NOMAD_ADDRESS from the one used by NGWPC then you should set that as well. Depending on the data being evaluated you might also want to adjust the job memory requirements in the "resources" block of the job definition. Please refer to the document docs/job_sizing_guide.md in this repo for guidance on how much memory to allocate to each autoeval-job based on the resolution of the data being evaluated.

## Submit a batch of pipelines

A batch of pipelines can be submitted using this command after it has been modified for the specifics of your batch:

```
docker run --rm \
  --entrypoint="" \
  -v $(pwd)/src:/app/src \
  -v $(pwd)/inputs:/app/inputs \
  -v $(pwd)/tools:/app/tools \
  -v $(pwd)/data:/data \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  --network host \
  autoeval-coordinator:local \
  python tools/submit_stac_batch.py \
   --batch_name pw_test_run \
   --output_root s3://fimc-data/autoeval/batches/trials/pw_test_run \
   --hand_index_path s3://fimc-data/autoeval/hand_output_indices/fim100_huc12_10m_index/ \
   --benchmark_sources "nws-fim-collection" \
   --item_list ./inputs/test-run.txt \
   --wait_seconds 10 \
   --stop_threshold 2 \
   --resume_threshold 0 \
   --use-local-creds \
   --stac_api_url http://localhost:8082/
```

In this command the script submit_stac_batch.py is submitted to the autoeval-coordinator:local instance through a docker run command. The docker run command contains all the arguments needed to configure the container invocation so that the container can successfully run a batch of pipelines. The arguments for submit_stac_batch.py are:

* --batch_name: This is the name of the batch that will be included in the nomad job definitions. It is usually timestamped to the hour to make it possible to query different batches in CloudWatch.
* --output_root: This is the directory that the batch outputs will be written to.
* --hand_index_path: This is the directory that contains the HAND index that will be used to assemble the necessary HAND outputs to run each pipeline.
* --benchmark_sources: This is the list of benchmark STAC collections that you want to be evaluated
* --item_list: This is a list of the specific STAC item id's that will be evaluated. There will be a pipeline job submitted for each item on this list.
* --wait_seconds: This is the number of seconds to wait before submitting each pipeline job to the Nomad API. This should be 10 or more seconds to avoid hammering the Nomad API.
* --stop_threshold: This is the maximum number of running pipelines that should be running on Nomad at once. Tests revealed that this shouldn't be more than 30 pipelines in parallel or else the server will be overwhelmed. Once this threshold is reached `submit_stac_batch.py` will pause pipeline job submission and wait until the resume_threshold is reached.
* --resume_threshold: This is the threshold at which pipelines will start being submitted again by `submit_stac_batch.py`. The resume threshold should be 10-15 pipeline jobs below the stop_threshold. Having two thresholds introduces a pause that ensures that each pipeline job is able to submit jobs at the inundate, mosaic, and agreement stages without getting crowded out by other newer jobs. This pause was necessary because of how Nomad's job scheduling works. If it didn't exist then the Nomad scheduler would tend to preferentially place jobs with the lower resource requirements. This results in pipelines being hung up for unreasonable lengths of time and increases the risk of pipeline failure.

## Evaluate the batch outcome

After a batch has run then the script tools/local_reports.py should be run from the same shell that you ran tools/submit_stac_batch.py from in the preceding step.

```
docker run --rm \
  --entrypoint="" \
  -v $(pwd)/tools:/app/tools \
  -v $(pwd)/inputs:/app/inputs \
  -v $(pwd)/local-logs:/app/local-logs \
  -v $(pwd)/local-reports:/app/local-reports \
  autoeval-coordinator:local \
  python tools/local_reports.py \
   --run_list inputs/test-run.txt \
   --log_dir local-logs/pw_test_run \
   --report_dir local-reports/pw_test_run
```

The first argument is the list of STAC item id's that were submitted by submit_stac_batch.py. The second argument is the batch name used by submit_stac_batch.py. The 3rd argument is where the report files will be written to.

The file unique_fail_aoi_names.txt in the results directory has the list of failed pipeline aoi's. Usually these pipelines failed because of Nomad api, S3, or credential errors and will succeed if the failed aoi's are resubmitted. To resubmit all you have to do is copy the contents of unique_fail_aoi_names.txt to a batch item list file and then start another batch using that item list as an input to submit_stac_batch.py

Refer to docs/intepreting-reports.md for more information on using a batch's reports to inspect a batch's outcome 

## Purge jobs

After you have assessed the outcome of a batch then it can be helpful to remove the jobs from the current batch in the Nomad API so that another batch can be run without the output from the previous batches jobs confusing the status of the next batch. Old jobs also increase the amount of memory being used by Nomad. Old Jobs should automatically be removed by the API after approx. 1 hr. Manual removal of the previously run jobs from the API can also be done by purging the jobs with the following command:

```
docker run --rm \
  --network host \
  --entrypoint="" \
  -v $(pwd)/tools:/app/tools \
  -e NOMAD_ADDR \
  autoeval-coordinator:local \
  python /app/tools/purge_dispatch_jobs.py
```

## Troubleshooting

If a docker run command fails then it can be helpful to substitute a bash command for the command that is failing in an interactive terminal Docker session and then run the command that is failing from the terminal. For example, to troubleshoot the reporting command you can change:

```
docker run --rm \
  --entrypoint="" \
  -v $(pwd)/tools:/app/tools \
  -v $(pwd)/inputs:/app/inputs \
  -v $(pwd)/local-logs:/app/local-logs \
  -v $(pwd)/local-reports:/app/local-reports \
  autoeval-coordinator:local \
  python tools/local_reports.py \
   --run_list inputs/test-run.txt \
   --log_dir local-logs/pw_test_run \
   --report_dir local-reports/pw_test_run
```

to:

```
docker run --rm -it \
  --entrypoint="" \
  -v $(pwd)/tools:/app/tools \
  -v $(pwd)/inputs:/app/inputs \
  -v $(pwd)/local-logs:/app/local-logs \
  -v $(pwd)/local-reports:/app/local-reports \
  autoeval-coordinator:local \
  bash
```

And then run:

```
python tools/local_reports.py \
 --run_list inputs/test-run.txt \
 --log_dir local-logs/pw_test_run \
 --report_dir local-reports/pw_test_run
```

from within the interactive terminal. Besides being able to run the command multiple times after editing the script you can also use ipdb and any other debugging tools.
