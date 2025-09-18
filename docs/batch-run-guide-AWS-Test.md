This document contains instructions for running a batch of autoeval pipelines in the AWS test account.

The test account instructions assume that you are interacting with a Nomad API that is set up in a way similar to the [nomad-runner](https://github.com/NGWPC/nomad-runner) deployment. This deployment uses a single Nomad server that sends jobs to a group of EC2 instances in an ASG group.

# Running a pipeline batch in the AWS test account

## Size the cluster

The ripple batch runs were executed using a c5.9xlarge server and between 10-40 r5a.xlarge clients. The more clients you are communicating with and the larger the instance size of the clients the larger your server instance needs to be. At these instance sizes for the pipeline batch jobs being run the maximum number of instances that could effectively be communicated with by the server was ~40. Currently the pipeline code is not designed to work with a Nomad API that autoscales clients because autoscaling causes jobs to be cancelled or lost and then rescheduled with a new dispatched job id. A recovery mechanism has not been implemented to deal with this event though it is planned. Because of this the number of clients needs to be set in the AWS autoscaling group at the start of a batch run by setting the "desired capacity" of the autoscaling group at the beginning of a run. For this approach to work the autoscaling job also needs to be turned off before the autoscaler capacity is set. A good rule of thumb for choosing the number of clients/desired capacity is to set the number of clients to half the number of pipelines that you want to run. If that number is higher than the max number of clients supported by the Nomad API then use the max number of clients.

Eval pipeline batches are not designed to be run on a Nomad cluster being used by other workloads. In the future, once the pipeline has implemented more robust job tracking and with a more robust Nomad API it could be possible for a batch to be run alongside other workloads. 

## Export your NOMAD_TOKEN and NOMAD_ADDR 

These environment variables are used by the batch submission code to determine which Nomad API to send requests to and authenticate to that API. They can be set with:

```
export NOMAD_ADDR="http://localhost:4646"
export NOMAD_TOKEN="token"
```

These variables will be read from your environment when you start the autoeval container.

## Refresh your S3 credentials

The batch run script needs access to the S3 bucket the pipelines will output data to. This is because it uploads AOI that are used by the pipeline to S3. You should refresh your credentials either in the environment that you will start your autoeval container from.

## Start the autoeval container

Start the autoeval container by running the following from the repo root:

```
docker compose -f docker-compose-dev.yml up -d
docker compose -f docker-compose-dev.yml exec autoeval-dev bash
```

You should execute the batch code from this container's shell.

## Configure Nomad Job definitions

Most of the environment variables in ./job_defs/test/ should already be configured but if you are using a different NOMAD_ADDRESS from the one used by NGWPC then you should set that as well. Depending on the data being evaluated you might also want to adjust the job memory requirements in the "resources" block of the job definition. Please refer to the document docs/job_sizing_guide.md in this repo for guidance on how much memory to allocate to each autoeval-job based on the resolution of the data being evaluated.

## Start the nomad memory monitor script

Open another autoeval-dev container shell that is different from the one you will use to run a batch of pipelines using another `docker compose exec autoeval-dev bash` command and then from the local repo root start the script that monitors the Nomad servers memory usage with `tools/nomad_memory_monitor.sh`. This should be started in a separate terminal from the one you will use to submit the batch. This terminal also needs to have valid NOMAD_ADDR and NOMAD_TOKEN environment variables. The memory monitor script will create a log file at `nomad_memory_usage.log` and will run the command `nomad system gc` after the nomad servers active memory allocation exceeds the value for `MEMORY_THRESHOLD_GIB` hardcoded at the top of the script. `MEMORY_THRESHOLD_GIB` value should be set to about 25-30% of your Nomad server's max memory.

Memory monitoring is necessary because after running jobs Nomad keeps old allocations and evaluations in memory for a configurable amount of time. If memory use gets too high then the server slows down and becomes unresponsive. The memory can be cleared on a set schedule by configuring the server (for example: every 15 minutes) but it was observed that the API could lose jobs during garbage collection events. So to minimize the number of garbage collection events while also ensuring that the server stayed responsive a dynamic approach was taken that monitors the memory usage of the server from the client side and clears the 

To run this script you need the Nomad cli installed. Instructions for installing the CLI [can be found here](https://developer.hashicorp.com/nomad/tutorials/get-started/gs-install).

## Submit a batch of pipelines

A batch of pipelines can be submitted using this command after it has been modified for the specifics of your batch:

```
 python tools/submit_stac_batch.py --batch_name fim100_huc12_3m_2025-08-21-15 --output_root s3://fimc-data/autoeval/batches/fim100_huc12_3m_non_calibrated/ --hand_index_path s3://fimc-data/autoeval/hand_output_indices/fim100_huc12_3m_index/ --benchmark_sources "ripple-fim-collection" --item_list /home/dylan.lee/autoeval-coordinator/inputs/ripple-fim-collection-3m-run4.txt --wait_seconds 10 --stop_threshold 30 --resume_threshold 15
```

The arguments are:

* --batch_name: This is the name of the batch that will be included in the nomad job definitions. It is usually timestamped to the hour to make it possible to query different batches in CloudWatch.
* --output_root: This is the directory that the batch outputs will be written to.
* --hand_index_path: This is the directory that contains the HAND index that will be used to assemble the necessary HAND outputs to run each pipeline.
* --benchmark_sources: This is the list of benchmark STAC collections that you want to be evaluated
* --item_list: This is a list of the specific STAC item id's that will be evaluated. There will be a pipeline job submitted for each item on this list.
* --wait_seconds: This is the number of seconds to wait before submitting each pipeline job to the Nomad API. This should be 10 or more seconds to avoid hammering the Nomad API.
* --stop_threshold: This is the maximum number of running pipelines that should be running on Nomad at once. Tests revealed that this shouldn't be more than 30 pipelines in parallel or else the server will be overwhelmed. Once this threshold is reached `submit_stac_batch.py` will pause pipeline job submission and wait until the resume_threshold is reached.
* --resume_threshold: This is the threshold at which pipelines will start being submitted again by `submit_stac_batch.py`. The resume threshold should be 10-15 pipeline jobs below the stop_threshold. Having two thresholds introduces a pause that ensures that each pipeline job is able to submit jobs at the inundate, mosaic, and agreement stages without getting crowded out by other newer jobs. This pause was necessary because of how Nomad's job scheduling works. If it didn't exist then the Nomad scheduler would tend to preferentially place jobs with the lower resource requirements. This results in pipelines being hung up for unreasonable lengths of time and increases the risk of pipeline failure.

## Evaluate the batch outcome

After a batch has run then the script tools/cloudwatch_reports.py should be run from the autoeval-dev shell that you ran tools/submit_stac_batch.py from.

The Test account that stores the cloudwatch logs currently needs different credentials from that used by the S3 bucket. You can update the credentials by exporting new 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', and 'AWS_SESSION_TOKEN' variables that work for the Test account cloudwatch service and then run the script using a modified form of:

```
./tools/cloudwatch_reports.py inputs/ripple-fim-collection-item-list.txt fim100_huc12_10m_2025-08-20-09  reports/ripple-10m-run3
```

The first argument is the list of STAC item id's that were submitted by submit_stac_batch.py. The second argument is the batch name used by submit_stac_batch.py. The 3rd argument is where the report files will be written to.

The file unique_fail_aoi_names.txt in the results directory has the list of failed pipeline aoi's. Usually these pipelines failed because of Nomad api, S3, or credential errors and will succeed if the failed aoi's are resubmitted. To resubmit all you have to do is copy the contents of unique_fail_aoi_names.txt to a batch item list file and then start another batch using that item list as an input to submit_stac_batch.py

Refer to docs/intepreting-reports.md for more information on using a batch's reports to inspect a batch's outcome 

## Shutdown the memory monitor and purge jobs

Kill the nomad_memory_monitor.sh script and then from that container run:

```
nomad system gc
python tools/purge_dispatch_jobs.py
```

This will clear all the jobs associated with the batch from the Nomad servers memory. This step ensures that the Nomad server stays responsive and makes it easier to use the Nomad UI to monitor the progress of the next batch that will be run.

If this is the last batch you will run you can now kill the instance of the autoeval-dev shell that was being used to monitor memory.

## Set the ASG to 1 client and turn autoscaler job back on

After you have successfully run your batches you should then set the desired capacity of the ASG back to 1 client to save on costs. The autoscaler job should also be turned back on in case the next user has a workload for which it is useful.
