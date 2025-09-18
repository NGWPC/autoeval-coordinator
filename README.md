## Autoeval Coordinator
### Description  
This repository contains an evaluation pipeline that works with HashiCorp Nomad to evaluate HAND generated extent flood inundation maps (FIMs) against benchmark FIMs. It takes a gpkg containing either a polygon or multipolygon geometry and then uses that to run and monitor batch jobs for each step along a FIM evaluation pipeline that evaluates flood scenarios for benchmark sources that intersect the AOI submitted by the user. 

The repository also contains a directory `tools/`, that assists the user in running batches of evaluation pipelines, evaluating the results of a batch, and working with the Nomad API.

While the current evaluation pipeline is primarily designed to generate HAND FIM extents or depths and then evaluate these against relevant benchmark sources it is possible that more pipelines will be added in the future to allow for evaluations of more types of FIMs or for different types of FIM evaluations.

### Getting Started Locally
1. Create `.env` file
2. Run `mkdir -p ./.data/nomad/data`
3. Run `docker compose -f docker-compose-local.yml up`
4. Register Jobs (see ./local-nomad/README.md)
5. Load the test stac data by running `./testdata/benchmark/load-test-stac-data.sh`
6. Create required container images from autoeval-jobs repo. Once cloned the autoeval-jobs repo and inside it, execute `docker build -f Dockerfile.gval -t autoeval-jobs-gval:local . && docker build -t autoeval-jobs:local .`
7. Build the container image inside this repo with `docker build -t autoeval-coordinator:local .`
8. Dispatch a pipeline job through Nomad UI or API (see example below)

**Tips for working with .env files:**
- The example.env is a good place to look to make a .env file that is configured for local deployment. This .env file can be stored as .env.local when not in use and copied to .env when local deployment is desired. Depending on which deployment configuration is desired different .env files can be saved locally within the repo without being tracked by git. For example, you could also have a .env.test environment file for deploying to the AWS Test account. 

**Example command for dispatching job in the local environment:**

Once you have gone through the steps above to spin up a local environment you can run a pipeline job with the test data using the following command:

```
docker exec nomad-server nomad job dispatch -meta="aoi=/testdata/query-polygon.gpkg" -meta="outputs_path=/outputs/test-run" -meta="hand_index_path=/testdata/hand/parquet-index/" -meta="benchmark_sources=usgs-fim-collection" -meta="tags=batch_name=HAND-V2 aoi_name=myaoi another_tag=dff" pipeline
```

If you prefer to use curl from your host machine then the request to post the job using the Nomad API is:

```
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "Meta": {
      "aoi": "/testdata/query-polygon.gpkg",
      "outputs_path": "/outputs/test-run",
      "hand_index_path": "/testdata/hand/parquet-index/",
      "benchmark_sources": "usgs-fim-collection",
      "tags": "batch_name=HAND-V2 aoi_name=myaoi another_tag=dff"
    },
    "IdPrefixTemplate": "[batch_name=HAND-V2,aoi_name=myaoi,another_tag=dff]"
  }' \
  http://localhost:4646/v1/job/pipeline/dispatch
```

This version can also be adapted to dispatch jobs to non-local Nomad servers.

### Arguments
- **HAND Version** 
  - The HAND version argument allows the user to specify a specific version of HAND to generate extents for. This argument is required.
- **Benchmark Source** 
  - This is a string that select which source will be used to evaluate HAND against. For example 'ripple-mip' will be used to select FEMA MIP data produced by ripple. This argument is required.
- **hand_index_path**
  - This argument provides the location the HAND index used to spatially query a given set of HAND outputs. The NGWPC hand-index repo contains more information about generating a HAND index for use in an evaluation.
- **Date Range** 
  - Certain Benchmark sources contain flood scenarios that have a time component to them. For example high water mark data is associated with the flood  event associated with a given survey. This argument allows for filtering a Benchmark source to only return benchmark data within a certain date range.
 
### Inputs
- **AOI**
  - This input is a geopackage that must contain either a polygon or multipolygon geometry. For every polygon the coordinator will generate a HAND extent and find benchmark data that lies within the polygon for the source selected by the user. The coordinator will then run all the rest of the jobs described in this repository to generate an evaluation for that polygon. 

### Outputs
- **output_path**
  This is the directory where the outputs of a pipeline will be written. The outputs written to this directory follow this format (here <test-case-id> is synonymous with <aoi-id>:

  - <test-case-id>/: the unique identifier, or test case, for the category of benchmark data used to generate metrics. For the PI7 ripple eval data this corresponds to a STAC item id in a given benchmark STAC collection. This could also be an ID for an AOI that returns multiple STAC items from the Benchmark STAC when used as a query AOI. In the example output the <test-case-id> is the STAC item id “11090202-ble” from the “ble-collection" benchmark stac collection.  
  - <test-case-id>__agg_metrics.csv: aggregated metrics for the test case. 
  - <test-case-id>__logs.txt: test case logs generated by the Pipeline 
  - <test-case-id>__results.json: A file containing metadata and references to written output file locations 
  - catchment_data_indices/: This directory contains files that point to catchment HAND data for each HAND catchment that will be inundated to compare to the benchmark scenarios being evaluated against 
    - catchment-<unique-catchment-id>.parquet: Files in this directory will be parquet files that contain the UUID assigned to that catchment in the HAND index 
  - <benchmark_type>/: This directory name is a shortened reference to the Benchmark STAC collection that the benchmark data for this evaluation was queried from. It is possible for a single AOI or test case to be evaluated against multiple benchmark collections so in some cases there could be multiple directories of this type with each directory containing evaluation results for that benchmark collections 
  - <scenario>/: Test case scenario, e.g., “ble-100yr”  
    - <test_case_id>-<scenario>__agreement.tif: The agreement raster for this scenario 
    - <test_case_id>-<scenario>__benchmark_mosiac.tif: The mosaiced benchmark raster used as the benchmark raster 
    - <test_case_id>-<scenario>__flowfile.csv: The merged flowfile used for this scenario 
    - <test_case_id>-<scenario>__inundate_mosiac.tif: The mosaiced HAND extent used as the candidate raster for this scenario 
    - <test_case_id>-<scenario>__metrics.csv: A single row CSV containing the metrics for this scenario. These CSV’s are aggregated together along with additional metadata to create the test cases agg_metrics.csv file 
    - catchment_extents/ 
      - <test_case_id>__<unique-catchment-id>.tif: The HAND extents for a single HAND catchment. These are merged together to form the inundate_mosaic.tif for the scenario 


### Running a batch of pipelines

The above instructions are for running a single test evaluation pipeline using a local nomad cluster. If you know which HAND outputs you want to evaluate and where its HAND index is located and you have access to the FIM Benchmark STAC this should be sufficient to run single pipelines. This repository also contains functionality for running batches of dozens to thousands of pipelines using either a local Nomad cluster running within the Parallel Works environment or a Nomad cluster deployed to the NGWPC AWS Test account. For more information on running batches please refer to `docs/batch-run-guide-ParallelWorks.md` and `docs/batch-run-guide-AWS-Test.md`.
