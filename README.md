## Eval Coordinator
### Description  
The coordinator is the entry point to the evaluation pipeline. It takes a gpkg containing either a polygon or multipolygon geometry and then uses that to run and monitor batch jobs for each step along the evaluation pipeline for all the polygons submitted by the user. 

The current evaluation pipeline is primarily designed to generate HAND FIM extents or depths and then evaluate these against relevant benchmark sources.

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
docker exec nomad-server nomad job dispatch -meta="aoi=/testdata/query-polygon.gpkg" -meta="outputs_path=/outputs/test-run" -meta="hand_index_path=/testdata/hand/parquet-index/" -meta="benchmark_sources=usgs-fim-collection" -meta tags="batch_name=HAND-V2 aoi_name=myaoi another_tag=dff" pipeline
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
- **Date Range** 
  - Certain Benchmark sources contain flood scenarios that have a time component to them. For example high water mark data is associated with the flood  event associated with a given survey. This argument allows for filtering a Benchmark source to only return benchmark data within a certain date range.
 
### Inputs
- **AOI**
  - This input is a geopackage that must contain either a polygon or multipolygon geometry. For every polygon the coordinator will generate a HAND extent and find benchmark data that lies within the polygon for the source selected by the user. The coordinator will then run all the rest of the jobs described in this repository to generate an evaluation for that polygon. 
