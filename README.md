## Coordinator (`coordinator`)  
### Description  
The coordinator is the entry point to the evaluation pipeline. It takes a gpkg containing either a polygon or multipolygon geometry and then uses that to run and monitor batch jobs for each step along the evaluation pipeline for all the polygons submitted by the user. 

The current evaluation pipeline is primarily designed to generate HAND FIM extents or depths and then evaluate these against relevant benchmark sources.

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
