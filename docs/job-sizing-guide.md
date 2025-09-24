The resource requirements of each Nomad job in the autoeval pipeline can vary according to the size and resolution of the AOIs being evaluated. The main resource that should be adjusted to efficiently use resources in the `memory` field in the `resources` block of the Nomad job definitions in `job_defs/` since this controls how many jobs at a given stage can be run at once.

Below are guidelines for each job for running FIM100 domain evaluations at different resolutions. These guidelines reflect doing evaluations over the FIM100 domain using AOIs associated with STAC items in the FIM benchmark STAC. If larger AOIs or a different domain is used then it will likely be necessary to play with the resources requested for each job to find a good fit for the evaluations being performed.

| Dataset | Inundate Job | Mosaic Job | Agreement Job |
|---------|--------------|------------|---------------|
| 10 meter FIM100 | 2 GB | 10 GB | 12 GB |
| 5 meter FIM100 | 2 GB | 12 GB | 20 GB |
| 3 meter FIM100 | 2 GB | 16 GB | 30 GB |

*Note: The pipeline job memory requirement is constant across all the resolutions. Experience has shown that 3 GB is enough memory for the pipeline job to avoid OOM errors.*
