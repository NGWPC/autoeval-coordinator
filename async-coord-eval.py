# draft of a coordinator script that initializes a single connection to the nomad event stream and then spins up the eval pipeline for as many pipelines as the user has submitted. Pipeline state is monitored by using callbacks and used to post jobs as other jobs upstream are finished. See design doc/readme for more explanation
import asyncio
from typing import Dict, Set, Callable, Awaitable, List, Optional
from enum import Enum
import uuid
import weakref

class PipelineState(Enum):
    INITIALIZING = "initializing"
    RUNNING_BATCH = "running_batch"
    RUNNING_SECONDARY = "running_secondary"
    COMPLETED = "completed"
    FAILED = "failed"

class JobConfig:
    def __init__(
        self,
        job_name: str,
        parameter_generator: Callable[[Dict, Optional[Dict]], Dict]
    ):
        self.job_name = job_name
        self.parameter_generator = parameter_generator

class NomadEventStream:
    def __init__(self):
        self._subscribers: Dict[str, Set[Callable]] = {}  # pipeline_id -> set of callbacks
        self._stream_task = None
        self._stream_lock = asyncio.Lock()
        self.connected = False
        self.last_event_time = None
        
    async def subscribe(self, pipeline_id: str, callback: Callable[[dict], Awaitable[None]]):
        """Subscribe a pipeline to the event stream"""
        if pipeline_id not in self._subscribers:
            self._subscribers[pipeline_id] = set()
        self._subscribers[pipeline_id].add(callback)
        
        async with self._stream_lock:
            if self._stream_task is None or self._stream_task.done():
                self._stream_task = asyncio.create_task(self._run_event_stream())

    async def unsubscribe(self, pipeline_id: str, callback: Callable):
        if pipeline_id in self._subscribers:
            self._subscribers[pipeline_id].discard(callback)
            if not self._subscribers[pipeline_id]:
                del self._subscribers[pipeline_id]

    async def _run_event_stream(self):
        retry_delay = 1
        max_retry_delay = 30
        
        while True:
            try:
                async with self._connect_to_nomad() as stream:
                    self.connected = True
                    retry_delay = 1
                    
                    async for event in stream:
                        self.last_event_time = asyncio.get_event_loop().time()
                        await self._dispatch_event(event)
                        
            except Exception as e:
                print(f"Event stream error: {e}")
                self.connected = False
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

    async def _dispatch_event(self, event: Dict):
        """Fan out events to relevant pipeline callbacks"""
        pipeline_id = event.get('Meta', {}).get('pipeline_id')
        if not pipeline_id:
            return
            
        if pipeline_id in self._subscribers:
            callbacks = self._subscribers[pipeline_id].copy()
            tasks = [callback(event) for callback in callbacks]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _connect_to_nomad(self):
        return await nomad_client.events.stream(
            type=['JobComplete'],
            timeout=300,
            heartbeat=60,
        )

class PolygonPipeline:
    def __init__(self, polygon_id: str, pipeline_id: str, polygon_data: Dict, job_configs: List[JobConfig]):
        self.polygon_id = polygon_id
        self.pipeline_id = pipeline_id
        self.instance_id = f"{pipeline_id}-{polygon_id}"
        self.polygon_data = polygon_data
        self.job_configs = job_configs
        self.state = PipelineState.INITIALIZING
        
        # Track job completion and results
        self.primary_jobs: Dict[str, Dict] = {}  # job_id -> metadata
        self.completed_primary_jobs: Set[str] = set()
        self.current_job_index = 0
        
        # Store results from each completed job
        self.job_results: Dict[str, List[Dict]] = {}
        
        # Store parameter sets from data service queries
        self.parameter_sets: List[Dict] = []

    async def initialize(self, data_service: DataService):
        """Query data services to determine parameter sets for this polygon"""
        self.parameter_sets = await data_service.query_for_polygon(self.polygon_data)
        print(f"Found {len(self.parameter_sets)} parameter sets for polygon {self.polygon_id}")
        return len(self.parameter_sets) > 0

    async def start(self, nomad_client):
        """Start the pipeline with first batch of jobs based on queried parameter sets"""
        self.state = PipelineState.RUNNING_BATCH
        first_job = self.job_configs[0]
        
        # Submit a job for each parameter set
        for i, params in enumerate(self.parameter_sets):
            job_id = f"{first_job.job_name}-{self.instance_id}-batch{i}"
            
            # Generate full parameters using the queried parameter set
            full_params = first_job.parameter_generator(self.polygon_data, params)
            full_params['batch_index'] = i
            full_params['batch_total'] = len(self.parameter_sets)
            
            # Submit job and store its ID
            await nomad_client.submit_job(
                job_id,
                full_params,
                meta={
                    'polygon_id': self.polygon_id,
                    'pipeline_id': self.pipeline_id,
                    'job_type': 'batch',
                    'batch_index': str(i),
                    'parameter_set': str(i)
                }
            )
            self.primary_jobs[job_id] = {'parameter_set': i}
        
        print(f"Started {len(self.parameter_sets)} jobs for polygon {self.polygon_id}")

    async def handle_event(self, event: Dict, nomad_client):
        """Handle job completion events and chain to next jobs"""
        if event['Type'] != 'JobComplete':
            return

        job_id = event['JobID']
        job_meta = event['Meta']
        job_result = event.get('Result', {})

        # Check if this is one of our jobs
        if not (job_meta.get('pipeline_id') == self.pipeline_id and 
                job_meta.get('polygon_id') == self.polygon_id):
            return

        if self.state == PipelineState.RUNNING_BATCH:
            if job_id in self.primary_jobs:
                print(f"Primary job {job_id} completed for polygon {self.polygon_id}")
                self.completed_primary_jobs.add(job_id)
                
                # Store the job's results with its parameter set index
                param_set_index = int(job_meta['parameter_set'])
                self.job_results.setdefault(self.job_configs[0].job_name, []).append({
                    'parameter_set': param_set_index,
                    'result': job_result,
                    'metadata': job_meta
                })

                # Check if all primary jobs are complete
                if self.completed_primary_jobs == set(self.primary_jobs.keys()):
                    print(f"All primary jobs completed for polygon {self.polygon_id}")
                    # Increment job index to move to first secondary job
                    self.current_job_index += 1
                    await self._start_next_job(nomad_client)

        elif self.state == PipelineState.RUNNING_SECONDARY:
            current_job = self.job_configs[self.current_job_index]
            expected_job_id = f"{current_job.job_name}-{self.instance_id}"

            if job_id == expected_job_id:
                # Store this job's results
                self.job_results[current_job.job_name] = [{
                    'result': job_result,
                    'metadata': job_meta
                }]

                # Move to next job
                self.current_job_index += 1
                if self.current_job_index < len(self.job_configs):
                    await self._start_next_job(nomad_client)
                else:
                    self.state = PipelineState.COMPLETED
                    print(f"Pipeline completed for polygon {self.polygon_id}")

    async def _start_next_job(self, nomad_client):
        """Start the next job in the pipeline using results from previous job"""
        next_job_config = self.job_configs[self.current_job_index]
        job_id = f"{next_job_config.job_name}-{self.instance_id}"

        # Get results from previous job
        prev_job_name = self.job_configs[self.current_job_index - 1].job_name
        prev_results = self.job_results.get(prev_job_name, [])

        # Generate parameters using previous results
        params = next_job_config.parameter_generator(
            polygon_data=self.polygon_data,
            prev_results=[r['result'] for r in prev_results],
            prev_metadata=[r['metadata'] for r in prev_results]
        )

        # Submit the job
        await nomad_client.submit_job(
            job_id,
            params,
            meta={
                'polygon_id': self.polygon_id,
                'pipeline_id': self.pipeline_id,
                'job_type': 'secondary',
                'job_index': str(self.current_job_index)
            }
        )

        self.state = PipelineState.RUNNING_SECONDARY
        print(f"Started {next_job_config.job_name} for polygon {self.polygon_id}")

class DataService:
    """Service to query data sources for parameter sets"""
    async def query_for_polygon(self, polygon_data: Dict) -> List[Dict]:
        """Query available data for a polygon and return parameter sets"""
        # This would be your actual implementation querying your data services
        # Example implementation:
        try:
            # Query your data services using polygon bbox or other properties
            available_data = await self._query_data_sources(polygon_data['bbox'])
            
            # Transform available data into parameter sets
            parameter_sets = []
            for data in available_data:
                parameter_sets.append({
                    'dataset_id': data['id'],
                    'temporal_range': data['timerange'],
                    'resolution': data['resolution'],
                    'data_path': data['path']
                })
            
            return parameter_sets
            
        except Exception as e:
            print(f"Error querying data for polygon {polygon_data['id']}: {e}")
            return []
            
    async def _query_data_sources(self, bbox: List[float]) -> List[Dict]:
        """Mock implementation of actual data source query"""
        # This would be replaced with your actual data service queries
        return [
            {
                'id': 'landsat_001',
                'timerange': ['2024-01-01', '2024-01-31'],
                'resolution': '10m',
                'path': '/data/landsat/001'
            },
            {
                'id': 'sentinel_001',
                'timerange': ['2024-01-01', '2024-01-31'],
                'resolution': '5m',
                'path': '/data/sentinel/001'
            }
        ]

class NomadClient:
    """Mock Nomad client implementation"""
    async def submit_job(self, job_id: str, params: Dict, meta: Dict) -> Dict:
        """Submit a job to Nomad"""
        print(f"Submitting job {job_id} with params: {params}")
        return {"ID": job_id}

    class events:
        @staticmethod
        async def stream(**kwargs):
            """Mock event stream implementation"""
            # In reality, this would connect to Nomad's event stream
            # For testing, you could yield mock events
            while True:
                await asyncio.sleep(5)  # Simulate event delay
                # Yield mock events...

class PipelineCoordinator:
    def __init__(self, nomad_client: NomadClient, data_service: DataService):
        self.nomad_client = nomad_client
        self.data_service = data_service
        self.event_stream = NomadEventStream()
        self.pipelines: Dict[str, PolygonPipeline] = {}
        
    async def start_pipeline(self, polygon_id: str, polygon_data: Dict, job_configs: List[JobConfig]):
        """Start a new pipeline instance for a polygon"""
        pipeline_id = str(uuid.uuid4())
        pipeline = PolygonPipeline(polygon_id, pipeline_id, polygon_data, job_configs)
        
        if not await pipeline.initialize(self.data_service):
            print(f"No parameter sets found for polygon {polygon_id}")
            return None
            
        self.pipelines[pipeline_id] = pipeline
        
        await self.event_stream.subscribe(
            pipeline_id,
            lambda event: pipeline.handle_event(event, self.nomad_client)
        )
        
        weakref.finalize(
            pipeline,
            lambda: asyncio.create_task(
                self.event_stream.unsubscribe(pipeline_id, pipeline.handle_event)
            )
        )
        
        await pipeline.start(self.nomad_client)
        return pipeline_id

    async def run_multipolygon_pipeline(self, multipolygon: List[Dict], job_configs: List[JobConfig]):
        """Start pipeline instances for all polygons"""
        pipeline_ids = []
        
        for polygon in multipolygon:
            pipeline_id = await self.start_pipeline(
                polygon['id'],
                polygon,
                job_configs
            )
            if pipeline_id:
                pipeline_ids.append(pipeline_id)
        
        return pipeline_ids

    def get_pipeline_status(self, pipeline_id: str) -> Optional[PipelineState]:
        """Get current status of a pipeline"""
        pipeline = self.pipelines.get(pipeline_id)
        return pipeline.state if pipeline else None

    async def wait_for_completion(self, pipeline_ids: List[str], timeout: float = None):
        """Wait for a set of pipelines to complete"""
        if not pipeline_ids:
            return
            
        async def wait_for_pipeline(pipeline_id: str):
            pipeline = self.pipelines.get(pipeline_id)
            if not pipeline:
                return
                
            while pipeline.state not in [PipelineState.COMPLETED, PipelineState.FAILED]:
                await asyncio.sleep(1)
            
            return pipeline.state
        
        tasks = [wait_for_pipeline(pid) for pid in pipeline_ids]
        return await asyncio.gather(*tasks, timeout=timeout)

def generate_primary_params(polygon_data: Dict, query_params: Dict) -> Dict:
    """Generate parameters for primary processing job"""
    return {
        "bbox": polygon_data['bbox'],
        "dataset_id": query_params['dataset_id'],
        "temporal_range": query_params['temporal_range'],
        "resolution": query_params['resolution'],
        "data_path": query_params['data_path'],
        "output_format": "geotiff"
    }

def generate_feature_params(polygon_data: Dict, prev_results: List[Dict], prev_metadata: List[Dict]) -> Dict:
    """Generate parameters for feature extraction job"""
    # Collect all output files from primary jobs
    processed_files = []
    for result, metadata in zip(prev_results, prev_metadata):
        processed_files.append({
            'file': result['output_file'],
            'dataset_id': metadata['dataset_id'],
            'temporal_range': metadata['temporal_range']
        })
    
    return {
        "input_files": processed_files,
        "bbox": polygon_data['bbox'],
        "feature_types": polygon_data.get('feature_types', ["water", "vegetation"]),
        "output_format": "geojson"
    }

def generate_aggregation_params(polygon_data: Dict, prev_results: List[Dict], prev_metadata: List[Dict]) -> Dict:
    """Generate parameters for final aggregation job"""
    return {
        "features": prev_results[0]['extracted_features'],
        "bbox": polygon_data['bbox'],
        "output_format": "geojson",
        "projection": polygon_data.get('projection', 'EPSG:4326')
    }

async def main():
    # Initialize services
    nomad_client = NomadClient()
    data_service = DataService()
    
    # Create coordinator
    coordinator = PipelineCoordinator(nomad_client, data_service)
    
    # Define job configurations
    job_configs = [
        JobConfig("data-processor", generate_primary_params),
        JobConfig("feature-extractor", generate_feature_params),
        JobConfig("result-aggregator", generate_aggregation_params)
    ]
    
    # Example multipolygon with metadata
    multipolygon = [
        {
            "id": "poly1",
            "bbox": [0, 0, 100, 100],
            "projection": "EPSG:4326",
            "feature_types": ["water", "vegetation", "urban"]
        },
        {
            "id": "poly2",
            "bbox": [100, 100, 200, 200],
            "projection": "EPSG:4326",
            "feature_types": ["water", "vegetation"]
        }
    ]
    
    try:
        # Start pipelines
        pipeline_ids = await coordinator.run_multipolygon_pipeline(multipolygon, job_configs)
        print(f"Started pipelines: {pipeline_ids}")
        
        # Wait for completion with timeout
        states = await coordinator.wait_for_completion(pipeline_ids, timeout=3600)
        
        # Check results
        for pipeline_id, state in zip(pipeline_ids, states):
            print(f"Pipeline {pipeline_id}: {state}")
            
    except Exception as e:
        print(f"Error running pipelines: {e}")
        raise
    finally:
        # Any cleanup if needed
        pass

if __name__ == "__main__":
    asyncio.run(main())
