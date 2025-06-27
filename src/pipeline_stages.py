import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from load_config import AppConfig
from pipeline_utils import PathFactory, PipelineResult

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    """Abstract base class for pipeline stages."""

    def __init__(self, config: AppConfig, nomad_service, data_service, path_factory: PathFactory, pipeline_id: str):
        self.config = config
        self.nomad = nomad_service
        self.data_svc = data_service
        self.path_factory = path_factory
        self.pipeline_id = pipeline_id

    @abstractmethod
    async def run(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Run this stage and return updated results."""
        pass

    @abstractmethod
    def filter_inputs(self, results: List[PipelineResult]) -> List[PipelineResult]:
        """Filter which results can be processed by this stage."""
        pass

    def log_stage_start(self, stage_name: str, input_count: int):
        """Log the start of a stage."""
        logger.debug(f"[{self.pipeline_id}] {stage_name}: Starting with {input_count} inputs")

    def log_stage_complete(self, stage_name: str, success_count: int, total_count: int):
        """Log the completion of a stage."""
        logger.debug(f"[{self.pipeline_id}] {stage_name}: {success_count}/{total_count} succeeded")

    def _get_base_meta_kwargs(self) -> Dict[str, str]:
        """Get common metadata kwargs for all job types."""
        return {
            "fim_type": self.config.defaults.fim_type,
            "registry_token": self.config.nomad.registry_token or "",
            "aws_access_key": self.config.s3.AWS_ACCESS_KEY_ID or "",
            "aws_secret_key": self.config.s3.AWS_SECRET_ACCESS_KEY or "",
            "aws_session_token": self.config.s3.AWS_SESSION_TOKEN or "",
        }