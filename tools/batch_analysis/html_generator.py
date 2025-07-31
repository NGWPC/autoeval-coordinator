"""HTML dashboard generation for batch analysis results."""

import logging
from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .models import FailedJobInfo

logger = logging.getLogger(__name__)


class HTMLGenerator:
    """Generates HTML dashboard reports from analysis results."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up Jinja2 environment
        template_dir = Path(__file__).parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def generate_dashboard(
        self,
        analysis_results: Dict[str, Any],
        failed_jobs: List[FailedJobInfo],
        unhandled_exceptions: List[FailedJobInfo],
        missing_metrics: List[Dict] = None,
        empty_metrics: List[Dict] = None,
        missing_agg_metrics: List[Dict] = None,
        missing_aois: List[str] = None,
    ) -> str:
        """Generate HTML dashboard from analysis results."""
        
        template = self.env.get_template('dashboard.html')
        
        # Prepare template context
        context = {
            'batch_name': analysis_results.get('batch_name', 'Unknown'),
            'analysis_timestamp': analysis_results.get('analysis_timestamp', 'Unknown'),
            'time_range_days': analysis_results.get('time_range_days', 'Unknown'),
            'submitted_pipelines_count': analysis_results.get('submitted_pipelines_count', 0),
            'failed_jobs_count': analysis_results.get('failed_jobs_count', 0),
            'unhandled_exceptions_count': analysis_results.get('unhandled_exceptions_count', 0),
            'failed_jobs': failed_jobs,
            'unhandled_exceptions': unhandled_exceptions,
            'reports_generated': analysis_results.get('reports_generated', []),
        }
        
        # Add metrics data if available
        if missing_metrics is not None:
            context.update({
                'missing_metrics_count': analysis_results.get('missing_metrics_count', 0),
                'empty_metrics_count': analysis_results.get('empty_metrics_count', 0),
                'missing_agg_metrics_count': analysis_results.get('missing_agg_metrics_count', 0),
                'missing_metrics': missing_metrics or [],
                'empty_metrics': empty_metrics or [],
                'missing_agg_metrics': missing_agg_metrics or [],
            })
        
        # Add missing AOIs data if available
        if missing_aois is not None:
            context.update({
                'missing_aois_count': analysis_results.get('missing_aois_count', 0),
                'missing_aois': missing_aois or [],
            })
        
        # Render template
        html_content = template.render(**context)
        
        # Write to file
        output_file = self.output_dir / "batch_analysis_dashboard.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated HTML dashboard: {output_file}")
        return str(output_file)