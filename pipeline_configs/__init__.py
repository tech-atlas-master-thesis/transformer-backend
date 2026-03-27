from typing import List

from pipelineFramework import PipelineConfig
from pipeline_configs.test import TEST_PIPELINE
from pipeline_configs.transform_main import TRANSFORMER_PIPELINE

PIPELINE_CONFIGS: List[PipelineConfig] = [TEST_PIPELINE, TRANSFORMER_PIPELINE]
