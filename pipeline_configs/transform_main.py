from pipelineFramework import LocalisationString, PipelineConfig
from pipeline_configs.dummy_step_factory import get_dummy_step
from pipelineFramework.server.common_steps.get_results_from_latest_pipeline import GetResultFromLatestPipeline
from pipeline_configs.transform_steps.main_step import MainTransformer

TRANSFORMER_PIPELINE = PipelineConfig(
    type="transform_main",
    display_name=LocalisationString("Transformer Pipeline", "Transformer Pipeline"),
    steps=[
        GetResultFromLatestPipeline(
            "getScraperResults",
            LocalisationString("Get results from Scraper Pipeline", "Get results from Scraper Pipeline"),
            None,
            "scraper_main",
            "getDataFFG",
        ),
        MainTransformer(),
        get_dummy_step(
            "getDataFWF",
            LocalisationString("[DUMMY_STEP] Get Data from FWF", "[DUMMY_STEP] Daten von FWF laden"),
        ),
    ],
    parallelize=True,
)
