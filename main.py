import logging

from dotenv import load_dotenv
from fastapi import FastAPI

from pipelineFramework import PipelineServer, add_common_api_calls
from pipeline_configs import PIPELINE_CONFIGS

logging.basicConfig(level=logging.DEBUG)
load_dotenv()

API_BASE_URL = "/api/transformer"

app = FastAPI(
    openapi_url=API_BASE_URL + "/openapi.json", docs_url=API_BASE_URL + "/docs", redoc_url=API_BASE_URL + "/redoc"
)
pipeline_server: PipelineServer = PipelineServer()
add_common_api_calls(app, pipeline_server, PIPELINE_CONFIGS, API_BASE_URL)
