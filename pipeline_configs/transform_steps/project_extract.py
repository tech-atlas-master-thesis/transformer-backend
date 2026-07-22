import json
from typing import Optional, Union, List, Dict, Any

import pandas as pd

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.scraper import GetScraperResults


class ProjectExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        SCRAPER_DATA: pd.DataFrame = results.get("getScraperResults")
        if SCRAPER_DATA is None or not isinstance(SCRAPER_DATA, pd.DataFrame):
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        RELEVANT_COLUMNS = [
            "externalId",
            "uri",
            "short",
            "title",
            "abstract",
            "bidding",
            "programme",
            "start",
            "end",
            "status",
            "keywords",
            "keyTechnologies",
            "organisations",
            "data_source",
        ]

        for column in RELEVANT_COLUMNS:
            if column not in SCRAPER_DATA.columns:
                SCRAPER_DATA[column] = None

        projects = SCRAPER_DATA[RELEVANT_COLUMNS].to_dict("records")
        yield projects, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "project_extract"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Extract Project Data", "Projekt Daten Extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GetScraperResults.name()]
