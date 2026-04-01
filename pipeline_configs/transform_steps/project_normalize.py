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


class ProjectNormalizeStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None):
        if results is None:
            results = {}
        SCRAPER_DATA: pd.DataFrame = results.get("getScraperResults")
        if SCRAPER_DATA is None or not isinstance(SCRAPER_DATA, pd.DataFrame):
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        print(SCRAPER_DATA.columns)
        projects = SCRAPER_DATA[
            [
                "id",
                "short_title",
                "long_title",
                "project_start",
                "project_end",
                "status",
                "keywords",
                "found_keyword",
                "abstract",
            ]
        ]
        yield projects, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "project_normalize"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Transform", "Transform")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getScraperResults"]
