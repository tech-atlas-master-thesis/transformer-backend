from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
)


class MainTransformer(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("getScraperResults")
        if not SCRAPER_DATA:
            raise FileNotFoundError("No scraper data found")
        yield "Data found"

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "transformMain"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Transform", "Transform")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return None
