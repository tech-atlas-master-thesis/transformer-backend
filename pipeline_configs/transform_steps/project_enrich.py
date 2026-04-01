from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)


class ProjectEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("project_normalize")
        if SCRAPER_DATA is None:
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO
        yield SCRAPER_DATA, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "project_enrich"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Transform", "Transform")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_normalize"]
