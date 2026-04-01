from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)


class OrganisationExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("getScraperResults")
        if SCRAPER_DATA is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO
        yield SCRAPER_DATA, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "organisation_extract"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Extract Organisation Data", "Organisationen extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getScraperResults"]
