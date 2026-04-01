from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)


class OrganisationEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("organisation_normalize")
        if SCRAPER_DATA is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO
        yield SCRAPER_DATA, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "organisation_enrich"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Enrich Organisation Data", "Organisationen mit Daten anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["organisation_normalize"]
