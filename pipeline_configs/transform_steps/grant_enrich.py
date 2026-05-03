from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)


class GrantEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        GRANTS = results.get("grant_normalize")
        if GRANTS is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO
        yield GRANTS, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "grant_enrich"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Enrich Grant data", "Förderdaten anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["grant_normalize"]
