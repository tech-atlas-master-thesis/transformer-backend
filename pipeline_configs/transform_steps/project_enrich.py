import json
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
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS = results.get("project_normalize")
        ORGANISATION_MAPPING: Dict[str, str] = results.get("organisation_database")
        # TODO: add grants
        # GRANT_MAPPING: Dict[str, str] = results.get("grant_database")
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        for project in PROJECTS:
            orgs = json.loads(project["organisations"])
            project["organisations"] = [ORGANISATION_MAPPING.get(org["organisationName"]) for org in orgs]
            project_leaders = [
                ORGANISATION_MAPPING.get(org["organisationName"]) for org in orgs if org["role_in_project"]
            ]
            project["projectLeader"] = (
                None
                if len(project_leaders) == 0
                else project_leaders[0] if len(project_leaders) == 1 else project_leaders
            )
            project["keywords"] = project["keywords"].split(", ")

        yield PROJECTS, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "project_enrich"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Enrich Project Data", "Projekt Daten Anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_normalize", "organisation_database", "grant_database"]
