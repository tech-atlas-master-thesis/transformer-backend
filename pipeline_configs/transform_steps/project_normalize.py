import json
from collections import defaultdict
from typing import Optional, Union, List, Dict, Any

import pandas as pd

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    get_fe_db_client,
)


class ProjectNormalizeStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get("project_extract")
        TECHNOLOGIES: Dict[str, List[str, str]] = results.get("technologies")
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        ORGANISATION_MAPPING: Dict[str, str] = results.get("organisation_database")
        GRANT_MAPPING: Dict[str, str] = results.get("grant_database")
        yield "Data found", EventType.INFO

        self.add_technology_ids(PROJECTS, TECHNOLOGIES)
        self.add_organisations(PROJECTS, ORGANISATION_MAPPING)
        self.add_grants(PROJECTS, GRANT_MAPPING)
        self.parse_keywords(PROJECTS)

        yield PROJECTS, EventType.RESULT

    def parse_keywords(self, projects: List[Dict[str, Any]]) -> None:
        for project in projects:
            project["keywords"] = project["keywords"].split(", ")

    def add_organisations(self, projects: List[Dict[str, Any]], organisation_mapping: Dict[str, str]) -> None:
        for project in projects:
            orgs = json.loads(project["organisations"])
            project["organisations"] = [organisation_mapping.get(org["organisationName"]) for org in orgs]
            project_leaders = [
                organisation_mapping.get(org["organisationName"])
                for org in orgs
                if org["role_in_project"] in ["Konsortialführer", "Einzelantragsteller"]
            ]
            project["projectLeader"] = (
                None
                if len(project_leaders) == 0
                else project_leaders[0] if len(project_leaders) == 1 else project_leaders
            )

    def add_technology_ids(self, projects: List[Dict[str, Any]], technologies: Dict[str, List[str, str]]) -> None:
        for project in projects:
            tech_ids = []
            for tech in json.loads(project["keyTechnologies"]):
                tech_id = technologies[tech]
                tech_ids.append(tech_id)
            project["keyTechnologies"] = tech_ids

    def add_grants(self, projects: List[Dict[str, Any]], grants: Dict[str, str]) -> None:
        for project in projects:
            grant = grants.get(project["bidding"])
            project["grant"] = grant
            del project["bidding"]
            del project["programme"]

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "project_normalize"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Normalize Project Data", "Projekt Daten Normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_extract", "organisation_database", "grant_database"]
