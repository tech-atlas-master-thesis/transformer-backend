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
        # TODO: add grants
        # GRANT_MAPPING: Dict[str, str] = results.get("grant_database")
        yield "Data found", EventType.INFO

        self.add_technology_ids(PROJECTS, TECHNOLOGIES)
        self.add_organisations(PROJECTS, ORGANISATION_MAPPING)
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
        fields_count = defaultdict(lambda: 0)
        tech_count = defaultdict(lambda: 0)
        for project in projects:
            tech_ids = []
            for tech in json.loads(project["keyTechnologies"]):
                tech_id, field_id = technologies[tech]
                fields_count[field_id] += 1
                tech_count[tech_id] += 1
                tech_ids.append(tech_id)
            project["keyTechnologies"] = tech_ids
        self.save_counts_to_database(tech_count, fields_count)

    def save_counts_to_database(self, tech_count: Dict[str, int], field_count: Dict[str, int]) -> None:
        fields_db = get_fe_db_client().get_collection("fields")
        techs_db = get_fe_db_client().get_collection("technologies")

        for tech_id, count in field_count.items():
            fields_db.update_one({"_id": tech_id}, {"$set": {"projects": count}})
        for tech_id, count in tech_count.items():
            techs_db.update_one({"_id": tech_id}, {"$set": {"projects": count}})

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "project_normalize"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Normalize Project Data", "Projekt Daten Normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_extract", "organisation_database", "grant_database"]
