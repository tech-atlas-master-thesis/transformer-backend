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
from pipelineFramework.server.db.helper import get_fe_db_client


class ProjectDatabaseStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None):
        if results is None:
            results = {}
        PROJECTS_DATA: pd.DataFrame = results.get("project_enrich")
        if PROJECTS_DATA is None or not isinstance(PROJECTS_DATA, pd.DataFrame):
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        project_db = get_fe_db_client().get_collection("projects")
        items = json.loads(PROJECTS_DATA.to_json(orient="records"))

        project_db.insert_many(items)

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "project_database"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Save Organisation Data to Database", "Organisationen Daten in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_enrich", "organisation_enrich", "grant_enrich"]
