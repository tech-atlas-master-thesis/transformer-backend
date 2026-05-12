from typing import Optional, Union, List, Dict, Any

from bson import ObjectId

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipelineFramework.server.db.helper import get_fe_db_client


class TechnologiesStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        TECH_CONFIG = results.get("getTechnologyConfiguration")
        DATASET = results.get("create_dataset")
        if DATASET is None:
            raise FileNotFoundError("No dataset found")
        if TECH_CONFIG is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        field_db = get_fe_db_client().get_collection("fields")
        tech_db = get_fe_db_client().get_collection("technologies")

        field_ids = field_db.insert_many([{**item, "projects": 0, "dataset": DATASET} for item in TECH_CONFIG])

        techs = []
        for field_id, field in zip(field_ids.inserted_ids, TECH_CONFIG):
            for tech in field.get("technologies", []):
                techs.append(
                    {
                        **tech,
                        "projects": 0,
                        "field": field_id,
                        "dataset": DATASET,
                    }
                )
        tech_ids = tech_db.insert_many(techs)

        yield dict(
            zip((tech["label"] for tech in techs), zip(tech_ids.inserted_ids, [t["field"] for t in techs]))
        ), EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "technologies"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Save Technologies to Database", "Technologies in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getTechnologyConfiguration", "create_dataset"]
