import datetime
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    Pipeline,
)
from pipelineFramework.server.api.dto import AuditInfoDto, UserDto
from pipelineFramework.server.db.helper import get_fe_db_client


class CreateDataSetStep(StepConfig):
    async def run(self, pipeline: Pipeline, **_):
        datasets = get_fe_db_client().get_collection("datasets")
        dataset = datasets.insert_one(
            {
                "pipelineType": pipeline.type,
                "pipeline": pipeline.id,
                "pipelineName": pipeline.name,
                "created": AuditInfoDto(
                    UserDto(123, "User", "user@email.com"), datetime.datetime.now(datetime.UTC)
                ).serialize(),
            }
        )
        yield f"DataSet {dataset.inserted_id} created", EventType.INFO
        yield dataset.inserted_id, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "create_dataset"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Create DataSet", "Datenset erstellen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return []
