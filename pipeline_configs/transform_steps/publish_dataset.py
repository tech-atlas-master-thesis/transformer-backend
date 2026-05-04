import datetime
from typing import Union, List

from bson import ObjectId

from pipelineFramework import (
    StepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    Pipeline,
)
from pipelineFramework.server.dto import AuditInfoDto, UserDto
from pipelineFramework.server.db.helper import get_fe_db_client


class PublishDataSetStep(StepConfig):
    async def run(self, pipeline: Pipeline, results, **_):
        datasets = get_fe_db_client().get_collection("datasets")
        DATASET_ID: ObjectId = results.get("create_dataset")
        yield f"Publishing DataSet with ID {DATASET_ID}", EventType.INFO
        dataset = datasets.insert_one(
            {
                "_id": DATASET_ID,
                "pipelineType": pipeline.type,
                "pipeline": pipeline.id,
                "pipelineName": pipeline.name,
                "created": AuditInfoDto(pipeline.created.by, datetime.datetime.now(datetime.UTC)).serialize(),
            }
        )
        yield f"DataSet {DATASET_ID} created", EventType.INFO
        yield dataset.inserted_id, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "publish_dataset"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Publish DataSet", "Datenset veröffentlichen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["create_dataset", "project_database", "organisation_database", "grant_database"]
