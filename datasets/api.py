import re
from dataclasses import dataclass
from typing import Optional, List, Any, Annotated

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Depends, Query

from datasets.dto import DatasetDto
from pipelineFramework import PaginatedListDto, PageDto, get_fe_db_client, require_all_entitlements

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")


@dataclass
class _DataSetObject:
    name: str
    collection: str
    search_fields: List[str]
    included_fields: List[str]


def _serialize_object_ids(obj: Any) -> Any:
    if isinstance(obj, list):
        for item in obj:
            _serialize_object_ids(item)
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, ObjectId):
                obj[key] = str(value)
            elif isinstance(value, list):
                _serialize_object_ids(value)
            elif isinstance(value, dict):
                _serialize_object_ids(value)


def _get_data_set_object(
    collection: str,
    search_fields: List[str],
    included_fields: List[str],
    dataset_id: str,
    search: Optional[str] = None,
    includeData: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> PaginatedListDto[Any]:
    dataset_db = get_fe_db_client().get_collection(collection)
    query = {
        "dataset": ObjectId(dataset_id),
    }
    # TODO: implement search
    # TODO: implement includeData
    if sort:
        single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
        sort_query = {field: int(order) for field, order in single_sorts}
    else:
        sort_query = {"_id": -1}
    dataset_items = dataset_db.find(query).sort(sort_query).skip(offset).limit(limit)
    total_records = dataset_db.count_documents(query)
    return PaginatedListDto(
        [_serialize_object_ids(dataset_item) for dataset_item in dataset_items],
        PageDto(offset, limit, total_records),
    )


def add_dataset_endpoints(app: FastAPI, api_base_url: str) -> None:
    @app.get(api_base_url + "/datasets")
    async def get_datasets(
        pipelineType: Annotated[Optional[List[str]], Query()] = None,
        pipelineName: Annotated[Optional[str], Query()] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ):
        dataset_db = get_fe_db_client().get_collection("datasets")
        query = {}
        if pipelineType:
            query["pipelineType"] = {"$in": pipelineType}
        if pipelineName:
            query["pipelineName"] = {"$regex": re.escape(pipelineName)}
        if sort:
            single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
            sort_query = {field: int(order) for field, order in single_sorts}
        else:
            sort_query = {"_id": -1}
        datasets = dataset_db.find(query).sort(sort_query).skip(offset).limit(limit)
        total_records = dataset_db.count_documents(query)
        return PaginatedListDto(
            [DatasetDto.from_entity(pipeline) for pipeline in datasets], PageDto(offset, limit, total_records)
        )

    @app.get(api_base_url + "/datasets/{dataset_id}")
    async def get_dataset(
        dataset_id: str,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ):
        dataset_db = get_fe_db_client().get_collection("datasets")
        dataset = dataset_db.find_one({"_id": ObjectId(dataset_id)})

        if not dataset:
            raise HTTPException(status_code=404, detail=f"Pipeline '{dataset_id}' not found")
        return DatasetDto.from_entity(dataset)

    @app.get(api_base_url + "/datasets/{dataset_id}/projects")
    async def get_projects(
        dataset_id: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        return _get_data_set_object(
            "projects",
            ["short", "title", "abstract"],
            ["keywords", "keyTechnologies", "organisations", "projectLeader"],
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )

    @app.get(api_base_url + "/datasets/{dataset_id}/organizations")
    async def get_organizations(
        dataset_id: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        return _get_data_set_object(
            "organisations",
            ["short", "title", "abstract"],
            ["keywords", "keyTechnologies", "organisations", "projectLeader"],
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )

    @app.get(api_base_url + "/datasets/{dataset_id}/grants")
    async def get_grants(
        dataset_id: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        return _get_data_set_object(
            "grants",
            ["short", "title", "abstract"],
            [],
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )
