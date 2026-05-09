import datetime
import json
import re
from dataclasses import dataclass
from typing import Optional, List, Any, Annotated

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Depends, Query
from starlette.responses import Response

from datasets.dto import DatasetDto
from pipelineFramework import (
    PaginatedListDto,
    PageDto,
    get_fe_db_client,
    require_all_entitlements,
    Lookup,
    custom_json_encoder,
)

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")


@dataclass
class DataSetObject:
    collection: str
    search_fields: List[str]
    included_fields: List[Lookup]


PROJECTS_DATA = DataSetObject(
    "projects",
    ["short", "title", "abstract"],
    [
        # Lookup("keywords", "keywords", "keywords", "keywords"),
        # Lookup("keyTechnologies", "keyTechnologies", "keyTechnologies", "keyTechnologies"),
        Lookup("organisations", "organisations", "_id", "organisations"),
        Lookup("organisations", "projectLeader", "_id", "projectLeader"),
    ],
)

ORGANISATIONS_DATA = DataSetObject(
    "organisations",
    ["name", "type", "website"],
    [],
)

GRANTS_DATA = []


def _serialize_object_ids(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_serialize_object_ids(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _serialize_object_ids(value) for key, value in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj


def _get_data_set_object(
    collection: str,
    search_fields: List[str],
    included_fields: List[Lookup],
    dataset_id: str,
    search: Optional[str] = None,
    include_data: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> PaginatedListDto[Any]:
    dataset_db = get_fe_db_client().get_collection(collection)
    query = {}
    aggregation = [{"$match": {"dataset": ObjectId(dataset_id)}}]
    query["dataset"] = ObjectId(dataset_id)
    if search:
        query["$or"] = [{field: {"$regex": re.escape(search), "$options": "$i"}} for field in search_fields]
        aggregation.append(
            {"$or": [{field: {"$regex": re.escape(search), "$options": "$i"}} for field in search_fields]}
        )
    if include_data:
        aggregation += [lookup.serialize() for lookup in included_fields]
    if sort:
        single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
        sort_query = {field: int(order) for field, order in single_sorts}
    else:
        sort_query = {"_id": -1}
    aggregation.append({"$sort": sort_query})
    dataset_items = dataset_db.aggregate(aggregation)
    total_records = dataset_db.count_documents(query)
    return PaginatedListDto(
        [_serialize_object_ids(dataset_item) for dataset_item in dataset_items],
        PageDto(offset, limit, total_records),
    )


def _get_data_set_export(
    collection: str,
    search_fields: List[str],
    included_fields: List[Lookup],
    dataset_id: str,
    search: Optional[str] = None,
    include_data: Optional[bool] = None,
) -> Response:
    dataset_db = get_fe_db_client().get_collection(collection)
    aggregation = [{"$match": {"dataset": ObjectId(dataset_id)}}]
    if search:
        aggregation.append(
            {"$or": [{field: {"$regex": re.escape(search), "$options": "$i"}} for field in search_fields]}
        )
    if include_data:
        aggregation += [lookup.serialize() for lookup in included_fields]
    dataset_items = dataset_db.aggregate(aggregation)
    response = Response(
        json.dumps([*dataset_items], default=custom_json_encoder),
        media_type="text/json",
    )
    response.headers["Content-Disposition"] = (
        f"inline; filename=DataSetExport_{dataset_id}_{collection}_{search if search else 'full'}_{'incl' if include_data else 'ref'}_{datetime.datetime.now(datetime.UTC).isoformat()}.json"
    )
    return response


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
            PROJECTS_DATA.collection,
            PROJECTS_DATA.search_fields,
            PROJECTS_DATA.included_fields,
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )

    @app.get(api_base_url + "/datasets/{dataset_id}/projects/export")
    async def get_project_export(
        dataset_id: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> Response:
        return _get_data_set_export(
            PROJECTS_DATA.collection,
            PROJECTS_DATA.search_fields,
            PROJECTS_DATA.included_fields,
            dataset_id,
            search,
            includeData,
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
            ORGANISATIONS_DATA.collection,
            ORGANISATIONS_DATA.search_fields,
            ORGANISATIONS_DATA.included_fields,
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )

    @app.get(api_base_url + "/datasets/{dataset_id}/organizations/export")
    async def get_organization_export(
        dataset_id: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> Response:
        return _get_data_set_export(
            ORGANISATIONS_DATA.collection,
            ORGANISATIONS_DATA.search_fields,
            ORGANISATIONS_DATA.included_fields,
            dataset_id,
            search,
            includeData,
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
            [],
            [],
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )

    @app.get(api_base_url + "/datasets/{dataset_id}/grants/export")
    async def get_grant_export(
        dataset_id: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> Response:
        return _get_data_set_export(
            "grants",
            [],
            [],
            dataset_id,
            search,
            includeData,
        )
