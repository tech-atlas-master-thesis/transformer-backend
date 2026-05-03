import json

import pandas as pd
import pytest

from pipelineFramework import EventType
from pipeline_configs.transform_steps.organisations_enrich import OrganisationEnrichStep
from pipeline_configs.transform_steps.organisations_extract import OrganisationExtractStep
from pipeline_configs.transform_steps.organisations_normalize import OrganisationNormalizeStep

SCRAPER_DATA = pd.read_csv("./tests/resources/scraper_main_FFG.csv")


@pytest.mark.asyncio
async def test_steps_full_data():
    results = {"getScraperResults": SCRAPER_DATA}

    extract_step = OrganisationExtractStep()
    normalize_step = OrganisationNormalizeStep()
    enrich_step = OrganisationEnrichStep()

    i = 0
    async for event, event_type in extract_step.run({}, results):
        match i:
            case 0:
                assert event_type == EventType.INFO
                assert event == "Extracted 141 unique organisations"
            case 1:
                with open("./tests/results/organisation_extract_step_full_data.json", "r") as f:
                    result = json.load(f)
                    assert event_type == EventType.RESULT
                    assert event == result
                results["organisation_extract"] = event
        i += 1

    i = 0
    async for event, event_type in normalize_step.run({}, results):
        match i:
            case 0:
                assert event_type == EventType.INFO
                assert event == "Data found"
            case 1:
                with open("./tests/results/organisation_normalize_step_full_data.json", "r") as f:
                    result = json.load(f)
                    assert event_type == EventType.RESULT
                    assert event == result
                results["organisation_normalize"] = event
        i += 1

    i = 0
    async for event, event_type in enrich_step.run({}, results):
        match i:
            case 0:
                assert event_type == EventType.INFO
                assert event == "Data found"
            case 1:
                with open("./tests/results/organisation_enrich_step_full_data.json", "r") as f:
                    result = json.load(f)
                    assert event_type == EventType.RESULT
                    assert event == result
                results["organisation_enrich"] = event
        i += 1
