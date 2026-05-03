import json
import math
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)


def get_non_null_value(value: float | str):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


class OrganisationExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("getScraperResults")
        if SCRAPER_DATA is None:
            raise FileNotFoundError("No organisation data found")
        organisations = []
        for collection in SCRAPER_DATA["organisations"]:
            for organisation in json.loads(collection):
                if any(organisation["organisationName"] == org["name"] for org in organisations):
                    continue
                organisations.append(
                    {
                        "name": get_non_null_value(organisation["organisationName"]),
                        "type": get_non_null_value(organisation["organisationType"]),
                        "website": get_non_null_value(organisation["organisationWebsite"]),
                        "address": {
                            "country": get_non_null_value(organisation["organisationCountry"]),
                            "state": get_non_null_value(organisation["organisationState"]),
                            "city": get_non_null_value(organisation["organisationCity"]),
                            "street": get_non_null_value(organisation["organisationStreet"]),
                        },
                    }
                )
        yield f"Extracted {len(organisations)} unique organisations", EventType.INFO
        yield organisations, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "organisation_extract"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Extract Organisation Data", "Organisationen extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getScraperResults"]
