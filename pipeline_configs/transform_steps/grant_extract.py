import json
import math
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


class GrantExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("getScraperResults")
        if SCRAPER_DATA is None:
            raise FileNotFoundError("No organisation data found")
        grants = []
        pd.DataFrame().itertuples()
        for _, grant_data in SCRAPER_DATA.iterrows():
            new_grant = {
                "name": grant_data["bidding"],
                "programme": grant_data["programme"],
                "projects": 0,
            }
            if any(new_grant == grant for grant in grants):
                continue
            grants.append(new_grant)
        yield f"Extracted {len(grants)} unique grants", EventType.INFO
        yield grants, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def name(self) -> str:
        return "grant_extract"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Extract Grant Data", "Förderdaten extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getScraperResults"]
