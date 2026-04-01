import asyncio

import pandas as pd

from pipeline_configs.transform_steps.project_normalize import ProjectNormalizeStep


async def main():
    step = ProjectNormalizeStep()

    data = pd.read_csv("./resources/scraper_main_FFG.csv")

    async for event in step.run({}, {"getScraperResults": data}):
        print(event)


if __name__ == "__main__":
    asyncio.run(main())
