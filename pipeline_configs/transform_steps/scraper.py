from pipelineFramework import GetResultFromLatestPipeline, LocalisationString

GetScraperResults = GetResultFromLatestPipeline(
    "getScraperResults",
    LocalisationString("Get results from Scraper Pipeline", "Ergebnisse der scraper Pipeline laden"),
    None,
    "Scraper Pipeline",
    "getDataFFG",
)

GetTechnologyConfiguration = GetResultFromLatestPipeline(
    "getTechnologyConfiguration",
    LocalisationString("Get Technology Configuration", "Technologie Konfiguration Laden"),
    None,
    "Scraper Pipeline",
    "getTechnologyConfiguration",
)
