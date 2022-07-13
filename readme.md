# SeattleCrimeData
A collection of Python scripts that scrapes Seattle Police Department (SPD) crime data from the official city site, performs various data cleaning activities and loads the data into a local MS SQL Server database.

*SPD crime data source: https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5*

### Motivation
While I do not live in Seattle, I am still very much connected through family, friends and frequent visits. Aware of the rising crime in major U.S. cities, I wanted to get a better idea of the level, type and areas of crime in Seattle. This is the motivation behind this project; to extract and analyze this data in order to reveal useful insights into crime in Seattle. 

### How to use?
This project is designed similar to that of a data pipeline, where the execution of the project is determined by a specific, intended order of steps. 

In order to access the dataset, we need a Socrata Open Data API (SODA) token which can be found here: *https://dev.socrata.com/foundry/data.seattle.gov/tazs-3rd5*


```Python
from seattle_scraping import ScrapeSeattleData

# Create an instance of the ScrapeSeattleData class, which represents the process of scraping the data
scraping_process = ScrapeSeattleData()


scraping_process.scrape(1)
```

