# SeattleCrimeData
A collection of Python scripts that scrapes Seattle Police Department (SPD) crime data from the official city site, performs various data cleaning activities and loads the data into a local MS SQL Server database.

*SPD crime data source: https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5*

### Motivation
While I do not live in Seattle, I am still very much connected through family, friends and frequent visits. Aware of the rising crime in major U.S. cities, I wanted to get a better idea of the level, type and areas of crime in Seattle. This is the motivation behind this project; to extract and analyze this data in order to reveal useful insights into crime in Seattle. 

### How to use?
This project is designed similar to that of a data pipeline, where the execution of the project is determined by a specific, intended order of steps. 

To begin with, we need a Socrata Open Data API (SODA) token to access the dataset. The SPD crime data API docs can be found below:
*https://dev.socrata.com/foundry/data.seattle.gov/tazs-3rd5*


Next, from the seattle_scraping file we import the ScrapeSeattleData class. This class creates an object that represents, what I like to think of as, the scraping process: 
```Python
from seattle_scraping import ScrapeSeattleData

scraping_process = ScrapeSeattleData()
```
