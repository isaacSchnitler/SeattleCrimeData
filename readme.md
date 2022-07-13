# SeattleCrimeData
A collection of Python scripts that scrapes Seattle Police Department (SPD) crime data from the official city site, performs various data cleaning activities and loads the data into a local MS SQL Server database.

*SPD crime data source: https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5*


### Motivation
While I do not live in Seattle, I am still very much connected through family, friends and frequent visits. Aware of the rising crime in major U.S. cities, I wanted to get a better idea of the level, type and areas of crime in Seattle. This is the motivation behind this project; to extract and analyze this data in order to reveal useful insights into crime in Seattle. 


### Important Notes
Data is updated on a daily basis, as opposed to real-time, meaning the most recent crime data will always be data from the day before.  

In order to access the dataset, a Socrata Open Data API (SODA) token is needed, which can be found here: *https://dev.socrata.com/foundry/data.seattle.gov/tazs-3rd5*


### Usage
This project is designed similar to that of a data pipeline, in that it follows a specific, intended sequence of steps. When used, this project sould be executed accordingly to get the best results. To make this process more straightfoward, the 

