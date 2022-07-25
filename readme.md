# SeattleCrimeData
A collection of Python scripts intended to automatically (through a job scheduler, like cron) scrape Seattle Police Department (SPD) crime data from the official city site, perform various data cleaning activities and load the data into a local MS SQL Server database.

*SPD crime data source: https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5*


## Motivation
While I do not live in Seattle, I am still very much connected through family, friends and frequent visits. Aware of the rising crime in major U.S. cities, I wanted to get a better idea of the level, type and areas of crime in Seattle. This is the motivation behind this project; to extract and analyze this data in order to reveal useful insights with regarad to crime in Seattle. 



## Notes

- The SPD crime data is updated on a daily basis, as opposed to real-time, meaning the most recent crime data will always be from the day before.  


- This project is designed similar to that of a data pipeline, in that it follows a specific, intended sequence of steps. 


- The *seattle_cleaning.py* cleaning script uses a unique audit feature, which logs values that are recognized as invalid and therefore nullified during the cleaning process in a separate table. For instance, say a record contains a precinct code value, like *NE*, that is not a valid precinct (*valid precinct codes: N, E, W, S, SW*). In this scenario, the *NE* value is nullified and audited, where the value, unique identifier, column name, and the reason why it was audited is logged in a separate table. 

