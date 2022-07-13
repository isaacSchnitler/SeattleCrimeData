# SeattleCrimeData
A collection of Python scripts that automatically (through a job scheduler, like cron) scrapes Seattle Police Department (SPD) crime data from the official city site, performs various data cleaning activities and loads the data into a local MS SQL Server database.

*SPD crime data source: https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5*


## Motivation
While I do not live in Seattle, I am still very much connected through family, friends and frequent visits. Aware of the rising crime in major U.S. cities, I wanted to get a better idea of the level, type and areas of crime in Seattle. This is the motivation behind this project; to extract and analyze this data in order to reveal useful insights with regarad to crime in Seattle. 



## Dataset Notes
The SPD crime data is updated on a daily basis, as opposed to real-time, meaning the most recent crime data will always be from the day before.  

In order to access the dataset, a Socrata Open Data API (SODA) token is needed, which can be found here: *https://dev.socrata.com/foundry/data.seattle.gov/tazs-3rd5*



## Project Notes
- This project is designed similar to that of a data pipeline, in that it follows a specific, intended sequence of steps. When used, this project sould be executed accordingly to get the best results. To make this process more straightfoward, the *seattle_loading.py* script can be run directly as the main program, which will scrape, clean and load the data in the correct sequence: 

```
IsaacSchnitler@Isaacs-MBP ~ % python3 seattle_loading.py
```

- The *seattle_cleaning.py* cleaning script uses a unique audit feature, which logs values that are recognized as invalid and therefore nullified during the cleaning process in a separate table. For instance, say a record contains a precinct code value, like *NW*, that is not a valid precinct (*valid precinct codes: N, S, E, W, SW*). In this scenario, the *NW* value is nullified and audited, where the value, unique identifier, column name, and the reason why it was audited is logged in a separate table. 

