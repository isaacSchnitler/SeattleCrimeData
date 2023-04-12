# SeattleCrimeData
A collection of Python scripts intended to automatically (through a job scheduler, like cron) extract Seattle Police Department (SPD) crime data from the official city site, perform various data cleaning & transformation tasks and load the data into a local MS SQL Server database for further analysis and reporting purposes. 

*SPD crime data source: https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5*


## Motivation
Living in the Greater Seattle Area and visiting Seattle often, I wanted to get a better idea of the level, type and areas of crime in Seattle. This is the motivation behind this project; to extract and analyze this data in order to reveal useful insights into the crime in Seattle. 



## Notes

The SPD crime data is updated on a daily basis and as such, the most recent crime data will always be from the day before. In addition, an audit feature is implemented that logs the...
- Number of values determined invalid per data "business rules" and therefore discarded (made null) 
- Each function's execution time
- Number of rows
- Size of batch in MB
  
...for each batch (a batch being one day's worth of data extracted, cleaned, loaded). This additional data are logged in separate tables, used to track    the performance of the script and potential changes in data "business rules". 
