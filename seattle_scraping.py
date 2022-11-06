import os
import pandas as pd

from sodapy import Socrata
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()


class ScrapeSeattleData:

    def __init__(self):
        self.domain = 'data.seattle.gov'

        self.app_token = os.getenv('APP_TOKEN')

        self.username = os.getenv('EMAIL_U')

        self.password = os.getenv('EMAIL_P')

        self.datasetID = os.getenv('DATASET_ID')

        self.raw_data = None

    def scrape(self, date=None, how='>='):
        """
        Summary: Scrapes crime data from SPD website. The 'date' param represents a date from which
                 to scrape data by and the 'how' param allows you to specify how data should be 
                 scraped with respect to the date. 

                 For instance, a 'date' param of '2022-06-15' and a 'how' param of '>' translates to:
                 "scrape all crime data after '2022-06-15'" 

        Params:
                 date: must be in yyyy-mm-dd format
                 how: must be one of the following logical operators: >, <, >=, <=, or =

        Returns:

                 Running 'scrape' scrapes the SPD Crime dataset and assigns it to the instance's
                 'raw_data' attribute in the form of a Pandas dataframe.
        """

        # If no argument is passed for the 'date' param, get the current day's date, subtract
        # one day, and format it as a yyyy-mm-dd string value
        if date is None:
            date = datetime.strftime(datetime.today().date() - timedelta(days=1), '%Y-%m-%d')

        # Connect with the client
        client = Socrata(domain=self.domain, app_token=self.app_token, username=self.username, password=self.password)

        # SQL query used to extract the data, filtered by the check_date date
        query = f'''
                            SELECT * 
                            WHERE DATE_TRUNC_YMD(report_datetime) {how} '{date}'
                            ORDER BY report_datetime desc 
                            LIMIT 100000000
                        '''

        results = client.get(dataset_identifier=self.datasetID, query=query)

        self.raw_data = pd.DataFrame.from_records(results)
