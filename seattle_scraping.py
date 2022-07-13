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

    def scrape(self, no_of_days=1):
        """
        Summary: Specify the number of days previous to today worth of data to collect. By
                default, it collects the previous day's data.
        """

        # Connect with the client
        client = Socrata(domain=self.domain, app_token=self.app_token, username=self.username, password=self.password)

        # Form the appropriate check date
        check_date = get_check_date(no_of_days)

        # SQL query used to extract the data, filtered by the check_date date
        query = f'''
                    SELECT * 
                    WHERE DATE_EXTRACT_Y(report_datetime) = {check_date.year}
                    AND DATE_EXTRACT_M(report_datetime) = {check_date.month}
                    AND DATE_EXTRACT_D(report_datetime) = {check_date.day}
                    ORDER BY report_datetime desc 
                    LIMIT 100000000
                '''

        results = client.get(dataset_identifier=self.datasetID, query=query)

        self.raw_data = pd.DataFrame.from_records(results)


def get_check_date(no_of_days):
    """
    Summary: Calculates the date of the data needed to scrape by subtracting some interval
            of time from the current date.
    """

    # Get the current days date and subtract the specified number of days
    check_date = datetime.today().date() - timedelta(days=no_of_days)

    # Return the check date as a datetime date
    return check_date
