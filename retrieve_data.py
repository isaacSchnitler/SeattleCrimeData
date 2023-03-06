
from json import loads
from os import getenv

from sodapy import Socrata
from datetime import datetime, timedelta
from dotenv import load_dotenv

from requests import get
from requests.exceptions import HTTPError, RequestException 
from pandas import DataFrame, json_normalize, read_csv




load_dotenv()

def socrata_api(date=None, how='>='):
        """
        Summary: Retrieves crime data from SPD website via the site's Socrata API. The preferred method of retrieving
                 data from SPD due to usage of an Application Token, which grants proper authentication to this script
                 & allows for unlimited requests. An example of how the 'date' & 'how' params operate: a 'date' param of
                 '2022-06-15' and a 'how' param of '>' translates to: "scrape all crime data after 2022-06-15". 

        Returns: Pandas DataFrame

        Params:
            date    :   represents date from which to scrape data by; must be in yyyy-mm-dd format
            how     :   allows you to specify how data sould be scraped with respect to date: >, <, >=, <=, or =
        """

    
        # Attributes necessary to authenticate & retrieve data from Socrata API
        domain = 'data.seattle.gov'
        app_token = getenv('APP_TOKEN')
        username = getenv('EMAIL_U')
        password = getenv('EMAIL_P')
        datasetID = getenv('DATASET_ID')


        # If no argument is passed for the 'date' param, get the current day's date, subtract
        # one day, and format it as a yyyy-mm-dd string value
        if date is None:
            date = datetime.strftime(
                                    datetime.today().date() - timedelta(days=1),
                                     '%Y-%m-%d'
                                     ) 

        # SQL query used to extract the data, filtered by the date
        query = f'''
                SELECT * 
                WHERE DATE_TRUNC_YMD(report_datetime) {how} '{date}'
                ORDER BY report_datetime desc 
                LIMIT 100000000
                '''

           
        # Connect with the Socrata API
        client = Socrata(
                    domain=domain, 
                    app_token=app_token, 
                    username=username, 
                    password=password
                    )


        # Retrieve data
        response = client.get(
                            dataset_identifier=datasetID, 
                            query=query
                            )


        # If successful, convert data to dataframe and return dataset
        dataset = DataFrame.from_records(response)


        # If the dataset is of NoneType, also raise an exception
        if dataset is None:  
            raise
        
        return dataset 
          


def odata_endpoint(date=None, how='>='):
    """
        Summary: Retrieves crime data from SPD website via the site's OData endpoint. The 2nd preferred method of retrieving
                 data from SPD because, similar to the Socrata API, it allows to filter which data to retrieve, so as to only
                 retrieve the necessary data (as specified by the 'date' & 'how' params. An example of how the 'date' &'how' 
                 params operate: a 'date' param of'2022-06-15' and a 'how' param of '>' translates to: "scrape all crime data 
                 after 2022-06-15". 

        Returns: Pandas DataFrame

        Params:
            date    :   represents date from which to scrape data by; must be in yyyy-mm-dd format
            how     :   allows you to specify how data sould be scraped with respect to date: >, <, >=, <=, or =
    """

    # If no argument is passed for the 'date' param, get the current day's date, subtract
        # one day, and format it as a yyyy-mm-dd string value
    if date is None:
        date = datetime.strftime(
                                datetime.today().date() - timedelta(days=1),
                                    '%Y-%m-%d'
                                    ) 

    api_response = get(
                    f'''https://data.seattle.gov/api/odata/v4/tazs-3rd5?$filter=report_datetime ge '{date}' '''
                    )

    raw_data = api_response.text
    parsed_json = loads(raw_data)

    dataset = json_normalize(parsed_json['value'])

    return dataset


def download_csv():
    """
        Summary: Retrieves crime data from SPD website via the site's OData endpoint. The 3rd & least preferred method of 
                 retrieving data from SPD because it does not allow to filter which data to retrieve, resulting in retrieving
                 the entirety of the data (little over ~1 million records as of when this was written). An example of how the
                 'date' & 'how' params operate: a 'date' param of'2022-06-15' and a 'how' param of '>' translates to: "scrape
                  all crime data after 2022-06-15". 

        Returns: Pandas DataFrame

        Params:
            date    :   represents date from which to scrape data by; must be in yyyy-mm-dd format
            how     :   allows you to specify how data sould be scraped with respect to date: >, <, >=, <=, or =
    """

    dataset = read_csv('https://data.seattle.gov/api/views/tazs-3rd5/rows.csv?accessType=DOWNLOAD')
    
    return dataset


def main():

    # List of data retrieval functions, ordered in terms of preference
    data_retrieval_functions = [socrata_api, odata_endpoint, download_csv]
    dataset = None

    # Attempt a data retrieval function, if it encounters an error, attempt the next one (w/the 
    # exception of Request Timeout error, in which case retry request)
    for count, function in enumerate(data_retrieval_functions):

        # Retry the request (5 additional times) if it times out (408 status code)
        for retry in range(5):

            # Attempt to retrieve data & if successful, break both loops & return dataset
            try: 
                dataset = function()
                retrieved_using = count+1
                break

            # Otherwise, catch HTTPError & if 408 status code, continue the 'retry' loop  
            # If not a 408 status code, break 'retry' loop & attempt next data retrieval function
            except HTTPError as error:
                if error.response.status_code == 408: 
                    continue

                break
                  
            # Catch any other request exceptions, and attempt next data retrieval function  
            except RequestException as error:
                break

            except:
                break


        # If dataset is of NoneType, 
        if dataset is None:
            continue

        break

    return dataset, retrieved_using 

        

if __name__ == '__main__':
    dataset, retrieved_using = main()
    print(retrieved_using)
    print(dataset)

    






        

        

        



    

