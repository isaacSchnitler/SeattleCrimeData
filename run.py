import src.clean_seattle_data as csd 
import src.retrieve_seattle_data as rsd 
import src.audit_functions
from requests.exceptions import HTTPError, RequestException 




# List of data retrieval functions, ordered in terms of retrieval preference
data_retrieval_functions = [rsd.socrata_api, rsd.odata_endpoint, rsd.download_csv]
dataset = None


# Attempt a data retrieval function, if it encounters an error, attempt the next one (w/the 
# exception of a Request Timeout error, in which case retry request 5 times)
for function in data_retrieval_functions:
        
        # Retry the request (5 additional times) if it times out (408 status code)
        for retry in range(5):
            # Attempt to retrieve data & if successful, break both loops & return dataset
            try: 
                dataset = function()
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





audit_table = src.audit_functions.create_functions_audit()
cleaning_process = csd.CleanSeattleData(raw_data=dataset)

cleaning = [
        
     
] 


for cleaning_method in cleaning:


