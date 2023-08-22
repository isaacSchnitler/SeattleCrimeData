import src.clean_seattle_data as csd 
import src.retrieve_seattle_data as rsd 
import src.audit_functions as audit
from requests.exceptions import HTTPError, RequestException 




# List of data retrieval functions, ordered in terms of retrieval preference
data_retrieval_functions = [
                            rsd.socrata_api,        # retrieve data via the Socrata API
                            rsd.odata_endpoint      # retrieve data via the Odata endpoint
                            ]

dataset = None


# Get data using one of the data retrieval functions & if an error occurs use the next function
# (w/the exception that if a function encounters )
for function in data_retrieval_functions:
        
        # Retry the request (5 additional times) if the server times out waiting for request (408 status code)
        for retry in range(5):
            # Attempt to get data using function & if successful, break retry loop
            try: 
                dataset = function()
                break


            # Otherwise, catch 408 HTTPError & continue 'retry' loop  
            # If not 408 HTTPError, break 'retry' loop & attempt next function
            except HTTPError as error:
                if error.response.status_code == 408: 
                    continue

                break   # HAVE TO ADD METHOD OF LOGGING OTHER ERRORS


            # Catch any other request exceptions and attempt next function  
            except RequestException as error:
                break


            except:
                break

        # If there's no data then try next function, otherwise we have data & can break loop
        if dataset is None:
            continue

        break


data_auditing_functions = [
                            audit.audit_dtypes, 
                            audit.audit_offense_datetime,
                            audit.audit_report_number,
                            audit.audit_mispelled_mcpp,
                            audit.audit_correct_na_loc_code,
                            audit.audit_correct_deci_degrees
                            ]

audit_table = audit.create_audit(audit_type='values')

for audit_function in data_auditing_functions:
    audit_table = audit_function(
                                seattle_data=dataset,
                                audit_table=audit_table
                                )



data_cleaning_functions = [
                            csd.cleanup_whitespace,
                            csd.cleanup_column_casing,
                            csd.cleanup_na_values,
                            csd.clear_non_crimes,
                            csd.cleanup_addresses,
                            csd.cleanup_dtypes,
                            csd.correct_offense_datetime,
                            csd.cleanup_report_number,
                            csd.cleanup_misspelled_mcpp,
                            csd.correct_na_loc_codes,
                            csd.correct_deci_degrees,
                            csd.cleanup_column_order,
                            csd.config_addresses,
                            csd.config_na_values
                        ]

audit_table_func = audit.create_audit(audit_type='functions')
function_timer = audit.AuditTimer()


for cleaning_function in data_cleaning_functions:
    function_timer.start() 

    data = cleaning_function(data)

    audit.audit_functions_insert(
                                audit_table = audit_table_func,
                                func_name   = cleaning_function.__name__,
                                runtime     = function_timer.stop()
                                )

