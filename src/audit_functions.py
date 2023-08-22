from functools import wraps
from time import time 
from pandas import DataFrame, to_datetime, to_numeric, read_csv
from datetime import datetime
from os import getenv


class TimeError(Exception):
    pass


class AuditTimer():

    def __init__(self): 
        self.start_time = None 

    
    def start(self):
        
        if self.start_time is not None: 
            raise TimeError('Timer is runnng. Use .stop() to stop it')

        self.start_time = time()

    
    def stop(self):

        if self.start_time is None:
            raise TimeError('Timer is not running. Use .start() to start it')

        elapsed_time = time() - self.start_time
        self.start_time = None 

        return elapsed_time 


def create_audit(audit_type):
    """
    Summary: Creates audit table to store information for values that were nulled in the cleaning process: 

                                        values_audit                                                          functions_audit

    +--------------------------+                                                           
    |     multi-level index    |                                                                
    +-------------+------------+-------------------+-------------+------------+                  +--------------+-------------+---------+
    | audited_col | offense_id | audited_reason_id | audited_val |    batch   |                  | audited_func |    batch    | runtime |
    +-------------+------------+-------------------+-------------+------------+                  +--------------+-------------+---------+
    | column_name |      1     |         1         |  2023-02-45 | 2023-04-08 |                  | column_name  |  2023-04-08 | 0.00345 |
    +-------------+------------+-------------------+-------------+------------+                  +--------------+-------------+---------+
    """

    if audit_type == 'values':
        # Create and return the audit table
        audit_table = DataFrame(columns=['audited_col', 'offense_id', 'audited_val', 'audited_reason_id', 'batch'])
        audit_table.set_index(
                               keys=['audited_col', 'offense_id'],
                               inplace=True
                               )
        

    if audit_type == 'functions':
        # Create and return the audit table
        audit_table = DataFrame(columns=['audited_function', 'batch', 'runtime'])

    return audit_table


def audit_values_insert(audit_table, audited_val, audit_reason):
    """
    Summary: takes audited_values, which are passed in as shown below, and reshapes it into a format that allows it to be 
             merged with audit table, inserting the new audited values into the audit table. 
    

            audited_values                                                          audit_table

                                                   +--------------------------+                              
                                                   |     multi-level index    |
    +------------+---------------+                 +-------------+------------+-------------------+-------------+------------+
    | offense_id |  column_name  |                 | audited_col | offense_id | audited_reason_id | audited_val |    batch   |
    +------------+---------------+                 +-------------+------------+-------------------+-------------+------------+
    |     1      |   2023-02-45  |    ------>      | column_name |      1     |         1         |  2023-02-45 | 2023-04-08 |
    |     2      |   9999-09-13  |                 | column_name |      2     |         1         |  9999-09-13 | 2023-04-08 |
    |     3      |   2O23-O1-O2  |                 | column_name |      3     |         1         |  2O23-O1-O2 | 2023-04-08 |
    +------------+---------------+                 +-------------+------------+-------------------+-------------+------------+

    Params: 
            source_audit_table: the source table in the merging operation
            target_audit_table: the dataframe with nulled values, used as the 
                                target table in the merging operation

            audit_reason_id: the ID describes the reason the value was nullified
    """

    # Set the offense_id as the index 
    audited_val.set_index('offense_id', inplace=True)

    # .stack() moves column_name to index, forming multi-level index w/offense_id
    # .stack() converts data to Series object, so call DataFrame() over it 
    audited_val = DataFrame(audited_val.stack())
    

    # .swaplevel() switches the multi-level index, column_name is now the outer index, offense_id is inner index
    audited_val = audited_val.swaplevel()

    # Sort the index & assign the audit reason id value
    audited_val.sort_index(inplace=True)
    audited_val['audited_reason_id'] = audit_reason

    # Rename the new column (created from .stack()) to audited_val
    audited_val.rename(columns={0: 'audited_val'}, inplace=True)

    # Rename the index 
    audited_val.index.names = ['audited_col', 'offense_id']

    audited_val['batch'] = datetime.today().strftime('%Y-%m-%d')

    audit_table = audit_table.combine_first(audited_val) # Merge & return the tables 

    audit_table = audit_table.astype(
                                        {
                                            'audited_val': str, 
                                            'audited_reason_id': str,
                                            'batch': str
                                        }
                                    )

    # audit_table[['audited_val','audited_reason_id','batch']] = audit_table[['audited_val','audited_reason_id','batch']].astype(str)

    return audit_table


def audit_functions_insert(audit_table, func_name, runtime):
    audit_table.loc[len(audit_table)] = [func_name, datetime.today().strftime('%Y-%m-%d'), runtime]

    return audit_table


def audit_dtypes(seattle_data, audit_table): 

    # For each datetime column ...
    for column in ['offense_start_datetime', 'offense_end_datetime', 'report_datetime']:

        # Retrieve and store records that do not conform to datetime format
        audited_values = (
                        seattle_data
                        [column]
                        .notnull() 
                        & 
                        to_datetime(
                                arg=seattle_data[column],
                                errors='coerce'
                                )
                                .isna()
                        )
        
        # For those records, retrieve the unique identifier (offense_id) and the value
        # in question. Insert these values into audit table to track what values are being made null
        audited_values = (
                        seattle_data
                        [audited_values]
                        [['offense_id', column]]
                        )
        
 
        audit_table = audit_values_insert(
                                            audit_table=audit_table, 
                                            audited_val=audited_values, 
                                            audit_reason=1
                                            )
        

    # Do the same with the numeric columns as done with the datetime columns
    for column in ('latitude', 'longitude'):

        audited_values = (
                        seattle_data
                        [column]
                        .notnull() 
                        & 
                        to_numeric(
                                arg=seattle_data[column],
                                errors='coerce'
                                )
                                .isna()
                        )
        
        audited_values = (
                        seattle_data
                        [audited_values]
                        [['offense_id', column]]
                        )
        
        audit_table = audit_values_insert(
                                        audit_table=audit_table,
                                        audited_val=audited_values,
                                        audit_reason=2 
                                        )
        
    return audit_table
        

def audit_offense_datetime(seattle_data, audit_table):

    # Retrieve and store records where the offense start datetime is after
    # the offense end datetime. From these, retrieve the offense_id and values in question
    audited_values = (
                    seattle_data[
                                to_datetime(seattle_data['offense_start_datetime'], errors='coerce') > 
                                to_datetime(seattle_data['offense_end_datetime'], errors='coerce')
                                ]
                                [['offense_id', 'offense_start_datetime', 'offense_end_datetime']]
                    )

    # Insert these values into audit table to track the values being made null
    audit_table = audit_values_insert(
                                    audit_table=audit_table,
                                    audited_val=audited_values,
                                    audit_reason='3'
                                    )

    return audit_table   


def audit_report_number(seattle_data, audit_table):

    valid_re        =   r'^\d{4}-\d{6}$'

    # Retrieve & store the offense_id and value in question
    audited_values = (
                    seattle_data[
                                ~seattle_data
                                ['report_number']
                                .str.contains(
                                            pat=valid_re,
                                            regex=True,
                                            na=False
                                            )
                                ]
                                [['offense_id', 'report_number']]
                    )
    
    # Insert these values into audit table to track what values are made nan
    audit_table = audit_values_insert(
                                    audit_table=audit_table,
                                    audited_val=audited_values, 
                                    audit_reason=4
                                    )
    
    return audit_table


def audit_mispelled_mcpp(seattle_data, audit_table):

    mcpp = read_csv(
                    filepath_or_buffer='/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/utils/mcpp.csv', 
                    dtype='O'
                    )
    
    audited_values = (
                        seattle_data[
                                            (
                                                ~seattle_data
                                                ['mcpp']
                                                .isin(mcpp['mcpp'])
                                                            
                                            )

                                            &

                                            (
                                                ~seattle_data
                                                ['mcpp']
                                                .isna()
                                            )
                                        ]
                                        [['offense_id', 'mcpp']]           
                        )
    
    audit_table = audit_values_insert(
                                    audit_table=audit_table, 
                                    audited_val=audited_values, 
                                    audit_reason=5
                                    )
    
    return audit_table


def audit_correct_na_loc_code(seattle_data, audit_table):

    # Load dataframe containing valid, corresponding precinct/sector/beat location code matchings, used to verify raw data against
    valid_loc_codes = read_csv(
                        filepath_or_buffer='/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/utils/location_codes.csv', 
                        dtype='O'
                        )
    
    loc_code_list = ['beat', 'sector', 'precinct']
        
    
    for loc_code in loc_code_list:

         # Audit & null any high-level location codes that do not have a valid/present low-level location code (& thus cannot correctly be determined)
        audited_values = (
                        seattle_data.loc[
                                            (
                                                # if the high-level loc code IS NOT a valid high-level loc code
                                                (
                                                ~seattle_data
                                                [loc_code]
                                                .isin(valid_loc_codes[loc_code])
                                                ) 
                                            
                                            & 

                                                # AND the high-level loc code IS NOT null, audit it
                                                (
                                                ~seattle_data[loc_code]
                                                .isna()
                                                )
                                            )
                                        ]
                                    
                                        [['offense_id', loc_code]]
                            ) 
    
        
        audit_table = audit_values_insert(
                                            audit_table=audit_table,
                                            audited_val = audited_values,
                                            audit_reason = 8
                                            )
        
    
    return audit_table


def audit_correct_deci_degrees(seattle_data, audit_table):

    seattle_data['longitude_audit'] = to_numeric(seattle_data['longitude'], errors='coerce')
    seattle_data['latitude_audit'] = to_numeric(seattle_data['latitude'], errors='coerce')

    # Audit the longitude if it's not at least within Washington's longitude range
    audited_values = (
                        seattle_data[
                                        ~(
                                            seattle_data
                                            ['longitude_audit']
                                            .between(-125.0, -116.5)
                                            )
                                    ]
                                    [['offense_id', 'longitude']]
                        )
    
    audit_table = audit_values_insert(
                                        audit_table=audit_table,
                                        audited_val=audited_values,
                                        audit_reason=10
                                        ) 


    audited_values = (
                        seattle_data[
                                        ~(
                                            seattle_data
                                            ['latitude_audit']
                                            .between(45.5, 49.0)
                                            )     
                                    ]
                                    
                                    [['offense_id', 'latitude']]
                                    ) 
    
    seattle_data.drop(
                        labels  = ['longitude_audit', 'latitude_audit'],
                        inplace = True,
                        axis    = 1
                    )
    

    audit_table = audit_values_insert(
                                        audit_table=audit_table,
                                        audited_val=audited_values,
                                        audit_reason=11
                                        )
    
    return audit_table

    
    