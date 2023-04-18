from functools import wraps
from time import time 
from pandas import DataFrame
from datetime import datetime


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


def audit_values_insert(audit_table, audited_val, audit_reason_id):
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
    audited_val['audited_reason_id'] = audit_reason_id

    # Rename the new column (created from .stack()) to audited_val
    audited_val.rename(columns={0: 'audited_val'}, inplace=True)

    # Rename the index 
    audited_val.index.names = ['audited_col', 'offense_id']

    audited_val['batch'] = datetime.today().strftime('%Y-%m-%d')

    # Merge the tables
    return audit_table.combine_first(audited_val)


def audit_functions_insert(audit_table, func_name, runtime):
    audit_table.loc[len(audit_table)] = [func_name, datetime.today().strftime('%Y-%m-%d'), runtime]

    return audit_table
    
    