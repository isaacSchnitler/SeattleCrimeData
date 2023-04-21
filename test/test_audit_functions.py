from sys import path 
path.insert(1, '/Users/IsaacSchnitler/Desktop/Projects/SeattleCrimeData/src')

import clean_seattle_data as sc
import audit_functions as audit
import unittest
import pandas as pd
import pandas.testing as pdt
import great_expectations as ge


class CleaningUnitTesting(unittest.TestCase):

    correct_dtypes = {
                      'report_number':              'object',
                      'offense_id':                 'object',
                      'offense_start_datetime':     'datetime64[ns]',
                      'report_datetime':            'datetime64[ns]',
                      'group_a_b':                  'object',
                      'crime_against_category':     'object',
                      'offense_parent_group':       'object',
                      'offense':                    'object',
                      'offense_code':               'object',
                      'precinct':                   'object',
                      'sector':                     'object',
                      'beat':                       'object',
                      'mcpp':                       'object',
                      '_100_block_address':         'object',
                      'longitude':                  'float64',
                      'latitude':                   'float64',
                      'offense_end_datetime':       'datetime64[ns]',
                      'street_1':                   'object',
                      'street_2':                   'object'
                      }
    

    def test_audit_val_table(self):
        expected_output = pd.read_csv('test/seattle_cleaning_testing_files/13_audit_table_output.csv')
        expected_output.set_index(['audited_col', 'offense_id'], inplace=True)

        input_df = audit.create_audit(audit_type='values')

        pdt.assert_frame_equal(input_df, expected_output)


    def test_audit_insert(self):
        expected_output = pd.read_csv('test/seattle_cleaning_testing_files/14_audit_insert_output.csv', dtype='O')
        expected_output.set_index(['audited_col', 'offense_id'], inplace=True)

        target_table = pd.read_csv('test/seattle_cleaning_testing_files/14_audit_insert_input.csv', dtype='O')

        input_df = audit.create_audit(audit_type='values')
        input_df = audit.audit_values_insert(input_df, target_table, '3')

        # self.display(input_df, expected_output)
        pdt.assert_frame_equal(input_df, expected_output) 


if __name__ == '__main__':
    unittest.main()