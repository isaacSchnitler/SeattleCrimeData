import unittest
import pandas.testing as pdt
from pandas import (
                    read_csv, 
                    to_datetime
                    )

from ..src.audit_functions import (
                                create_audit, 
                                audit_values_insert, 
                                audit_dtypes, 
                                audit_offense_datetime, 
                                audit_report_number, 
                                audit_mispelled_mcpp, 
                                audit_correct_na_loc_code,
                                audit_correct_deci_degrees
                                )


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
    

    audit_table_dtypes = {
                            'audited_val':          str,
                            'audited_reason_id':    str,
                            'batch':                str
                            }
    

    # Checked
    def test_create_audit_val(self):

        # setup 
        expected_output = read_csv('/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/01_create_audit_val.csv')
        expected_output.set_index(
                                    keys=['audited_col', 'offense_id'],
                                    inplace=True
                                    )

        # test
        test_audit_table = create_audit(audit_type='values')

        pdt.assert_frame_equal(test_audit_table, expected_output)


    # Checked
    def test_create_audit_func(self):
        
        # setup
        expected_output = read_csv('/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/02_create_audit_func_output.csv')

        # test
        test_audit_table = create_audit(audit_type='functions')

        pdt.assert_frame_equal(test_audit_table, expected_output)


    # Checked
    def test_audit_values_insert(self):
        
        # setup
        test_audit_table = create_audit(audit_type='values')
        
        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/03_audit_values_insert_output.csv'
                                    , dtype='O'
                                    )
        
        expected_output.set_index(
                                    keys=['audited_col', 'offense_id'], 
                                    inplace=True
                                    )
        
        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')
        
        audited_values = read_csv('/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/03_audit_values_insert_input.csv', dtype='O')


        # test
        test_audit_table = audit_values_insert(test_audit_table, audited_values, '3')

        # self.display(input_df, expected_output)
        pdt.assert_frame_equal(test_audit_table, expected_output) 


    def test_audit_functions_insert(self):
        pass


    def test_audit_dtypes(self):
        
        # setup 

        test_data = read_csv('/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/05_audit_dtypes_input.csv')

        test_audit_table = create_audit(audit_type='values')

        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/05_audit_dtypes_output.csv',
                                    dtype=CleaningUnitTesting.audit_table_dtypes
                                    )

        expected_output.set_index(
                                    keys=['audited_col', 'offense_id'], 
                                    inplace=True
                                    )

        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')

        # test
        test_audit_table = audit_dtypes(
                                        seattle_data=test_data,
                                        audit_table=test_audit_table
                                        )

        pdt.assert_frame_equal(test_audit_table, expected_output) 


    def test_audit_offense_datetime(self):

        # setup 
        test_data = read_csv(
                            '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/seattle_cleaning_testing_files/07_correct_offense_datetime_input.csv',
                            dtype=CleaningUnitTesting.audit_table_dtypes
                            )
        
        test_audit_table = create_audit(audit_type='values')

        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/06_audit_offense_datetime_output.csv',
                                    dtype=CleaningUnitTesting.audit_table_dtypes
                                    )
        
        expected_output.set_index(
                                    keys=['audited_col', 'offense_id'],
                                    inplace=True
                                    )
        
        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')

        # test 
        test_audit_table = audit_offense_datetime(
                                                    seattle_data=test_data,
                                                    audit_table=test_audit_table
                                                    )
                                                     
        
        pdt.assert_frame_equal(test_audit_table, expected_output, check_dtype=False) 


    def test_audit_report_number(self):
        
        # setup 
        test_data = read_csv(
                            '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/seattle_cleaning_testing_files/08_cleanup_report_number_input.csv',
                            dtype=CleaningUnitTesting.audit_table_dtypes
                            )
        
        test_audit_table = create_audit(audit_type='values')

        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/07_audit_report_number_output.csv',
                                    dtype=CleaningUnitTesting.audit_table_dtypes
                                    )
        
        expected_output.set_index(
                                    keys=['audited_col', 'offense_id'],
                                    inplace=True
                                    )
        
        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')

        # test
        test_audit_table = audit_report_number(
                                                seattle_data=test_data,
                                                audit_table=test_audit_table
                                                )
        
        pdt.assert_frame_equal(test_audit_table, expected_output)


    # checkee
    def test_audit_mispelled_mcpp(self):

        # setup
        test_data = read_csv(
                            '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/seattle_cleaning_testing_files/09_cleanup_misspelled_mcpp_input.csv' ,
                            dtype=CleaningUnitTesting.audit_table_dtypes
                            )
        
        test_audit_table = create_audit(audit_type='values')

        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/08_audit_report_number_output.csv',
                                    dtype=CleaningUnitTesting.audit_table_dtypes
                                    )
        
        expected_output.set_index(
                                keys=['audited_col', 'offense_id'],
                                inplace=True
                                )
        
        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')

        # test 
        test_audit_table = audit_mispelled_mcpp(
                                            seattle_data=test_data,
                                            audit_table=test_audit_table
                                            )
        
        pdt.assert_frame_equal(test_audit_table, expected_output)


    # checked
    def test_audit_correct_na_loc_code(self):

        # setup 
        test_data = read_csv(
                            '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/09_audit_correct_na_loc_codes_input.csv',
                            dtype=CleaningUnitTesting.audit_table_dtypes
                            )
        
        test_audit_table = create_audit(audit_type='values')
        
        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/09_audit_correct_na_loc_codes_output.csv',
                                    dtype=CleaningUnitTesting.audit_table_dtypes
                                    )
        
        expected_output.set_index(
                                keys=['audited_col', 'offense_id'],
                                inplace=True
                                )
        
        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')
        
        # test
        test_audit_table = audit_correct_na_loc_code(
                                                    seattle_data=test_data,
                                                    audit_table=test_audit_table
                                                    )
        
        pdt.assert_frame_equal(test_audit_table, expected_output)


    # checked
    def test_audit_correct_deci_degrees(self):

        # setup 
        test_data = read_csv(
                            '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/seattle_cleaning_testing_files/11_correct_deci_degrees_input.csv',
                            dtype=CleaningUnitTesting.audit_table_dtypes
                            )
        
        test_audit_table = create_audit(audit_type='values')


        expected_output = read_csv(
                                    '/Users/IsaacSchnitler/Desktop/p_projects/SeattleCrimeData/test/audit_functions_testing_files/10_audit_correct_deci_degrees_output.csv',
                                    dtype=CleaningUnitTesting.audit_table_dtypes
                                    )
        
        expected_output.set_index(
                                keys=['audited_col', 'offense_id'],
                                inplace=True
                                )
        
        expected_output['batch'] = to_datetime('today').strftime('%Y-%m-%d')
        
        # test
        test_audit_table =  audit_correct_deci_degrees(
                                                        seattle_data=test_data,
                                                        audit_table=test_audit_table
                                                        )
        
        pdt.assert_frame_equal(test_audit_table, expected_output)


if __name__ == '__main__':
    unittest.main()