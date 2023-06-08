
from sys import path 
path.insert(1, '/Users/IsaacSchnitler/Desktop/Projects/SeattleCrimeData/src')

import clean_seattle_data as sc
import audit_functions as audit
import unittest
from pandas import read_csv
from pandas.testing import assert_frame_equal
from great_expectations import from_pandas


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


    def export_to_csv(self, input_df, expected_output):
        input_df.raw_data.to_csv(
                                path_or_buffer='actual_output.csv', 
                                index=False
                                )

        expected_output.raw_data.to_csv(
                                        path_or_buffer='expected_output.csv',
                                        index=False
                                        )


    def test_cleanup_whitespace(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/01_cleanup_whitespace_input.csv', 
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/01_cleanup_whitespace_output.csv', 
                                dtype='O'
                                )

        input_df.cleanup_whitespace()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_cleanup_column_casing(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/02_cleanup_column_casing_input.csv', 
                                                        dtype='O')
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/02_cleanup_column_casing_output.csv',
                                dtype='O'
                                )

        input_df.cleanup_column_casing()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_cleanup_na_values(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/03_cleanup_na_values_input.csv',
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/03_cleanup_na_values_output.csv', 
                                dtype='O'
                                )

        input_df.cleanup_na_values()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_clear_non_crimes(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/04_clear_non_crimes_input.csv', 
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/04_clear_non_crimes_output.csv',
                                dtype='O'
                                )

        input_df.clear_non_crimes()

        # self.display(input_df.raw_data, expected_output)
        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_cleanup_addresses(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/05_cleanup_addresses_input.csv',
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/05_cleanup_addresses_output.csv',
                                dtype='O'
                                )

        input_df.cleanup_addresses()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_cleanup_dtypes(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/06_cleanup_dtypes_input.csv', 
                                                        dtype='O'
                                                        )
                                        )
        
        flag_df = read_csv(
                        filepath_or_buffer='test/seattle_cleaning_testing_files/06_cleanup_dtypes_flag.csv'
                        )

        input_df.cleanup_dtypes()
        input_gdf = from_pandas(pandas_df=input_df.raw_data)

        for column, dtype in CleaningUnitTesting.correct_dtypes.items():
            
            if input_gdf.expect_column_values_to_be_of_type(column, dtype)['success']:
                assert_frame_equal(
                                left=input_df.raw_data,
                                right=input_df.raw_data
                                )

            else:
                print(f'The "{column}" does not maintain the intended "{dtype}" datatype.')
                assert_frame_equal(
                                left=input_df.raw_data,
                                right=flag_df
                                )


    def test_correct_offense_datetime(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/07_correct_offense_datetime_input.csv', 
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/07_correct_offense_datetime_output.csv',
                                dtype='O'
                                )

        input_df.correct_offense_datetime()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_cleanup_report_number(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/08_cleanup_report_number_input.csv',
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = sc.CleanSeattleData(raw_data=read_csv(
                                                            filepath_or_buffer='test/seattle_cleaning_testing_files/08_cleanup_report_number_output.csv',
                                                            dtype='O'
                                                            )
                                                )

        input_df.cleanup_dtypes()
        expected_output.cleanup_dtypes()

        input_df.cleanup_report_number()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output.raw_data
                        )


    def test_cleanup_misspelled_mcpp(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/09_cleanup_misspelled_mcpp_input.csv',
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/09_cleanup_misspelled_mcpp_output.csv',
                                dtype='O'
                                )

        input_df.cleanup_misspelled_mcpp()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_correct_na_loc_codes(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/10_correct_na_loc_codes_input.csv',
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/10_correct_na_loc_codes_output.csv',
                                dtype='O'
                                )

        input_df.correct_na_loc_codes()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_correct_deci_degrees(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/11_correct_deci_degrees_input.csv',
                                                        dtype={'longitude': 'float64', 'latitude': 'float64'}
                                                        )
                                        )       

        expected_output = read_csv(
                                    filepath_or_buffer='test/seattle_cleaning_testing_files/11_correct_deci_degrees_output.csv',
                                    dtype={
                                            'longitude': 'float64', 
                                            'latitude': 'float64'
                                            }
                                    )

        input_df.correct_deci_degrees()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


    def test_config_na_values(self):
        input_df = sc.CleanSeattleData(raw_data=read_csv(
                                                        filepath_or_buffer='test/seattle_cleaning_testing_files/12_config_na_values_input.csv',
                                                        dtype='O'
                                                        )
                                        )
        
        expected_output = read_csv(
                                filepath_or_buffer='test/seattle_cleaning_testing_files/12_config_na_values_output.csv',
                                dtype='O'
                                )

        input_df.config_na_values()

        assert_frame_equal(
                        left=input_df.raw_data,
                        right=expected_output
                        )


if __name__ == '__main__':
    unittest.main()