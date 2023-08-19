from pandas import read_csv
from unittest import TestCase, main
from great_expectations import from_pandas
from pandas.testing import assert_frame_equal

from ..src.clean_seattle_data import ( 
                                    cleanup_whitespace,
                                    cleanup_column_casing,
                                    cleanup_na_values,
                                    clear_non_crimes,
                                    cleanup_addresses,
                                    cleanup_dtypes,
                                    correct_offense_datetime,
                                    cleanup_report_number,
                                    cleanup_misspelled_mcpp,
                                    correct_na_loc_codes,
                                    correct_deci_degrees,
                                    config_na_values
                                    )   





class CleaningUnitTesting(TestCase):

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
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/01_cleanup_whitespace_input.csv', 
                            dtype='O'
                            )
                                
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/01_cleanup_whitespace_output.csv', 
                                dtype='O'
                                )

        input_df = cleanup_whitespace(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_cleanup_column_casing(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/02_cleanup_column_casing_input.csv', 
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/02_cleanup_column_casing_output.csv',
                                dtype='O'
                                )

        input_df = cleanup_column_casing(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_cleanup_na_values(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/03_cleanup_na_values_input.csv',
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/03_cleanup_na_values_output.csv', 
                                dtype='O'
                                )

        input_df = cleanup_na_values(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_clear_non_crimes(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/04_clear_non_crimes_input.csv', 
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/04_clear_non_crimes_output.csv',
                                dtype='O'
                                )

        input_df = clear_non_crimes(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_cleanup_addresses(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/05_cleanup_addresses_input.csv',
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/05_cleanup_addresses_output.csv',
                                dtype='O'
                                )

        input_df = cleanup_addresses(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_cleanup_dtypes(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/06_cleanup_dtypes_input.csv', 
                            dtype='O'
                            )
                                        
        
        flag_df = read_csv(
                        filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/06_cleanup_dtypes_flag.csv'
                        )

        input_df = cleanup_dtypes(seattle_data=input_df)
        input_gdf = from_pandas(pandas_df=input_df)

        for column, dtype in CleaningUnitTesting.correct_dtypes.items():
            
            if input_gdf.expect_column_values_to_be_of_type(column, dtype)['success']:
                assert_frame_equal(
                                left=input_df,
                                right=input_df
                                )

            else:
                print(f'The "{column}" column does not maintain the intended "{dtype}" data type.')
                assert_frame_equal(
                                left=input_df,
                                right=flag_df
                                )


    def test_correct_offense_datetime(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/07_correct_offense_datetime_input.csv', 
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/07_correct_offense_datetime_output.csv',
                                dtype='O'
                                )

        input_df = correct_offense_datetime(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_cleanup_report_number(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/08_cleanup_report_number_input.csv',
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                    filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/08_cleanup_report_number_output.csv',
                                    dtype='O'
                                    )
                                                

        input_df        = cleanup_dtypes(seattle_data=input_df)
        expected_output = cleanup_dtypes(seattle_data=input_df)

        input_df        = cleanup_report_number(seattle_data=input_df)

        assert_frame_equal(
                            left=input_df,
                            right=expected_output
                            )


    def test_cleanup_misspelled_mcpp(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/09_cleanup_misspelled_mcpp_input.csv',
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/09_cleanup_misspelled_mcpp_output.csv',
                                dtype='O'
                                )

        input_df = cleanup_misspelled_mcpp(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_correct_na_loc_codes(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/10_correct_na_loc_codes_input.csv',
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/10_correct_na_loc_codes_output.csv',
                                dtype='O'
                                )

        input_df = correct_na_loc_codes(input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_correct_deci_degrees(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/11_correct_deci_degrees_input.csv',
                            dtype={'longitude': 'float64', 'latitude': 'float64'}
                            )
                                        

        expected_output = read_csv(
                                    filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/11_correct_deci_degrees_output.csv',
                                    dtype={
                                            'longitude': 'float64', 
                                            'latitude' : 'float64'
                                            }
                                    )

        input_df = correct_deci_degrees(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


    def test_config_na_values(self):
        input_df = read_csv(
                            filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/12_config_na_values_input.csv',
                            dtype='O'
                            )
                                        
        
        expected_output = read_csv(
                                filepath_or_buffer='SeattleCrimeData/test/seattle_cleaning_testing_files/12_config_na_values_output.csv',
                                dtype='O'
                                )

        input_df = config_na_values(seattle_data=input_df)

        assert_frame_equal(
                        left=input_df,
                        right=expected_output
                        )


if __name__ == '__main__':
    main()