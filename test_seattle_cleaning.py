import seattle_cleaning as sc
import unittest
import pandas as pd
import pandas.testing as pdt
import great_expectations as ge

pd.options.display.width = None
pd.options.display.max_columns = None


class CleaningUnitTesting(unittest.TestCase):

    correct_dtypes = {'report_number':              'object',
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
                      'street_2':                   'object'}

    def display(self, input_df, expected_output):
        print(f'The output is:')
        print(input_df)
        print(f'\nThe expected output is:')
        print(expected_output)

    def test_cleanup_whitespace(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/01_cleanup_whitespace_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/01_cleanup_whitespace_output.csv', dtype='O')

        input_df.cleanup_whitespace()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_cleanup_column_casing(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/02_cleanup_column_casing_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/02_cleanup_column_casing_output.csv', dtype='O')

        input_df.cleanup_column_casing()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_cleanup_na_values(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/03_cleanup_na_values_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/03_cleanup_na_values_output.csv', dtype='O')

        input_df.cleanup_na_values()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_clear_non_crimes(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/04_clear_non_crimes_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/04_clear_non_crimes_output.csv', dtype='O')

        input_df.clear_non_crimes()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_cleanup_addresses(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/05_cleanup_addresses_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/05_cleanup_addresses_output.csv', dtype='O')

        input_df.cleanup_addresses()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_cleanup_dtypes(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/06_cleanup_dtypes_input.csv', dtype='O'))
        flag_df = pd.read_csv('unit_testing_files/06_cleanup_dtypes_flag.csv')

        input_df.cleanup_dtypes()
        input_gdf = ge.from_pandas(input_df.raw_data)

        for column, dtype in CleaningUnitTesting.correct_dtypes.items():
            if input_gdf.expect_column_values_to_be_of_type(column, dtype)['success']:
                pdt.assert_frame_equal(input_df.raw_data, input_df.raw_data)

            else:
                print(f'The "{column}" does not maintain the intended "{dtype}" datatype.')
                pdt.assert_frame_equal(input_df.raw_data, flag_df)

    def test_correct_offense_datetime(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/07_correct_offense_datetime_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/07_correct_offense_datetime_output.csv', dtype='O')

        input_df.correct_offense_datetime()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_cleanup_report_number(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/08_cleanup_report_number_input.csv', dtype='O'))
        expected_output = sc.CleanSeattleData(pd.read_csv('unit_testing_files/08_cleanup_report_number_output.csv', dtype='O'))

        input_df.cleanup_dtypes()
        expected_output.cleanup_dtypes()

        input_df.cleanup_report_number()

        # self.display(input_df.raw_data, expected_output.raw_data)
        pdt.assert_frame_equal(input_df.raw_data, expected_output.raw_data)

    def test_cleanup_misspelled_mcpp(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/09_cleanup_misspelled_mcpp_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/09_cleanup_misspelled_mcpp_output.csv', dtype='O')

        input_df.cleanup_misspelled_mcpp()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_correct_na_loc_codes(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/10_correct_na_loc_codes_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/10_correct_na_loc_codes_output.csv', dtype='O')

        input_df.correct_na_loc_codes()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_correct_deci_degrees(self):
        input_df = sc.CleanSeattleData(
            pd.read_csv('unit_testing_files/11_correct_deci_degrees_input.csv',
                        dtype={'longitude': 'float64', 'latitude': 'float64'})
                              )

        expected_output = pd.read_csv('unit_testing_files/11_correct_deci_degrees_output.csv',
                                      dtype={'longitude': 'float64', 'latitude': 'float64'})

        input_df.correct_deci_degrees()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_config_na_values(self):
        input_df = sc.CleanSeattleData(pd.read_csv('unit_testing_files/12_config_na_values_input.csv', dtype='O'))
        expected_output = pd.read_csv('unit_testing_files/12_config_na_values_output.csv', dtype='O')

        input_df.config_na_values()

        # self.display(input_df.raw_data, expected_output)
        pdt.assert_frame_equal(input_df.raw_data, expected_output)

    def test_audit_table(self):
        expected_output = pd.read_csv('unit_testing_files/13_audit_table_output.csv')
        expected_output.set_index(['audited_col', 'offense_id'], inplace=True)

        input_df = sc.audit_table()

        pdt.assert_frame_equal(input_df, expected_output)

    def test_audit_insert(self):
        expected_output = pd.read_csv('unit_testing_files/14_audit_insert_output.csv', dtype='O')
        expected_output.set_index(['audited_col', 'offense_id'], inplace=True)

        target_table = pd.read_csv('unit_testing_files/14_audit_insert_input.csv', dtype='O')

        input_df = sc.audit_table()
        input_df = sc.audit_insert(input_df, target_table, '3')

        # self.display(input_df, expected_output)
        pdt.assert_frame_equal(input_df, expected_output)


if __name__ == '__main__':
    unittest.main()
