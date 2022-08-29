import os
import numpy as np
import pandas as pd

from fuzzywuzzy import process
from dotenv import load_dotenv

load_dotenv()


class CleanSeattleData:

    # List of designated, common missing values
    missing_values = ['UNKNOWN', '99', 99, 'OOJ', '<NULL>', '<Null>', 'NULL',
                      'null', 'nil', 'empty', '-', 'NA', 'n/a', 'na']

    # Create dataframe containing valid, corresponding precinct/sector/beat location codes
    loc_codes = pd.read_csv(os.getenv('LOC_CODES'), dtype='O')

    # Create dataframe containing valid, corresponding precinct/mcpp location codes
    mcpp = pd.read_csv(os.getenv('MCPP'), dtype='O')

    def __init__(self, raw_data):
        self.raw_data = raw_data

        self.audit_table = audit_table()

    def cleanup_whitespace(self):
        """
        Summary: Corrects various whitespace issues, such as:
                > Strips any leading/ending whitespace
                > In columns where there isn't supposed to be whitespace, removes the whitespace
                > In columns where there is supposed to be whitespace:
                    -Replace multiple spaces with a single space
                    -Replace spaced dashes/forward slashes with non-spaced dashes/forward slashes
                        -i.e. word - word -> word-word
                        -i.e. word / word -> word/word

                    -Replace non-spaced ampersands with spaced ampersands
                        -i.e. word&word -> word & word
        """

        # Regex strings used to identify various whitespace issues
        multi_space_re = r'\s{2,}'
        dash_re = r'\s+-\s+'
        slash_re = r'\s+/\s+'
        amp_re = r'(\w)\s*&\s*(\w)'

        # Strip the values for each column of any leading/ending whitespaces
        for column in list(self.raw_data):
            self.raw_data[f'{column}'] = self.raw_data[f'{column}'].str.strip().replace('', np.nan)

            # For columns that are not supposed to have spaces
            if column in ['report_number', 'offense_id', 'group_a_b', 'crime_against_category',
                          'offense_code', 'precinct', 'sector', 'beat']:

                # Remove any spaces
                self.raw_data[f'{column}'] = self.raw_data[f'{column}'].str.replace(' ', '')

            # For columns that are supposed to have spaces
            if column in ['offense_parent_group', 'offense', 'mcpp', '_100_block_address']:
                # Replace multiple spaces with a SINGLE SPACE
                self.raw_data[column] = self.raw_data[column].str.replace(multi_space_re, ' ', regex=True)

                # Replace spaced dashes with SINGLE DASH
                self.raw_data[column] = self.raw_data[column].str.replace(dash_re, '-', regex=True)

                # Replace spaced forward slashes with SINGLE FORWARD SLASH
                self.raw_data[column] = self.raw_data[column].str.replace(slash_re, '/', regex=True)

                # Replace non-spaced ampersand with a SPACED AMPERSAND
                self.raw_data[column] = self.raw_data[column].str.replace(amp_re, '\\1 & \\2', regex=True)

    def cleanup_column_casing(self):
        """
        Summary: Uppercase any columns containing letters.
        """

        # For columns with letters
        for column in ['group_a_b', 'crime_against_category', 'offense_parent_group', 'offense',
                       'offense_code', 'precinct', 'sector', 'beat', 'mcpp', '_100_block_address']:

            # Uppercase those letters
            self.raw_data[column] = self.raw_data[column].str.upper()

    def cleanup_na_values(self):
        """
        Summary: Replaces any common, designated missing values (specified in the
                missing_values class attribute) or column specific missing value indicators
                with a np.nan value.
        """

        # For each column
        for column in list(self.raw_data):
            # Replace specific, designated missing values with a np.nan value
            self.raw_data.loc[self.raw_data[column].isin(CleanSeattleData.missing_values), [column]] = np.nan

        # The _100_block_address column uses a distinct address value as a
        # sort of missing value indicator/placeholder/etc. These distinct address
        # values contain the OFTH, OFND and OFRD non-existant street names
        self.raw_data.loc[
            self.raw_data['_100_block_address'].str.contains(r"OFTH|OFND|OFRD", na=False, regex=True),
                         ['_100_block_address']
                         ] = np.nan

        # Longitude/latitude use 0's as missing longitude/latitude values
        for column in ['latitude', 'longitude']:
            self.raw_data.loc[self.raw_data[column].str.contains(r'^0', regex=True), [column]] = np.nan

    def clear_non_crimes(self):
        """
        Summary: Removes records with a 'NOT_A_CRIME' value in the crime_against_category. These
                records represent justifiable homicides (self-defense), which are not crimes but
                are still reported. For the purpose and scope of this project/data, I am interested
                only in crimes.
        """

        # Drop records with a 'NOT_A_CRIME' value in the crime_against_category column.
        if (self.raw_data['crime_against_category'] == 'NOT_A_CRIME').any():
            self.raw_data.drop(
                                labels=self.raw_data[self.raw_data['crime_against_category'] == 'NOT_A_CRIME'].index,
                                inplace=True
                               )

    def cleanup_addresses(self):
        """
        Summary: Corrects a variety of address value issues, such as:
                    > Replace X's with 0's in block information
                        -i.e. 5XX BLOCK OF PIKE ST -> 500 BLOCK OF PIKE ST

                    > Fixes inconsistent road labels
                        -i.e. AV -> AVE
        """

        # Regex strings used below
        x_re = r'\d+X+\s'
        ave_re = r'\s+AV\s+'

        # Replace X's (5XX BLOCK OF PIKE ST) with 0's (500 BLOCK OF PIKE ST)
        self.raw_data.loc[
            self.raw_data['_100_block_address'].str.contains(x_re, regex=True, na=False), ['_100_block_address']
                         ] = self.raw_data['_100_block_address'].str.replace('X', '0')

        # Use consistent avenue abbreviations, replace any instances of 'AV' with 'AVE'
        self.raw_data.loc[
            self.raw_data['_100_block_address'].str.contains(ave_re, na=False, regex=True), ['_100_block_address']
                         ] = self.raw_data['_100_block_address'].str.replace(r'AV', 'AVE', regex=True)

    def cleanup_dtypes(self):
        """
        Summary: Convert datetime and numeric columns into their respective data types,
                meanwhile tracking and nullifying values that do not conform to the data type
                through the audit table.
        """

        # For each datetime column ...
        for column in ['offense_start_datetime', 'offense_end_datetime', 'report_datetime']:

            # Retrieve and store records that do not conform to datetime format
            audited_values = self.raw_data[column].notnull() & pd.to_datetime(self.raw_data[column],
                                                                              errors='coerce').isna()

            # For those records, retrieve the unique identifier (offense_id) and the value
            # in question. Insert these values into audit table to track what values are being
            # made null
            audited_values = self.raw_data[audited_values][['offense_id', column]]
            self.audit_table = audit_insert(self.audit_table, audited_values, 1)

            # Then perform the data conversion to datetime
            self.raw_data[column] = pd.to_datetime(self.raw_data[column], errors='coerce')

        # Do the same with the numeric columns as done with the datetime columns
        for column in ['longitude', 'latitude']:

            audited_values = self.raw_data[column].notnull() & pd.to_numeric(self.raw_data[column],
                                                                             errors='coerce').isna()

            audited_values = self.raw_data[audited_values][['offense_id', column]]
            self.audit_table = audit_insert(self.audit_table, audited_values, 2)

            self.raw_data[column] = pd.to_numeric(self.raw_data[column], errors='coerce')

    def correct_offense_datetime(self):
        """
        Summary: Audit and nullify offense start/end datetime values where the offense start
                datetime is greater than, or after, the offense end datetime
        """

        # Retrieve and store records where the offense start datetime is after
        # the offense end datetime. From these, retrieve the offense_id and values in question
        audited_values = self.raw_data[self.raw_data['offense_start_datetime']
                                       > self.raw_data['offense_end_datetime']] \
                                       [['offense_id', 'offense_start_datetime', 'offense_end_datetime']]

        # Insert these values into audit table to track the values being made null
        self.audit_table = audit_insert(self.audit_table, audited_values, 3)

        # Nullify these offense start/end datetime values
        self.raw_data.loc[self.raw_data['offense_start_datetime']
                          > self.raw_data['offense_end_datetime'],
                          ['offense_start_datetime', 'offense_end_datetime']] = np.nan

    def cleanup_report_number(self):
        """
        Summary: Attempt to correct report numbers that do not conform to the valid format: four digits
                and six digits separated by a dash (1234-567890). This method can fix a variety of report
                numbers issues, such as:
                    > Replace letter o/O's with 0's
                        -i.e. 1234-56789O -> 1234-567890

                    > Replace common, invalid delimiters with the correct delimiter (-)
                        -i.e. 1234_567890  -> 1234-567890

                    > Insert a delimiter into report numbers with no delimiter but that
                      have the correct number of digits
                        -i.e. 1234567890 -> 1234-567890

                    > Insert a delimiter into report numbers with no delimiter but 1+
                      zeroes between four and six digits
                        -i.e. 1234000567890 -> 1234-567890

                    > Report numbers with long back digits (>6) led by 0's and removes the
                      leading 0's
                        -i.e. 1234-000567890 -> 1234-567890

                    > Report numbers with short front digits (<4) are replaced by the year
                      of the report date (a common pattern with in the data set)
                        -i.e. 12-567890 -> 2022-567890
        """

        # Regex strings used below
        valid_re = r'^\d{4}-\d{6}$'
        o_re = r'[O,o]'
        delim_re = r'^\d*[=,_]\d*$'
        no_delim_re = r'^\d{10}$'
        zero_delim_re = r'^\d{4}0+\d{6}$'
        long_re = r'-0+\d{6,}$'
        short_re = r'^\d{,3}-'

        # Check if any report numbers do not follow the valid format (1234-567890)
        if (~self.raw_data['report_number'].str.contains(valid_re, regex=True, na=False)).any():

            # Replace LETTER O's
            if self.raw_data['report_number'].str.contains(o_re, regex=True, na=False).any():

                self.raw_data.loc[
                    self.raw_data['report_number'].str.contains(o_re, regex=True, na=False), ['report_number']
                                 ] = self.raw_data['report_number'].str.replace('O', '0').str.replace('o', '0')

            # Corrects incorrect DELIMITERS
            if self.raw_data['report_number'].str.contains(delim_re, regex=True, na=False).any():

                self.raw_data.loc[
                    self.raw_data['report_number'].str.contains(delim_re, regex=True, na=False), ['report_number']
                                 ] = self.raw_data['report_number'].str.split('=|_|\\*|,|;|/|\\\\').str[0] \
                                     + '-' \
                                     + self.raw_data['report_number'].str.split('=|_|\\*|,|;|/|\\\\').str[1]

            # Corrects NO DELIMITERS (with correct number of digits)
            if self.raw_data['report_number'].str.contains(no_delim_re, regex=True, na=False).any():

                self.raw_data.loc[
                    self.raw_data['report_number'].str.contains(no_delim_re, regex=True, na=False), ['report_number']
                                 ] = self.raw_data['report_number'].str[:4] \
                                     + '-'\
                                     + self.raw_data['report_number'].str[-6:]

            # Corrects any ZERO PLACEHOLDER/DELIMITERS
            if self.raw_data['report_number'].str.contains(zero_delim_re, regex=True, na=False).any():

                self.raw_data.loc[
                    self.raw_data['report_number'].str.contains(zero_delim_re, regex=True, na=False), ['report_number']
                                 ] = self.raw_data['report_number'].str[:4] \
                                     + '-' \
                                     + self.raw_data['report_number'].str[-6:]

            # Corrects any LONG BACK HALF DIGITS
            if self.raw_data['report_number'].str.contains(long_re, regex=True, na=False).any():

                self.raw_data.loc[
                    self.raw_data['report_number'].str.contains(long_re, regex=True, na=False), ['report_number']
                                 ] = self.raw_data['report_number'].str.split('-').str[0] \
                                     + '-' \
                                     + self.raw_data['report_number'].str[-6:]

            # Corrects any SHORT FRONT DIGITS
            if self.raw_data['report_number'].str.contains(short_re, regex=True, na=False).any():

                self.raw_data.loc[
                    self.raw_data['report_number'].str.contains(short_re, regex=True, na=False), ['report_number']
                                 ] = self.raw_data['report_datetime'].dt.strftime('%Y') \
                                     + '-' \
                                     + self.raw_data['report_number'].str[-6:]

            # Null any INVALID REPORT NUMBERS
            if (~self.raw_data['report_number'].str.contains(valid_re, regex=True, na=False)).any():

                # Retrieve and store the offense_id and value in question
                audited_values = self.raw_data[~self.raw_data['report_number'].str.contains(valid_re,
                                                                                            regex=True,
                                                                                            na=False)] \
                                                                                       [['offense_id', 'report_number']]

                # Insert these values into audit table to track what values are made null
                self.audit_table = audit_insert(self.audit_table, audited_values, 4)

                # Finally, null these values
                self.raw_data.loc[
                    ~self.raw_data['report_number'].str.contains(valid_re, regex=True, na=False), ['report_number']
                                 ] = np.nan

    def cleanup_misspelled_mcpp(self):
        """
        Summary: Attempts to correct invalid (potentially misspelled) mcpp (micro-community) values with
                a valid mcpp value using fuzzy string matching and a list of all the valid mcpp's (the mcpp
                class attribute).

                First, it checks if records with an invalid mcpp also have a valid precinct location code
                (each mcpp belongs to a corresponding precinct). If so, narrow the list of mcpp's to those
                belonging to that precinct and if that match is >85%, make the match.

                Records without a valid precinct location code attempt to make the match from the entire
                list of mcpp's, making the match if the match is >90%

                Records that don't make the match are tracked with the audit table and are nullified.
        """

        # Checks for records where mcpp is invalid and not null.
        # These record's mcpp value could be misspelled
        if (self.raw_data[(~self.raw_data['mcpp'].isin(CleanSeattleData.mcpp['mcpp'])) &
                          (~self.raw_data['mcpp'].isna())]).any:

            # If so, check if those same records have a valid/present precinct code.
            # If the record has a valid/present precinct, we can narrow down the possible mcpp values
            if (self.raw_data[(~self.raw_data['mcpp'].isin(CleanSeattleData.mcpp['mcpp'])) &
                              (~self.raw_data['mcpp'].isna()) &
                              (self.raw_data['precinct'].isin(CleanSeattleData.mcpp['precinct']))]).any:

                # If so, assign those records to a dataframe
                precinct_exists = self.raw_data[(~self.raw_data['mcpp'].isin(CleanSeattleData.mcpp['mcpp'])) &
                                                (~self.raw_data['mcpp'].isna()) &
                                                (self.raw_data['precinct'].isin(CleanSeattleData.mcpp['precinct']))]

                # Iterate through those records
                for index, row in precinct_exists.iterrows():

                    # If the match is > 85%, fill the invalid mcpp with the match
                    if process.extractOne(
                                         row['mcpp'],
                                         CleanSeattleData.mcpp.loc[CleanSeattleData.mcpp['precinct'] == row['precinct']]['mcpp']
                                         )[1] > 85:

                        self.raw_data.loc[self.raw_data['offense_id'] == row['offense_id'], ['mcpp']] = \
                            process.extractOne(
                                        row['mcpp'],
                                        CleanSeattleData.mcpp.loc[CleanSeattleData.mcpp['precinct'] == row['precinct']]['mcpp']
                                               )[0]

                    else:
                        # Otherwise, audit the record and null the mcpp value
                        audited_values = self.raw_data[self.raw_data['offense_id'] == row['offense_id']] \
                                                                                                [['offense_id', 'mcpp']]

                        self.audit_table = audit_insert(self.audit_table, audited_values, 5)
                        self.raw_data.loc[self.raw_data['offense_id'] == row['offense_id'], ['mcpp']] = np.nan

            # If the record's mcpp is invalid but not null
            # and said record does not have a valid/present precinct code
            if (self.raw_data[(~self.raw_data['mcpp'].isin(CleanSeattleData.mcpp['mcpp'])) &
                              (~self.raw_data['mcpp'].isna()) &
                              (~self.raw_data['precinct'].isin(CleanSeattleData.mcpp['precinct']))]).any:

                # Assign such records to a dataframe
                precinct_doesnt_exist = self.raw_data[(~self.raw_data['mcpp'].isin(CleanSeattleData.mcpp['mcpp'])) &
                                                      (~self.raw_data['mcpp'].isna()) &
                                                      (~self.raw_data['precinct'].isin(CleanSeattleData.loc_codes['precinct']))]

                # Iterate through the dataframe
                for index, row in precinct_doesnt_exist.iterrows():

                    # If the proposed match is > 90% then fill the invalid mcpp value with the match
                    if process.extractOne(row['mcpp'], CleanSeattleData.mcpp['mcpp'])[1] > 90:
                        self.raw_data.loc[self.raw_data['offense_id'] == row['offense_id'], ['mcpp']] = \
                                                       process.extractOne(row['mcpp'], CleanSeattleData.mcpp['mcpp'])[0]

                    else:
                        # Otherwise, audit and null the invalid mcpp value
                        audited_values = self.raw_data[self.raw_data['offense_id'] == row['offense_id']] \
                                                                                                [['offense_id', 'mcpp']]

                        self.audit_table = audit_insert(self.audit_table, audited_values, 6)
                        self.raw_data.loc[self.raw_data['offense_id'] == row['offense_id'], ['mcpp']] = np.nan

    #def correct_mismatched_loc_codes(self):
        #self.raw_data['sector']

    def correct_na_loc_codes(self):
        """
        Summary: Attempts to fill in missing location codes using information from lower-level location
        codes (i.e. beat B1 belongs to sector B which belongs to precinct N) (i.e. mcpp Alki belongs
        to precinct SW).

        Beats (low-level) can determine sectors (high-level), sectors (low-level) can determine
        precincts (high-level), and mcpps (low-level) can determine precincts (high-level).

        """
        loc_code_pairings = [('sector', 'beat'), ('precinct', 'sector')]

        # Audit any beat location codes that are invalid but not null, then
        # null those values
        audited_values = self.raw_data.loc[~(self.raw_data['beat'].isin(CleanSeattleData.loc_codes['beat'])) &
                                           ~(self.raw_data['beat'].isna())] \
                                           [['offense_id', 'beat']]

        self.audit_table = audit_insert(self.audit_table, audited_values, 7)
        self.raw_data.loc[~(self.raw_data['beat'].isin(CleanSeattleData.loc_codes['beat'])), ['beat']] = np.nan

        # Valid low-level location codes can determine unknown high-level location codes.
        # For instance, if we have a missing sector code but a valid/present beat code,
        # we can then determine the appropriate missing sector code.
        for high_loc, low_loc in loc_code_pairings:

            # Check for any records with invalid/missing high-level location codes
            # that also have a valid/present low-level location code
            if ((~self.raw_data[high_loc].isin(CleanSeattleData.loc_codes[high_loc])) &
                (self.raw_data[low_loc].isin(CleanSeattleData.loc_codes[low_loc]))).any():

                # Assign those records to a dataframe
                missing_sectors = self.raw_data.loc[(~self.raw_data[high_loc].isin(CleanSeattleData.loc_codes[high_loc])) &
                                                    (self.raw_data[low_loc].isin(CleanSeattleData.loc_codes[low_loc]))]

                # Iterate through these records and fill the invalid/missing high-level
                # location code with its corresponding valid location code
                for index, row in missing_sectors.iterrows():
                    self.raw_data.loc[self.raw_data['offense_id'] == row['offense_id'], [high_loc]] = \
                                CleanSeattleData.loc_codes.loc[CleanSeattleData.loc_codes[low_loc] == row[low_loc], high_loc].iloc[0]

            # Audit and null any high-level location codes that do not have a
            # valid/present low-level location code
            audited_values = self.raw_data.loc[~(self.raw_data[high_loc].isin(CleanSeattleData.loc_codes[high_loc])) &
                                               ~(self.raw_data[high_loc].isna())] \
                                               [['offense_id', high_loc]]

            self.audit_table = audit_insert(self.audit_table, audited_values, 8)

            self.raw_data.loc[~(self.raw_data[high_loc].isin(CleanSeattleData.loc_codes[high_loc])), [high_loc]] = np.nan

        # Here I do the same as above, except for the precinct and mcpp. It's separate from the
        # above piece of code because mcpp does not correspond with sector and beat location codes.
        for high_loc, low_loc in [('precinct', 'mcpp')]:
            if ((~self.raw_data[high_loc].isin(CleanSeattleData.mcpp[high_loc])) &
                (self.raw_data[low_loc].isin(CleanSeattleData.mcpp[low_loc]))).any():

                missing_sectors = self.raw_data.loc[(~self.raw_data[high_loc].isin(CleanSeattleData.mcpp[high_loc])) &
                                                    (self.raw_data[low_loc].isin(CleanSeattleData.mcpp[low_loc]))]

                for index, row in missing_sectors.iterrows():
                    self.raw_data.loc[self.raw_data['offense_id'] == row['offense_id'], [high_loc]] = \
                                CleanSeattleData.mcpp.loc[CleanSeattleData.mcpp[low_loc] == row[low_loc], high_loc].iloc[0]

            audited_values = self.raw_data.loc[~(self.raw_data[high_loc].isin(CleanSeattleData.mcpp[high_loc])) &
                                               ~(self.raw_data[high_loc].isna())] \
                                               [['offense_id', high_loc]]

            self.audit_table = audit_insert(self.audit_table, audited_values, 9)
            self.raw_data.loc[~(self.raw_data[high_loc].isin(CleanSeattleData.loc_codes[high_loc])), [high_loc]] = np.nan

    def correct_deci_degrees(self):
        """
        Summary: Checks if the longitude and latitude are within Washington State's longitude and
                latitude; if not, audit and nullify these values.
        """

        # Confirm the longitude is at least within Washington's longitude range
        # Otherwise audit and null the value
        if (self.raw_data[~(self.raw_data['longitude'].between(-125.0, -116.5))]).any:
            audited_values = self.raw_data[~(self.raw_data['longitude'].between(-125.0, -116.5))] \
                            [['offense_id', 'longitude']]

            self.audit_table = audit_insert(self.audit_table, audited_values, 10)

            self.raw_data.loc[
                ~(self.raw_data['longitude'].between(-125.0, -116.5)), ['longitude']
                            ] = np.nan

        # Confirm the latitude is at least within Washington's latitude range
        # Otherwise null the value
        if (self.raw_data[~(self.raw_data['latitude'].between(45.5, 49.0))]).any:
            audited_values = self.raw_data[~(self.raw_data['latitude'].between(45.5, 49.0))] \
                [['offense_id', 'latitude']]

            self.audit_table = audit_insert(self.audit_table, audited_values, 11)

            self.raw_data.loc[
                ~(self.raw_data['latitude'].between(45.5, 49.0)), ['latitude']
                             ] = np.nan

    def cleanup_column_order(self):
        """
        Summary: Orders the columns in a consistent manner.
        """

        self.raw_data = self.raw_data[['report_number', 'offense_id', 'offense_start_datetime', 'offense_end_datetime',
                                       'report_datetime', 'group_a_b', 'crime_against_category', 'offense_parent_group',
                                       'offense', 'offense_code', 'precinct', 'sector', 'beat', 'mcpp',
                                       '_100_block_address', 'longitude', 'latitude']]

    def config_addresses(self):
        """
        Summary: Breaks apart/separates multivalued address values into two, separate rows:
                    ... 26TH AVE NE / NE BLAKELEY ST -> ... 26TH AVE NE
                                                        ... NE BLAKELEY ST
        """

        def split_address(address_col: pd.Series):
            return address_col.str.split('/', expand=True)

        columns = [col for col in list(self.raw_data) if col != '_100_block_address']

        # Create a df with the address data split into two columns
        split_address_df = split_address(self.raw_data['_100_block_address'])

        # Assign the split address df a multi-index consisting of every column except the '_100_block_address' column
        split_address_df.index = self.raw_data.set_index(columns).index

        # Replace null values (np.nan) with a non-null value ('na') so the row is not lost
        # in the following command
        split_address_df[0].fillna('na', inplace=True)

        self.raw_data = split_address_df.stack().reset_index(columns)

        # Rename the column
        self.raw_data.rename(columns={0: '_100_block_address'}, inplace=True)

        # Revert the non-null value back to a null value
        self.raw_data['_100_block_address'].replace('na', np.nan, inplace=True)

    def config_na_values(self):
        """
        Summary: Changes all np.NaN and np.NaT values to None, so when entered
                into SQL Server database, missing values can be entered as NULL.
        """

        # Convert all columns to Object data type to allow for None
        for col in list(self.raw_data):
            self.raw_data[col] = self.raw_data[col].astype('O')

        # Replace all np.NaN and np.NaT to None values
        # so when inserted into SQL Server database, it converts cleanly to NULL
        self.raw_data.where(pd.notnull(self.raw_data), None, inplace=True)


def audit_table():
    """
    Summary: Creates audit table to store information for values that were nullified in
            the cleaning process.
    """

    # Create and return the audit table
    the_audit_table = pd.DataFrame(columns=['audited_col', 'offense_id', 'audited_val', 'audited_reason_id'])
    return the_audit_table.set_index(['audited_col', 'offense_id'])


def audit_insert(source_audit_table, target_audit_table, audited_reason_id):
    """
    Summary: Takes a source_audit_table argument which is used as the source table in
            the merging operation. The target_audit_table argument represents a dataframe
            containing nullified values, and is used as the target table in the merging
            operation. The audit_reason_id argument represents an ID describes for what
            reason the value was nullified.
    """

    # Take the target_audit_table and re-structure it to match that of the
    # source_audit_table (the audit table) so it can be merged
    target_audit_table.set_index('offense_id', inplace=True)

    target_audit_table = pd.DataFrame(target_audit_table.stack())

    target_audit_table = target_audit_table.swaplevel()

    target_audit_table.sort_index(inplace=True)
    target_audit_table['audited_reason_id'] = audited_reason_id

    target_audit_table.rename(columns={0: 'audited_val'}, inplace=True)
    target_audit_table.index.names = ['audited_col', 'offense_id']

    # Return the merged tables
    return source_audit_table.combine_first(target_audit_table)


def clean(raw_data):
    """
    Summary: Brings together all cleaning activities/methods and performs
            them in the intended order.
    """

    cleaning_process = CleanSeattleData(raw_data)

    cleaning_process.cleanup_whitespace()
    cleaning_process.cleanup_column_casing()
    cleaning_process.cleanup_na_values()

    cleaning_process.clear_non_crimes()

    cleaning_process.cleanup_addresses()
    cleaning_process.cleanup_dtypes()

    cleaning_process.correct_offense_datetime()

    cleaning_process.cleanup_report_number()

    cleaning_process.cleanup_misspelled_mcpp()

    # cleaning_process.correct_mismatched_loc_codes
    cleaning_process.correct_na_loc_codes()
    cleaning_process.correct_deci_degrees()

    cleaning_process.cleanup_column_order()

    cleaning_process.config_addresses()
    cleaning_process.config_na_values()

    return cleaning_process.raw_data, cleaning_process.audit_table


if __name__ == '__main__':
    clean()







