
from os import getenv
import numpy as np
from numpy import nan
import pandas as pd
from pandas import read_csv, to_datetime, to_numeric, DataFrame, notnull, merge

from fuzzywuzzy.process import extractOne
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


class CleanSeattleData:
    """
        Summary: A collection of (14) cleaning methods specific to SPD crime data. An instance
                 of this class represents an instance of the 'cleaning process'. The order 
                 the methods are defined in is the order intended to be executed in.

                 To simplify the cleaning process, the clean() function was made, which will 
                 create an instance of the CleanSeattleData class with the raw crime data and 
                 execute methods in the correct order, returning the clean data set and the 
                 audit table with identified invalid values

        Params:

                 raw_data: the raw SPD crime data must be passed as a param 
    """

    # List of designated, common missing values representations
    missing_values = ['UNKNOWN', '99', 99, 'OOJ', '<NULL>', '<Null>', 'NULL',
                      'null', 'nil', 'empty', '-', 'NA', 'n/a', 'na']

    # Load dataframe containing valid, corresponding precinct/sector/beat location code matchings, used to
    # verify raw data against
    loc_codes = read_csv(getenv('LOC_CODES'), dtype='O')

    # Load dataframe containing valid, corresponding precinct/mcpp location code pairings used to verify 
    # raw data against
    mcpp = read_csv(getenv('MCPP'), dtype='O')

    def __init__(self, raw_data):
        self.raw_data = raw_data

        self.audit_table = create_audit_table()

    def cleanup_whitespace(self):
        """
        Summary: Corrects various whitespace issues, such as:
                > Strips any leading/ending whitespaces
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

            # Replace empty string columns w/nan
            self.raw_data[f'{column}'] = self.raw_data[f'{column}'].str.strip().replace('', nan)

            # For columns that are not supposed to have spaces
            if column in ['report_number', 'offense_id', 'group_a_b', 'crime_against_category',
                          'offense_code', 'precinct', 'sector', 'beat']:

                # Replace spaces with no space
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
        Summary: Make text column casing consistent; uppercase any columns containing letters
        """

        # For columns with letters
        for column in ['group_a_b', 'crime_against_category', 'offense_parent_group', 'offense',
                       'offense_code', 'precinct', 'sector', 'beat', 'mcpp', '_100_block_address']:

            # Uppercase those letters
            self.raw_data[column] = self.raw_data[column].str.upper()

    def cleanup_na_values(self):
        """
        Summary: Replaces any common, designated missing values (given in the missing_values
                 class attribute) or column specific missing value indicators with a np.nan value
        """

        # For each column
        for column in list(self.raw_data):
            # Replace specific, designated missing values with a nan value (for consistency)
            self.raw_data.loc[self.raw_data[column].isin(CleanSeattleData.missing_values), [column]] = nan

        # The _100_block_address column uses a distinct address value as a
        # sort of missing value indicator/placeholder/etc. These distinct address
        # values contain the OFTH, OFND and OFRD non-existant street names
        self.raw_data.loc[
            self.raw_data['_100_block_address'].str.contains(r"OFTH|OFND|OFRD", na=False, regex=True),
                         ['_100_block_address']
                         ] = nan

        # Longitude/latitude use 0's as missing longitude/latitude values
        for column in ['latitude', 'longitude']:
            self.raw_data.loc[
                        self.raw_data[column].str.contains(r'^0', regex=True), 
                             [column]
                             ] = nan

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
        Summary: Corrects address value issues, such as:
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
            audited_values = self.raw_data[column].notnull() & to_datetime(self.raw_data[column],
                                                                           errors='coerce').isna()

            # For those records, retrieve the unique identifier (offense_id) and the value
            # in question. Insert these values into audit table to track what values are being
            # made null
            audited_values = self.raw_data[audited_values][['offense_id', column]]
            self.audit_table = audit_insert(self.audit_table, audited_values, 1)

            # Then perform the actual data conversion to datetime
            self.raw_data[column] = to_datetime(self.raw_data[column], errors='coerce')

        # Do the same with the numeric columns as done with the datetime columns
        for column in ['longitude', 'latitude']:

            audited_values = self.raw_data[column].notnull() & to_numeric(self.raw_data[column],
                                                                          errors='coerce').isna()

            audited_values = self.raw_data[audited_values][['offense_id', column]]
            self.audit_table = audit_insert(self.audit_table, audited_values, 2)

            self.raw_data[column] = to_numeric(self.raw_data[column], errors='coerce')

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
                          ['offense_start_datetime', 'offense_end_datetime']] = nan

    def cleanup_report_number(self):
        """
        Summary: Attempt to correct report numbers that do not conform to the valid format: four digits
                and six digits separated by a dash (1234-567890). This method can fix a variety of report
                numbers issues, such as:
                    > Replace letter o/O's with 0's
                        -i.e. 1234-56789O -> 1234-567890

                    > Replace common but invalid delimiters with the correct delimiter (-)
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
                      of the report date (a common pattern within the dataset)
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

        # Check if any report numbers do not follow the valid format (1234-567890); if so, perform the following checks
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

            # Nan any INVALID REPORT NUMBERS that couldn't be fixed
            if (~self.raw_data['report_number'].str.contains(valid_re, regex=True, na=False)).any():

                # Retrieve & store the offense_id and value in question
                audited_values = self.raw_data[~self.raw_data['report_number'].str.contains(valid_re,
                                                                                            regex=True,
                                                                                            na=False)] \
                                                                                       [['offense_id', 'report_number']]

                # Insert these values into audit table to track what values are made nan
                self.audit_table = audit_insert(self.audit_table, audited_values, 4)

                # Finally, nan these values
                self.raw_data.loc[
                    ~self.raw_data['report_number'].str.contains(valid_re, regex=True, na=False), ['report_number']
                                 ] = nan

    def cleanup_misspelled_mcpp(self):
        """
        Summary: Attempts to correct invalid (potentially misspelled) mcpp (micro-community) values with
                 a valid mcpp value using fuzzy string matching and a list of all the valid mcpp's (the mcpp
                 class attribute).

                 Creates two additional columns, match & match_certainty, which hold the best match & the percentage
                 score for said match. If the match score is >= 85%, it assigns the actual mcpp value the match. If
                 the match score is < 85%, mcpp values are audited & made null. 
        """

        # Checks for records where mcpp is invalid and not null; these record's mcpp value could be misspelled
        if (
            self.raw_data[
                        (~self.raw_data['mcpp'].isin(CleanSeattleData.mcpp['mcpp'])) &    # Checks that the mcpp is NOT in list of valid mcpp's
                        (~self.raw_data['mcpp'].isna())                                   # And, that the mcpp is NOT nan
                        ]
            ).any: 

            # Create 'match' column which, for each row, compares mcpp value to list of valid mcpp values & returns best match;
            # fyi, extractOne returns tuple containing the match & its score -> (match, score)
            self.raw_data['match'] = self.raw_data.apply(
                                                    lambda row: extractOne(
                                                                    query=row['mcpp'],                      # the row's mcpp value
                                                                    choices=CleanSeattleData.mcpp['mcpp']   # the list of valid mcpp's 
                                                                    )[0],                                   # the match
                                                                axis=1                          
                                                    )

            # Create 'match_certainty' column which, for each row, compares mcpp value to list of valid mcpp values & returns the match score
            self.raw_data['match_certainty'] = self.raw_data.apply(
                                                lambda row: extractOne(
                                                                query=row['mcpp'],                          # the row's mcpp value
                                                                choices=CleanSeattleData.mcpp['mcpp']       # the list of valid mcpp's 
                                                                )[1],                                       # the match
                                                            axis=1  
                                                )   
  
            # Assign the actual mcpp value the match if the score is >= 85%
            self.raw_data.loc[
                            self.raw_data['match_certainty'] >= 85, 
                            ['mcpp'] 
                            ] = self.raw_data['match']


            # Otherwise, audit those values that did not meet the 85% threshold
            audited_values = self.raw_data[
                                    self.raw_data['match_certainty'] < 85
                                    ][['offense_id', 'mcpp']]
                

            # Insert into audit table 
            self.audit_table = audit_insert(self.audit_table, audited_values, 5)


            # Now that values have been audited, null them
            self.raw_data.loc[
                            self.raw_data['match_certainty'] < 85, 
                            ['mcpp'] 
                            ] = nan


            # Drop the 'match' & 'match_certainty' columns, as they are no longer needed
            self.raw_data.drop(
                            labels=['match', 'match_certainty'],
                            axis=1,
                            inplace=True
                            )


    #def correct_mismatched_loc_codes(self):
        #self.raw_data['sector']

    def correct_na_loc_codes(self):
        """
        Summary: Attempts to fill in MISSING location codes using information from lower-level location
                 codes (i.e. beat B1 belongs to sector B which belongs to precinct N) (or, i.e. mcpp Alki belongs
                 to precinct SW).

                 Location codes follow a hierarchy, like so:
                    Precincts -> Sectors -> Beats
                    Precincts -> MCPPs

                 With that, we can know Beats (low-level) can determine sectors (high-level), sectors (low-level) 
                 can determine precincts (high-level), and mcpps (low-level) can determine precincts (high-level).
        """
        loc_code_pairings = [
                            ('sector', 'beat', 'loc_codes'), 
                            ('precinct', 'sector', 'loc_codes'),
                            ('precinct', 'mcpp', 'mcpp')
                            ]

 
        # Audit any BEAT location codes that are invalid but not null, then null those values
        audited_values = self.raw_data.loc[
                                        ~(self.raw_data['beat'].isin(CleanSeattleData.loc_codes['beat'])) &  # Check that beat is NOT in list containing valid beats
                                        ~(self.raw_data['beat'].isna())                                      # and, that beat is NOT null
                                        ][['offense_id', 'beat']]             

        self.audit_table = audit_insert(self.audit_table, audited_values, 7)
        self.raw_data.loc[
                        ~(self.raw_data['beat'].isin(CleanSeattleData.loc_codes['beat'])),
                        ['beat']
                        ] = nan

        # Valid low-level location codes can determine unknown high-level location codes.
        # For instance, if we have a missing sector code but a valid/present beat code,
        # we can then determine the appropriate missing sector code.
        for high_loc, low_loc, valid_df in loc_code_pairings:

            self.raw_data = merge(
                                left=self.raw_data,
                                right=getattr(CleanSeattleData, valid_df)[[high_loc, low_loc]],
                                how='left',
                                on=low_loc,
                                suffixes=('_actual', '_correct')
                                )
            

            self.raw_data.loc[
                                (
                                    (~self.raw_data[high_loc + '_actual'].isin(getattr(CleanSeattleData, valid_df)[high_loc])) &    # Check that actual high-level loc value is NOT in valid list
                                    (self.raw_data[low_loc].isin(getattr(CleanSeattleData, valid_df)[low_loc]))                     # Check that low-level loc value IS IN valid list
                                ),
                                [high_loc+'_actual']                    # Actual high-level loc code                                     

                            ] = self.raw_data[high_loc + '_correct']    # Replace actual high-level loc code(^) with the correct high-level loc
                                                                        # code, as determined by the merge w/the dataframe containing valid value's matchings


            self.raw_data.drop(
                            labels=high_loc+'_correct',
                            axis=1,
                            inplace=True
                            )

            self.raw_data.rename(
                            columns={high_loc+'_actual': high_loc},
                            inplace=True
                            )       


            # Audit & null any high-level location codes that do not have a valid/present low-level location code (& thus cannot correctly be determined)
            audited_values = self.raw_data.loc[
                                                (
                                                    (~self.raw_data[high_loc].isin(getattr(CleanSeattleData, valid_df)[high_loc])) &
                                                    (~self.raw_data[high_loc].isna())
                                                )
                                               ][['offense_id', high_loc]]

            if valid_df == 'loc_codes':
                audited_reason_id = 8

            if valid_df == 'mcpp':
                audited_reason_id = 9

            self.audit_table = audit_insert(
                                source_audit_table = self.audit_table,
                                target_audit_table = audited_values,
                                audited_reason_id = audited_reason_id
                                )

            self.raw_data.loc[
                            ~(self.raw_data[high_loc].isin(getattr(CleanSeattleData, valid_df)[high_loc])),
                             [high_loc]
                             ] = nan


            # Drop duplicate rows as a result of the merge: each value in raw data will have multiple matches in the valid
            # dataframe, adding all those additional matching lines (which all contain the same value)
            self.raw_data.drop_duplicates(inplace=True)


    def correct_deci_degrees(self):
        """
        Summary: Checks if the longitude and latitude are within Washington State's longitude and
                latitude; if not, audit and nullify these values.
        """

        # Confirm the longitude is at least within Washington's longitude range; otherwise audit and null the value
        if (
            self.raw_data[
                        ~(self.raw_data['longitude'].between(-125.0, -116.5))
                        ]
            ).any:

            audited_values = self.raw_data[
                                    ~(self.raw_data['longitude'].between(-125.0, -116.5))
                                    ][['offense_id', 'longitude']]

            self.audit_table = audit_insert(self.audit_table, audited_values, 10)

            self.raw_data.loc[
                            ~(self.raw_data['longitude'].between(-125.0, -116.5)),
                            ['longitude']
                            ] = nan

        # Confirm the latitude is at least within Washington's latitude range
        # Otherwise null the value
        if (
            self.raw_data[
                        ~(self.raw_data['latitude'].between(45.5, 49.0))
                        ]
            ).any:

            audited_values = self.raw_data[
                                        ~(self.raw_data['latitude'].between(45.5, 49.0))
                                        ][['offense_id', 'latitude']]

            self.audit_table = audit_insert(self.audit_table, audited_values, 11)

            self.raw_data.loc[
                            ~(self.raw_data['latitude'].between(45.5, 49.0)),
                            ['latitude']
                            ] = nan


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
        self.raw_data['_100_block_address'].replace('na', nan, inplace=True)


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
        self.raw_data.where(notnull(self.raw_data), None, inplace=True)


def create_audit_table():
    """
    Summary: Creates audit table to store information for values that were nullified in
             the cleaning process
    """

    # Create and return the audit table
    the_audit_table = DataFrame(columns=['audited_col', 'offense_id', 'audited_val', 'audited_reason_id', 'batch'])

    return the_audit_table.set_index(['audited_col', 'offense_id'])


def audit_insert(audit_table, audited_values, audited_reason_id):
    """
    Summary: takes audited_values, which are passed in as shown below, and reshapes it into a format that allows it to be 
             merged with audit table, inserting the new audited values into the audit table. 
    

            audited_values                                                        audit_table

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

    # Take the target_audit_table and re-structure it to match that of the
    # source_audit_table (the audit table) so it can be merged

    # Set the offense_id as the index 
    audited_values.set_index('offense_id', inplace=True)

    
    # .stack() moves column_name to index, forming multi-level index w/offense_id
    # .stack() converts data to Series object, so call DataFrame() over it 
    audited_values = DataFrame(audited_values.stack())

    
    # .swapelevel() switches the multi-level index, column_name is now the outer index, offense_id is inner index
    audited_values = audited_values.swaplevel()

    # Sort the index & assign the audit reason id value
    audited_values.sort_index(inplace=True)
    audited_values['audited_reason_id'] = audited_reason_id

    # Rename the new column (created from .stack()) to audited_val
    audited_values.rename(columns={0: 'audited_val'}, inplace=True)

    # Rename the index 
    audited_values.index.names = ['audited_col', 'offense_id']

    audited_values['batch'] = datetime.today().strftime('%Y-%m-%d')

    # Merge the tables
    return audit_table.combine_first(audited_values)


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







