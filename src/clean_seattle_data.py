from os import getenv
from numpy import nan
import pandas as pd
from pandas import read_csv, to_datetime, to_numeric, notnull, merge

from fuzzywuzzy.process import extractOne
from dotenv import load_dotenv

load_dotenv()



def cleanup_whitespace(seattle_data):
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
    multi_space_re  =   r'\s{2,}'
    dash_re         =   r'\s+-\s+'
    slash_re        =   r'\s+/\s+'
    amp_re          =   r'(\w)\s*&\s*(\w)'


    # Strip the values for each column (other than longitude/latitude columns) of any leading/ending whitespaces
    for column in list(seattle_data):

        # Replace empty string columns w/nan
        seattle_data[f'{column}'] = (
                                        seattle_data
                                        [f'{column}']
                                        .str.strip()
                                        .replace(
                                                to_replace='',
                                                value=nan
                                                )
                                        )

        # For columns that are not supposed to have spaces
        if column in ['beat','sector','precinct','group_a_b','offense_id','offense_code','report_number', 'crime_against_category']:

            # Replace spaces with no space
            seattle_data[f'{column}'] = (
                                            seattle_data
                                            [f'{column}']
                                            .str.replace(
                                                        pat=' ',
                                                        repl=''
                                                        )
                                        )

        # For columns that are supposed to have spaces
        if column in ['offense_parent_group', 'offense', 'mcpp', '_100_block_address']:

            # Replace multiple spaces with a SINGLE SPACE
            seattle_data[column] = (
                                    seattle_data
                                    [column]
                                    .str.replace(
                                                pat=multi_space_re,
                                                repl=' ',
                                                regex=True
                                                )
                                    )

            # Replace spaced dashes with SINGLE DASH
            seattle_data[column] = (
                                    seattle_data
                                    [column]
                                    .str.replace(
                                                pat=dash_re,
                                                repl='-',
                                                regex=True
                                                )
                                    )

            # Replace spaced forward slashes with SINGLE FORWARD SLASH
            seattle_data[column] = (
                                    seattle_data
                                    [column]
                                    .str.replace(
                                                pat=slash_re,
                                                repl='/',
                                                regex=True
                                                )
                                    )

            # Replace non-spaced ampersand with a SPACED AMPERSAND
            seattle_data[column] = (
                                    seattle_data
                                    [column]
                                    .str.replace(
                                                pat=amp_re,
                                                repl='\\1 & \\2',
                                                regex=True
                                                )
                                    )
            
    return seattle_data


def cleanup_column_casing(seattle_data):
    """
    Summary: Make text column casing consistent; uppercase any columns containing letters
    """

    # For columns with letters
    for column in ['group_a_b', 'crime_against_category', 'offense_parent_group', 'offense',
                    'offense_code', 'precinct', 'sector', 'beat', 'mcpp', '_100_block_address']:

        # Uppercase those letters
        seattle_data[column] = (
                                seattle_data
                                [column]
                                .str.upper()
                                )
        
    return seattle_data


def cleanup_na_values(seattle_data):
    """
    Summary: Replaces any common, designated missing values (given in the missing_values
                class attribute) or column specific missing value indicators with a np.nan value
    """

    # List of designated, common missing values representations
    missing_values = ['UNKNOWN', '99', 99, 'OOJ', '<NULL>', '<Null>', 'NULL',
                    'null', 'nil', 'empty', '-', 'NA', 'n/a', 'na']

    # For each column
    for column in list(seattle_data):

        # Replace specific missing values with a nan value (for consistency)
        seattle_data.loc[
                        seattle_data
                        [column]
                        .isin(values=missing_values),

                        [column]
                        ] = nan


    # The _100_block_address column uses a distinct address value as a sort of missing value indicator/placeholder/etc.
    # These distinct address values contain the OFTH, OFND and OFRD non-existant street names
    seattle_data.loc[
                    seattle_data
                    ['_100_block_address']
                    .str.contains(
                                pat=r"OFTH|OFND|OFRD",
                                na=False,
                                regex=True
                                ),

                        ['_100_block_address']
                        ] = nan


    # Longitude/latitude use 0's as missing longitude/latitude values
    for column in ['latitude', 'longitude']:
        seattle_data.loc[
                        seattle_data
                        [column]
                        .str.contains(
                                    pat=r'^0',
                                    regex=True
                                    ), 

                            [column]
                            ] = nan
        

    return seattle_data


def clear_non_crimes(seattle_data):
    """
    Summary: Removes records with a 'NOT_A_CRIME' value in the crime_against_category. These
                records represent justifiable homicides (self-defense), which are not crimes but
                are still reported. For the purpose and scope of this project/data, I am interested
                only in crimes.
    """

    # Drop records with a 'NOT_A_CRIME' value in the crime_against_category column.
    if (seattle_data['crime_against_category'] == 'NOT_A_CRIME').any():

        seattle_data.drop(
                            labels=seattle_data[
                                                seattle_data['crime_against_category'] == 'NOT_A_CRIME'
                                                ]
                                                .index,
                            inplace=True
                            )
        
    return seattle_data 


def cleanup_addresses(seattle_data):
    """
    Summary: Corrects address value issues, such as:
                > Replace X's with 0's in block information
                    -i.e. 5XX BLOCK OF PIKE ST -> 500 BLOCK OF PIKE ST

                > Fixes inconsistent road labels
                    -i.e. AV -> AVE
    """

    # Regex strings used below
    x_re    =   r'\d+X+\s'
    ave_re  =   r'\s+AV\s+'

    # Replace X's (5XX BLOCK OF PIKE ST) with 0's (500 BLOCK OF PIKE ST)
    seattle_data.loc[
                    seattle_data
                    ['_100_block_address']
                    .str.contains(
                                pat=x_re,
                                regex=True,
                                na=False
                                ), 

                    ['_100_block_address']
                        ] = \
                        (seattle_data
                        ['_100_block_address']
                        .str.replace(
                                    pat='X',
                                    repl='0'
                                    )
                        )

    # Use consistent avenue abbreviations, replace any instances of 'AV' with 'AVE'
    seattle_data.loc[
                    seattle_data
                    ['_100_block_address']
                    .str.contains(
                                pat=ave_re,
                                na=False,
                                regex=True
                                ), 

                    ['_100_block_address']
                    ] = \
                        (seattle_data
                        ['_100_block_address']
                        .str.replace(
                                    pat=r'AV', 
                                    repl='AVE', 
                                    regex=True
                                    )
                        )

    
    return seattle_data


def cleanup_dtypes(seattle_data):
    """
    Summary: Convert datetime and numeric columns into their respective data types,
                meanwhile tracking and nullifying values that do not conform to the data type
                through the audit table.
    """

    # For each datetime column ...
    for column in ['offense_start_datetime', 'offense_end_datetime', 'report_datetime']:

        # Convert data type to datetime
        seattle_data[column] = to_datetime(
                                        arg=seattle_data[column], 
                                        errors='coerce'
                                        )


    # Do the same with the numeric columns as done with the datetime columns
    for column in ['longitude', 'latitude']:

        seattle_data[column] = to_numeric(
                                        arg=seattle_data[column], 
                                        errors='coerce'
                                        )


    return seattle_data


def correct_offense_datetime(seattle_data):
    """
    Summary: Null offense start/end datetime values where the offense start
            datetime is greater than, or after, the offense end datetime
    """

      
    # Null offense start/end datetime values where the offense end datetime is 
    # before the offense start datetime
    seattle_data.loc[
                    seattle_data['offense_start_datetime'] > 
                    seattle_data['offense_end_datetime'],
                    ['offense_start_datetime', 'offense_end_datetime']
                    ] = nan
    

    return seattle_data


def cleanup_report_number(seattle_data):
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
                    zeroes between the first four and last six digits
                    -i.e. 1234000567890 -> 1234-567890

                > Report numbers with long back digits (>6) AND leading 0's removes the
                  leading 0's
                    -i.e. 1234-000567890 -> 1234-567890

                > Report numbers with short front digits (<4) are replaced by the year
                    of the report date (a common pattern within the dataset)
                    -i.e. 12-567890 -> 2022-567890
    """

    # Regex strings used below
    valid_re        =   r'^\d{4}-\d{6}$'
    o_re            =   r'[O,o]'
    delim_re        =   r'^\d*[=,_]\d*$'
    no_delim_re     =   r'^\d{10}$'
    zero_delim_re   =   r'^\d{4}0+\d{6}$'
    long_re         =   r'-0+\d{6,}$'
    short_re        =   r'^\d{,3}-'


    # Check if any report numbers do not follow the valid format (1234-567890); if so, perform the following checks
    if (
        ~seattle_data['report_number'].str.contains(
                                                    pat=valid_re,
                                                    regex=True,
                                                    na=False
                                                    )
                                                    ).any():

        # Replace LETTER O's
        seattle_data.loc[
                        seattle_data
                        ['report_number']
                        .str.contains(
                                    pat=o_re,
                                    regex=True,
                                    na=False
                                    ), 
                                    
                                    ['report_number']
                        ] = (
                            seattle_data
                            ['report_number']
                            .str.replace(
                                        pat='O',
                                        repl='0'
                                        )
                                        .str.replace(
                                                    pat='o',
                                                    repl='0'
                                                    )
                            )


        # Corrects incorrect DELIMITERS
        seattle_data.loc[
                        seattle_data
                        ['report_number']
                        .str.contains(
                                    pat=delim_re,
                                    regex=True,
                                    na=False
                                    ), 
                                    
                                    ['report_number']
                        ] = (
                            seattle_data
                            ['report_number']
                            .str.split(pat='=|_|\\*|,|;|/|\\\\').str[0] \
                            
                            + '-' \
                            
                            + seattle_data
                                ['report_number']
                                .str.split(pat='=|_|\\*|,|;|/|\\\\').str[1]
                            )


        # Corrects NO DELIMITERS (with correct number of digits)
        seattle_data.loc[
                        seattle_data
                        ['report_number']
                        .str.contains(
                                    pat=no_delim_re,
                                    regex=True,
                                    na=False
                                    ), 
                                    
                                    ['report_number']
                        ] = (
                            seattle_data
                            ['report_number'].str[:4] \
                            
                            + '-'\
                            
                            + seattle_data
                            ['report_number'].str[-6:]
                            )


        # Corrects any ZERO PLACEHOLDER/DELIMITERS
        seattle_data.loc[
                        seattle_data
                        ['report_number']
                        .str.contains(
                                    pat=zero_delim_re,
                                    regex=True,
                                    na=False
                                    ),
                                    
                                    ['report_number']
                        ] = (
                            seattle_data
                            ['report_number'].str[:4] \
                            
                            + '-' \
                            
                            + seattle_data
                            ['report_number'].str[-6:]
                            )


        # Corrects any LONG BACK HALF DIGITS
        seattle_data.loc[
                        seattle_data
                        ['report_number']
                        .str.contains(
                                    pat=long_re,
                                    regex=True,
                                    na=False
                                    ),
                                    
                                    ['report_number']
                        ] = (
                            seattle_data
                            ['report_number']
                            .str.split('-').str[0] \
                            
                            + '-' \
                            
                            + seattle_data
                                ['report_number'].str[-6:])


        # Corrects any SHORT FRONT DIGITS
        seattle_data.loc[
                        seattle_data
                        ['report_number']
                        .str.contains(
                                    pat=short_re,
                                    regex=True,
                                    na=False
                                    ), 
                                    
                                    ['report_number']
                        ] = (
                            seattle_data
                            ['report_datetime'].dt.strftime('%Y') \
                            
                            + '-' \
                            
                            + seattle_data
                            ['report_number'].str[-6:]
                            )


        # Null any INVALID REPORT NUMBERS that couldn't be fixed
        if (
            ~seattle_data
            ['report_number']
            .str.contains(
                        pat=valid_re, 
                        regex=True, 
                        na=False
                        )
            ).any():

            # Finally, nan these values
            seattle_data.loc[
                            ~seattle_data
                            ['report_number']
                            .str.contains(
                                        pat=valid_re, 
                                        regex=True, 
                                        na=False
                                        ), 
                                        
                                        ['report_number']
                                ] = nan
            
    return seattle_data


def cleanup_misspelled_mcpp(seattle_data):
    """
    Summary: Attempts to correct invalid (potentially misspelled) mcpp (micro-community) values with
                a valid mcpp value using fuzzy string matching and a list of all the valid mcpp's (the mcpp
                class attribute).

                Creates two additional columns, match & match_certainty, which hold the best match & the percentage
                score for said match. If the match score is >= 85%, it assigns the actual mcpp value the match. If
                the match score is < 85%, mcpp values are audited & made null. 
    """

    # Load dataframe containing valid, corresponding precinct/mcpp location code pairings used to verify raw data against
    mcpp = read_csv(
                    filepath_or_buffer=getenv('MCPP'), 
                    dtype='O'
                    )

    # Checks for records where mcpp is invalid and not null; these record's mcpp value could be misspelled
    if (
        seattle_data[
                        (
                            ~seattle_data                   # Checks that the mcpp is NOT in list of valid mcpp's
                            ['mcpp']
                            .isin(mcpp['mcpp'])
                        ) 
                        
                        &    

                        (
                            ~seattle_data                   # And, that the mcpp is NOT nan
                            ['mcpp']
                            .isna()
                        )                                   
                    ]
        ).any: 


        # Create 'match' column which, for each row, compares mcpp value to list of valid mcpp values & returns best match;
        # fyi, extractOne returns tuple containing the match & its score -> (match, score)
        seattle_data['match'] = (
                                    seattle_data
                                    .apply(
                                            lambda row: extractOne(
                                                                    query=row['mcpp'],      # the row's mcpp value
                                                                    choices=mcpp['mcpp']    # the list of valid mcpp's 
                                                                    )[0],                   # the match
                                                        axis=1                          
                                            )
                                )

        # Create 'match_certainty' column which, for each row, compares mcpp value to list of valid mcpp values & returns the match score
        seattle_data['match_certainty'] = (
                                            seattle_data
                                            .apply(
                                                    lambda row: extractOne(
                                                                            query=row['mcpp'],         # the row's mcpp value
                                                                            choices=mcpp['mcpp']       # the list of valid mcpp's 
                                                                            )[1],                      # the match
                                                                                                        
                                                                axis=1  
                                                    ) 
                                            )  

        # Assign the actual mcpp value the match if the score is >= 85%
        seattle_data.loc[
                            seattle_data
                            ['match_certainty'] 
                            >= 85, 

                            ['mcpp'] 
                            ] = (
                                seattle_data
                                ['match']
                                )             

        # Now that values have been audited, null them
        seattle_data.loc[
                            seattle_data
                            ['match_certainty'] 
                            < 85, 

                            ['mcpp'] 
                        ] = nan


        # Drop the 'match' & 'match_certainty' columns, as they are no longer needed
        seattle_data.drop(
                            labels=['match', 'match_certainty'],
                            axis=1,
                            inplace=True
                            )

    return seattle_data


#def correct_mismatched_loc_codes(self):
    #self.raw_data['sector']


def correct_na_loc_codes(seattle_data):
    """
    Summary:    Attempts to fill in MISSING location codes using information from lower-level location
                codes (i.e. beat B1 belongs to sector B which belongs to precinct N) (or, i.e. mcpp Alki belongs
                to precinct SW).

                Location codes follow two seperate hierarchies, like so:
                Precincts -> Sectors -> Beats
                Precincts -> MCPPs

                With that, we can know Beats (low-level) can determine sectors (high-level), sectors (low-level) 
                can determine precincts (high-level), and mcpps (low-level) can determine precincts (high-level).
    """

    # Load dataframe containing valid, corresponding precinct/sector/beat location code matchings, used to verify raw data against
    loc_codes = read_csv(
                        filepath_or_buffer=getenv('LOC_CODES'), 
                        dtype='O'
                        )


    # Load dataframe containing valid, corresponding precinct/mcpp location code pairings used to verify raw data against
    mcpp = read_csv(
                    filepath_or_buffer=getenv('MCPP'), 
                    dtype='O'
                    )
    

    loc_code_pairings = [
                        ('sector', 'beat', loc_codes), 
                        ('precinct', 'sector', loc_codes),
                        ('precinct', 'mcpp', mcpp)
                        ]


    # if the beat value is not a valid beat, make it null (we can't recover it)
    seattle_data.loc[
                        ~(
                        seattle_data
                        ['beat']
                        .isin(loc_codes['beat'])
                        ),

                    ['beat']
                    ] = nan


    # Valid low-level location codes can determine unknown high-level location codes. For instance, if we have a missing 
    # sector code but a valid/present beat code, we can then determine the appropriate missing sector code
    for high_loc, low_loc, valid_df in loc_code_pairings:


        seattle_data = merge(
                            left        =   seattle_data,
                            right       =   valid_df[[high_loc, low_loc]],
                            how         =   'left',
                            on          =   low_loc,
                            suffixes    =   ('_actual', '_correct')
                            )
        

        seattle_data.loc[
                            (
                                (
                                ~seattle_data                                             # Check that actual high-level loc value is NOT in valid list
                                [high_loc + '_actual']
                                .isin(valid_df[high_loc])
                                ) 
                            
                            &                                                             # AND

                                (
                                seattle_data[low_loc]                                     # Check that low-level loc value IS IN valid list
                                .isin(valid_df[low_loc])
                                )                     
                            ),

                            [high_loc + '_actual']                                            # Actual high-level loc code                                     
                        ] = (
                            seattle_data                                           # Replace actual high-level loc code(^) with the correct high-level loc
                            [high_loc + '_correct']                                 # code, as determined by the merge w/the dataframe containing valid value's matchings
                            )    
                                                                    


        seattle_data.drop(
                        labels=high_loc+'_correct',
                        axis=1,
                        inplace=True
                        )

        seattle_data.rename(
                            columns={high_loc+'_actual': high_loc},
                            inplace=True
                            )       

        seattle_data.loc[
                        ~(
                        seattle_data
                        [high_loc]
                        .isin(valid_df[high_loc])
                        ),

                        [high_loc]
                        ] = nan


        # Drop duplicate rows as a result of the merge: each value in raw data will have multiple matches in the valid
        # dataframe, adding all those additional matching lines (which all contain the same value)
        seattle_data.drop_duplicates(inplace=True)


    return seattle_data


def correct_deci_degrees(seattle_data):
    """
    Summary: Checks if the longitude and latitude are within Washington State's longitude and
            latitude; if not, audit and nullify these values.
    """

    # Confirm the longitude is at least within Washington's longitude range; otherwise audit and null the value
    if (
        seattle_data[
                        ~(
                        seattle_data
                        ['longitude']
                        .between(-125.0, -116.5)
                        )
                    ]
        ).any:

     

        seattle_data.loc[
                            ~(
                            seattle_data
                            ['longitude']
                            .between(-125.0, -116.5)
                            ),

                        ['longitude']
                        ] = nan


    # Confirm the latitude is at least within Washington's latitude range
    # Otherwise null the value
    if (
        seattle_data[
                        ~(
                        seattle_data
                        ['latitude']
                        .between(45.5, 49.0)
                        )
                    ]
        ).any:

        seattle_data.loc[
                            ~(
                            seattle_data
                            ['latitude']
                            .between(45.5, 49.0)
                            ),
                            
                            ['latitude']
                        ] = nan
            

    return seattle_data


def cleanup_column_order(seattle_data):
    """
    Summary: Orders the columns in a consistent manner.
    """

    seattle_data = seattle_data[
                                    [
                                    'report_number', 'offense_id', 'offense_start_datetime', 'offense_end_datetime',
                                    'report_datetime', 'group_a_b', 'crime_against_category', 'offense_parent_group',
                                    'offense', 'offense_code', 'precinct', 'sector', 'beat', 'mcpp',
                                    '_100_block_address', 'longitude', 'latitude'
                                    ]
                                ]


    return seattle_data


def config_addresses(seattle_data):
    """
    Summary: Breaks apart/separates multivalued address values into two, separate rows:
                ... 26TH AVE NE / NE BLAKELEY ST -> ... 26TH AVE NE
                                                    ... NE BLAKELEY ST
    """

    def split_address(address_col: pd.Series):
    
        return (
                address_col.str
                .split(
                    pat='/', 
                    expand=True
                    )
                )

    # make list of columns excluding the '_100_block_address' column 
    columns = [
                col for col 
                in list(seattle_data) 
                if col != '_100_block_address'
            ]

    # Create a df with the address data split into two columns
    split_address_df = (
                        split_address(
                                        address_col=seattle_data['_100_block_address']
                                        )
                        )

    # Assign the split address df a multi-index consisting of every column except the '_100_block_address' column
    split_address_df.index = (
                                seattle_data
                                .set_index(columns)
                                .index
                                )

    # Replace null values (np.nan) with a non-null value ('na') so the row is not lost
    # in the following command
    split_address_df[0].fillna(
                            value='na', 
                            inplace=True
                            )
    

    seattle_data = (
                    split_address_df
                    .stack()
                    .reset_index(columns)
                    )

    # Rename the column
    seattle_data.rename(
                    columns={0: '_100_block_address'},
                    inplace=True
                    )


    # Revert the non-null value back to a null value
    seattle_data['_100_block_address'].replace(
                                            to_replace='na', 
                                            value=nan, 
                                            inplace=True
                                            )
    

    return seattle_data


def config_na_values(seattle_data):
    """
    Summary: Changes all np.NaN and np.NaT values to None, so when entered
            into SQL Server database, missing values can be entered as NULL.
    """

    # Convert all columns to Object data type to allow for None
    for col in list(seattle_data):

        seattle_data[col] = (
                            seattle_data
                            [col]
                            .astype(dtype='O')
                            )

    # Replace all np.NaN and np.NaT to None values
    # so when inserted into SQL Server database, it converts cleanly to NULL
    seattle_data.where(
                    cond=notnull(seattle_data), 
                    other=None, 
                    inplace=True
                    )
    
    return seattle_data