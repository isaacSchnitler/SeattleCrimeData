import seattle_scraping
import seattle_cleaning
import pyodbc
import sys
import os

from dotenv import load_dotenv
from datetime import timedelta
from datetime import datetime

load_dotenv()

# Database connection credentials
db_username = os.getenv('DB_U')
db_password = os.getenv('DB_P')
database = os.getenv('DB_NAME')

# SQL insert statement for the clean data
insert_into_tblCrime = '''
    INSERT INTO [SeattleCrimeData].[dbo].[tblCrime] 
    (
        [report_number],
        [offense_id],
        [offense_start_datetime],
        [offense_end_datetime],
        [report_datetime],
        [group],
        [crime_category],
        [offense_parent_group],
        [offense],
        [offense_code],
        [precinct],
        [sector],
        [beat],
        [mcpp],
        [address],
        [longitude],
        [latitude],
        [street_1],
        [street_2]
    )

    VALUES 
    (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
'''

# SQL insert statement for the audit data
insert_into_tblAudit = '''
    INSERT INTO [SeattleCrimeData].[dbo].[tblAudit]
    (
        [audited_column],
        [offense_id],
        [audited_value],
        [audit_reason_id]
    )
    
    VALUES
    (
    ?, ?, ?, ?
    )
'''


def main():
    """
    Summary: Brings together all components:
                > Scrapes the data
                > Cleans the data
                > Prepares data as record sets
                > Connects to the database
                > Removes data >1-year-old from the database
                > And inserts the data into the database
    """

    # Scrape yesterday's data (or 1 day ago)
    scraping_process = seattle_scraping.ScrapeSeattleData()
    scraping_process.scrape()

    # Clean the data
    clean_data, audit_data = seattle_cleaning.clean(scraping_process.raw_data)

    # Transform the dataframes into record sets
    clean_data_record_set, audit_data_record_set = convert_into_record_sets(clean_data, audit_data)

    # Establish a connection with the database
    cursor = establish_connection()

    # Attempt to remove data
    remove_data(cursor)

    # Insert the new data into the database
    insert_data(cursor, clean_data_record_set, audit_data_record_set)


def convert_into_record_sets(clean_data, audit_data):
    """
    Summary: Transforms the clean data and audit table dataframes into record sets, the
            suitable format/structure for the data to be inserted.
    """

    clean_data = clean_data.values.tolist()

    audit_data.reset_index(inplace=True)
    audit_data = audit_data.values.tolist()

    return clean_data, audit_data


def establish_connection():
    """
    Summary: Attempt to connect to SQL Server database and if successful, return cursor
            object. Otherwise, exit the process.
    """
    try:
        conn = pyodbc.connect(f'''DSN={database};UID={db_username};PWD={db_password}''')

    except Exception as e:
        print(e)
        sys.exit()

    else:
        return conn.cursor()


def remove_data(cursor_object):
    """
    Summary: Attempt to remove data from the database where the crime (according to the report date/time)
            is more than a year old. For the purpose and scope of this project, I am focused on only timely
             (within a year) data.
    """

    # Get the date from 366 days ago
    date_limit = datetime.strftime(datetime.today().date() - timedelta(days=366), '%Y-%m-%d')

    delete_statement_tblcrime = f'''
                                    DELETE FROM [SeattleCrimeData].[dbo].[tblAudit]
                                    WHERE offense_id in (
                                                        SELECT offense_id
                                                        FROM [SeattleCrimeData].[dbo].[tblCrime]
                                                        WHERE report_datetime < '{date_limit}'
                                                        )
                                    
    
                                    DELETE FROM [SeattleCrimeData].[dbo].[tblCrime]
                                    WHERE report_datetime < '{date_limit}'
                                '''

    try:
        cursor_object.execute(delete_statement_tblcrime)

    except Exception as e:
        print(e)
        cursor_object.close()
        sys.exit()

    else:
        cursor_object.commit()


def insert_data(cursor_object, clean_data, audit_data):
    """
    Summary: Attempt to insert the data into the SQL Server database and, if successful, commit
            the transaction. Otherwise, if unsuccessful, rollback the transaction and close the
            connection.
    """

    try:
        # Insert the clean data as long as it's not empty, otherwise pass
        if clean_data:
            cursor_object.executemany(insert_into_tblCrime, clean_data)

        # Insert the audit data as long as it's not empty, otherwise pass
        if audit_data:
            cursor_object.executemany(insert_into_tblAudit, audit_data)

        else:
            pass

    except Exception as e:
        print(e)
        cursor_object.close()
        sys.exit()

    else:
        cursor_object.commit()
        cursor_object.close()


if __name__ == '__main__':
    main()




