import seattle_scraping
import seattle_cleaning
import seattle_loading


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
    scraping_process.scrape('2021-07-24')

    # Clean the data
    clean_data, audit_data = seattle_cleaning.clean(scraping_process.raw_data)

    # Transform the dataframes into record sets
    clean_data_record_set, audit_data_record_set = seattle_loading.convert_into_record_sets(clean_data, audit_data)

    # Establish a connection with the database
    cursor = seattle_loading.establish_connection()

    # Attempt to remove data
    seattle_loading.remove_data(cursor)

    # Insert the new data into the database
    seattle_loading.insert_data(cursor, clean_data_record_set, audit_data_record_set)


if __name__ == '__main__':
    main()




