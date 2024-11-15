import logging
from rds_connection import connect_to_rds
from rds_data_processor import RDSDataProcessor


# Set up logging
logging.basicConfig(filename="process_log.txt", level=logging.INFO)


try:
    # Connect to the RDS database
    connection = connect_to_rds()


    # Check if the connection is successful
    if connection:
        logging.info("Successfully connected to RDS.")
       
        # Initialize your data processor with the connection
        processor = RDSDataProcessor(connection)
       
        # Create the table
        processor.create_table()


        # Load data into RDS from CSV
        processor.load_data('people-1000.csv')


        # Fetching data from RDS and transforming it
        raw_data = processor.fetch_data_from_rds()
        if raw_data is not None:
            # Transform the data
            transformed_data = processor.transform_data(raw_data)


            # Store the transformed data in RDS
            processor.store_transformed_data_to_rds(transformed_data)
        else:
            logging.error("No data fetched from RDS.")
    else:
        logging.error("Failed to connect to RDS.")
except Exception as e:
    logging.error(f"Unexpected error occurred: {e}")
    print("An error occurred. Check the log file for details.")
