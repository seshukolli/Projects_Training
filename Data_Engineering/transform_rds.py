import logging
import pandas as pd
import numpy as np
from rds_connection import connect_to_rds


# Configure logging
logging.basicConfig(level=logging.INFO, filename="transformation_log.txt", filemode="a",
                    format="%(asctime)s - %(levelname)s - %(message)s")


class DataTransformer:
    def __init__(self, connection):
        self.connection = connection


    def fetch_data(self, table_name):
        """Fetch data from the specified table in the database."""
        query = f"SELECT * FROM {table_name};"
        data = pd.read_sql(query, self.connection)
        logging.info(f"Data fetched from '{table_name}'.")
        return data


    def transform_data(self, data):
        """Apply multiple transformations and return the final transformed DataFrame."""
       
        # 1. Null value handling
        data = data.fillna({
            'First Name': 'Unknown',
            'Last Name': 'Unknown',
            'Sex': 'Not Specified',
            'Email': 'no_email@example.com',
            'Phone': '000-000-0000'
        })
        logging.info("Null values handled.")


        # 2. Group by 'Sex' and add a count column (for demonstration)
        data['Sex Count'] = data.groupby('Sex')['Sex'].transform('count')
        logging.info("Added 'Sex Count' column based on grouping by 'Sex'.")


        # 3. Additional transformation example: Filter by Job Title
        data = data[data['Job Title'] != 'Unknown']
        logging.info("Filtered out entries with 'Unknown' Job Title.")


        # 4. Concatenate first and last names into a new 'Full Name' column
        data['Full Name'] = data['First Name'] + ' ' + data['Last Name']
        logging.info("Created 'Full Name' column by combining 'First Name' and 'Last Name'.")


        # 5. Dropping duplicate rows (if any exist)
        data = data.drop_duplicates()
        logging.info("Dropped duplicate rows.")


        # 6. Sort data by 'Date of birth'
        data = data.sort_values(by='Date of birth')
        logging.info("Sorted data by 'Date of birth'.")


        # Return the final transformed DataFrame
        return data


    def store_data_row_by_row(self, data, table_name):
        """Store final transformed data row-by-row in the specified table."""
        connection = connect_to_rds()
        if connection:
            cursor = connection.cursor()


            # Drop table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    `Index` INT PRIMARY KEY,
                    `User Id` INT,
                    `First Name` VARCHAR(50),
                    `Last Name` VARCHAR(50),
                    `Sex` VARCHAR(10),
                    `Email` VARCHAR(100),
                    `Phone` VARCHAR(20),
                    `Date of birth` DATE,
                    `Job Title` VARCHAR(100),
                    `Sex Count` INT,
                    `Full Name` VARCHAR(100)
                );
            """)
            logging.info(f"Table '{table_name}' created.")


            # Insert each row into the table
            insert_query = f"""
                INSERT INTO {table_name} (
                    `Index`, `User Id`, `First Name`, `Last Name`, `Sex`, `Email`,
                    `Phone`, `Date of birth`, `Job Title`, `Sex Count`, `Full Name`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """


            for _, row in data.iterrows():
                values = (
                    row['Index'], row['User Id'], row['First Name'], row['Last Name'],
                    row['Sex'], row['Email'], row['Phone'], row['Date of birth'],
                    row['Job Title'], row['Sex Count'], row['Full Name']
                )
                cursor.execute(insert_query, values)
           
            # Commit the transaction
            connection.commit()
            logging.info(f"Data successfully inserted into '{table_name}' table.")


            # Close cursor and connection
            cursor.close()
            connection.close()
        else:
            logging.error("Failed to connect to database for data storage.")


if __name__ == "__main__":
    # Connect to the RDS database
    connection = connect_to_rds()


    if connection:
        transformer = DataTransformer(connection)


        # Fetch data from the main table
        original_data = transformer.fetch_data("people1000")


        # Perform transformations and get the final transformed DataFrame
        final_data = transformer.transform_data(original_data)


        # Store only the final transformed data in a new table
        transformer.store_data_row_by_row(final_data, "people1000_final_transformed")


        # Close the main connection
        connection.close()
    else:
        logging.error("Failed to connect to RDS.")
