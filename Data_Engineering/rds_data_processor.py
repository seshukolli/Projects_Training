import logging
import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


from rds_connection import connect_to_rds


class RDSDataProcessor:
    def __init__(self, connection):
        self.connection = connection
       
    def create_table(self):
        connection = connect_to_rds()
        if connection:
            cursor = connection.cursor()
            cursor.execute("DROP TABLE IF EXISTS people1000")  # Drop table if it exists
            cursor.execute("""
                CREATE TABLE people1000 (
                    `Index` INT PRIMARY KEY,
                    `User Id` INT,
                    `First Name` VARCHAR(50),
                    `Last Name` VARCHAR(50),
                    `Sex` VARCHAR(10),
                    `Email` VARCHAR(100),
                    `Phone` VARCHAR(20),
                    `Date of birth` DATE,
                    `Job Title` VARCHAR(100)
                );


            """)
            print("Table creation: successful")
            cursor.close()
            connection.close()
        else:
            print("Table creation: unsuccessful")


   
    def load_data(self, csv_file_path):
       # Connect to the database
        connection = connect_to_rds()
       
        if connection:
            cursor = connection.cursor()


            # Load CSV data and replace NaN with None
            data = pd.read_csv(csv_file_path)
            data = data.replace({np.nan: None})


            # Prepare insert query template
            insert_query = """
                INSERT INTO people1000 (
                    `Index`, `User Id`, `First Name`, `Last Name`, `Sex`, `Email`,
                    `Phone`, `Date of birth`, `Job Title`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """




            # Insert each row in the table
            # Insert each row in the table
            for _, row in data.iterrows():
                values = (
                    row['Index'], row['User Id'], row['First Name'], row['Last Name'],
                    row['Sex'], row['Email'], row['Phone'], row['Date of birth'], row['Job Title']
                )
                cursor.execute(insert_query, values)




            # Commit the transaction
            connection.commit()
            print("Data loaded into SP500 table successfully")


            # Close cursor and connection
            cursor.close()
            connection.close()
        else:
            print("Failed to connect to database for data loading")




# Ensure logging is configured to capture information
logging.basicConfig(level=logging.INFO)


# Run the function with a connection object (replace with actual connection)
if __name__ == "__main__":
    connection = connect_to_rds()  # Ensure this function returns a valid connection
    processor = RDSDataProcessor(connection)
    processor.create_table()  # First, ensure the table is created
    processor.load_data('your_file.csv')  # Provide the correct path to your CSV file
