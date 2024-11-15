
import pymysql
import logging


# Configure logging
logging.basicConfig(filename='rds_connection.log', level=logging.INFO)


def connect_to_rds():
    host = 'mydatabase.croe8gsu428c.us-west-1.rds.amazonaws.com'
    port = 3306
    user = 'admin'
    password = 'Seshu123('
    database = 'MyFlexonDB'


    try:
        # Establishing the connection to RDS
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        logging.info("Successfully connected to RDS.")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to RDS: {e}")
        return None


