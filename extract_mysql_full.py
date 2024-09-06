# Review codebase AS IS
# Set up S3 and relevant connection

import pymysql
import configparser
import logging
from contextlib import contextmanager
from file_and_directory_check import check_directory_and_file
import csv

# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler to log messages to a file
file_handler = logging.FileHandler('./logs/main_file.log')
file_handler.setLevel(logging.DEBUG)

# Create a console handler to print messages to the terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter for the log messages
formatter = logging.Formatter('%(name)s - %(lineno)d - %(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Add the formatter to both handlers
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)




# Directory and File Check
directory="./"
file_name="pipeline.conf"

expected_directory = directory
expected_file_name = file_name
# Need to make this to stop program if check fails
check_directory_and_file(expected_directory, file_name)


# Load configuration
def load_config(config_file=directory+file_name):

    parser = configparser.ConfigParser()
    parser.read(config_file)
    return {
        'hostname': parser.get("mysql_config", "hostname"),
        'port': parser.getint("mysql_config", "port"),
        'username': parser.get("mysql_config", "username"),
        'dbname': parser.get("mysql_config", "database"),
        'password': parser.get("mysql_config", "password")
    }


# Context manager for MySQL connection
@contextmanager
def mysql_connection(config):
    conn = None
    try:
        conn = pymysql.connect(
            host=config['hostname'],
            user=config['username'],
            password=config['password'],
            db=config['dbname'],
            port=config['port']
        )
        logger.info("MySQL connection established")
        yield conn
    except pymysql.MySQLError as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("MySQL connection closed")


def main():
    try:
        config = load_config()  # Ensure load_config is defined or imported
        logger.info(config)
        with mysql_connection(config) as conn:  # Ensure mysql_connection is defined or imported
            m_query = "SELECT * FROM dppr.Orders;"
            local_filename = "order_extract.csv"

            with conn.cursor() as m_cursor:
                m_cursor.execute(m_query)
                results = m_cursor.fetchall()

                with open(local_filename, 'w') as fp:
                    csv_w = csv.writer(fp, delimiter='|')
                    csv_w.writerows(results)

    except Exception as e:
        logger.exception("An error occurred during database operations")

if __name__ == "__main__":
    main()


# load data to S3 bucket

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key =  parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto.clent('s3',
                aws_access_key = access_key,
                aws_secret_access_key = secret_key
                )
s3_file = local_filename

s3.upload_file(local_filename, bucket_name, s3_file)