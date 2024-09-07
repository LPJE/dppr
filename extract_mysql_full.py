import csv
import configparser
import logging
from contextlib import contextmanager
import boto3
import pymysql
from file_and_directory_check import check_directory_and_file


def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('./logs/main_file.log')
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(name)s - %(lineno)d - %(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


def load_config(config_file='pipeline.conf'):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    try:
        config = {
            'hostname': parser.get("mysql_config", "hostname"),
            'port': parser.getint("mysql_config", "port"),
            'username': parser.get("mysql_config", "username"),
            'dbname': parser.get("mysql_config", "database"),
            'password': parser.get("mysql_config", "password"),
            'aws_access_key': parser.get("aws_boto_credentials", "access_key"),
            'aws_secret_key': parser.get("aws_boto_credentials", "secret_key"),
            'bucket_name': parser.get("aws_boto_credentials", "bucket_name")
        }
    except configparser.NoSectionError as e:
        logger.error(f"Configuration section missing: {e}")
        raise
    except configparser.NoOptionError as e:
        logger.error(f"Configuration option missing: {e}")
        raise

    return config


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
    config = load_config()

    # Check directory and file
    if not check_directory_and_file("./", "pipeline.conf"):
        logger.error("Directory or file check failed.")
        return

    try:
        with mysql_connection(config) as conn:
            m_query = "SELECT * FROM dppr.Orders;"
            local_filename = "order_extract.csv"

            with conn.cursor() as m_cursor:
                m_cursor.execute(m_query)
                results = m_cursor.fetchall()

                with open(local_filename, 'w') as fp:
                    csv_w = csv.writer(fp, delimiter='|')
                    csv_w.writerows(results)

            # Upload to S3
            s3 = boto3.client(
                's3',
                aws_access_key_id=config['aws_access_key'],
                aws_secret_access_key=config['aws_secret_key']
            )
            s3_file = local_filename
            s3.upload_file(local_filename, config['bucket_name'], s3_file)

    except Exception as e:
        logger.exception("An error occurred during database operations")


if __name__ == "__main__":
    main()
