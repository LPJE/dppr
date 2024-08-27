import pymysql
import configparser
import logging
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
def load_config(config_file="pipeline.conf"):
    parser = configparser.ConfigParser()
    parser.read(config_file)
    return {
        'hostname': parser.get("mysql_config", "hostname"),
        'port': parser.getint("mysql_config", "port"),
        'username': parser.get("mysql_config", "username"),
        'dbname': parser.get("mysql_config", "dbname"),
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

# Main execution
if __name__ == "__main__":
    try:
        config = load_config()
        with mysql_connection(config) as conn:
            # Placeholder for further database operations
            pass
    except Exception as e:
        logger.exception("An error occurred during database operations")
