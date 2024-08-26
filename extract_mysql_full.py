# INCLUDE LOGGING
# INCLUDE SIMPLE TESTS
# INCLUDE LINTER
# INCLUDE FORMATTER
# put all of this on a different branch than main.

import pymysql
import csv
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read("pipleine.conf")

hostname = parser.get("mysql_config","hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config","username")
dbname = parser.get("mysql_config", "dbname")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port)
                       )

if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established")