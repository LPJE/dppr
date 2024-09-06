import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")

print(parser.sections())  # Should include 'mysql_config'

hostname = parser.get('mysql_config', "hostname")
print("Hostname:", hostname)
