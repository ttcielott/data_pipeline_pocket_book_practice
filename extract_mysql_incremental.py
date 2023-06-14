import configparser
import psycopg2
import boto3

# initialise a connection to the Redshift database
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")
username = parser.get("aws_creds", "username")
dbname = parser.get("aws_creds", "database")
password = parser.get("aws_creds", "password")
rs_conn = psycopg2.connect("dbname=" + dbname
        + " user=" + username
        + " password=" + password
        + " host=" + hostname
        + " port=" + port)
if rs_conn is None:
    print("Error connecting to the Redshift database")
else:
    print("Redshift connection established!")

# fetch the last updated date
rs_query = """SELECT COALESCE(MAX(LastUpdated, '1900-01-01')
            FROM Orders;"""
rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_query)
result = rs_cursor.fetchone()

# there is only one row and column returned
last_updated_warehouse = result[0]

rs_cursor.close()
rs_conn.commit()



