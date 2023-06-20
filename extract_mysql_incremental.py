import configparser
import redshift_connector
import pymysql
import csv
import boto3

# initialise a connection to the Redshift database
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")
username = parser.get("aws_creds", "username")
dbname = parser.get("aws_creds", "database")
password = parser.get("aws_creds", "password")

print(username)

rs_conn = redshift_connector.connect(
     host=hostname,
     database=dbname,
     port=int(port),
     user=username,
     password=password
     )
if rs_conn is None:
    print("Error connecting to the Redshift database")
else:
    print("Redshift connection established!")

# fetch the last updated date
rs_query = """SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
              FROM Orders;"""
rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_query)
result = rs_cursor.fetchone()

# there is only one row and column returned
last_updated_warehouse = result[0]
print(last_updated_warehouse)

rs_cursor.close()
rs_conn.commit()
rs_conn.close()

# get the MySQL connection info and connect
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host = hostname,
                       user = username,
                       password = password,
                       db = dbname,
                       port= int(port))

if conn is None:
    print("Error connecting to the MySQL database")
else:
    print("MySQL connection established")

m_query = """SELECT *
          FROM Orders
          WHERE LastUpdated > %s;"""
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query, (last_updated_warehouse,))
results = m_cursor.fetchall()

with open(local_filename,'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

m_cursor.close()
conn.close()

load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client('s3',
                  aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

s3_file = local_filename

s3.upload_file(
    local_filename,
    bucket_name,
    s3_file
)