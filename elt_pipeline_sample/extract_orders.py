import psycopg2
import configparser
import csv
import boto3

local_path = '/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice'
# get the postgresSQL database RDS credentials
parser = configparser.ConfigParser()
parser.read(f"{local_path}/pipeline.conf")
hostname = parser.get("postgres_config", "hostname")
username = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
port = parser.get("postgres_config", "port")
dbname = parser.get("postgres_config", "database")

# define the db connection
conn = psycopg2.connect(
    dbname = dbname,
    user = username,
    password = password,
    host = hostname,
    port = port
)

if conn is None:
    print("Error connecting to the PostgreSQL database")
else:
    print("PostgreSQL connection established!")

# run a full extraction of the Orders table
p_query = "SELECT * FROM Orders;"
local_filename= f"{local_path}/elt_pipeline_sample/order_extract.csv"
p_cursor = conn.cursor()
p_cursor.execute(p_query)
results = p_cursor.fetchall()

# write the fetched result to a pipe-delimited CSV file
with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter ='|') # pipe '|' as a delimiter
    csv_w.writerows(results)

p_cursor.close()
conn.close()

# load the AWS boto credentials values
parser = configparser.ConfigParser()
parser.read(f"{local_path}/pipeline.conf")
access_key = parser.get("aws_boto_credentials", 'access_key')
secret_key = parser.get("aws_boto_credentials", 'secret_key')
bucket_name = parser.get("aws_boto_credentials", 'bucket_name')

s3 = boto3.client('s3',
                  aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

s3_file = 'order_extract.csv'

s3.upload_file(local_filename, bucket_name, s3_file)