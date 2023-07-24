from configparser import ConfigParser
import boto3
import csv
import psycopg2

# get db Redshift connection info
parser = ConfigParser()
parser.read("/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice/pipeline.conf")
hostname = parser.get('aws_creds', 'host')
username = parser.get('aws_creds', 'username')
password = parser.get('aws_creds', 'password')
port = parser.get('aws_creds', 'port')
dbname = parser.get('aws_creds', 'database')

# connect to the redshift cluster
rs_conn = psycopg2.connect("dbname=" + dbname
                           + " user=" + username
                           + " password=" + password
                           + " host=" + hostname
                           + " port=" + port)

if rs_conn:
    print("reshift connection: success!")

# load the account_id and iam_role from the conf files
parser = ConfigParser()
parser.read("/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice/pipeline.conf")
account_id = parser.get('aws_boto_credentials', 'account_id')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')
iam_role = parser.get('aws_creds', 'iam_role')

# run the COPY command to ingest into Redshift
file_path = ("s3://"
             + bucket_name
             + "/dag_run_extract.csv")
role_string = ("arn:aws:iam::"
               + account_id
               + ":role/" + iam_role)
sql = """
      COPY dag_run_history
      (id, dag_id, execution_date,
      state, run_id, external_trigger,
      end_date, start_date)  
      """
sql = sql + " FROM %s "
sql = sql + " iam_role %s;"

cur = rs_conn.cursor()
cur.execute(sql, (file_path, role_string))

# close the cursor and commit the transaction
cur.close()
rs_conn.commit()

# close the connection
rs_conn.close()
