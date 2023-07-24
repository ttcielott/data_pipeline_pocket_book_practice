import configparser
import psycopg2

# get db Redshift connection info
parser = configparser.ConfigParser()
parser.read("/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice/pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")
# connect to the redshift cluster
rs_conn = psycopg2.connect(
            "dbname=" + dbname
            + " user=" + user
            + " password=" + password
            + " host=" + host
            + " port=" + port)

with open('create_tables.sql', 'r') as file:
    rs_sql = file.read()

rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_sql)
rs_cursor.close()
rs_conn.commit()