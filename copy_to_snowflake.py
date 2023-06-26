import snowflake.connector
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
username = parser.get("snowflat_cred", "username")
password = parser.get("snowflat_cred", "password")
account_name = parser.get("snowflat_cred", "account_name")

snow_conn = snowflake.connector(
    user = username,
    password = password,
    account = account_name
)

# load the multiple files into the table at once
sql = """
     COPY INTO destination_table
     FROM @my_s3_stage
     pattern = '.*extract.*.csv';
"""

cur = snow_conn.cursor()
cur.execute(sql)
cur.close()
snow_conn.close()
