import configparser
import psycopg2

local_path = '/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice'
parser = configparser.ConfigParser()
parser.read(f"{local_path}/pipeline.conf")
hostname = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")
username = parser.get("aws_creds", "username")
dbname = parser.get("aws_creds", "database")
password = parser.get("aws_creds", "password")

print(username)

# redshift_connector doesn't work, so I used psycopg2 for this case
rs_conn = psycopg2.connect(
        "dbname=" + dbname
        + " user=" + username
        + " password=" + password
        + " host=" + hostname
        + " port=" + port)
if rs_conn is None:
    print("Error connecting to the Redshift database")
else:
    print("Redshift connection established!")

# load the account_id and iam_role from the conf file
parser = configparser.ConfigParser()
parser.read(f"{local_path}/pipeline.conf")
account_id = parser.get("aws_boto_credentials", "account_id")
iam_role = parser.get("aws_creds", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# run the COPY command to load the file into Redshift
# CAUTION: before running this code, 
# you should already have the table, Order in the Redshift database
file_path = ("s3://"
             + bucket_name
             + "/customer_extract.csv")
role_string = ("arn:aws:iam::"
               + account_id
               + ":role/" + iam_role)
sql = """truncate public.Customers;
        copy public.Customers
        from %s
        iam_role %s;"""

# create a cursor object and execute the COPY
cur = rs_conn.cursor()
cur.execute(sql, (file_path, role_string))

# close the cursor and commit the transaction
cur.close()
rs_conn.commit()

# close the connection
rs_conn.close()