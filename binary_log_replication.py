from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication
import csv
import boto3

# get the MySQL connection info
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")

mysql_settings = {
    "host": hostname,
    "port": int(port),
    "user": username,
    "passwd": password
}

b_stream = BinLogStreamReader(
    connection_settings = mysql_settings,
    server_id = 100,
    only_events = [row_event.DeleteRowsEvent,
                   row_event.WriteRowsEvent,
                   row_event.UpdateRowsEvent]
                   )

# parse out the event attributes
order_events = []

for binlogevent in b_stream:
    for row in binlogevent.rows:
        # this example of code will write events related to the Orders table
        print(f'table name is {binlogevent.table}')
        if binlogevent.table == 'Orders':
            event = {}
            # row_event is imported from pymysqlreplication 
            if isinstance(
                binlogevent, row_event.DeleteRowsEvent
            ):
                event["action"] = "delete"
                # add all key and values from row["value"], which is in dictionary style format.
                event.update(row["value"].items())
            elif isinstance(
                binlogevent, row_event.UpdateRowsEvent
            ):
                event["action"] = "update"
                event.update(row["after_values"].items())
                
            elif isinstance(
                binlogevent, row_event.WriteRowsEvent
            ):
                event["action"] = "insert"
                event.update(row["values"].items())

            # append a single event dictionary to the list, order_events
            order_events.append(event)


b_stream.close()

print(order_events)

# select the first event and extract the key names
keys = order_events[0].keys()

local_filename = 'orders_extract_binlog.csv'

with open(local_filename, 'w', newline = '') as output_file:
    dict_writer = csv.DictWriter(output_file, keys, delimiter = '|')
    dict_writer.writerows(order_events)

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# load csv onto S3 bucket
s3 = boto3.client('s3',
                  aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

s3_file = local_filename

s3.upload_file(local_filename, bucket_name, s3_file)
