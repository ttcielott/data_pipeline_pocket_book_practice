import configparser
import pymysql
import pandas as pd

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("mysql_config", "hostname")
username = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
database = parser.get("mysql_config", "database")
port = parser.get("mysql_config", "port")

# create a database connection
conn = pymysql.connect(
    host = host,
    user = username,
    password = password,
    database = database,
    port = int(port)
    )
print("mysql database connection - success!")
cursor = conn.cursor()


# SCD (slowly changing dimension)
# a slowly changing dimension in data management and data warehousing is a dimension which contains relatively static data 
# which can change slowly but unpredictably
# SCD example: customer address

# One of the most frequently used method of handling this scenario is "Kimball modeling".
# In case that the analytics team utilises the change history of the dimension, and the data ingestion is fully refreshed(entire table copy) each time, 
# it's neccessary to keep a full history of such changes between each ingestion.
# There are three types of the methods, in this practice, we are going to use Type -2.

# Slowly Changing Dimension Type - 2: Preserve the history by adding new record to a table each change to an entity
# including the date range that the record was valid

scd_table_query = ["DROP TABLE IF EXISTS Customers_scd;",
                   """
                    CREATE TABLE Customers_scd (CustomerId int, 
                                                CustomerName varchar(20), 
                                                CustomerCountry varchar(10), 
                                                ValidFrom timestamp, 
                                                Expired timestamp);

                    """, 
                    """
                    INSERT INTO Customers_scd VALUES(
                        100,'Jane','USA','2019-05-01 7:01:10','2020-06-20 8:15:34');
                    """,
                    """
                    -- having expired date far in the future  makes querying the table less error prone
                    INSERT INTO Customers_scd
                    -- the code example in the book use 2199 as the year of expired, 
                    -- I have changed it as below as mysql's max timestamp is 2038
                    VALUES(100,'Jane','UK','2020-06-20 8:15:34', '2037-12-31 00:00:00');
                    """]


# execute the scd table query
for command in scd_table_query:
    cursor.execute(command)
    conn.commit()

print("Customers scd table creation and insertion - success!")

# query the customer record at the time of the order
query = """
        SELECT o.OrderId,
               o.OrderDate,
               c.CustomerName,
               c.CustomerCountry
        FROM Orders o
        INNER JOIN Customers_scd c ON o.CustomerId = c.CustomerId
        WHERE o.OrderDate BETWEEN c.ValidFrom AND c.Expired
        ORDER BY o.OrderDate;
        """

# fetch the result
cursor.execute(query)
results = cursor.fetchall()
print("Fetching Query - success!")

cursor.close()
conn.close()

# show the results in the dataframe
print(cursor.description)
column_names = [i[0] for i in cursor.description]
print(pd.DataFrame(results, columns = column_names))