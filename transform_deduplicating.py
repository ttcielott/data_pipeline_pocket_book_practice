import configparser
import pymysql
import time

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
username = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
database = parser.get("mysql_config", "database")
port = parser.get("mysql_config", "port")

def db_conn(host, user, pwd, db, port):
    conn = pymysql.connect(host = host,
                        user = user,
                        password = pwd,
                        database = db,
                        port = int(port))
    cursor = conn.cursor()
    return conn, cursor


def create_table_with_duplicates():
    conn, cursor = db_conn(hostname, username, password, database, port)

    # create the table, order with the duplicate records
    commands = ["DROP TABLE IF EXISTS Orders;",
                """
                CREATE TABLE Orders(OrderId int,
                                OrderStatus varchar(30),
                                LastUpdated timestamp);
                """,
                "INSERT INTO Orders VALUES(1,'Backordered', '2020-06-01');",
                "INSERT INTO Orders VALUES(1,'Shipped', '2020-06-09');",
                "INSERT INTO Orders VALUES(2,'Shipped', '2020-07-11');",
                "INSERT INTO Orders VALUES(1,'Shipped', '2020-06-09');",
                "INSERT INTO Orders VALUES(3,'Shipped', '2020-07-12');"
                ]

    for command in commands:
        cursor.execute(command)

    conn.commit()
    cursor.close()

    print("create_table_with_duplicates - Success!")

# deduplicating records method 1.

# create the table including duplicate records
create_table_with_duplicates()

# record the start time
method1_start_time = time.time()

# deduplicating query
conn, cursor = db_conn(hostname, username, password, database, port)
commands = ["""CREATE TABLE Orders_distinct AS
               SELECT DISTINCT OrderID, OrderStatus, LastUpdated FROM Orders;""",
            "TRUNCATE Orders;",
            """INSERT INTO Orders
               SELECT * FROM Orders_distinct;""",
            "DROP TABLE Orders_distinct;"]

for command in commands:
    cursor.execute(command)

conn.commit()
cursor.close()
conn.close()

print("deduplicating records method 1. - Success!")

# record the end time
method1_end_time = time.time()

print(f"processing time for method 1. {method1_end_time - method1_start_time}")





# deduplicating records method 2.

# create the table including duplicate records
create_table_with_duplicates()

# record the start time
method2_start_time = time.time()

# deduplicating query
conn, cursor = db_conn(hostname, username, password, database, port)
commands = ["""CREATE TABLE all_orders AS
               SELECT OrderID, 
                      OrderStatus,
                      LastUpdated,
                      ROW_NUMBER() OVER(PARTITION BY OrderID,
                                                     OrderStatus,
                                                     LastUpdated)
                      AS dup_count 
               FROM Orders;""",
            "TRUNCATE Orders;",
            """INSERT INTO Orders
                           (OrderID, OrderStatus, LastUpdated)
               SELECT OrderID,
                      OrderStatus,
                      LastUpdated
               FROM all_orders
               WHERE dup_count = 1;""",
            "DROP TABLE all_orders;"]

for command in commands:
    cursor.execute(command)

conn.commit()
cursor.close()
conn.close()

print("deduplicating records method 2. - Success!")

# record the end time
method2_end_time = time.time()

print(f"processing time for method 2. {method2_end_time - method2_start_time}")

