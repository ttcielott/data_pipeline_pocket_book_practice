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


# CDC (change data capture) shows the current state of all orders, but also their full history 
# let's create the example table of the data ingested by  CDC.

orders_cdc_sql = ["DROP TABLE IF EXISTS Orders_cdc;",
                  """
                  CREATE TABLE Orders_cdc 
                  (EventType varchar(20), OrderId int, OrderStatus varchar(20), 
                  LastUpdated timestamp);
                  """,
                  """
                  INSERT INTO Orders_cdc 
                  VALUES('insert',1,'Backordered', '2020-06-01 12:00:00');
                  """,
                  """
                  INSERT INTO Orders_cdc 
                  VALUES('update',1,'Shipped', '2020-06-09 12:00:25');
                  """,
                  """
                  INSERT INTO Orders_cdc 
                  VALUES('delete',1,'Shipped', '2020-06-10 9:05:12');
                  """,
                  """
                  INSERT INTO Orders_cdc 
                  VALUES('insert',2,'Backordered', '2020-07-01 11:00:00');
                  """,
                  """
                  INSERT INTO Orders_cdc 
                  VALUES('update',2,'Shipped', '2020-07-09 12:15:12');
                  """,
                  """
                  INSERT INTO Orders_cdc 
                  VALUES('insert',3,'Backordered', '2020-07-11 13:10:12');
                  """
                  ]
for command in orders_cdc_sql:
    cursor.execute(command)
    conn.commit()

print("Orders_cdc - Created & Populated!")

# how to model data stored in this way
# depends on what questions the data model sets out to answer

print("""Data Modeling:
    When reporting on the current status of all orders
    for use on an operational dashboard
""")

# make a table of the latest update date by order id
# filter Order_cdc table by inner joining with it
# get count number by order status
data_modeling_1 = ["DROP TABLE IF EXISTS orders_current;",
                   """
                   CREATE TABLE orders_current(
                                order_status varchar(20),
                                order_count int
                   );
                   """,
                   """
                   INSERT INTO orders_current
                   (order_status, order_count)
                    WITH o_latest AS
                    (
                        SELECT
                            OrderId, 
                            MAX(LastUpdated) AS max_updated
                        FROM Orders_cdc
                        GROUP BY OrderId
                    )
                    SELECT
                        o.OrderStatus,
                        COUNT(*) AS order_count
                    FROM Orders_cdc o
                    INNER JOIN o_latest
                        ON o_latest.OrderId = o.OrderId
                            AND o_latest.max_updated = o.LastUpdated
                    GROUP BY o.OrderStatus;
                    """]

for i, command in enumerate(data_modeling_1):
    cursor.execute(command)
    conn.commit()
    print(f'command no. {i} - success!')

fetch_data_model_1 = """
                     SELECT * FROM orders_current;
                     """

cursor.execute(fetch_data_model_1)
results = cursor.fetchall()
column_names = [i[0] for i in cursor.description]
print(f"""\nData Model 1. Current Status of all orders
{pd.DataFrame(results, columns = column_names)}\n
""")
      
print("Take 'deletion' into account")
data_modeling_2 = ["DROP TABLE IF EXISTS orders_current;",
                   """
                   CREATE TABLE orders_current(
                                order_status varchar(20),
                                order_count int
                   );
                   """,
                   """
                   INSERT INTO orders_current
                   (order_status, order_count)
                    WITH o_latest AS
                    (
                        SELECT
                            OrderId, 
                            MAX(LastUpdated) AS max_updated
                        FROM Orders_cdc
                        GROUP BY OrderId
                    )
                    SELECT
                        o.OrderStatus,
                        COUNT(*) AS order_count
                    FROM Orders_cdc o
                    INNER JOIN o_latest
                        ON o_latest.OrderId = o.OrderId
                            AND o_latest.max_updated = o.LastUpdated
                    WHERE o.EventType <> 'delete'
                    GROUP BY o.OrderStatus;
                    """]

for i, command in enumerate(data_modeling_2):
    cursor.execute(command)
    conn.commit()
    print(f'command no. {i} - success!')


fetch_data_model_2 = """
                     SELECT * FROM orders_current;
                     """

cursor.execute(fetch_data_model_2)
results = cursor.fetchall()
column_names = [i[0] for i in cursor.description]
print(f"""\nData Model 2. Current Status of all orders (ingnore deletion)
{pd.DataFrame(results, columns = column_names)}\n
""")
      
print("how long, orders take to go from a backordered to shipped status\n")

data_modeling_3 = ["DROP TABLE IF EXISTS orders_time_to_ship;",
                   """
                   CREATE TABLE orders_time_to_ship(
                                OrderId int,
                                backordered_days int
                   );
                   """,
                   """
                   INSERT INTO orders_time_to_ship
                   (OrderId, backordered_days)
                    WITH o_backordered AS
                    (
                        SELECT
                            OrderId, 
                            MIN(LastUpdated) AS first_backordered
                        FROM Orders_cdc
                        WHERE OrderStatus = 'Backordered'
                        GROUP BY OrderId
                    ),
                    o_shipped AS
                    (
                        SELECT
                            OrderId,
                            MIN(LastUpdated) AS first_shipped
                            FROM Orders_cdc
                        WHERE OrderStatus = 'Shipped'
                        GROUP BY OrderId
                    )
                    -- MySQL doesn't have interval datatype, so I used the function, TIMESTAMPDIFF
                    SELECT
                        b.OrderId,
                        TIMESTAMPDIFF(DAY, b.first_backordered, s.first_shipped) AS backordered_days
                    FROM o_backordered b
                    INNER JOIN o_shipped s
                        ON s.OrderId = b.OrderId;
                    """]

for i, command in enumerate(data_modeling_3):
    cursor.execute(command)
    conn.commit()
    print(f'command no. {i} - success!')


fetch_data_model_3 = """
                     SELECT * FROM orders_time_to_ship;
                     """

cursor.execute(fetch_data_model_3)
results = cursor.fetchall()
column_names = [i[0] for i in cursor.description]
print(f"""\nData Model 3. Backordered Days
{pd.DataFrame(results, columns = column_names)}\n
""")
      
avg_backordered_days = """
                     SELECT AVG(backordered_days) FROM orders_time_to_ship;
                     """

cursor.execute(avg_backordered_days)
results = cursor.fetchall()
column_names = [i[0] for i in cursor.description]
print(f"""Average Backordered Days
{pd.DataFrame(results, columns = column_names)}\n
""")
cursor.close()
conn.close()

if not conn.open:
    print("mysql database connection - closed!")