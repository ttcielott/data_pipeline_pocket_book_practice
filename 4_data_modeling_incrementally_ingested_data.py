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

# incremental data ingestion: 
# data contains the current state of the source data
# & historical records from since the ingestion started

# create and populate the table, Customer_staging in MySQL database
# which is the example of incrementally loaded tables
print("PART 2 : data modeling - start")
customer_staging_sql = [
    "DROP TABLE IF EXISTS Customers_staging",
    """CREATE TABLE Customers_staging ( CustomerId int,
                    CustomerName varchar(20), CustomerCountry varchar(10), LastUpdated timestamp
                    );""",
    "INSERT INTO Customers_staging VALUES(100,'Jane','USA','2019-05-01 7:01:10');",
    "INSERT INTO Customers_staging VALUES(101,'Bob','UK','2020-01-15 13:05:31');",
    "INSERT INTO Customers_staging VALUES(102,'Miles','UK','2020-01-29 9:12:00');",
    "INSERT INTO Customers_staging VALUES(100,'Jane','UK','2020-06-20 8:15:34');"
]
for command in customer_staging_sql:
    cursor.execute(command)
    conn.commit()

# The questions the model needs to answer
# Q1. How much revenue was generated from orders placed from a given country in a given month?
# Q2. How many orders were placed on a given day?
data_modeling_sql_v1 = [
                    "DROP TABLE IF EXISTS order_summary_daily_current",
                    """CREATE Table order_summary_daily_current(
                        order_date date,
                        customer_country varchar(10),
                        order_revenue decimal(65,2),
                        order_count int
                        );
                    """, 
                    """
                    INSERT INTO order_summary_daily_current
                    (order_date, customer_country, order_revenue, order_count)
                    WITH customers_current AS
                    (
                        SELECT CustomerId,
                            MAX(LastUpdated) AS latest_update
                        FROM Customers_staging
                        GROUP BY CustomerId
                    )
                    SELECT 
                        o.OrderDate AS order_date,
                        cs.CustomerCountry AS order_country,
                        SUM(o.OrderTotal) AS order_revenue,
                        COUNT(o.OrderId) AS order_count
                    FROM Orders o
                    INNER JOIN customers_current cc
                        ON o.CustomerId = cc.CustomerId
                    INNER JOIN Customers_staging cs
                        ON cc.CustomerId = cs.CustomerId
                            AND cc.latest_update = cs.LastUpdated
                    GROUP BY o.OrderDate, cs.CustomerCountry
                    ORDER BY o.OrderDate, cs.CustomerCountry;
                    """
]

for command in data_modeling_sql_v1:
    cursor.execute(command)
    conn.commit()
print("PART 1 : data modeling - success!")

# order revenue by order month and order country
# in this case, order revenues are calculated into the latest customer country
# (the country that a customer lived in the past is to be ignored)

q1_query = """
            SELECT MONTH(order_date) AS order_month, customer_country, SUM(order_revenue) 
            FROM order_summary_daily_current
            GROUP BY order_month, customer_country
            ORDER BY order_month, customer_country;
           """

cursor.execute(q1_query)
results = cursor.fetchall()
print(f"""PART 1 : query -
order revenue of each month by order country(past living country ignored)
{pd.DataFrame(results)}""")
      


# if instead you want to allocate orders to the country that the customers lived
# in at the time of order, you need the different logic in your data model
# find the customer staging record when the order was placed
# let's create data model of which name is 'order_summary_daily_pit'
# 'pit' refers to 'point-in-time'

print("PART 2 : data modeling - start")
data_modeling_sql_v2 = [
                    "DROP TABLE IF EXISTS order_summary_daily_pit",
                    """CREATE Table order_summary_daily_pit(
                        order_date date,
                        customer_country varchar(10),
                        order_revenue decimal(65,2),
                        order_count int
                        );
                    """, 
                    """
                    INSERT INTO order_summary_daily_pit
                    (order_date, customer_country, order_revenue, order_count)
                    -- Using CTE
                    -- find the lastest one among updated dates by customer id and order id
                    WITH customers_pit AS
                    (
                        SELECT cs.CustomerId,
                               o.OrderId,
                               MAX(LastUpdated) AS max_update_date
                        FROM Orders o
                        INNER JOIN Customers_staging cs
                            ON cs.CustomerId = o.CustomerId
                            AND cs.LastUpdated <= o.OrderDate
                        GROUP BY cs.CustomerId, o.OrderId
                    )
                    SELECT 
                        o.OrderDate AS order_date,
                        cs.CustomerCountry AS order_country,
                        SUM(o.OrderTotal) AS order_revenue,
                        COUNT(o.OrderId) AS order_count
                    FROM Orders o
                    INNER JOIN customers_pit cp
                        ON o.CustomerId = cp.CustomerId
                        AND o.OrderId = cp.OrderId
                    INNER JOIN Customers_staging cs
                        ON cp.CustomerId = cs.CustomerId
                            AND cp.max_update_date = cs.LastUpdated
                    GROUP BY o.OrderDate, cs.CustomerCountry
                    ORDER BY o.OrderDate, cs.CustomerCountry;
                    """
]

for command in data_modeling_sql_v2:
    cursor.execute(command)
    conn.commit()

print("PART 2 : data modeling - success!")

q1_query_2 = """
            SELECT MONTH(order_date) AS order_month, customer_country, SUM(order_revenue) 
            FROM order_summary_daily_pit
            GROUP BY order_month, customer_country
            ORDER BY order_month, customer_country;
           """

cursor.execute(q1_query_2)
results2 = cursor.fetchall()
print(f"""PART 2 : query -
order revenue of each month by order country(past living country considered)
{pd.DataFrame(results2)}""")

cursor.close()
conn.close()
if not conn.open:
    print("mysql database connection - closed") 