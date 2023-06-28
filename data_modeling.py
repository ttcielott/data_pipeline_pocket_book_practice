# create a data model that can be queried to answer the following questions
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

# Q1. How much revenue was generated from orders placed from a given country in a given month?
# Q2. How many orders were placed on a given day?

# In this case, the granularity of the model is daily
# the smallest unit of time in the requirements is by day
# time in requirements : "on a given day", "in a given month"
# If the model only needs to provide measures by month, 
# then setting daily granu‐ larity isn’t necessary
# , and will only increase the number of records your model needs to store and query.

# you need a table that includes total revenue per each date, order country
data_model_sql = ["DROP TABLE IF EXISTS order_summary_daily",
                  """CREATE TABLE IF NOT EXISTS order_summary_daily (
                    order_date date,
                    order_country varchar(10),
                    total_revenue decimal(65, 2),
                    order_count int
                    );
                  """,
                  """
                    INSERT INTO order_summary_daily
                    (order_date, order_country, total_revenue, order_count)
                    SELECT
                    o.OrderDate as order_date,
                    c.CustomerCountry as order_country,
                    SUM(o.OrderTotal) as total_revenue,
                    COUNT(o.OrderId) as order_count
                    FROM Orders o
                    INNER JOIN Customers c
                    ON o.CustomerId = c.CustomerId
                    GROUP BY o.OrderDate, c.CustomerCountry;
                    """]

for command in data_model_sql:
    cursor.execute(command)
    conn.commit()

print("data model - success!")
# now, query the model in order to answer the questions set out in the requirements

q1_query = """
            SELECT MONTH(order_date) AS order_month, order_country, SUM(total_revenue) AS order_revenue
            FROM order_summary_daily
            GROUP BY order_month, order_country
            ORDER BY order_month, order_country;
           """
cursor.execute(q1_query)
q1_answer = cursor.fetchall()
q1_df = pd.DataFrame(q1_answer, columns = ['order_month', 'order_country', 'order_revenue'])
print(f"""How much revenue was generated from orders placed from a given country in a given month?
{q1_df}""")
      

q2_query = """
            SELECT 
            order_date,
            SUM(order_count) AS total_orders
            FROM order_summary_daily
            GROUP BY order_date
            ORDER BY order_date;
           """
cursor.execute(q2_query)
q2_answer = cursor.fetchall()
q2_df = pd.DataFrame(q2_answer, columns = ['order_date', 'total_orders'])
print(f"""How many orders were placed on a given day?
{q2_df}""")
      
cursor.close()
conn.close()

