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

# Modeling Append-Only Data
# The case that each record is some kind of event that never changes
# example: a table of all page views on a website
# Modeling append-only data is similar to modeling fully refreshed data

page_view_sql = ["DROP TABLE IF EXISTS PageViews",
                 """
                 CREATE TABLE PageViews(
                    CustomerId int,
                    ViewTime timestamp,
                    UrlPath varchar(250),
                    utm_medium varchar(50)
                 );
                 """,
                 """
                 INSERT INTO PageViews VALUES(100,'2020-06-01 12:00:00',
                 '/home','social');
                 """,
                 """
                 INSERT INTO PageViews
                 VALUES(100,'2020-06-01 12:00:13', '/product/2554',NULL);
                 """,                 
                 """
                 INSERT INTO PageViews VALUES(101,'2020-06-01 12:01:30',
                 '/product/6754','search');
                 """,
                 """
                 INSERT INTO PageViews VALUES(102,'2020-06-02 7:05:00', '/home','NULL');
                 """,
                 """
                 INSERT INTO PageViews VALUES(101,'2020-06-02 12:00:00',
                 '/product/2554','social');
                 """
                 ]

for command in page_view_sql:
    cursor.execute(command)
    conn.commit()
print("pageviews table - created")

# Questions to answer 
# Q1. How many page views are there for each UrlPath on the site by day?
# Q2. How many page views do customers from each country generate each date?

# granularity : daily
# attributes required: the date, urlpath, country, 
# metrics required: a count of page views

# create the model

data_modeling_sql = ["DROP TABLE IF EXISTS pageviews_daily",
                     """
                     -- structure of the model
                     CREATE TABLE pageviews_daily (
                                    view_date date,
                                    url_path varchar(250),
                                    customer_country varchar(50),
                                    view_count int
                     );
                     """,
                     """
                     INSERT INTO pageviews_daily (view_date, url_path, customer_country, view_count)
                     SELECT CAST(p.ViewTime as Date) AS view_date,
                            p.UrlPath as url_path,
                            c.CustomerCountry as customer_country,
                            COUNT(*) AS view_count
                     FROM PageViews p
                     LEFT JOIN Customers c
                     ON p.CustomerId = c.CustomerId
                     GROUP BY view_date, url_path, customer_country;            
                    """
                ]

for command in data_modeling_sql:
    cursor.execute(command)
    conn.commit()
print("data model: pageviews_daily table - created")

print("Q1. How many page views do customers from each country generate each day? \n")

q1_query = """
           SELECT view_date,
                  customer_country,
                  SUM(view_count) 
           FROM pageviews_daily
           GROUP BY view_date, customer_country
           ORDER BY view_date, customer_country;
           """

cursor.execute(q1_query)
results = cursor.fetchall()
print(pd.DataFrame(results))


print("Now rec")