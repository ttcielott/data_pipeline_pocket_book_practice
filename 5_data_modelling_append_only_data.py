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



new_pageview_sql = ["""
                    INSERT INTO PageViews
                    VALUES(102, '2020-06-02 12:03:42', '/home', NULL);
                    """,
                    """
                    INSERT INTO PageViews
                    VALUES(101, '2020-06-03 12:25:01', '/product/567', 'social');
                    """
                    ]
for command in new_pageview_sql:
    cursor.execute(command)
    conn.commit()

print("Now two new records were added to the table, 'PageView' (which is append-only data) \n")

print("""How to Update the Model: Approach 1. find the records of which dates are greater than
 the latest record date in the data model \n""")

update_model_1 = ["DROP TABLE IF EXISTS pageviews_daily_2;",
                 """
                 -- get all record from the current pageviews_daily(data model)
                 CREATE TABLE pageviews_daily_2 AS
                 SELECT * FROM pageviews_daily;
                 """,
                 """
                 INSERT INTO pageviews_daily_2
                 (view_date, url_path, customer_country, view_count)
                 SELECT CAST(p.ViewTime as Date) as view_date,
                        p.UrlPath as url_path,
                        c.CustomerCountry AS customer_country,
                        COUNT(*) AS view_count
                 FROM PageViews p
                 LEFT JOIN Customers c
                    ON p.CustomerId = c.CustomerId
                 WHERE p.ViewTime > (SELECT MAX(view_date) FROM pageviews_daily_2)
                 GROUP BY view_date, url_path, customer_country;
                 """
                ]

for command in update_model_1:
    cursor.execute(command)
    conn.commit()

b4update_model_query = """
                      SELECT * FROM pageviews_daily
                      ORDER BY view_date, url_path, customer_country;
                      """

updated_model_query = """
                      SELECT * FROM pageviews_daily_2
                      ORDER BY view_date, url_path, customer_country;
                      """
cursor.execute(b4update_model_query)
b4updated = cursor.fetchall()
print(f'pageviews_daily (Data Model Before Update) \n {pd.DataFrame(b4updated)}\n')

cursor.execute(updated_model_query)
updated = cursor.fetchall()
print(f'pageviews_daily_2 (Data Model After Update) \n {pd.DataFrame(updated)}\n')

print("""two new records were added to PageView (append-only table), but this data model shows 4 more records.
Why? It's because the code found the PageView records of which dates are greater than 
the maximum date of the date in pageviews_daily (current data model), 
but it is comparison between timestamp and date; PageView uses timestamp while pageviews_daily uses date, 
so that caused the duplicate.

Maximum date in pageviews_daily_2: 2020-06-02
Dates in updated PageView:
2020-06-01 12:00:00
2020-06-01 12:00:13
2020-06-01 12:01:30
2020-06-02 7:05:00
2020-06-02 12:00:00
2020-06-02 12:03:42 (newly added)
2020-06-03 12:25:01 (newly added)

What the code will collect from PageView as newly added data, and add to pageviews_daily_2:

Dates in updated PageView:
2020-06-01 12:00:00 already in 'pageviews_daily'
2020-06-01 12:00:13 already in 'pageviews_daily'
2020-06-01 12:01:30 already in 'pageviews_daily'
2020-06-02 7:05:00  already in 'pageviews_daily' (greater than 2020-06-02 00:00:00 ? YES -> add again) duplicate
2020-06-02 12:00:00 already in 'pageviews_daily' (greater than 2020-06-02 00:00:00 ? YES -> add again) duplicate
2020-06-02 12:03:42 (greater than 2020-06-02 00:00:00 ? YES)
2020-06-03 12:25:01 (greater than 2020-06-02 00:00:00 ? YES)

""")


print("""How to Update the Model: Approach 2. find the records of which timestamps are greater than
 the latest record timestamp in the data model \n""")


update_model_2 = ["DROP TABLE IF EXISTS pageviews_daily_3;",
                 """
                 -- get all record from the current pageviews_daily(data model)
                 CREATE TABLE pageviews_daily_3 AS
                 SELECT * FROM pageviews_daily;
                 """,
                 """
                 INSERT INTO pageviews_daily_3
                 (view_date, url_path, customer_country, view_count)
                 SELECT CAST(p.ViewTime as Date) as view_date,
                        p.UrlPath as url_path,
                        c.CustomerCountry AS customer_country,
                        COUNT(*) AS view_count
                 FROM PageViews p
                 LEFT JOIN Customers c
                    ON p.CustomerId = c.CustomerId
                 WHERE p.ViewTime > '2020-06-02 12:00:00'
                 GROUP BY view_date, url_path, customer_country;
                 """
                ]

for command in update_model_2:
    cursor.execute(command)
    conn.commit()

b4update_model_query2 = """
                      SELECT * FROM pageviews_daily
                      ORDER BY view_date, url_path, customer_country;
                      """

updated_model_query2 = """
                      SELECT * FROM pageviews_daily_3
                      ORDER BY view_date, url_path, customer_country;
                      """
cursor.execute(b4update_model_query2)
b4updated2 = cursor.fetchall()
print(f'pageviews_daily (Data Model Before Update) \n {pd.DataFrame(b4updated2)}\n')

cursor.execute(updated_model_query2)
updated2 = cursor.fetchall()
print(f'pageviews_daily_3 (Data Model After Update) \n {pd.DataFrame(updated2)}\n')

print("""In this case, new records were asked correctly.
Row 3,4 could have been combined into a single one with a `view_count` value of 2
It's wasteful to store data we don't need. 
Though the sample table is small in this case, 
but it's not uncommon for such tables to have many billion records in reality.
The number of unnecessary duplicated records add up and wastes storage and future query time.
""")

print("""How to Update the Model: Approach 3. 
1. make a copy of the data model with a temporary name with all records up through the second to last dat that it currently contains.
2. insert all records from the source table into the copy starting on the next day.
3. truncate the existing data model table and load the data from the table with a temporary name.
4. drop the temporary table. \n""")


update_model_3 = ["DROP TABLE IF EXISTS temp_pageviews_daily;",
                 """
                 -- get all record from the current pageviews_daily up through the second to last day
                 CREATE TABLE temp_pageviews_daily AS
                 SELECT * FROM pageviews_daily
                 WHERE view_date < (SELECT MAX(view_date) FROM pageviews_daily);
                 """,
                 """
                 INSERT INTO temp_pageviews_daily
                 (view_date, url_path, customer_country, view_count)
                 SELECT CAST(p.ViewTime as Date) as view_date,
                        p.UrlPath as url_path,
                        c.CustomerCountry AS customer_country,
                        COUNT(*) AS view_count
                 FROM PageViews p
                 LEFT JOIN Customers c
                    ON p.CustomerId = c.CustomerId
                 WHERE p.ViewTime > (SELECT MAX(view_date) FROM pageviews_daily)
                 GROUP BY view_date, url_path, customer_country;
                 """,
                 "TRUNCATE TABLE pageviews_daily;",
                 """
                 INSERT INTO pageviews_daily
                 SELECT * FROM temp_pageviews_daily;
                 """,
                 "DROP TABLE temp_pageviews_daily;"
                ]

for command in update_model_3:
    cursor.execute(command)
    conn.commit()

updated_model_query3 = """
                      SELECT * FROM pageviews_daily
                      ORDER BY view_date, url_path, customer_country;
                      """

print(f'pageviews_daily_2 (Data Model of Approach 2) \n {pd.DataFrame(updated2)}\n')

cursor.execute(updated_model_query3)
updated3 = cursor.fetchall()
print(f'pageviews_daily (Data Model Improved) \n {pd.DataFrame(updated3)}\n')

cursor.close()
conn.close()

if not conn.open:
    print('mysql database connection - closed!')

