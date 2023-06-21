import psycopg2
import configparser

# get the postgresSQL database RDS credentials
parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
hostname = parser.get("postgres_config", "hostname")
username = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
port = parser.get("postgres_config", "port")
dbname = parser.get("postgres_config", "database")

# define the db connection
conn = psycopg2.connect(
    dbname = dbname,
    user = username,
    password = password,
    host = hostname,
    port = port
)

cursor = conn.cursor()

pg_query = """
           CREATE TABLE Orders ( OrderId int, 
                                OrderStatus varchar(30), 
                                LastUpdated timestamp);
            INSERT INTO Orders
            VALUES(1,'Backordered', '2020-06-01 12:00:00');
            INSERT INTO Orders
            VALUES(1,'Shipped', '2020-06-09 12:00:25');
            INSERT INTO Orders
            VALUES(2,'Shipped', '2020-07-11 3:05:00');
            INSERT INTO Orders
            VALUES(1,'Shipped', '2020-06-09 11:50:00');
            INSERT INTO Orders
            VALUES(3,'Shipped', '2020-07-12 12:00:00');

           """

cursor.execute(pg_query)
conn.commit()
cursor.close()
conn.close()