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
           SELECT * FROM Orders
           LIMIT 1;

           """

cursor.execute(pg_query)
result = cursor.fetchone()
print(result)

cursor.close()
conn.close()