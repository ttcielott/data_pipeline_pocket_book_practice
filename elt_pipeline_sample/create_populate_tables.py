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
            DROP TABLE IF EXISTS Orders;
       
            CREATE TABLE Orders ( OrderId int,
                                    OrderStatus varchar(30),
                                    OrderDate timestamp, 
                                    CustomerId int,
                                    OrderTotal decimal(65,2)
                                    );
       
            INSERT INTO Orders VALUES(1,'Shipped','2020-06-09',100,50.05);
            INSERT INTO Orders VALUES(2,'Shipped','2020-07-11',101,57.45);
            INSERT INTO Orders VALUES(3,'Shipped','2020-07-12',102,135.99);
            INSERT INTO Orders VALUES(4,'Shipped','2020-07-12',100,43.00);
            
            DROP TABLE IF EXISTS Customers;

            CREATE TABLE Customers (
                CustomerId int,
                CustomerName varchar(20),
                CustomerCountry varchar(10)
            );

            INSERT INTO Customers VALUES(100,'Jane','USA');
            INSERT INTO Customers VALUES(101,'Bob','UK');
            INSERT INTO Customers VALUES(102,'Miles','UK');
           """

cursor.execute(pg_query)
conn.commit()
cursor.close()
conn.close()