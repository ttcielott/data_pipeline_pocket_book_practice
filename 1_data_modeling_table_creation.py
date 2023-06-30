import configparser
import pymysql

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
username = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
database = parser.get("mysql_config", "database")
port  = parser.get("mysql_config", "port")

conn = pymysql.connect(host = hostname,
                       user = username,
                       password = password,
                       port = int(port),
                       database = database)
cursor = conn.cursor()

commands = ["DROP TABLE IF EXISTS Orders",
       """
       CREATE TABLE Orders ( OrderId int,
                             OrderStatus varchar(30),
                             OrderDate timestamp, 
                             CustomerId int,
                             OrderTotal decimal(65,2)
                             );
       """,
       "INSERT INTO Orders VALUES(1,'Shipped','2020-06-09',100,50.05);",
        "INSERT INTO Orders VALUES(2,'Shipped','2020-07-11',101,57.45);",
        "INSERT INTO Orders VALUES(3,'Shipped','2020-07-12',102,135.99);",
        "INSERT INTO Orders VALUES(4,'Shipped','2020-07-12',100,43.00);",
        "DROP TABLE IF EXISTS Customers",
        """CREATE TABLE Customers (
            CustomerId int,
            CustomerName varchar(20),
            CustomerCountry varchar(10)
        );""",
        "INSERT INTO Customers VALUES(100,'Jane','USA');",
        "INSERT INTO Customers VALUES(101,'Bob','UK');",
        "INSERT INTO Customers VALUES(102,'Miles','UK');"]

for command in commands:
    cursor.execute(command)

conn.commit()
cursor.close()
conn.close()
