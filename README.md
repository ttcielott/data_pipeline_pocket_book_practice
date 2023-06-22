# The Book, Data Pipeline Pocket Reference [Code Practice]

This repo is about my personal code practices that I followed along the code exercises from the book, ["Data Pipelines Pocket Reference"](https://www.oreilly.com/library/view/data-pipelines-pocket/9781492087823/) written by ['James Densmore'](https://www.linkedin.com/in/jamesdensmore/)
<br>
<br>
!['Data Pipelines Pocket Reference'](https://learning.oreilly.com/library/cover/9781492087823/250w/)
-----


## Local Running
### 1. Initial Setup
1) Create a virtual environment

   for linux(Mac) user
   ```
   python3 -m venv .venv
   ```
   
   for window user
   ```
   py -m venv
   
   ```
   
2) Activate the virtual environment

   for linux(Mac) user
   ```
   source .venv/bin/activate
   ```
   
   for window user
   ```
   .venv\Scripts\activate.bat
   
   ```
   
3) Install packages in the virtual environment
   
   ```
   pip install -r requirements.txt
   ```

-----

### 2. MySQL Database
You can install MySQL on your local computer, but I set up the database with docker.
Please refer to [database/docker-compose.yml](database/docker-compose.yml).
<br>
<br>
### 3. AWS Redshift Serverless Setup & Python Connection
Please refer to the following documentations:
- [documentation/01_aws_redshift_serverless_setup.md](documentation/01_aws_redshift_serverless_setup.md)
- [documentation/02_aws_redshift_serverless_python_connection.md](documentation/02_aws_redshift_serverless_python_connection.md)


### 4. PostgresSQL Database
As with MySQL database, there are several ways to create PostgresSQL database, but I used AWS RDS service. Learn [how to create and connect a PostgresSQL Database with Amazon RDS](https://aws.amazon.com/getting-started/hands-on/create-connect-postgresql-db/).


### 5. MongoDB Atlas
Atlas is a fully managed MongoDB service and includes a free-for-life tier with plenty of storage and computing power for learning and running samples. The code example of 'Extracting Data from MongoDB' in this book uses MongoDB Atlas, so I also used it for this practice.
Learn [how to install the Atlas CLI and set up the Atlas](https://www.mongodb.com/docs/atlas/cli/stable/).

   - How to Fix Error: `[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate`

      **Original code for MongoClient in the book**
      ```
      mongo_client = MongoClient(
                "mongodb+srv://" + username
                + ":" + password
                + "@" + hostname
                + "/" + database_name
                + "?retryWrites=true&"
                + "w=majority&ssl=true&"
                + "ssl_cert_reqs=CERT_NONE")
      ```

      However, for me it raised the error mentioned above, so I used the following code to fix the issue.(`certifi` should be installed and imported to your python script.)

      ```
      mongo_client = MongoClient(
         f"""mongodb+srv://{username}:{password}@{username.lower()}.y7gdbex.mongodb.net/?retryWrites=true&w=majority""",
         tlsCAFile = certifi.where()
         )
      ```
      For whole code, please refer to [sample_mongodb.py](sample_mongodb.py)
      


