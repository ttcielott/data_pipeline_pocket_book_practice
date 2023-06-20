# The Book, Data Pipeline Pocket Reference [Code Practice]
This repo is about my personal code practices that I followed along the book from ["Data Pipelines Pocket Reference"](https://www.oreilly.com/library/view/data-pipelines-pocket/9781492087823/) written by ['James Densmore'](https://www.linkedin.com/in/jamesdensmore/)
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
