# AWS Redshift Severless Connection to Python

1. Go to AWS Redshift console page
2. Click the workgroup you want connect
3. Copy 'Endpoint' in 'General information'
<br><img src = screenshots/aws_redshift_serverless_wg_endpoint.png width = 500></img>
<br>

**How to find credentials for connection to python**
<br>

From the endpoint you just copied in no.3, you can get host name, database name, and port number <br>

endpoint: dana-workinggroup.704141972584.eu-west-2.redshift-serverless.amazonaws.com:5439/dev <br>
    - host : dana-workinggroup.704141972584.eu-west-2.redshift-serverless.amazonaws.com <br>
    - port : 5439 <br>
    - database : dev <br>
*You also need your user name and password. You can use database admin user name and password that you set when creating this workgroup. (Please refer to `documentation/aws_redshift_serverless_setup.md` in this repository.)

4. Use the credentials in python script
<br> Please refer to `extract_mysql_incremental.py` in this repository.
