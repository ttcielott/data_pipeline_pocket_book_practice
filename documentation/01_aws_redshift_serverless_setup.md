# AWS Redshift Severless Setup

1. Go to AWS Redshift console page
2. Select Redshift Severless
3. Click 'create workgroup'
<br>
<img src = screenshots/aws_redshift_serverless_create_workgroup.png width = 500></img>
4. Fill in the workgroup name and click 'Next'
<br>
<img src = screenshots/aws_redshift_serverless_workgroup_name.png width = 500></img>
5. Fill in the namespace
<br>
<img src = screenshots/aws_redshift_serverless_namespace_name.png width = 500></img>
6. Tick 'Customize admin user credentials' and set admin user name and admin user password
* Remember the user name and user password for the time when connecting Redshift to Python
<br>
<img src = screenshots/aws_redshift_serverless_db_pwd.png width = 500></img>
7. Click 'Next'
<br>
<br>

**Make changes in the VPC security group to communicate between Python and Redshift**
<br>
8. From the main page, click the workgroup you have just created.
<br>
<img src = screenshots/aws_redshift_serverless_main_workgroup.png width = 500></img>
<br>
9. Scroll down, find 'Network and security', and then click the 'security group'
<br>
<img src = screenshots/aws_redshift_serverless_security_group.png width = 500></img>
<br>
10. In the security group page you just opened, click 'Inbound rules' tab and then 'Edit inbound rules'.
<br>
<img src = screenshots/aws_redshift_serverless_sg_inbound.png width = 500></img>
<br>
11. Click 'add rule', set inputs as the following picture and then save rules.
<br>
<img src = screenshots/aws_redshift_serverless_sg_inbound_rule_setting.png width = 500></img>
<br>
12. Go back to the workgroup page, find 'Network and security', and then click the 'Edit'.
<br>
<img src = screenshots/aws_redshift_serverless_publicly_accessible01.png width = 500></img>
<br>
13. Scroll down, tick the 'Turn on publicly accessbile', and save changes.
<br>
<img src = screenshots/aws_redshift_serverless_publicly_accessible02.png width = 500></img>
