# this is a dag description file. It should be located in your airflow dag folder 
# check out your dags folder directory in ~airflow/airflow.cfg

# this slave dag will sense the external dag, master_simple_dag, of which dag description file is in the same folder
# by default, the ExternalTaskSensor will check for the run of the external_dag_id with the current schedule of the DAG it belongs to.
# therefore, to make it work, the external dag shouldn't be triggered manually
# and also two dags should be run on the same execution date and the same schedule interval
# if the two DAGs run on different schedules, then it's best to specify which run of a master dag a slave should check on.
# by using either the execution_delta or execution_date_fn parameter
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
        'owner': 'airflow',    
        # 'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        }  

# the value in parameter schedule_interval and start_date should be indentical to that of simple dag.
dag = DAG(
    dag_id = 'sensor_test',
    default_args=default_args,
    description = 'DAG with a sensor',
    schedule_interval = timedelta(hours = 1),
    start_date = datetime.now()
)

sensor1 = ExternalTaskSensor(
    task_id = 'dag_sensor',
    external_dag_id = 'simple_dag', 
    external_task_id = 'print_end',
    dag = dag,
    timeout = 60*60,
    mode = 'reschedule'
)

email1 = EmailOperator(
    task_id = 'send_email',
    to = 'ttcielott@gmail.com',
    subject = 'sensor test - complete',
    html_content = "Date: {{ ds }}",
    dag = dag
)

sensor1 >> email1