# this is a dag description file. It should be located in your airflow dag folder 
# check out your dags folder directory in ~airflow/airflow.cfg
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
local_path = '/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice/elt_pipeline_sample/'
validation_path = '/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice/data_validation/'

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

dag = DAG(
    'elt_pipeline_sample_with_validation',
    description = 'A sample ELT pipelin with validation task',
    default_args=default_args,
    schedule_interval = timedelta(days =1),
    start_date = days_ago(1)
    )

extract_orders_task = BashOperator(
    task_id = 'extract_order_data',
    bash_command = f'python {local_path}extract_orders.py',
    dag = dag
)

extract_customers_task = BashOperator(
    task_id = 'extract_customer_data',
    bash_command = f'python {local_path}extract_customers.py',
    dag = dag
)

load_orders_task = BashOperator(
    task_id = 'load_orders_data',
    bash_command = f'python {local_path}load_orders.py',
    dag = dag
)

load_customers_task = BashOperator(
    task_id = 'load_customers_data',
    bash_command = f'python {local_path}load_customers.py',
    dag = dag
)

# set -e tells bash to stop execution of the script on an error
# if that happends, no downstream task will execute
 
check_order_rowcount_task = BashOperator(
    task_id = 'check_order_rowcount',
    bash_command = f'set -e; python {validation_path}validation.py {validation_path}order_count.sql {validation_path}order_full_count.sql equals',
    dag = dag
)

revenue_model_task = PostgresOperator(
    task_id = 'build_data_model',
    postgres_conn_id = 'redshift_dw',
    sql = 'order_revenue_model.sql',
    dag = dag
)

send_email = EmailOperator(
    task_id = 'send_email',
    to = 'ttcielott@gmail.com',
    subject = 'ELT pipeline sample - complete',
    html_content = "Date: {{ ds }}",
    dag = dag
)

# redefine the depency order of the DAG
# This ensures that after the load_orders_task, the validation task runs, 
# followed by the revenue_model_task once both the validation is completed (and passed) 
# and the load_customers_task has completed successfully
extract_orders_task >> load_orders_task
extract_customers_task >> load_customers_task
load_orders_task >> check_order_rowcount_task
check_order_rowcount_task >> revenue_model_task
load_customers_task >> revenue_model_task
revenue_model_task >> send_email