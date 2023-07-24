from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

localfile_path = '/Users/haneul/Desktop/data_pipeline_follow_along/data_pipeline_pocket_book_practice/monitoring_pipeline_performance/'

dag = DAG(
    dag_id = 'pipeline_performance',
    description = 'Performance measurement pipeline',
    start_date = days_ago(1),
    schedule_interval = timedelta(days = 1)
)

extract_airflow_task = BashOperator(task_id = 'extract_airflow',
                               bash_command= f'python {localfile_path}airflow_extract.py',
                               dag = dag)

load_airflow_task = BashOperator(task_id = 'load_airflow',
                               bash_command= f'python {localfile_path}airflow_load.py',
                               dag = dag)

dag_history_model_task = PostgresOperator(task_id = 'dag_history_model',
                                     postgres_conn_id = 'redshift_dw',
                                     sql = 'dag_history_daily.sql',
                                     dag = dag
)

validation_history_model_task = PostgresOperator(task_id = 'validation_history_model',
                                     postgres_conn_id = 'redshift_dw',
                                     sql = 'validator_summary_model.sql',
                                     dag = dag
)

extract_airflow_task >> load_airflow_task
load_airflow_task >> dag_history_model_task
load_airflow_task >> validation_history_model_task