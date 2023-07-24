# Monitor pipeline performance
This folder is about creating pipeline that extracts the records from the table, dag_run in your airflow database, loads them on your the data warehouse, which is Redshift in this repository's exercise, and finally build two data model summarising the pipeline performance: `dag_history_daily` and `validator_summary_daily`.

Please follow the steps below to implement the pipeline.

1. Activate a virtual environment

    ```
    source .env/bin/activate
    ```
2. Move to data_validation folder
    ```
    cd monitoring_pipeline_performance

3. Create the tables that will receive airflow dag run history and validation run history

    ```
    python create_tables.py
    ```

4. Move or copy the dag description file to airflow dags folder.

    ```
    cp pipeline_performance.py ~/airflow/dags
    ```
5. Move or copy two sql files for data modeling to airflow dags folder.
    ```
    cp dag_history_daily.sql validation_summary_daily.sql ~/airflow/dags
    ```
6. Open a new terminal and run `airflow scheduler`.

7. Open a new terminal and run `airflow webserver`.

8. Activate the dag, pipeline_performance.