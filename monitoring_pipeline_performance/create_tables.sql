--create this table on the data warehouse (in this exercise, redshift)

-- table, 'dag_run_history' keeps the history of each DAG runs
DROP TABLE IF EXISTS dag_run_history;
CREATE TABLE dag_run_history(
    id int, 
    dag_id varchar(250),
    execution_date timestamp,
    state varchar(250),
    run_id varchar(250),
    external_trigger boolean,
    end_date timestamp,
    start_date timestamp
);

-- table, validation_run_history keeps the history of the validation test results
DROP TABLE IF EXISTS validation_run_history;
CREATE TABLE validation_run_history(
    script_1 varchar(255),
    script_2 varchar(255),
    comp_operator varchar(10),
    test_result varchar(20),
    test_run_at timestamp
);

