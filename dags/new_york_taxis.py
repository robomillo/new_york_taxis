from datetime import timedelta
from load_taxi_data.load_taxi_data_from_s3 import copy_mapping_file_to_pg, load_s3_data_to_pg
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
        'new_york_taxis',
        default_args=default_args,
        description='new_york_taxis pipeline',
        schedule_interval=timedelta(days=30),
        start_date=days_ago(2),
        tags=['example'],
)

init_db_schemas = PostgresOperator(
        task_id='init_db',
        postgres_conn_id='etl_database',
        sql='initiliaz_db_schemas/intialize_db_schemas.sql',
        dag=dag
)
create_taxi_tables = PostgresOperator(
        task_id='trip_data_tables',
        postgres_conn_id='etl_database',
        sql='load_taxi_data/create_taxi_tables.sql',
        dag=dag
)

load_mapping_data = PythonOperator(
        task_id='load_mapping_data',
        python_callable=copy_mapping_file_to_pg,
        dag=dag
)

load_yellow_taxi_data = PythonOperator(
        task_id='load_taxi_data',
        python_callable=load_s3_data_to_pg,
        op_kwargs={"taxi_type": 'yellow'},
        dag=dag
)

transform_taxi_data = PostgresOperator(
        task_id='transform_taxi_data',
        postgres_conn_id='etl_database',
        sql='transform_taxi_data/create_datasets.sql',
        dag=dag
)

init_db_schemas >> create_taxi_tables >> [load_yellow_taxi_data, load_mapping_data] >> transform_taxi_data
