from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from Postgres_To_S3_ETL.extract import extract_data_from_postgres
from Postgres_To_S3_ETL.load import load_data_to_s3



# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 7),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'postgres_to_s3_dag',
    default_args=default_args, #Passes through the default_args
    description='Extracts and Loads NYC Fire Incident Data and NYC Traffic Data to AWS S3 Bucket',
    schedule_interval='* */3 * * *',  # Every 3 hours
    catchup=False,
    max_active_runs=1,
) as dag:



    # Extract Task
    extract_files_from_postgres_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data_from_postgres, #Calls the extract_data function from above
        
    )

    #Load Task
    load_fire_incidents_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_s3, #Calls the load_data function from above
    )



## Set up the task dependencies
extract_files_from_postgres_task >> load_fire_incidents_task
