from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from Postgres_To_S3_ETL.extract import extract_data_from_postgres
from Postgres_To_S3_ETL.load import load_data_to_s3

from airflow.sensors.external_task import ExternalTaskSensor



# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 29),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'postgres_to_s3_task_sensor_dag',
    default_args=default_args, #Passes through the default_args
    description='Extracts and Loads NYC Fire Incident Data and NYC Traffic Data to AWS S3 Bucket',
    #schedule_interval='* */3 * * *',  # Every 3 hours
    #schedule_interval='*/5 * * * *',  # Every 5 minutes
    schedule_interval="0 0 1 * *", #Every Month
    catchup=False,
    max_active_runs=1,
) as dag:
    
     wait_for_fire_incidents_dag = ExternalTaskSensor(
        task_id='wait_for_fire_incidents_dag',
        external_dag_id='etl_nyc_fire_incidents_dag',  # DAG ID for Fire Incidents DAG
        external_task_id=None,  # Wait for the entire DAG to complete
        mode='poke',
        timeout=600  # Adjust timeout as needed
    )
     
     wait_for_traffic_data_dag = ExternalTaskSensor(
        task_id='wait_for_traffic_data_dag',
        external_dag_id='etl_nyc_traffic_dag',  # DAG ID for Traffic Data DAG
        external_task_id=None,  # Wait for the entire DAG to complete
        mode='poke',
        timeout=600  # Adjust timeout as needed
    )
     #Extract Task
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
     [wait_for_fire_incidents_dag, wait_for_traffic_data_dag] >> extract_files_from_postgres_task >> load_fire_incidents_task