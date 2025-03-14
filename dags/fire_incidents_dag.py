from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
#from pull_fire_incidents import pull_data_via_api

from pull_fire_incidents import extract_fire_incidents_data
from pull_fire_incidents import transform_fire_incidents_data
from pull_fire_incidents import load_fire_incidents_data
#from transformations_pyspark import testing_transformations_pyspark

#Variables used for ETL Process 
api_url='data.cityofnewyork.us'
token='xoIfIdDlHq6gGzxqLqbUeMpsG'
dataset_id='8m42-w767'
limit_rows=10000
username='root'
password='root'
host_name='fire_incidents_db_container'
port=5432
database='fire_incidents_db'
tbl_name='fire_incidents_tbl'


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
    'etl_nyc_fire_incidents_dag',
    default_args=default_args, #Passes throught the default_args
    description='Extracts Transforms and Loads NYC Fire Incident Data',
    schedule_interval='* */3 * * *',  # Every 3 hours
    catchup=False,
    max_active_runs=1,
) as dag:





    #Extract Function
    def extract_data(**kwargs):
        json_extracted_data = extract_fire_incidents_data(api_url,token,dataset_id,limit_rows) #Calls the extract_fire_incidents_data from the pull_fire_incidents.py file
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='extract_data_xcom', value= json_extracted_data) #Pushes the json_extracted_data output to an xcom variable so it can be pulled in the transform task

    # Extract Task
    extract_fire_incidents_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data, #Calls the extract_data function from above
        
    )



    #Transform Function
    def transform_data(**kwargs):
        task_instance = kwargs['ti']
        extracted_data = task_instance.xcom_pull(task_ids='extract_data_task',key='extract_data_xcom') #Pulls the extract_data_xcom xcom variable which contains the json serialized data from the previous task
        json_transformed_data = transform_fire_incidents_data(extracted_data)
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='transformed_data_xcom', value= json_transformed_data) #Pushes the json_transformed_data output to an xcom variable so it can be pulled in the load task
        
    #Transform Task
    transform_fire_incidents_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data, #Calls the transform_data function from above
    )




    #Load Function
    def load_data(**kwargs):
        task_instance = kwargs['ti']
        load_data = task_instance.xcom_pull(task_ids='transform_data_task',key='transformed_data_xcom') #Pulls the transformed_data_xcom xcom variable which contains the json serialized data from the previous task
        load_fire_incidents_data(load_data,username,password,host_name,port,database,tbl_name)


    #Load Task
    load_fire_incidents_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data, #Calls the load_data function from above
    )



## Set up the task dependencies
extract_fire_incidents_task >> transform_fire_incidents_task >> load_fire_incidents_task
