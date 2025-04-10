from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from Fire_Incidents_Traffic_ETL.extract import extract_data_via_api
from Fire_Incidents_Traffic_ETL.transform_traffic_data_pyspark import main_traffic_nyc_pyspark_transformations
from Fire_Incidents_Traffic_ETL.load import load_data_to_postgres


#Variables used for ETL Process 
api_url='data.cityofnewyork.us'
token='xoIfIdDlHq6gGzxqLqbUeMpsG'
dataset_id='7ym2-wayt'
limit_rows=200000
username='root'
password='root'
host_name='fire_incidents_db_container'
port=5432
database='fire_incidents_db'
tbl_name='nyc_traffic_tbl'
data_source ='traffic_data'
schema_name = 'traffic_schema'
year_from = 2017
year_to = 2024

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
    'etl_nyc_traffic_dag',
    default_args=default_args, #Passes throught the default_args
    description='Extracts Transforms and Loads NYC Traffic Data',
    #schedule_interval='* */3 * * *',  # Every 3 hours
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
) as dag:





    #Extract Function
    def extract_data(**kwargs):
        json_extracted_data = extract_data_via_api(api_url,token,dataset_id,limit_rows,data_source,year_from,year_to) #Calls the extract_traffic_data from the extract.py file
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
        json_transformed_data = main_traffic_nyc_pyspark_transformations(extracted_data,data_source)
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
        load_data_to_postgres(load_data,username,password,host_name,port,database,tbl_name,data_source,schema_name)


    #Load Task
    load_fire_incidents_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data, #Calls the load_data function from above
    )



## Set up the task dependencies
extract_fire_incidents_task >> transform_fire_incidents_task >> load_fire_incidents_task
