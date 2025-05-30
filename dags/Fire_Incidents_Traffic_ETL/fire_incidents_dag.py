from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from Fire_Incidents_Traffic_ETL.extract import extract_data_via_api
from Fire_Incidents_Traffic_ETL.transform_pyspark import main_pyspark_transformations
from Fire_Incidents_Traffic_ETL.load import load_data_to_postgres
from Fire_Incidents_Traffic_ETL.other_functions import get_date_range


#Variables used for ETL Process 
api_url='data.cityofnewyork.us'
token='token_alphanumeric'
dataset_id='8m42-w767'
limit_rows=10000
username='root'
password='root'
host_name='fire_incidents_db_container'
port=5432
database='fire_incidents_db'
tbl_name='fire_incidents_tbl'
data_source = "fire_incident_data"
schema_name = 'fire_incidents_schema'
#incident_date_time_from, incident_date_time_to = get_date_range(datetime.today()) #To pull today's date ranges.
incident_date_time_from, incident_date_time_to = get_date_range(datetime(2024,5,1))
offset = 1000

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
    'etl_nyc_fire_incidents_dag',
    default_args=default_args, #Passes throught the default_args
    description='Extracts Transforms and Loads NYC Fire Incident Data',
    #schedule_interval='*/5 * * * *',  # Every 5 minutes
    schedule_interval="0 0 1 * *", #Every Month
    catchup=False,
    max_active_runs=1,
) as dag:





    #Extract Function
    def extract_data(**kwargs):
        offset_counter = extract_data_via_api(api_url,token,dataset_id,limit_rows,data_source,incident_date_time_from,incident_date_time_to,offset) #Calls the extract_fire_incidents_data from the pull_fire_incidents.py file
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='extract_data_xcom', value= offset_counter) #Pushes the offset_counter to an xcom variable so it can be pulled in the transform task

    # Extract Task
    extract_fire_incidents_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data, #Calls the extract_data function from above
        
    )



    #Transform Function
    def transform_data(**kwargs):
        task_instance = kwargs['ti']
        extracted_offset_counter = task_instance.xcom_pull(task_ids='extract_data_task',key='extract_data_xcom') #Pulls the extract_data_xcom xcom variable which contains the offset_counter variable from the previous task
        main_pyspark_transformations(extracted_offset_counter,data_source)
        
        
    #Transform Task
    transform_fire_incidents_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data, #Calls the transform_data function from above
    )




    ##Load Function
    def load_data(**kwargs):
        task_instance = kwargs['ti']
        load_data_to_postgres(username,password,host_name,port,database,tbl_name,data_source,schema_name)

    #Load Task
    load_fire_incidents_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data, #Calls the load_data function from above
    )



## Set up the task dependencies
extract_fire_incidents_task >> transform_fire_incidents_task >> load_fire_incidents_task

