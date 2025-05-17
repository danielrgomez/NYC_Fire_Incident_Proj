from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from pull_fire_incidents import pull_data_via_api

def hello_world():
    print("Hello World")


# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_fire_incidents_dag',
    default_args=default_args,
    description='Extracts Transforms and Loads NYC Fire Incident Data',
    #schedule_interval=timedelta(days=1),
    schedule_interval='0 */3 * * *',
    start_date=datetime(2025, 3, 7),
    catchup=False,
    max_active_runs=1,
)



etl_task = BashOperator(
        task_id='task_etl_fire_incident_using_bash',
        bash_command=(
            'docker run --rm \\'
            '--network=pg-network-fire-incidents \\'
            'nyc_fire_incidents:v001 \\'
            '--api_url=data.cityofnewyork.us \\'
            '--token=xoIfIdDlHq6gGzxqLqbUeMpsG \\'
            '--dataset_id=8m42-w767 \\'
            '--limit_rows=10000 \\'
            '--username=root \\'
            '--password=root \\'
            '--host_name=fire_incidents_db_container \\'
            '--port=5432 \\'
            '--database=fire_incidents_db \\'
            '--tbl_name=fire_incidents_tbl'
        ),dag=dag
    )
## Set up the task dependencies
etl_task


#
## Initialize the DAG
#with DAG(
#    dag_id='task_etl_fire_incident_using_bash',
#    default_args=default_args,
#    schedule_interval=None,
#    #schedule_interval='*/5 * * * *',
#    start_date=datetime(2025, 3, 7),
#    catchup=False,
#) as dag:
#
#    # BashOperator to run the Docker container
#    etl_task = BashOperator(
#        task_id='task_etl_fire_incident_using_bash',
#        bash_command=(
#            'docker run --rm \\'
#            '--network=pg-network-fire-incidents \\'
#            'nyc_fire_incidents:v001 \\'
#            '--api_url=data.cityofnewyork.us \\'
#            '--token=xoIfIdDlHq6gGzxqLqbUeMpsG \\'
#            '--dataset_id=8m42-w767 \\'
#            '--limit_rows=10000 \\'
#            '--username=root \\'
#            '--password=root \\'
#            '--host_name=fire_incidents_db_container \\'
#            '--port=5432 \\'
#            '--database=fire_incidents_db \\'
#            '--tbl_name=fire_incidents_tbl'
#        ),
#    )
#
#



#etl_task = BashOperator(
#    task_id='task_etl_fire_incident_using_bash',
#    bash_command='''
#      docker run -it \
#      --network=pg-network-fire-incidents \
#      nyc_fire_incidents:v001 \
#      --api_url=data.cityofnewyork.us \
#      --token=xoIfIdDlHq6gGzxqLqbUeMpsG \
#      --dataset_id=8m42-w767 \
#      --limit_rows=10000 \
#      --username=root \
#      --password=root \
#      --host_name=fire_incidents_db_container \
#      --port=5432 \
#      --database=fire_incidents_db \
#      --tbl_name=fire_incidents_tbl
#    ''',
#    dag=dag,
#)


## Define the task
#hello_world_task = PythonOperator(
#    task_id='hello_world_task',
#    python_callable=hello_world,
#    dag=dag,
#)



## BashOperator to execute the Python script with variables passed
#task_etl_fire_incident = BashOperator(
#    task_id='task_etl_fire_incident',
#    bash_command=(
#        'python3 ./pull_fire_incidents.py --api_url {{ params.api_url }} --token {{ params.token }}'
#        '--dataset_id {{ params.dataset_id }} --limit_rows {{ params.limit_rows }}'
#        '--username {{ params.username }} --password {{ params.password }}'
#        '--host_name {{ params.host_name }} --port {{ params.port }}'
#        '--database {{ params.database }} --tbl_name {{ params.tbl_name }}'
#    ),
#    params={
#      'api_url':"data.cityofnewyork.us",
#      'token':'xoIfIdDlHq6gGzxqLqbUeMpsG',
#      'dataset_id':'8m42-w767',
#      'limit_rows':'10000',
#      'username':'root',
#      'password':'root',
#      'host_name':'fire_incidents_db_container',
#      'port':'5432',
#      'database':'fire_incidents_db',
#      'tbl_name':'fire_incidents_tbl'
#    },
#)
#
#


#python3 ./dags/pull_fire_incidents.py 
#--api_url data.cityofnewyork.us 
#--token xoIfIdDlHq6gGzxqLqbUeMpsG 
#--dataset_id 8m42-w767 
#--limit_rows 10000 
#--username root 
#--password root 
#--host_name fire_incidents_db_container 
#--port 5432 
#--database fire_incidents_db 
#--tbl_name fire_incidents_tbl


