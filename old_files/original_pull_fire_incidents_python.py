#api_url = data.cityofnewyork.us
#token = cfnfpltKjZgR6Z9rNps2rY8Xn
#dataset_id = 8m42-w767
#limit_rows = 50000
#username = root
#password = root
#host = fire_incidents_db
#port = 5432
#database = fire_incidents



import pandas as pd
import argparse
import requests
import psycopg2
from sqlalchemy import create_engine
from tenacity import retry, wait_exponential, stop_after_attempt
from sodapy import Socrata

def pull_data_via_api(parameters):
#def pull_data_via_api(api_url,token,dataset_id,limit_rows,username,password,host_name,port,database,tbl_name):
#def pull_data_via_api(*args, **kwargs):

    api_url = parameters.api_url
    token = parameters.token
    dataset_id = parameters.dataset_id
    limit_rows = parameters.limit_rows
    username = parameters.username
    password = parameters.password
    host_name = parameters.host_name
    port = parameters.port
    database = parameters.database
    tbl_name = parameters.tbl_name

    #api_url = kwargs.get('api_url')
    #token = kwargs.get('token')
    #dataset_id = kwargs.get('dataset_id')
    #limit_rows = kwargs.get('limit_rows')
    #username = kwargs.get('username')
    #password = kwargs.get('password')
    #host_name = kwargs.get('host_name')
    #port = kwargs.get('port')
    #database = kwargs.get('database')
    #tbl_name = kwargs.get('tbl_name')

    

    #Sts client to client field using Socrata
    client = Socrata(api_url, token)

    #Gets results from client limit to 50000. Uses the retry decorator from library tenacity. This is used because the connection is sometimes not successful on the first try.
    #Instead it retries for up to 5 attempts. On the first try it will wait 2 seconds, second retry for 4 seconds, third for 8 seconds, etc. for up to 16 seconds.
    #That is why the multiplier=2, a min=2, and max=16.
    @retry(wait=wait_exponential(multiplier=2, min=2, max=16), stop=stop_after_attempt(5))
    def get_data_from_api(client,data_set,limit_rows):
        results = client.get(data_set,limit=limit_rows)
        return results
    try:
        #results = client.get("8m42-w767", limit=50)
        results = get_data_from_api(client,dataset_id,limit_rows)
        print("Connected to API...")
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
    
    #Gets results from client limit to 50000
    #results = client.get(dataset_id, limit=limit_rows)

    #Creates a pandas dataframe using the results from client
    df = pd.DataFrame.from_records(results)

    #Converting fields to correct data types
    #Date Time Conversions
    df.incident_datetime = pd.to_datetime(df.incident_datetime)
    df.first_assignment_datetime = pd.to_datetime(df.first_assignment_datetime)
    df.first_activation_datetime = pd.to_datetime(df.first_activation_datetime)
    df.incident_close_datetime = pd.to_datetime(df.incident_close_datetime)
    #No Longer a field
    ##df.first_on_scene_datetime = pd.to_datetime(df.first_on_scene_datetime)

    #Float Conversion
    df.dispatch_response_seconds_qy = df.dispatch_response_seconds_qy.astype(float)
    df.incident_response_seconds_qy = df.incident_response_seconds_qy.astype(float)
    df.incident_travel_tm_seconds_qy = df.incident_travel_tm_seconds_qy.astype(float)
    df.engines_assigned_quantity = df.engines_assigned_quantity.astype(float)
    df.ladders_assigned_quantity = df.ladders_assigned_quantity.astype(float)
    df.other_units_assigned_quantity = df.other_units_assigned_quantity.astype(float)

    #Creating the engine postgressql://username:password@host:port/db_name
    engine = create_engine(f'postgresql://{username}:{password}@{host_name}:{port}/{database}')

    #Defines a schema, names it to fire_incidents_schema, and then assigns it to postgres
    print(pd.io.sql.get_schema(df,name='fire_incidents_schema',con=engine))

    #Creates the table in postgres with only the field names. Name = yellow_taxi_data, Engine is the postgres database, if_exists = 'replace' if a table already exists with this name it will replace it
    df.head(n=0).to_sql(name= tbl_name,con=engine,if_exists='replace')

    #Function to create batches of rows using the dataframe as a parameter and batchsize as another parameter. 
    #It breaks down the dataframe into separate batches of size equal to batchsize.
    start = 0
    batchsize = 1000
    def create_batches_of_rows(dataframe,batchsize):
        start = 0
        while start < len(df) + 1:
            yield df.iloc[start:start + batchsize]
            start += batchsize

    #Creates a list of batches. Parses the dataframe and the batchsize through the create_batches_of_rows function and sets the variable batches to the list
    batches = list(create_batches_of_rows(df,100))


    #Loops through each one of the batches and appends the batch to the postgressql database.
    counter = 1
    for batch in batches:
        batch.to_sql(name='fire_incidents_tbl', con=engine, if_exists='append')
        print(f'Batch Number {counter} Loaded to Postgres.....')
        counter += 1
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest API Data to Postgres")
    
    parser.add_argument('--api_url',required=True,help='url where the data is located')
    parser.add_argument('--token',required=True,help='token needed for authenticated client')
    parser.add_argument('--dataset_id',required=True,help='identifies the dataset. In this case it is the Fire Incident Dispatch Data')
    parser.add_argument('--limit_rows',required=True,help='limits the number of rows to be extracted via the API')
    parser.add_argument('--username',required=True,help='username for postgres')
    parser.add_argument('--password',required=True,help='password for postgres')
    parser.add_argument('--host_name',required=True,help='postgres host')
    parser.add_argument('--port',required=True,help='postgres port it is running on')
    parser.add_argument('--database',required=True,help='name of database')
    parser.add_argument('--tbl_name',required=True,help='name of table')

    args = parser.parse_args()

    pull_data_via_api(args)

