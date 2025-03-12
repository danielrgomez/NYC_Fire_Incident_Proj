import pandas as pd
import argparse
import requests
import psycopg2
from sqlalchemy import create_engine
from tenacity import retry, wait_exponential, stop_after_attempt
from sodapy import Socrata
    

def extract_fire_incidents_data(api_url,token,dataset_id,limit_rows):


    print('Extracting NYC Fire Incidents Data via API....')

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
    
    #Creates a pandas dataframe using the results from client
    df = pd.DataFrame.from_records(results)

    print('Extracting is Complete')
    
    #Must serialize the dataframe into json format in order to save the data to the XCom Variable for the next airflow task
    json_extracted_data = df.to_json()
    #Returns the converted json variable
    return json_extracted_data



def transform_fire_incidents_data(transform_json_data):

    #The transform_json_data is converted to a Pandas Dataframe
    df = pd.read_json(transform_json_data)  

    print('Transforming NYC Fire Incidents Data....')

    #Converting fields to correct data types
    #Date Time Conversions
    df.incident_datetime = pd.to_datetime(df.incident_datetime)
    df.first_assignment_datetime = pd.to_datetime(df.first_assignment_datetime)
    df.first_activation_datetime = pd.to_datetime(df.first_activation_datetime)
    df.incident_close_datetime = pd.to_datetime(df.incident_close_datetime)


    #Float Conversion
    df.dispatch_response_seconds_qy = df.dispatch_response_seconds_qy.astype(float)
    df.incident_response_seconds_qy = df.incident_response_seconds_qy.astype(float)
    df.incident_travel_tm_seconds_qy = df.incident_travel_tm_seconds_qy.astype(float)
    df.engines_assigned_quantity = df.engines_assigned_quantity.astype(float)
    df.ladders_assigned_quantity = df.ladders_assigned_quantity.astype(float)
    df.other_units_assigned_quantity = df.other_units_assigned_quantity.astype(float)

    print('Transformations are Complete')

    #Must serialize the dataframe into json format in order to save the data to the XCom Variable for the next airflow task. Ensures date_formate = 'iso' to maintain the correct dat formatting.
    json_transformed_data = df.to_json(date_format="iso")
    #Returns the formatted json data.
    return json_transformed_data
    


def load_fire_incidents_data(load_json__data,username,password,host_name,port,database,tbl_name):



    print('Loading NYC Fire Incidents Data to Postgres DB....')

    #The load_json__data is converted to a Pandas Dataframe
    df = pd.read_json(load_json__data)

    #Creating the engine postgressql://username:password@host:port/db_name
    engine = create_engine(f'postgresql://{username}:{password}@{host_name}:{port}/{database}')

    #Defines a schema, names it to fire_incidents_schema, and then assigns it to postgres
    print(pd.io.sql.get_schema(df,name='fire_incidents_schema',con=engine))

    #Creates the table in postgres with only the field names. Name = fire_incidents_tbl, Engine is the postgres database, if_exists = 'replace' if a table already exists with this name it will replace it
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
        batch.to_sql(name=tbl_name, con=engine, if_exists='append')
        print(f'Batch Number {counter} Loaded to Postgres.....')
        counter += 1

    print('Load is Complete')


