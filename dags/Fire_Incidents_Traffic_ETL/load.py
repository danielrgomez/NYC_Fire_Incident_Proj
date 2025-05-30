import pandas as pd
from sqlalchemy import create_engine
from Fire_Incidents_Traffic_ETL.other_functions import convert_to_date_time_using_pands
from Fire_Incidents_Traffic_ETL.other_functions import read_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import remove_temp_file


def load_data_to_postgres(username,password,host_name,port,database,tbl_name,data_source,schema_name):



    print('Loading NYC Fire Incidents Data to Postgres DB....')
    
    json_transformed_data = read_temp_file(data_source,"all_json_data",'transform')

    #The load_json__data is converted to a Pandas Dataframe
    df = pd.read_json(json_transformed_data)

    #Coverts date fields preload
    if data_source == "fire_incident_data":
        date_fields_to_convert = ["incident_datetime",
                                "first_assignment_datetime",
                                "first_activation_datetime",
                                "incident_close_datetime",
                                "first_on_scene_datetime"]
        df = convert_to_date_time_using_pands(df,date_fields_to_convert)
        print('Convert Dates Preload')
    elif data_source == "traffic_data":
        date_fields_to_convert = ["report_date_time"]
        df = convert_to_date_time_using_pands(df,date_fields_to_convert)
        print('Convert Dates Preload')

    #Creating the engine postgressql://username:password@host:port/db_name
    engine = create_engine(f'postgresql://{username}:{password}@{host_name}:{port}/{database}')

    #Defines a schema, names it to fire_incidents_schema, and then assigns it to postgres
    print(pd.io.sql.get_schema(df,name=schema_name,con=engine))

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
    batches = list(create_batches_of_rows(df,1000))


    #Loops through each one of the batches and appends the batch to the postgressql database.
    counter = 1
    for batch in batches:
        batch.to_sql(name=tbl_name, con=engine, if_exists='append')
        print(f'Batch Number {counter} Loaded to Postgres.....')
        counter += 1

    
    print("Deleted transformed json data")
    remove_temp_file(data_source,"all_json_data",'transform')
    


    print('Load is Complete')