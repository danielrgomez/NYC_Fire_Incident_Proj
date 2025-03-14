import pandas as pd
from sqlalchemy import create_engine

def load_fire_incidents_data(load_json__data,username,password,host_name,port,database,tbl_name):



    print('Loading NYC Fire Incidents Data to Postgres DB....')
    print("New LOAD FILE")

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