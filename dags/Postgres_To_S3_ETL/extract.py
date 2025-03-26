import psycopg2
import csv
import boto3
from tenacity import retry, wait_exponential, stop_after_attempt
import os


def export_data_to_csv(database,user_name,pwd,host_name,port_number,tbl_name,data_name):
    conn = psycopg2.connect(
        dbname=database,
        user=user_name,
        password=pwd,
        host=host_name,
        port=port_number
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM " + f'{tbl_name}')

    current_directory = os.getcwd()
    print("Current Directory:", current_directory)


    #Save to temp folder
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, 'temp_csv_files')

    # Create the temp folder if it doesn't exist
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, 'exported_' + f'{data_name}'+'.csv')

    # Write the JSON output to the file
    #with open(file_path, "w", encoding="utf-8") as file:
    #    json.dump(results, file, indent=4)
    #print("JSON Temp File Written")
    
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([i[0] for i in cursor.description])  # Write headers
        writer.writerows(cursor.fetchall())  # Write data
    
    cursor.close()
    conn.close()



def extract_data_from_postgres():
    database='fire_incidents_db'
    user_name='root'
    pwd='root'
    host_name='fire_incidents_db_container'
    port_number=5432
    fire_incidents_tbl_name='fire_incidents_tbl'
    traffic_tbl_name='nyc_traffic_tbl'
    fire_incidents_data_name='nyc_fire_incidents_data'
    traffic_data_name='nyc_traffic_data'
    #Export nyc fire incidents data
    export_data_to_csv(database,user_name,pwd,host_name,port_number,fire_incidents_tbl_name,fire_incidents_data_name)
    #Export nyc traffic incident data
    export_data_to_csv(database,user_name,pwd,host_name,port_number,traffic_tbl_name,traffic_data_name)