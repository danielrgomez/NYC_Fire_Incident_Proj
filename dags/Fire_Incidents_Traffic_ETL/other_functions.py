import os
import json
import pandas as pd
from datetime import datetime, timedelta




def get_date_range(execution_date):
    # First day of the previous month
    first_day = (execution_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    # Last day of the previous month
    last_day = execution_date.replace(day=1) - timedelta(days=1)

    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")

#Function to convert fields to date time
def convert_to_date_time_using_pands(df,date_fields_to_convert):
    for field in date_fields_to_convert:
        df[field] = pd.to_datetime(df[field])
    return df


def write_temp_file(results,data_source,offset_counter,sub_folder_name):
    #Save to temp folder
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, f'temp/{sub_folder_name}')

    # Create the temp folder if it doesn't exist
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, f'{data_source}_temp_{offset_counter}')

    # Write the JSON output to the file
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=4)
    
    
    print(f"JSON Temp File Written {data_source}_temp_{offset_counter}.json")

def read_temp_file(data_source,offset_counter,sub_folder_name):
    # Get the current directory and define the temp folder path
    print(f"Reading JSON Temp File: {data_source}_temp_{offset_counter}.json to json_data variable")
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, f'temp/{sub_folder_name}')
    
    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, f'{data_source}_temp_{offset_counter}')
    #file_path = os.path.join(temp_folder, f'{data_source}_temp')

    # Read the JSON file
    with open(file_path, "r", encoding="utf-8") as file:
        json_data = json.load(file)
    
    return json_data

def remove_temp_file(data_source,offset_counter,sub_folder_name):
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, f'temp/{sub_folder_name}')
    file_path = os.path.join(temp_folder, f'{data_source}_temp_{offset_counter}')
    # Delete the temporary file
    os.remove(file_path)
    print(f"Temporary file deleted: {file_path}")



