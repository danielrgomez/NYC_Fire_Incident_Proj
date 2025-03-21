import os
import json
import pandas as pd

#Function to convert fields to date time
def convert_to_date_time_using_pands(df,date_fields_to_convert):
    for field in date_fields_to_convert:
        df[field] = pd.to_datetime(df[field])
    return df


def write_temp_file(results,data_source):
    #Save to temp folder
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, 'temp')

    # Create the temp folder if it doesn't exist
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, f'{data_source}_temp')

    # Write the JSON output to the file
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=4)
    print("JSON Temp File Written")

def read_temp_file(data_source):
    # Get the current directory and define the temp folder path
    print("Reading JSON Temp File to json_data variable")
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, 'temp')
    

    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, f'{data_source}_temp')

    # Read the JSON file
    with open(file_path, "r", encoding="utf-8") as file:
        json_data = json.load(file)
    
    return json_data

def remove_temp_file(data_source):
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, 'temp')
    file_path = os.path.join(temp_folder, f'{data_source}_temp')
    # Delete the temporary file
    os.remove(file_path)
    print(f"Temporary file deleted: {file_path}")



