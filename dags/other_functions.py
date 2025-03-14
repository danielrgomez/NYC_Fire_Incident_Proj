import os
import json


def write_temp_file(results):
    print("hello world")
    #Save to temp folder
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, "temp")

    # Create the temp folder if it doesn't exist
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, "temp.json")

    # Write the JSON output to the file
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=4)

def read_temp_file():
    # Get the current directory and define the temp folder path
    print("Reading JSON Temp File to json_data variable")
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, "temp")
    print(current_dir)

    # Define the path to the JSON file
    file_path = os.path.join(temp_folder, "temp.json")
    print("Printing file path: ")
    print(file_path)

    # Read the JSON file
    with open(file_path, "r", encoding="utf-8") as file:
        json_data = json.load(file)
    
    return json_data

def remove_temp_file():
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, "temp")
    file_path = os.path.join(temp_folder, "temp.json")
    # Delete the temporary file
    os.remove(file_path)
    print(f"Temporary file deleted: {file_path}")
