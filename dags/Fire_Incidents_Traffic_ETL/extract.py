import pandas as pd
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from sodapy import Socrata
from Fire_Incidents_Traffic_ETL.other_functions import write_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import remove_temp_file


def extract_data_via_api(api_url,token,dataset_id,limit_rows,data_source,param_from,param_to,offset):

    print('Extracting Data via API....')
    #Sts client to client field using Socrata
    client = Socrata(api_url, token)

    #Gets results from client limit to 50000. Uses the retry decorator from library tenacity. This is used because the connection is sometimes not successful on the first try.
    #Instead it retries for up to 5 attempts. On the first try it will wait 2 seconds, second retry for 4 seconds, third for 8 seconds, etc. for up to 16 seconds.
    #That is why the multiplier=2, a min=2, and max=16.
    print("Trying to connect to API...")
    
    @retry(wait=wait_exponential(multiplier=2, min=2, max=16), stop=stop_after_attempt(5))
    def get_data_from_api(api_url,dataset_id,param_from,param_to,offset):
        # Define the API endpoint
        if data_source == "fire_incident_data":
            url = f"https://{api_url}/resource/{dataset_id}.json?$where=incident_datetime >= '{param_from}T00:00:00' AND incident_datetime < '{param_to}T00:00:00'&$limit=1000&$offset={offset}"
        elif data_source == "traffic_data":
            url = f"https://{api_url}/resource/{dataset_id}.json?$where=yr='{param_from}'&$limit=1000&$offset={offset}"

        # Make the GET request
        
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
        else:
            print(f"Error: {response.status_code}")
        
        return data
    try:
        offset_counter = 1000
        results = get_data_from_api(api_url,dataset_id,param_from,param_to,offset)

        df = pd.DataFrame.from_records(results)

        json_data = df.to_json(orient='records')
        print("Writing json temp file to temp folder")
        write_temp_file(json_data,data_source,offset_counter,'extract')
        
        while len(results) == 1000:
            
            offset_counter += 1000
            results = get_data_from_api(api_url,dataset_id,param_from,param_to,offset_counter)
            df = pd.DataFrame.from_records(results)
            json_data = df.to_json(orient='records')
            print(f"Writing json temp file to temp folder. Currently On : {offset_counter}")
            write_temp_file(json_data,data_source,offset_counter,'extract')

         
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
  
    
    #Returns the converted json variable
    print("Extraction Complete")
    

    return offset_counter




















