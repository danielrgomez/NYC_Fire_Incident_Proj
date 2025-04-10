import pandas as pd
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from sodapy import Socrata
from Fire_Incidents_Traffic_ETL.other_functions import write_temp_file



def extract_data_via_api(api_url,token,dataset_id,limit_rows,data_source,param_from,param_to):

    print('Extracting Data via API....')
    #Sts client to client field using Socrata
    client = Socrata(api_url, token)

    #Gets results from client limit to 50000. Uses the retry decorator from library tenacity. This is used because the connection is sometimes not successful on the first try.
    #Instead it retries for up to 5 attempts. On the first try it will wait 2 seconds, second retry for 4 seconds, third for 8 seconds, etc. for up to 16 seconds.
    #That is why the multiplier=2, a min=2, and max=16.
    print("Trying to connect to API...")
    #@retry(wait=wait_exponential(multiplier=2, min=2, max=16), stop=stop_after_attempt(5))
    #def get_data_from_api(client,data_set,limit_rows):
    #    results = client.get(data_set,limit=limit_rows)
    #    return results
    #try:
    #    results = get_data_from_api(client,dataset_id,limit_rows)
    #    print("Connected to API")
    #    
    #except requests.exceptions.RequestException as e:
    #    print(f"Failed to fetch data from API: {e}")


    #NEWNEWNEWNEW

    @retry(wait=wait_exponential(multiplier=2, min=2, max=16), stop=stop_after_attempt(5))
    def get_data_from_api(api_url,dataset_id,param_from,param_to):
        # Define the API endpoint
        url = f"https://{api_url}/resource/{dataset_id}.json"
        if data_source == "fire_incident_data":
            params = {
                "$where": f"incident_datetime >= '{param_from}T00:00:00' AND incident_datetime <= '{param_to}T00:00:00'"
            }
        elif data_source == "traffic_data":
            params = {
                "$where": f"yr >= '{param_from}' AND yr <= '{param_to}'"
            }
        # Make the GET request
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print(f"Error: {response.status_code}")
        
        return data
    try:
        #results = client.get("8m42-w767", limit=50)
        results = get_data_from_api(api_url,dataset_id,param_from,param_to)
        print("Connected to API")
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")





    #NEWNEWNEWNEW
    
    #Writing temp json file to temp folder
    print("Writing json temp file to temp folder")
    write_temp_file(results,data_source)

    #Creates a pandas dataframe using the results from client
    df = pd.DataFrame.from_records(results)
  
    #Must serialize the dataframe into json format in order to save the data to the XCom Variable for the next airflow task
    json_extracted_data = df.to_json()
    print('json_extracted_data serialized')
    
    #Returns the converted json variable
    print("Extraction Complete")
    return json_extracted_data




















