import pandas as pd
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from sodapy import Socrata
from other_functions import write_temp_file



def extract_fire_incidents_data(api_url,token,dataset_id,limit_rows):

    print('Extracting NYC Fire Incidents Data via API....')
    #Sts client to client field using Socrata
    client = Socrata(api_url, token)

    #Gets results from client limit to 50000. Uses the retry decorator from library tenacity. This is used because the connection is sometimes not successful on the first try.
    #Instead it retries for up to 5 attempts. On the first try it will wait 2 seconds, second retry for 4 seconds, third for 8 seconds, etc. for up to 16 seconds.
    #That is why the multiplier=2, a min=2, and max=16.
    print("Trying to connect to API...")
    @retry(wait=wait_exponential(multiplier=2, min=2, max=16), stop=stop_after_attempt(5))
    def get_data_from_api(client,data_set,limit_rows):
        results = client.get(data_set,limit=limit_rows)
        return results
    try:
        results = get_data_from_api(client,dataset_id,limit_rows)
        print("Connected to API")
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
    
    #Writing temp json file to temp folder
    print("Writing json temp file to temp folder")
    write_temp_file(results)

    #Creates a pandas dataframe using the results from client
    df = pd.DataFrame.from_records(results)
  
    #Must serialize the dataframe into json format in order to save the data to the XCom Variable for the next airflow task
    json_extracted_data = df.to_json()
    print('json_extracted_data serialized')
    
    #Returns the converted json variable
    print("Extraction Complete")
    return json_extracted_data




















