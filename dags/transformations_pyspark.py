import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import to_timestamp, to_date
import json
import pandas as pd

#spark = SparkSession.builder.appName("Transformations_NYC_Fire_Incidents").getOrCreate()
#print(spark.version)
#Function to clean null values, The function takes in the following paramters: pyspark dataframe, column name to clean, each of the broughs values to switch to.
def clean_null_values(df,column_name_to_clean,bronx_value,brooklyn_value,manhattan_value,queens_value,staten_value):
    df = df.withColumn(
    column_name_to_clean,
    when(col(column_name_to_clean).isNull() & (col("alarm_box_borough") == "BRONX"),bronx_value)
    .when(col(column_name_to_clean).isNull() & (col("alarm_box_borough") == "BROOKLYN"),brooklyn_value)
    .when(col(column_name_to_clean).isNull() & (col("alarm_box_borough") == "MANHATTAN"),manhattan_value)
    .when(col(column_name_to_clean).isNull() & (col("alarm_box_borough") == "QUEENS"),queens_value)
    .when(col(column_name_to_clean).isNull() & (col("alarm_box_borough") == "RICHMOND / STATEN ISLAND"),staten_value)
    .otherwise(col(column_name_to_clean))
)
    return df




def pyspark_transformations(json_results):
    
    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()

    # Validate JSON format
    try:
        json.loads(json_results)
        print("Valid JSON")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")

    # Parse the JSON and flatten it
    parsed_json = json.loads(json_results)
    df = pd.DataFrame(parsed_json)
    print("Parse the JSON and flatten it")
    # Convert the DataFrame to a list of dictionaries
    
    flat_json = df.to_dict(orient="records")
    print("Set json to dictionary")

    df = spark.read.json(spark.sparkContext.parallelize(flat_json))
    df.show()

    #json_results = f"[{json_results}]"
    #print("Json Results Type: !!")
    #print(type(json_results))
    #print(json_results)
    #json_results = json_results.replace("'", '"')
    
    #json_list = json.loads(json_results)
    #print("json_list Type: !!")
    #print(type(json_list))
#
    #keys = list(json_list.keys())[:3]  # Get the first 3 keys
    #for key in keys:
    #    print(key, ":", json_list[key])
#   

    #print("Printing JSON LIST")     
    #print(json_list)
    #df = spark.read.json(json_results)

    ## Validate JSON format
    #try:
    #    json.loads(json_list)
    #    print("json_list is Valid JSON")
    #except json.JSONDecodeError as e:
    #    print(f"json_list is Invalid JSON: {e}")

    # Convert single JSON object into a list
    #json_results_list = [json_results]
    
    #processed_json = json.dumps(json_results)
    #print("Processed JSON Type:")
    #print(type(processed_json))

    

#    print("Show Corrupt Record: ")
#    # Read the JSON file
#    df_corrupt = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(spark.sparkContext.parallelize([json_list]))
#
#    # Filter for corrupt records
#    corrupt_records = df_corrupt.filter(df_corrupt["_corrupt_record"].isNotNull())
#
#    # Show the corrupt records
#    corrupt_records.show(truncate=False)
#
#    
#    
#    #The values presented below correspond to the first entry identified for each field within their respective boroughs. For instance, in the case of the Bronx, the first zip code encountered in the dataset was 10451.  
#    #For the null values, it is assumed that the newly assigned values will approximate the actual values as closely as possible.
#    df = clean_null_values(df,"zipcode",10451,11201,10001,11004,10301)
    df = clean_null_values(df,"policeprecinct",40,60,1,100,120)
#    df = clean_null_values(df,"citycouncildistrict",8,33,1,19,49)
#    df = clean_null_values(df,"communitydistrict",201,301,101,401,501)
#    df = clean_null_values(df,"communityschooldistrict",7,13,1,7,31)
#    df = clean_null_values(df,"congressionaldistrict",13,7,7,3,11)
#
    print("Number of pliiceprecinct null values: ")
    print(df.where(df["policeprecinct"].isNull()).select("starfire_incident_id","zipcode","alarm_box_borough").count())

    #df = spark.read.json(json_results)
    print("hello world")
    