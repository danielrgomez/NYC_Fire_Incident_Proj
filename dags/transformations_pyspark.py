from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import to_timestamp, to_date
import json
import pandas as pd

from pyspark.sql.functions import max
from pyspark.sql.functions import min
from pyspark.sql.functions import avg

#Temp File Functions
from other_functions import write_temp_file
from other_functions import read_temp_file
from other_functions import remove_temp_file


import os
import json


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

def categorize_float_fields(df,column_name,none,very_low,low,medium,high,very_high):
    # Returns the max response
    max_quantity = df.agg(max(column_name).alias("max_response_alias")).collect()[0]
    max_quantity = max_quantity["max_response_alias"] 

    # Returns the min quantity
    min_quantity = df.agg(min(column_name).alias("min_response_alias")).collect()[0]
    min_quantity = min_quantity["min_response_alias"]

    #Calculates the category interval this is to determine the intervals between each category. 5 Categories were chosen.
    category_interval = (max_quantity - min_quantity) / 5
    
    #Categorizes each quantity column using the range between the max and min
    df = df.withColumn(
        "category_" + column_name,
        when((col(column_name) == 0),none)
        .when((col(column_name) > 0) & (col(column_name) <= category_interval),very_low)
        .when((col(column_name) > category_interval) & (col(column_name) <= (category_interval*2)),low)
        .when((col(column_name) > (category_interval*2)) & (col(column_name) <= (category_interval*3)),medium)
        .when((col(column_name) > (category_interval*3)) & (col(column_name) <= (category_interval*4)),high)
        .otherwise(very_high)
    )
    
    return df


#Renaming Columns and Casting to Float Types
def clean_column(df,column_name_before,column_name_after):
    df = df.withColumnRenamed(column_name_before, column_name_after)

    df = df.withColumn(column_name_after, col(column_name_after).cast("float"))
    
    return df

#Validate json format
def validate_json_format(json_results):
    try:
        json.loads(json_results)
        print("Valid JSON")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")


#Create JSON String from the Pyspark Dataframe
def create_json_string_from_df(df):
    # Convert DataFrame to an RDD of JSON strings
    json_rdd = df.toJSON()

    # Collect the JSON strings into a Python list
    json_list = json_rdd.collect()

    # Combine the list of JSON strings into a single JSON array string
    json_string = "[" + ",".join(json_list) + "]"
    
    return json_string




#Main Transformations using Pyspark
def main_pyspark_transformations(json_results):
    
    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()

    # Validate JSON format
    validate_json_format(json_results)
   
    #Reads json temp file
    read_json_data = read_temp_file()

    #Removes json temp file
    remove_temp_file()

    #Creates the spark data frame
    df = spark.read.json(spark.sparkContext.parallelize([read_json_data]))
    df.show()


    #The values presented below correspond to the first entry identified for each field within their respective boroughs. For instance, in the case of the Bronx, the first zip code encountered in the dataset was 10451.  
    #For the null values, it is assumed that the newly assigned values will approximate the actual values as closely as possible.
    df = clean_null_values(df,"zipcode",10451,11201,10001,11004,10301)
    df = clean_null_values(df,"policeprecinct",40,60,1,100,120)
    df = clean_null_values(df,"citycouncildistrict",8,33,1,19,49)
    df = clean_null_values(df,"communitydistrict",201,301,101,401,501)
    df = clean_null_values(df,"communityschooldistrict",7,13,1,7,31)
    df = clean_null_values(df,"congressionaldistrict",13,7,7,3,11)

    print("Number of congressionaldistrict null values: ")


    #Convert to date time
    df = df.withColumn("incident_datetime", to_timestamp(df["incident_datetime"]))
    df = df.withColumn("first_assignment_datetime", to_timestamp(df["first_assignment_datetime"]))
    df = df.withColumn("first_activation_datetime", to_timestamp(df["first_activation_datetime"]))
    df = df.withColumn("incident_close_datetime", to_timestamp(df["incident_close_datetime"]))

    #Convert to floats
    df = df.withColumn("dispatch_response_seconds_qy", df["dispatch_response_seconds_qy"].cast("float"))
    df = df.withColumn("incident_response_seconds_qy", df["incident_response_seconds_qy"].cast("float"))
    df = df.withColumn("incident_travel_tm_seconds_qy", df["incident_travel_tm_seconds_qy"].cast("float"))
    df = df.withColumn("engines_assigned_quantity", df["engines_assigned_quantity"].cast("float"))
    df = df.withColumn("ladders_assigned_quantity", df["ladders_assigned_quantity"].cast("float"))
    df = df.withColumn("other_units_assigned_quantity", df["other_units_assigned_quantity"].cast("float"))


    #Function to categorize the response times and other quantity type fields. This will be used to aggregate data for OLAP usage.
    df = categorize_float_fields(df,"dispatch_response_seconds_qy","None","Very Low","Low","Medium","High","Very High")
    df = categorize_float_fields(df,"incident_response_seconds_qy","None","Very Low","Low","Medium","High","Very High")
    df = categorize_float_fields(df,"incident_travel_tm_seconds_qy","None","Very Low","Low","Medium","High","Very High")
    df = categorize_float_fields(df,"engines_assigned_quantity","None","Minimal","Limited","Moderate","Substantial","Abundant")
    df = categorize_float_fields(df,"ladders_assigned_quantity","None","Minimal","Limited","Moderate","Substantial","Abundant")
    df = categorize_float_fields(df,"other_units_assigned_quantity","None","Minimal","Limited","Moderate","Substantial","Abundant")



    #Calculating Averages for response times by each borough
    total_avg_dispatch_response_seconds_qy_per_borough = df.groupBy("alarm_box_borough").agg(avg("dispatch_response_seconds_qy")).alias("total_avg_dispatch_response_seconds_qy_per_borough")
    total_incident_travel_tm_seconds_qy_per_borough = df.groupBy("alarm_box_borough").agg(avg("incident_travel_tm_seconds_qy")).alias("total_incident_travel_tm_seconds_qy_per_borough")
    total_incident_response_seconds_qy_per_borough = df.groupBy("alarm_box_borough").agg(avg("incident_response_seconds_qy")).alias("total_incident_response_seconds_qy_per_borough")

    # Join the average back to the original DataFrame
    df = df.join(total_avg_dispatch_response_seconds_qy_per_borough, on="alarm_box_borough", how="left")
    df = df.join(total_incident_travel_tm_seconds_qy_per_borough, on="alarm_box_borough", how="left")
    df = df.join(total_incident_response_seconds_qy_per_borough, on="alarm_box_borough", how="left")

    
    df = clean_column(df,"avg(dispatch_response_seconds_qy)","total_avg_dispatch_response_seconds_qy_per_borough")
    df = clean_column(df,"avg(incident_travel_tm_seconds_qy)","total_avg_incident_travel_tm_seconds_qy_per_borough")
    df = clean_column(df,"avg(incident_response_seconds_qy)","total_avg_incident_response_seconds_qy_per_borough")

    #Total Resources Assigned to an Incident. Total quantity of Engines, Ladders, and Other Units.
    df = df.withColumn(
        "total_resources_assigned_quantity",
        col("engines_assigned_quantity") + col("ladders_assigned_quantity") + col("other_units_assigned_quantity")
    )

    json_string = create_json_string_from_df(df)

    return json_string
    