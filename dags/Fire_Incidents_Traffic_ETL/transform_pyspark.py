from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import to_timestamp
import json
from pyspark.sql.functions import max
from pyspark.sql.functions import min
from pyspark.sql.functions import avg
from Fire_Incidents_Traffic_ETL.other_functions import read_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import write_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import remove_temp_file
import os

#Function to clean null values, The function takes in the following paramters: pyspark dataframe, dictionary of fields to clean and values to use when null, field to aggregate by, and the aggregate values
def clean_null_values(df,null_fields_and_values,aggregate_field,aggregate_values):
    
    for field in null_fields_and_values:
        
        df = df.withColumn(
        field,
        when(col(field).isNull() & (col(aggregate_field) == aggregate_values[0]),null_fields_and_values[field][0])
        .when(col(field).isNull() & (col(aggregate_field) == aggregate_values[1]),null_fields_and_values[field][1])
        .when(col(field).isNull() & (col(aggregate_field) == aggregate_values[2]),null_fields_and_values[field][2])
        .when(col(field).isNull() & (col(aggregate_field) == aggregate_values[3]),null_fields_and_values[field][3])
        .when(col(field).isNull() & (col(aggregate_field) == aggregate_values[4]),null_fields_and_values[field][4])
        .otherwise(col(field))
    )
    return df

#Function to convert fields to date time
def convert_to_date_time(df,date_fields_to_convert):
    for field in date_fields_to_convert:
        df = df.withColumn(field, to_timestamp(df[field]))
    return df

#Function to convert fields to floats
def convert_to_float(df,date_fields_to_convert):
    for field in date_fields_to_convert:
        df = df.withColumn(field, df[field].cast("float"))
    return df

#Function to categorize floats into aggregate buckets
def categorize_float_fields(df,fields_to_categorize):
    
    for field in fields_to_categorize:
    
        # Returns the max response
        max_quantity = df.agg(max(field).alias("max_response_alias")).collect()[0]
        max_quantity = max_quantity["max_response_alias"] 
    
        # Returns the min quantity
        min_quantity = df.agg(min(field).alias("min_response_alias")).collect()[0]
        min_quantity = min_quantity["min_response_alias"]
    
        #Calculates the category interval this is to determine the intervals between each category. 5 Categories were chosen.
        category_interval = (max_quantity - min_quantity) / 5
        
        #Categorizes each quantity column using the range between the max and min
        df = df.withColumn(
            "category_" + field,
            when((col(field) == 0),fields_to_categorize[field][0])
            .when((col(field) > 0) & (col(field) <= category_interval),fields_to_categorize[field][1])
            .when((col(field) > category_interval) & (col(field) <= (category_interval*2)),fields_to_categorize[field][2])
            .when((col(field) > (category_interval*2)) & (col(field) <= (category_interval*3)),fields_to_categorize[field][3])
            .when((col(field) > (category_interval*3)) & (col(field) <= (category_interval*4)),fields_to_categorize[field][4])
            .otherwise(fields_to_categorize[field][5])
        )
    
    return df


#Renaming Columns and Casting to Float Types
def clean_column(df,column_name_before,column_name_after):
    df = df.withColumnRenamed(column_name_before, column_name_after)
    df = df.withColumn(column_name_after, col(column_name_after).cast("float"))
    return df

#Calcualtes averages of float fields
def calculate_averages(df,fields_to_calculate_averages,aggregate_field):
    for field in fields_to_calculate_averages:
        total_avg = df.groupBy(aggregate_field).agg(avg(field)).alias("total_avg_"+field+"_per_borough")
        df = df.join(total_avg, on=aggregate_field, how="left")
        df = clean_column(df,f'avg({field})',f'total_avg_{field}_per_borough')
    return df

#Sums numerical fields to create a total amount
def sum_fields(df,sum_field_name,fields_to_sum):
    df = df.withColumn(
        sum_field_name,
        col(fields_to_sum[0]) + col(fields_to_sum[1]) + col(fields_to_sum[2])
    )
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




#--------------Main Transformations using Pyspark--------------
def main_pyspark_transformations(offset_counter,data_source):
    
    print("Starting PySpark Transformations...")

    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()


    # Validate JSON format
    #validate_json_format(json_results)
    offset = 1000
    #while offset < offset_counter:
        

    #Reads json temp file
    #read_json_data = read_temp_file(data_source,offset,'extract')
    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, f'temp/extract/')
    read_json_data = []
    while offset < offset_counter + 1000:
        json_offset_data = read_temp_file(data_source,offset,'extract')
        read_json_data.append(json_offset_data)
        offset += 1000
#
    #spark = SparkSession.builder.appName("ReadJSON").getOrCreate()
    #directory_path = ""

    #print(read_json_data[0])

    #Removes json temp file
    #remove_temp_file(data_source)


    #Creates the spark data frame
    #df = spark.read.json(spark.sparkContext.parallelize([read_json_data]))
    df = spark.read.json(spark.sparkContext.parallelize([read_json_data][0]))
    

    print(f"Pyspark count number of rows {df.count()}")

    #The values presented below correspond to the first entry identified for each field within their respective boroughs. For instance, in the case of the Bronx, the first zip code encountered in the dataset was 10451.  
    #For the null values, it is assumed that the newly assigned values will approximate the actual values as closely as possible.
    null_fields_and_values = {"zipcode":[10451,11201,10001,11004,10301],
                        "policeprecinct":[40,60,1,100,120],
                        "citycouncildistrict":[8,33,1,19,49],
                        "communitydistrict":[201,301,101,401,501],
                        "communityschooldistrict":[7,13,1,7,31],
                        "congressionaldistrict":[13,7,7,3,11]}

    aggregate_field = "incident_borough"
    aggregate_values = ["BRONX","BROOKLYN","MANHATTAN","QUEENS","RICHMOND / STATEN ISLAND"]
    df = clean_null_values(df,null_fields_and_values,aggregate_field,aggregate_values)
    print("Null values are clean")


    #Convert to floats
    numerical_fields_to_convert = ["dispatch_response_seconds_qy",
                                "incident_response_seconds_qy",
                                "incident_travel_tm_seconds_qy",
                                "engines_assigned_quantity",
                                "ladders_assigned_quantity",
                                "other_units_assigned_quantity"]
    df = convert_to_float(df,numerical_fields_to_convert)
    print("Converted Fields to Floats")


    #Function to categorize the response times and other quantity type fields. This will be used to aggregate data for OLAP usage.
    fields_to_categorize = {"dispatch_response_seconds_qy":["None","Very Low","Low","Medium","High","Very High"],
    "incident_response_seconds_qy":["None","Very Low","Low","Medium","High","Very High"],
    "incident_travel_tm_seconds_qy":["None","Very Low","Low","Medium","High","Very High"],
    "engines_assigned_quantity":["None","Minimal","Limited","Moderate","Substantial","Abundant"],
    "ladders_assigned_quantity":["None","Minimal","Limited","Moderate","Substantial","Abundant"],
    "other_units_assigned_quantity":["None","Minimal","Limited","Moderate","Substantial","Abundant"]}
    df = categorize_float_fields(df,fields_to_categorize)
    print("Categorized fields based on 5 aggregates")


    #Calculate Averages for dispatch_response_seconds_qy, incident_travel_tm_seconds_qy, and incident_response_seconds_qy by incident_borough
    aggregate_field = "incident_borough"
    fields_to_calculate_averages = ["dispatch_response_seconds_qy","incident_travel_tm_seconds_qy","incident_response_seconds_qy"]
    df = calculate_averages(df,fields_to_calculate_averages,aggregate_field)
    print("Caculated Averages for response times by each borough")
    

    #Total Resources Assigned to an Incident. Total quantity of Engines, Ladders, and Other Units.
    sum_field_name = "total_resources_assigned_quantity"
    fields_to_sum = ["engines_assigned_quantity","ladders_assigned_quantity","other_units_assigned_quantity"]
    df = sum_fields(df,sum_field_name,fields_to_sum)
    print("Created total_resources_assigned_quantity column")
    



    #Creates a json string from the dataframe
    json_string = create_json_string_from_df(df)
    print("Created json string from PySpark dataframe")

    
    #print(f"Writing json temp file to temp transform folder. Currently On : {offset}")
    write_temp_file(json_string,data_source,"all_json_data",'transform')
    
    offset = 1000
    while offset < offset_counter + 1000:
        remove_temp_file(data_source,offset,'extract')
        offset += 1000

    #offset += 1000

    print("Transformations Complete!")
    #return json_string
    return 'Transformations Completed'
    