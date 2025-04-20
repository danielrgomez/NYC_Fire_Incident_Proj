from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_timestamp, to_timestamp, concat, lit, upper, length
from pyspark.storagelevel import StorageLevel
import json
from Fire_Incidents_Traffic_ETL.other_functions import read_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import remove_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import write_temp_file
import os


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

def clean_up_date_time(date_time_fields,df):
    #Clean up column
    for fields in date_time_fields:
        df = df.withColumn(
                fields,
                when(length(col(fields)) == 1 ,concat(lit("0"),col(fields)))
                .otherwise(col(fields)))
    return df


def main_traffic_nyc_pyspark_transformations(offset_counter,data_source):
    
    print("Starting PySpark Transformations...")

    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
    
    
    
    offset = 1000
    print(f"offset counter: {offset_counter}")

    current_dir = os.getcwd()
    temp_folder = os.path.join(current_dir, f'temp/extract/')
    read_json_data = []
    while offset < offset_counter + 1000:
        json_offset_data = read_temp_file(data_source,offset,'extract')
        read_json_data.append(json_offset_data)
        offset += 1000
    
    #Creates the spark data frame
    df = spark.read.json(spark.sparkContext.parallelize([read_json_data][0]))

    #Partition dataframe into 8 partitions. Partitions are created to process each in parallel across the cluster. Each partition is handled by an executer in spark enabling distributed computation.
    df = df.repartition(8)
    
    #Persists data by saving DataFrame in memory and disk to avoid recomputing multiple times in a workflow. Persisting helps retain intermediate resutls for reuse
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    print(f"Pyspark count number of rows {df.count()}")

    #Transformation Function 
    date_time_fields = ['m','d','hh','mm']
    df = clean_up_date_time(date_time_fields,df)
    print("Date and Time Fields Cleaned")

    #Create new field called report_date_time which concatenates yr,m,d,hh,mm and converts to datetime field in dataframe
    df = df.withColumn("report_date_time",concat(df.yr,lit("-"),df.m,lit("-"),df.d,lit(" "),df.hh,lit(":"),df.mm,lit(":00")))
    df = df.withColumn("report_date_time", to_timestamp(df.report_date_time, "yyyy-MM-dd HH:mm:ss"))
    print("Create report_date_time")

    #Make Borough Upper Case
    df = df.withColumn("boro", upper(df.boro))
    print("Uppercase boro")

    #Staten Island becomes RICHMOND / STATEN ISLAND
    df = df.withColumn(
        'boro',
        when(col('boro') == 'STATEN ISLAND','RICHMOND / STATEN ISLAND')
        .otherwise(col('boro'))
    )
    print("Converted Staten Island Values")

    #Convert vol to integer
    df = df.withColumn('vol', df['vol'].cast("int"))
    print("Cast vol to integer")

    #Coalesce to 1 partittion. Reduces the number of partitions when the dataset becomes smaller after all of the transformations above.
    df = df.coalesce(1)

    json_string = create_json_string_from_df(df)
    print("Created json string from PySpark dataframe")

    write_temp_file(json_string,data_source,"all_json_data",'transform')

    offset = 1000
    while offset < offset_counter + 1000:
        remove_temp_file(data_source,offset,'extract')
        offset += 1000

    print("Transformations Complete!")

    return 'Transformations Completed'
