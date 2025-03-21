from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_timestamp, to_date, to_timestamp, concat, lit, upper, length
import json
from Fire_Incidents_Traffic_ETL.other_functions import read_temp_file
from Fire_Incidents_Traffic_ETL.other_functions import remove_temp_file


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


def main_traffic_nyc_pyspark_transformations(json_results,data_source):
    
    print("Starting PySpark Transformations...")

    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
    
        # Validate JSON format
    validate_json_format(json_results)


    #Reads json temp file
    read_json_data = read_temp_file(data_source)


    #Removes json temp file
    remove_temp_file(data_source)


    #Creates the spark data frame
    df = spark.read.json(spark.sparkContext.parallelize([read_json_data]))

    #Clean up month column
    df = df.withColumn(
            'm',
            when(length(df.m) == 1 ,concat(lit("0"),df.m))
            .otherwise(df.m))
    #Clean up day column
    df = df.withColumn(
            'd',
            when(length(df.d) == 1 ,concat(lit("0"),df.d))
            .otherwise(df.d))
    #Clean up month column
    df = df.withColumn(
            'hh',
            when(length(df.hh) == 1 ,concat(lit("0"),df.hh))
            .otherwise(df.hh))
    #Clean up day column
    df = df.withColumn(
            'mm',
            when(length(df.mm) == 1 ,concat(lit("0"),df.mm))
            .otherwise(df.mm))

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

    json_string = create_json_string_from_df(df)
    print("Created json string from PySpark dataframe")

    print("Transformations Complete!")
    return json_string
