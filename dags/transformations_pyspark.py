import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import to_timestamp, to_date
import json

#spark = SparkSession.builder.appName("Transformations_NYC_Fire_Incidents").getOrCreate()
#print(spark.version)


def pyspark_transformations(json_results):
    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    # Validate JSON format
    try:
        json.loads(json_results)
        print("Valid JSON")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")

    json_results = f"[{json_results}]"
        
    json_list = json.loads(json_results)
        
    df = spark.read.json(spark.sparkContext.parallelize([json_list]))
    df.show()
    #df = spark.read.json(json_results)
    print("hello world")
    