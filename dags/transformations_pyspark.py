import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import to_timestamp, to_date

#spark = SparkSession.builder.appName("Transformations_NYC_Fire_Incidents").getOrCreate()
#print(spark.version)


def pyspark_transformations(json_results):
    spark = SparkSession.builder \
        .appName("FireIncidents") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    #json_results_as_a_string = str(json_results)
    #df = spark.read.json(spark.sparkContext.parallelize([json_results]))
    print("hello world")
    