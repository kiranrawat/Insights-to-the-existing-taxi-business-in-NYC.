import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime
from pyspark.sql import SparkSession, functions as func, types
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
import time
from datetime import datetime
import preprocess as ps


def main(input_data):  
    #data = ps.load_clean_data(input_data,input_fare).cache()
    #load preprocessed file here
    data = spark.read.option("header","true").csv(input_data)
    selected_fields = data.select('pickup_datetime','dropoff_datetime').cache()
    weekly_pickups = selected_fields.groupBy(func.weekofyear('pickup_datetime')).agg(func.count('*').alias('weekly_pickups'))   
    #total pickups and dropoffs monthly trend
    monthly_pickups = selected_fields.groupBy(func.month('pickup_datetime')).agg(func.count('*').alias('monthly_pickups'))   
    #save the analysis repsonse in json files
    weekly_pickups.write.format('json').mode('overwrite').save('Weekly-Pickups')
    monthly_pickups.write.format('json').mode('overwrite').save('Monthly-Pickups')
   
    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    main(input_data)

