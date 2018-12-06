import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime
from pyspark.sql import SparkSession, functions as func, types
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
import time
from datetime import datetime
import preprocess as ps


def main(input_data, input_fare):  
    data = ps.load_clean_data(input_data,input_fare).cache()
    selected_fields = data.select('pickup_datetime','dropoff_datetime')
    weekly_pickups = selected_fields.groupBy(func.weekofyear('pickup_datetime')).agg(func.count('*').alias('weekly_pickups'))
    weekly_dropoffs = selected_fields.groupBy(func.weekofyear('dropoff_datetime')).agg(func.count('*').alias('weekly_dropoffs'))
    
    #total pickups and dropoffs monthly trend
    monthly_pickups = selected_fields.groupBy(func.month('pickup_datetime')).agg(func.count('*').alias('monthly_pickups'))
    monthly_dropoffs = selected_fields.groupBy(func.month('dropoff_datetime')).agg(func.count('*').alias('monthly_dropoffs'))
    
    #save the analysis repsonse in json files
    weekly_pickups.write.format('json').save("Weekly-Pickups")
    weekly_dropoffs.write.format('json').save("Weekly-Dropoffs")
    monthly_pickups.write.format('json').save("Monthly-Pickups")
    monthly_dropoffs.write.format('json').save("Monthly-Dropoffs")
    
    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    main(input_data, input_fare)

