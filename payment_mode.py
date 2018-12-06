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
    selected_fields =  data.select('payment_type','tip_amount')
    #payment type vs tip amount
    payment_df = selected_fields.groupby('payment_type',).agg(func.count('tip_amount').alias('tip_amount'))
    payment_df.write.format('json').save("payment_tip")
    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    main(input_data, input_fare)

