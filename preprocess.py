import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.catalog.clearCache()


#creating udf to split the time in 4 diffrent timespaces
def set_timerange(pickup_datetime):
    full_date_time = pickup_datetime.split(' ')[-1]
    timepart = full_date_time.split(':',1)[0]
    if (int(timepart) <= 12) & (int(timepart) > 5):
        return "Morning"
    elif (int(timepart) < 17) & (int(timepart) > 12):
        return "Afternoon"
    elif (int(timepart) < 22) & (int(timepart) > 17):
        return "Evening"
    else:
        return "Night"
  

# the columns  headers across diff csvs are unevenly formatted , to sort the issue creating schemas to before load 
def load_clean_data(input_data, input_fare):
    # main logic starts here 
    tripdata_schema = types.StructType([
    types.StructField('medallion', types.StringType(), False),
    types.StructField('hack_license', types.StringType(), True),
    types.StructField('vendor_id', types.StringType(), True),
    types.StructField('rate_code', types.StringType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('pickup_datetime', types.StringType(), True),
    types.StructField('dropoff_datetime', types.StringType(), True),
    types.StructField('passenger_count', types.StringType(), True),
    types.StructField('trip_time_in_secs', types.StringType(), True),
    types.StructField('trip_distance', types.StringType(), True),
    types.StructField('pickup_longitude', types.StringType(), True),
    types.StructField('pickup_latitude', types.StringType(), True),
    types.StructField('dropoff_longitude', types.StringType(), True),
    types.StructField('dropoff_latitude', types.StringType(), True)
]) 

    tripfare_schema = types.StructType([
    types.StructField('medallion', types.StringType(), True),
    types.StructField('hack_license', types.StringType(), True),
    types.StructField('vendor_id', types.StringType(), True),
    types.StructField('pickup_datetime', types.StringType(), True),
    types.StructField('payment_type', types.StringType(), True),
    types.StructField('fare_amount', types.StringType(), True),
    types.StructField('surcharge', types.StringType(), True),
    types.StructField('mta_tax', types.StringType(), True),
    types.StructField('tip_amount', types.StringType(), True),
    types.StructField('tolls_amount', types.StringType(), True),
    types.StructField('total_amount', types.StringType(), True)
]) 

    #reading the files and cleaning the data 
    tripdata = spark.read.csv(input_data, header=False, schema=tripdata_schema)  
    filtertripdata = tripdata.filter((tripdata['trip_distance']>0) & (tripdata['pickup_longitude']!=0) & (tripdata['pickup_latitude']!=0) & (tripdata['dropoff_longitude']!=0) & (tripdata['dropoff_latitude']!=0)).drop('store_and_fwd_flag')
    fare_df = spark.read.csv(input_fare, header=False, schema=tripfare_schema)
    joined_df = filtertripdata.join(fare_df,['medallion', 'hack_license','pickup_datetime'],"inner").drop(fare_df['pickup_datetime']).drop(fare_df['vendor_id']).drop(fare_df['surcharge']).drop(fare_df['mta_tax'])
    #calling the udf to spearate the datetime
    date_time_udf = functions.udf(set_timerange, types.StringType())
    final_df = joined_df.withColumn('time_of_day',date_time_udf(joined_df['pickup_datetime'])) 
    #final_df.write.option("header","true").csv(output_file,mode='overwrite')
    return final_df
