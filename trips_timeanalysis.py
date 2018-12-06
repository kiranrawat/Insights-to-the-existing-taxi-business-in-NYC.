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
    print("data joined")
    selected_fields = data.select('vendor_id','time_of_day','payment_type','trip_time_in_secs','passenger_count')
    # weekdays as a tuple
     #—>CHECK THE CAB BOOKINGS FOR EACH PROVIDER , OR ON A WHOLE  IN ONE OF THE ABOVE  6 PEAK TIMINGS 
    booking_df = selected_fields.groupby('vendor_id','time_of_day').agg(func.count('*').alias('count_bookings')) 
    print("first analysis done")
    # DURATION OF THE TRIP —VS—  PASSENGER COUNT—VS—  IN DIFFERENT  6 PARTS OF THE DAY 
    trip_pas_time = selected_fields.groupby('time_of_day').agg(func.sum('trip_time_in_secs').alias('total_duration'),func.sum('passenger_count').alias('total_passangers'))
    print("second analysis done")
      #CSH - VS -CRD — COUNT
    mostpref_paymnt = selected_fields.groupby('payment_type').agg(func.count('*').alias('count'))
    print("third analysis done")
    #total no. of trips in different times of the day
    total_trips = selected_fields.groupby('time_of_day').agg(func.count('*').alias('count_trips'))
    print("fourth analysis done")
    '''
    #total pickups and dropoffs weekly trend
    get_weekname_udf = func.udf(get_weekname, types.StringType())
    newdata = data.withColumn('day',get_weekname_udf(data['pickup_datetime'])) 
    
    get_monthname_udf = functions.udf(get_monthname, types.StringType())
    result_df = newdata.withColumn('month',get_monthname_udf(data['pickup_datetime'])) 
    '''
    
    booking_df.write.format('json').save("cab_bookings")
    trip_pas_time.write.format('json').save("trip_pass_time")
    mostpref_paymnt.write.format('json').save("payment")
    total_trips.write.format('json').save("vendor_bookings")

    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    main(input_data, input_fare)

