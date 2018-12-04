import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
from geopy.extra.rate_limiter import RateLimiter
import time
from datetime import datetime
from pygeocoder import Geocoder
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import preprocess as ps


def main(input_data, input_fare, output_file):
    data = ps.load_clean_data(input_data,input_fare,output_file).cache()
     #—>CHECK THE CAB BOOKINGS FOR EACH PROVIDER , OR ON A WHOLE  IN ONE OF THE ABOVE  6 PEAK TIMINGS 
    booking_df = data.groupby('vendor_id','time_of_day').agg(functions.count('*').alias('count_bookings'))
    booking_df.show()
    # DURATION OF THE TRIP —VS—  PASSENGER COUNT—VS—  IN DIFFERENT  6 PARTS OF THE DAY 
    mostpref_paymnt = data.groupby('time_of_day').agg(functions.sum('trip_time_in_secs').alias('total_duration'),functions.sum('passenger_count').alias('total_passangers'))
    mostpref_paymnt.show()
      #CSH - VS -CRD — COUNT
    mostpref_paymnt = data.groupby('payment_type').agg(functions.count('*').alias('count'))
    mostpref_paymnt.show()
    #CAS -VS CRD — DIFFERENT PARTS OF THE DAY
    trip_df = data.groupby('payment_type','vendor_id').agg(functions.count('*').alias('count_trips'))
    trip_df.show()
    #total no. of trips in different times of the day
    mostpref_paymnt = data.groupby('time_of_day').agg(functions.count('*').alias('count_trips'))
    mostpref_paymnt.show()
    #booking_df.write.format('json').save(output_file)

if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    output_file = sys.argv[3]
    main(input_data, input_fare, output_file)

