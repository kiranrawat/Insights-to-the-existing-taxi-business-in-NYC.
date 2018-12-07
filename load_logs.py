import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, json
from elasticsearch import Elasticsearch, helpers
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession, functions as sf, types, Row
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.catalog.clearCache()

# add more functions as necessary
def latlong_to_loc(coor):
    print(coor)
    geolocator = Nominatim(user_agent="nyctaxi")
    return geolocator.reverse(coor)

def main(input_data, input_fare, output):
    # main logic starts here
    tripdata = spark.read.option("header","true").csv(input_data)
    data_df = tripdata.filter((tripdata['trip_distance']>0) & (tripdata['pickup_longitude']!=0) & (tripdata['pickup_longitude']<180.00) & (tripdata['pickup_longitude']>-180) &  (tripdata['pickup_latitude']!=0) & (tripdata['pickup_latitude']<90.00) & (tripdata['pickup_latitude']>-90.00) & (tripdata['dropoff_longitude']!=0) & (tripdata['dropoff_latitude']!=0) & (tripdata['dropoff_latitude']<90.00) & (tripdata['dropoff_latitude']>-90.00) & (tripdata['dropoff_longitude']<180.00) & (tripdata['dropoff_longitude']>-180)).drop('store_and_fwd_flag')
    #data_df.show()
    fare_df = spark.read.option("header","true").csv(input_fare)
    #fare_df.show()
    '''
        geolocator = Nominatim(user_agent="nyctaxi")
        location = geolocator.reverse("40.741245, -73.978775")
        print(location.raw)'''
    joined_df = data_df.join(fare_df, [data_df['medallion']==fare_df['medallion'], data_df['hack_license']==fare_df[' hack_license'], data_df['pickup_datetime']==fare_df[' pickup_datetime']]).select(data_df['*'],fare_df[' total_amount'].alias('total_amount'), (sf.concat(data_df['pickup_latitude'], sf.lit(","), data_df['pickup_longitude'])).alias('pickupLoc'), (sf.concat(data_df['dropoff_latitude'], sf.lit(","), data_df['dropoff_longitude'])).alias('dropoffLoc'))
    joined_df.write.format('json').save(output)

if __name__ == '__main__':
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    output = sys.argv[3]
    main(input_data, input_fare, output)
