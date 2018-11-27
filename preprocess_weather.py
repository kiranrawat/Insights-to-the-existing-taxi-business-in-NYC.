import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime
from pygeocoder import Geocoder
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.catalog.clearCache()

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def latlong_to_city(longg, latt):
   print("inside function")
   station = Geocoder.reverse_geocode(longg, latt)
   return station

def main(input_data, input_fare, input_weather):
    # main logic starts here
    
    tripdata = spark.read.option("header","true").csv(input_data)
    data_df = tripdata.filter((tripdata['trip_distance']>0) & (tripdata['pickup_longitude']!=0) & (tripdata['pickup_latitude']!=0) & (tripdata['dropoff_longitude']!=0) & (tripdata['dropoff_latitude']!=0)).drop('store_and_fwd_flag')
    #data_df.show()
    '''
    fare_df = spark.read.option("header","true").csv(input_fare)
    fare_df.show()
    '''
    weather_data = spark.read.option("header","true").csv(input_weather)
    #get the weather station from new york only
    station_list = ['US1NYRC0001','US1NYRC0002','US1NYWC0003',
                    'US1NYRL0005','USW00094745','US1NYKN0025',
                    'USC00301309','USW00094728','US1NYQN0002',
                    'USW00054787','USW00014732','US1NYNS0006',
                    'US1NYNS0007', 'US1NYWC0009']
    weather_df = weather_data.select((weather_data['STATION']),(weather_data['NAME']),(weather_data['LATITUDE']),
                                    (weather_data['LONGITUDE']),(weather_data['DATE']),(weather_data['PRCP']),
                                    (weather_data['SNOW']))
    weather_filterdf = weather_df.filter(weather_df['STATION'].isin(station_list))
    
    #calculate the distance between places based on longitude and latitude information
    #get_loc_udf = functions.udf(latlong_to_city, types.StringType())
    location_df = data_df.withColumn('station',latlong_to_city(data_df['pickup_longitude'],data_df['pickup_latitude']))
    location_df.printSchema()
    location_df.show()
   
    #joined_df = data_df.withColumn('logitude_diff', data_df['pickup_longitude'] - weather_filterdf['LONGITUDE']).withColumn('latitude_diff', data_df['pickup_latitude'] - weather_filterdf['LATITUDE'])
    #location_df.show()
    #weather_filterdf.show()

    #data_df['pickup_latitude'] - fare_df['LATITUDE'])    
    #joined_df.show()                                
    

if __name__ == '__main__':
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    input_weather = sys.argv[3]
    #tablename = sys.argv[3]
    main(input_data, input_fare, input_weather)
