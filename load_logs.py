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
    data_df = tripdata.filter((tripdata['trip_distance']>0) & (tripdata['pickup_longitude']!=0) & (tripdata['pickup_latitude']!=0) & (tripdata['dropoff_longitude']!=0) & (tripdata['dropoff_latitude']!=0)).drop('store_and_fwd_flag')
    #data_df.show()
    fare_df = spark.read.option("header","true").csv(input_fare)
    #fare_df.show()
    '''
        geolocator = Nominatim(user_agent="nyctaxi")
        location = geolocator.reverse("40.741245, -73.978775")
        print(location.raw)'''
    joined_df = data_df.join(fare_df, [data_df['medallion']==fare_df['medallion'], data_df['hack_license']==fare_df[' hack_license'], data_df['pickup_datetime']==fare_df[' pickup_datetime']]).select(data_df['*'],fare_df[' total_amount'])
    joined_df.count()
    # driver with max distance
    monthly_dist = joined_df.groupBy('medallion', 'hack_license', sf.month('pickup_datetime')).agg(sf.sum('trip_distance').alias('sum_dist'), sf.sum(' total_amount').alias('sum_amount'), sf.sum('trip_time_in_secs').alias('sum_trip'))
    #monthly_dist.join(joined_df, [data_df['medallion']==fare_df['medallion'], data_df['hack_license']==fare_df[' hack_license'], data_df['pickup_datetime']==fare_df[' pickup_datetime']]).
    #abc = monthly_dist.rdd.map(customFunction)
    #print(abc.take(10))
#    print(abc)

    #monthly_dist.orderBy(monthly_dist['sum(trip_distance)'].desc()).show()
    #monthly_dist.groupBy('month(pickup_datetime)').agg(sf.max('sum(trip_distance)')).show()
    
    
    weekly_dist = joined_df.groupBy('medallion', 'hack_license', sf.weekofyear('pickup_datetime')).agg(sf.sum('trip_distance'))
    #weekly_dist.orderBy(weekly_dist['sum(trip_distance)'].desc()).show()
    #weekly_dist.groupBy('weekofyear(pickup_datetime)').agg(sf.max('sum(trip_distance)')).show()
    
    # driver with most fare collected
    monthly_fare = joined_df.groupBy('medallion', 'hack_license', sf.month('pickup_datetime')).agg(sf.sum(' total_amount'))
    weekly_fare = joined_df.groupBy('medallion', 'hack_license', sf.weekofyear('pickup_datetime')).agg(sf.sum(' total_amount'))
    
    # driver with most time travelled
    monthly_time = joined_df.groupBy('medallion', 'hack_license', sf.month('pickup_datetime')).agg(sf.sum('trip_time_in_secs'))
    weekly_fare = joined_df.groupBy('medallion', 'hack_license', sf.weekofyear('pickup_datetime')).agg(sf.sum('trip_time_in_secs'))

# Driver with most efficiency based on distance and time
#monthly_effD = joined_df.groupBy('medallion', 'hack_license', sf.month('pickup_datetime')).agg(sf.sum('trip_distance'),sf.sum('trip_time_in_secs'),('sum(trip_distance)'/'sum(trip_time_in_secs)') ).show()
#    es = Elasticsearch()
#    doc = {
#    'author': 'kimchy',
#    'text': 'Elasticsearch: cool. bonsai cool.',
#    'timestamp': datetime.datetime.now()
#    }
#    res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
#    print(res['result'])
    monthly_dist.write.format('json').save(output)

if __name__ == '__main__':
    input_data = sys.argv[1]
    input_fare = sys.argv[2]
    output = sys.argv[3]
    #keyspace = sys.argv[2]
    #tablename = sys.argv[3]
    main(input_data, input_fare, output)
