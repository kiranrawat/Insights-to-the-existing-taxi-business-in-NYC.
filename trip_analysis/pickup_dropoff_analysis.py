import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime
from pyspark.sql import SparkSession, functions as func, types
from pyspark.sql.functions import count
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()

'''
udf to map the rate_code to airport name
by looking into the dataset documentation,
we know rate_code = 2 is for 'JFK Airport' 
& ate_code = 3 is for 'Newark Airport'
'''
def get_name(ratecode):
    if (ratecode == '2'):
        airport_name = 'JFK Airport'
        return  airport_name
    elif (ratecode == '3'):
        airport_name = 'Newark Airport'
        return airport_name


def main(input_data):  
    #load preprocessed file here
    data = spark.read.option("header","true").csv(input_data)
    selected_fields = data.filter((data['rate_code']==2) | (data['rate_code']==3)).select('rate_code','pickup_datetime','time_of_day','dropoff_datetime','pickup_longitude', 'pickup_latitude').cache()
    #registered a user defined function get_name to retrieve the name of airport based on rate_code
    get_airportname = func.udf(get_name, types.StringType())
    final_df = selected_fields.withColumn('airport_name',get_airportname(selected_fields['rate_code'])).cache()
    #total trips from 'JFK airport' and 'Newark airport' in different parts of the time
    airport_trips = final_df.groupBy('airport_name','time_of_day').agg(count('*').alias('cnt')).alias('trips')
    #writing the analysis result in json format
    airport_trips.write.format('json').mode('overwrite').save('analysis_result/Airport_Trips')
    #total pickups in a week
    weekly_pickups = selected_fields.groupBy(func.weekofyear('pickup_datetime')).agg(count('*').alias('weekly_pickups'))  
    #writing the analysis result in json format 
    weekly_pickups.write.format('json').mode('overwrite').save('analysis_result/Weekly-Pickups')
    #total pickups in a month
    monthly_pickups = selected_fields.groupBy(func.month('pickup_datetime')).agg(count('*').alias('monthly_pickups'))   
    #writing the analysis result in json format
    monthly_pickups.write.format('json').mode('overwrite').save('analysis_result/Monthly-Pickups')
      
    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    main(input_data)

