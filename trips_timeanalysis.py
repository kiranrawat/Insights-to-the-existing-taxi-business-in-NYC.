import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as func, types
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()



def main(input_data):
    #load preprocessed file 
    data = spark.read.option("header","true").csv(input_data)
    #selecting the fields which we need for analysis
    selected_fields = data.select('vendor_id','time_of_day','payment_type','trip_time_in_secs','passenger_count','tip_amount').cache()
    #taxi bookings for each provider in 4 different parts of the time
    booking_df = selected_fields.groupby('vendor_id','time_of_day').agg(func.count('*').alias('count_bookings')) 
    #duration of the trip vs passenger count vs time of the day
    trip_pas_time = selected_fields.groupby('time_of_day').agg(func.sum('trip_time_in_secs').alias('total_duration'),func.sum('passenger_count').alias('total_passangers'))
    #Cash vs card count
    mostpref_paymnt = selected_fields.groupby('payment_type').agg(func.count('*').alias('count'))
    #total no. of trips in different times of the day
    total_trips = selected_fields.groupby('time_of_day').agg(func.count('*').alias('count_trips'))
    #avearge tip amount each time of the day
    tips = selected_fields.groupby('vendor_id','time_of_day').agg(func.avg('tip_amount').alias('avg_tips'))
    
    booking_df.write.format('json').mode('overwrite').save('vendor_trips_time')
    trip_pas_time.write.format('json').mode('overwrite').save('trip_pass_time')
    mostpref_paymnt.write.format('json').mode('overwrite').save('paymentmode')
    total_trips.write.format('json').mode('overwrite').save('trips_timeofday')
    tips.write.format('json').mode('overwrite').save("tips_timeofday")
    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    main(input_data)

