import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as sf, types, Row
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.catalog.clearCache()

def main(input_data):
    # main logic starts here
    tripdata = spark.read.option("header","true").csv(input_data)
    # aggregation over all records
    df = tripdata.select(sf.avg('trip_distance').alias('avg_distance'),
                         sf.avg('total_amount').alias('avg_fare'),
                         sf.avg('trip_time_in_secs').alias('avg_duration'),
                         sf.sum('passenger_count').alias('total_passengers'),
                         sf.count('*').alias('total_trips'))
    df.write.format('json').mode('overwrite').save('analysis_result/all_records')

    # aggregation over all records on monthly basis
    month_df = tripdata.groupBy(sf.month('pickup_datetime').alias('p_month')).agg(sf.count('*').alias('total_trips'),
                                                                                  sf.sum('passenger_count').alias('total_passengers'),
                                                                                  sf.avg('total_amount').alias('avg_fare'),
                                                                                  sf.avg('trip_distance').alias('avg_distance'),
                                                                                  sf.avg('trip_time_in_secs').alias('avg_duration'))
    month_df.write.format('json').mode('overwrite').save('analysis_result/all_records_month')
    
    # aggregation over all records on weekly basis
    week_df = tripdata.groupBy(sf.weekofyear('pickup_datetime').alias('p_week')).agg(sf.count('*').alias('total_trips'),
                                                                              sf.sum('passenger_count').alias('total_passengers'),
                                                                              sf.avg('total_amount').alias('avg_fare'),
                                                                              sf.avg('trip_distance').alias('avg_distance'),
                                                                              sf.avg('trip_time_in_secs').alias('avg_duration'))
    week_df.write.format('json').mode('overwrite').save('analysis_result/all_records_week')
    
    # aggregation over all records based on ratecode
    ratecode_df = tripdata.groupBy('rate_code').agg(sf.count('*').alias('total_trips'),
                                                    sf.sum('passenger_count').alias('total_passengers'),
                                                    sf.avg('total_amount').alias('avg_fare'),
                                                    sf.avg('trip_distance').alias('avg_distance'),
                                                    sf.avg('trip_time_in_secs').alias('avg_duration'))
    ratecode_df.write.format('json').mode('overwrite').save('analysis_result/all_records_ratecode')
    
    # aggregation over all records based on type of payment
    payment_type_df = tripdata.groupBy('payment_type').agg(sf.count('*').alias('total_trips'),
                                                           sf.sum('passenger_count').alias('total_passengers'),
                                                           sf.avg('total_amount').alias('avg_fare'),
                                                           sf.avg('trip_distance').alias('avg_distance'),
                                                           sf.avg('trip_time_in_secs').alias('avg_duration'))
    payment_type_df.write.format('json').mode('overwrite').save('analysis_result/all_records_payment')
    
    # aggregation over all records based on vendor
    vendor_df = tripdata.groupBy('vendor_id').agg(sf.count('*').alias('total_trips'),
                                                  sf.avg('passenger_count').alias('avg_passengers'),
                                                  sf.avg('total_amount').alias('avg_fare'),
                                                  sf.avg('trip_distance').alias('avg_distance'),
                                                  sf.avg('trip_time_in_secs').alias('avg_duration'))
    vendor_df.write.format('json').mode('overwrite').save('analysis_result/all_records_vendor')


if __name__ == '__main__':
    input_data = sys.argv[1]
    main(input_data)
