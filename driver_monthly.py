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
    monthly_summary = tripdata.groupBy('medallion', 'hack_license', sf.month('pickup_datetime').alias('p_month')).agg(sf.sum('trip_distance').alias('sum_dist'),
                                                                                                                       sf.sum('total_amount').alias('sum_amount'),
                                                                                                                       sf.sum('trip_time_in_secs').alias('sum_trip'),
                                                                                                                       sf.sum('passenger_count').alias('passenger_count')).show()
    monthly_summary.write.format('json').mode('overwrite').save('analysis_result/driver_monthly')

if __name__ == '__main__':
    input_data = sys.argv[1]
    main(input_data)
