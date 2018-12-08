import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions as func
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()

def main(input_data):  
    data = spark.read.option("header","true").csv(input_data)
    selected_fields =  data.select('payment_type','tip_amount')
    #payment type vs tip amount
    payment_df = selected_fields.groupby('payment_type',).agg(func.count('tip_amount').alias('tip_amount'))
    payment_df.write.format('json').mode('overwrite').save('analysis_result/payment_tip')
    #most preferable payment types
    mostpref_paymnt = selected_fields.groupby('payment_type').agg(func.count('*').alias('count'))
    mostpref_paymnt.write.format('json').mode('overwrite').save('analysis_result/paymentmode')
    
if __name__ == '__main__':
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.catalog.clearCache()
    input_data = sys.argv[1]
    main(input_data)

