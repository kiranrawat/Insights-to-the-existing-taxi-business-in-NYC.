import sys,os 
# set environment variable PYSPARK_SUBMIT_ARGS
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ${SPARK_HOME}/jars/elasticsearch-hadoop-6.5.1/dist/elasticsearch-spark-20_2.11-6.5.1.jar pyspark-shell'
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, json, random
from elasticsearch import Elasticsearch, helpers
from pyspark.sql import SparkSession, functions as sf, types, Row
spark = SparkSession.builder.appName('NYC TAXI').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
spark.catalog.clearCache()

def format_data(data):
    return (random.randint(1,1000000), data)

def main(input_folder, index_name):
    es_conf = {
    "es.nodes" : 'localhost', # specify the node that we are sending data to (this should be the master)
    "es.port" : '9200', # specify the port in case it is not the default port
    "es.resource" : index_name+'/drecord_monthly', # specify a resource in the form 'index/doc-type'
    "es.input.json" : "yes", # is the input JSON?
    "es.batch.size.entries":"500",
    "es.batch.write.refresh":"False",
    "es.batch.write.retry.count":"1",
    "es.batch.write.retry.wait":"10s"
    }
    df = spark.read.json(input_folder).toJSON()
    rdd = df.map(lambda x: format_data(x))
    rdd.saveAsNewAPIHadoopFile(
                               path='-',
                               outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                               keyClass="org.apache.hadoop.io.NullWritable",
                               valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                               conf=es_conf)

if __name__ == '__main__':
    input_folder = sys.argv[1]
    index_name = sys.argv[2]
    main(input_folder, index_name)
