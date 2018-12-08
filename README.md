# Insights-to-the-existing-taxi-business-in-NYC

- Datasets
  trip_data.7z (contains trip related information like pickup/dropoff date and time, rate code, vendor id, pickup/dropoff location's longitude and latitude values)
  trip_fare.7z (contains trip's fare details like payment type, surcharges, tips, taxes, total amount paid )
  
- Setup
1. Download and install the following based on your operating system:
    - Spark (https://spark.apache.org/downloads.html)
    - ElasticSearch (https://www.elastic.co/downloads/elasticsearch) 
    - Kibana (https://www.elastic.co/downloads/kibana)
    - ES-Hadoop (https://www.elastic.co/products/hadoop)
    
Our projects is divided into four main parts:
1. Pre-processing Data
First we begin by setting up the environment variable so that we can use it easily later. Example:
export SPARK_HOME = /usr/local/spark
As part of preprocessing step, we collected data, read it into dataframes, cleaned it by removing rows having zero distance values, zero trip duration values and by restricting longitude and latitude values within 100 miles from the center of New York city and removing the outliers. We also dropped the columns that were out of the scope of our analysis. Then we joined data and fare dataframes keeping all the necessary columns and stored the final output as csv files, so that it can be accessed easily when analyzing data.

To do preprocessing we need three inputs- first path to the folder containing trip_data files for all 12 months, then path to the folder containing trip_fare files for all 12 months and third input would be path to the folder where the final output is stored. The following command can be used to preprocess records:

${SPARK_HOME}/bin/spark-submit preprocess.py data/trip_data_1 data/trip_fare_1 dataset
assuming data is present in the data folder and  final output will be stored n dataset folder

2. Analysing Data
3. Load data in ElasticSearch
4. Visualization
