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
As part of preprocessing step, we collected data, cleaned it by removing rows having zero distance values, zero trip duration and by restricting longitude and latitude values within 100 miles from the center of New York city. We also removed the columns that were out of scope of our analysis.

2. Analysing Data
3. Load data in ElasticSearch
4. Visualization
