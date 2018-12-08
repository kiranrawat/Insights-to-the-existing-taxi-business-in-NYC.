# Insights-to-the-existing-taxi-business-in-NYC

## Dataset
trip_data.7z (contains trip related information like pickup/dropoff date and time, rate code, vendor id, pickup/dropoff location's longitude and latitude values) trip_fare.7z (contains trip's fare details like payment type, surcharges, tips, taxes, total amount paid )
  
## Setup
Download and install the following based on your operating system:
- Spark (https://spark.apache.org/downloads.html)
- ElasticSearch (https://www.elastic.co/downloads/elasticsearch) 
- Kibana (https://www.elastic.co/downloads/kibana)
- ES-Hadoop (https://www.elastic.co/products/hadoop)


## Project Components
Our projects is divided into four main parts:
### Pre-processing Data 
- First we begin by setting up the environment variable so that we can use it easily later. Example:
export SPARK_HOME = /usr/local/spark
- As part of preprocessing step, we collected data, read it into dataframes, cleaned it by removing rows having zero distance values, zero trip duration values and by restricting longitude and latitude values within 100 miles from the center of New York city and removing the outliers. We also dropped the columns that were out of the scope of our analysis.
- Then we joined data and fare dataframes keeping all the necessary columns and stored the final output as csv files, so that it can be accessed easily when analyzing data.
- To do preprocessing we need three inputs- first path to the folder containing trip_data files for all 12 months, then path to the folder containing trip_fare files for all 12 months and third input would be path to the folder where the final output is stored. The following command can be used to preprocess records:

`${SPARK_HOME}/bin/spark-submit preprocess.py` path_to_tripdata_folder/trip_data_1 path_to_tripfare_folder/trip_fare_1 output

### Analysing Data
After creating the preprocessed data, we have divided our analysis into four parts:
- General analysis - Includes weekly and monthly trends based on vendors, rate_code, average fare, average distance, average duration, total passengers, total trips and other general insights on complete data.
- Driver Based analysis - Included weekly and monthly trends for total trips, average trips, total fare, total passenegers, trip duration.
- Time of the day based analysis - We divided pickup_datetime in 4 different part namely morning, afternoon, evening and night. Based on time_of_day we performed analysis to find total trips, average tips,total bookings for each provider,duration of the trip vs passenger count.
- Payment  - Includes analysis on tip vs payment mode and most favoured payment mode.
 
All analysis files can be found in trips_analysis folder and the following command can be used to run any of the analysis files:

`${SPARK_HOME}/bin/spark-submit analysis_file_name.py` path_to_preprocessed_data_folder
 
Here each analysis file requires the path to preprocessed data folder as input.


### Load data in ElasticSearch
After analysing the preprossed data and generating json output files for each analysis, we saved our data in ElasticSearch. For this we need to decided how to index our data, and mapping for each index in ElasticSearch. First we begin by staring ElasticSearch on our machine. Go to the main ElasticSeach folder and run the command bin/elasticsearch to get it running. We can check the status by opening url http://localhost:9200/  which gives us general about the running elastic cluster.

Once ElasticSearch is up and running, we can create index and define its mapping using the following curl command(note this command creates an index named monthlysummary and document named driverrecord in the given index with mapping as specified in the JSON body):

```sh
curl -XPUT "http://localhost:9200/monthlysummary" -H 'Content-Type: application/json' -d'
{
"mappings":{
    "driverrecord":{
        "properties":{
            "medallion":{"type":"keyword"},
            "hack_license": {"type":"keyword"},
            "month_number":{"type":"integer"},
            "total_dist":{"type":"double"},
            "total_fare":{"type":"double"},
            "trip_count":{"type":"double"},
            "passenger_count":{"type":"integer"}
            }
        }
    }
}'
```

This can also be done by writing PUT request in Dev tool's section in Kibana if you dont want to use curl command. Next we use file logElastic.py in our main folder to transfer data corresponding to an amalysis to ElasticSearch. File logElastic.py needs one input i.e the name of the folder whose data we want to transfer. The following command can be used to load logs in ElasticSearch:

${SPARK_HOME}/bin/spark-submit --jars path_to_the_jar_file/elasticsearch-spark-20_2.11-6.5.1.jar logElastic.py analysis_result/analysis_folder_name

logElastic.py file reads all json files from the input folder(analysis_folder_name) mentioned in the command and loads then to ElasticSearch. Number of documents indexed in your indexd can be checked by hitting the url http://localhost:9200/_cat/indices.

### Visualization
We are using Kibana Dashboard to visualize our aggregated data.
For this we need to run Kibana. Go to the main Kibana folder in your machine and use bin/kibana command to start kibana. After you see 'Status changed from yellow to green - Ready' in the logs being displayed after running the command, open the url http://localhost:5601/app/kibana#/home?_g=() to access Kibana.

For creating visualizations corresponding to our stored data, we first need to define index patterns which tell Kibana which ElasticSearch indices you want to explore. This can be done by going to the management section in Kibana and defining the name of index pattern there. Then we need to go to the visualization section and choose graphs/maps that suits our analysis. 
then head to the dashboard section in Kibana, create a new dashboard, click on add button to add the visualizations you created.
