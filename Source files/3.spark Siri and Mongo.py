import boto3
import findspark
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime,udf,struct, to_json
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import pymongo
import datetime
import dns
import pandas as pd
import haversine as hs
from haversine import Unit

import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points

routes = [9872,9857]#,'9873']

os.environ['SPARK_HOME'] = "/home/naya/spark-2.4.7-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=com.qubole.spark/spark-sql-kinesis_2.11/1.1.3-spark_2.4 pyspark-shell'

findspark.init()


MongoDbBICluster =   "mongodb+srv://<UserName>:<Password>@tranmongodbcluster.3sb01.mongodb.net/?retryWrites=true&w=majority"

MongoDb = MongoDbBICluster

stream_name_Pub = 'TranOnline'
stream_name_Sub = 'tranafterspark'

awsAccessKeyId = "AKIAUXXXXXXXXXX" 
awsSecretKey = "HAKT4cOdXXXXXXXXXX"

today = datetime.datetime.today()
todat_str = today.strftime("%Y-%m-%d")

spark = SparkSession.builder \
         .master('local[*]') \
         .appName('PySparkKinesis') \
         .getOrCreate()

#Connect to Mongo
client = pymongo.MongoClient(MongoDb)
db = client["TranDB"]
Trips_collection = db["Trips"]

#Retrieve from mongo the information of today's schedules for the selected routes
trip_schedule = Trips_collection.find({ '$and' : [
                                                    {"DepartureDate" : todat_str},
                                                    {"route_id" : { '$in': routes}}
                                                ]
                                        },{"_id" :0,"trip_id_of_agency":1,"stop_time_json": 1})

#convect to Pandas DataFrame
df_schedule = pd.DataFrame(list(trip_schedule))

#convect to Spark DataFrame
spark_schedule = spark.createDataFrame(df_schedule)

spark_schedule = spark_schedule.withColumnRenamed("trip_id_of_agency","Short_Trip_Id")


TripSchema = StructType() \
            .add("DataFrameRef", StringType()) \
            .add("DatedVehicleJourneyRef", StringType()) \
            .add("DestinationRef", StringType()) \
            .add("DirectionRef", StringType()) \
            .add("LineRef", StringType()) \
            .add("OperatorRef", StringType()) \
            .add("OriginAimedDepartureTime", StringType()) \
            .add("PublishedLineName", StringType()) \
            .add("RecordedAtTime", StringType()) \
            .add("VehicleLocation_Latitude", StringType()) \
            .add("VehicleLocation_Longitude", StringType()) \
            .add("Velocity", StringType())

#Get Stream Data From Kinesis
kinesisDF = spark \
        .readStream \
        .format('kinesis') \
        .option('streamName', stream_name_Pub) \
        .option('endpointUrl', 'https://kinesis.eu-north-1.amazonaws.com')\
        .option('region', 'eu-north-1') \
        .option('awsAccessKeyId', awsAccessKeyId ) \
        .option('awsSecretKey', awsSecretKey ) \
        .option('startingposition', 'LATEST')\
        .load()

#Format the data by schema
kinesisDF = kinesisDF \
  .selectExpr("cast (data as STRING) jsonData") \
  .select(from_json("jsonData", TripSchema).alias("trips")) \
  .select("trips.*")


kinesisDF = kinesisDF.withColumn('RecordedAtTime', from_unixtime(kinesisDF.RecordedAtTime/1000-2*60*60,'HH:mm:ss'))

kinesisDF = kinesisDF.withColumnRenamed("DatedVehicleJourneyRef","Short_Trip_Id")

#Enrich the stream information by adding the data of the schedules from mongo
kinesisDF = kinesisDF.join(spark_schedule,"Short_Trip_Id")

#Search for the station closest to the current bus location
#And enriching the streams with the data of the nearest station and its distance from the bus
def get_nearest_Point(bus_lon,bus_lat,trip_Stops):

    bus_lat = float(bus_lat)
    bus_lon = float(bus_lon)
    bus_loc = (bus_lon, bus_lat)
    bus_point = Point(bus_lon, bus_lat)

    trip_Stops_json = json.loads(trip_Stops)
    stops = pd.DataFrame.from_dict(trip_Stops_json)
    gdf_stops = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat))

    queried_geom, nearest_geom = nearest_points(bus_point, gdf_stops.geometry.unary_union)
    nearest_geom_loc = (nearest_geom.x, nearest_geom.y)

    meters_from_stop = int(hs.haversine(bus_loc, nearest_geom_loc, unit=Unit.METERS))

    arrival_time = gdf_stops.loc[gdf_stops.geometry == nearest_geom].values[0][0]
    stop_id = gdf_stops.loc[gdf_stops.geometry == nearest_geom].values[0][1]
    stop_sequence = gdf_stops.loc[gdf_stops.geometry == nearest_geom].values[0][2]

    return [meters_from_stop, arrival_time, stop_id, stop_sequence]

nearest_Point_schema = StructType([
    StructField("nearest_meters_from_stop", IntegerType(), False),
    StructField("nearest_arrival_time", StringType(), False),
    StructField("nearest_stop_id", IntegerType(), False),
    StructField("nearest_stop_sequence", IntegerType(), False)
])

get_nearest_Point_udf = udf(lambda row: get_nearest_Point(row[0], row[1], row[2]), nearest_Point_schema)

kinesisDF = kinesisDF.withColumn("nearest", get_nearest_Point_udf(struct([col('VehicleLocation_Longitude'), col('VehicleLocation_Latitude'),col('stop_time_json')])))\
	.select('*','nearest.*')

#Formatting a dataframe to kinesis format
kinesisDF = kinesisDF.select("Short_Trip_Id",to_json(struct("*")))
kinesisDF = kinesisDF.withColumnRenamed('structstojson(named_struct(NamePlaceholder(), unresolvedstar()))','json_data')

#Sending the data to Kinesis
def wrtietoKinesis(live_data):
    row_data = live_data['json_data']
    row_partitionKey = live_data['Short_Trip_Id']
    print(row_partitionKey, row_data)
    print(len(row_data))
    if len(row_data) > 0 :
        kinesis_client = boto3.client('kinesis', region_name="eu-north-1")
        kinesis_client.put_record(
            StreamName=stream_name_Sub,
            Data=row_data,
            PartitionKey=row_partitionKey)

kinesisDF.writeStream.foreach(wrtietoKinesis).start().awaitTermination()
