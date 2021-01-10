import boto3
import time
from datetime import datetime
import pymongo
import dns
import json

today = datetime.today().strftime("%Y-%m-%d")

MongoDbFreeCluster = "mongodb+srv://<UserName>:<Password>@trancluster.3sb01.mongodb.net/?retryWrites=true&w=majority"
MongoDbBICluster =   "mongodb+srv://<UserName>:<Password>@tranmongodbcluster.3sb01.mongodb.net/?retryWrites=true&w=majority"

MongoDb = MongoDbBICluster
client = pymongo.MongoClient(MongoDb)
db = client["TranDB"]

busAtStation_collection = db["BusAtStation"]

kinesisStream = boto3.client('kinesis', region_name="eu-north-1")
shard_id = 'shardId-000000000000' #we only have one shard
shard_it = kinesisStream.get_shard_iterator(StreamName ="tran.bus.in.station", ShardId = shard_id, ShardIteratorType="LATEST",Timestamp=datetime(2015, 1, 1))["ShardIterator"]

#Take the information from kinesis and save it in Mongo
while 1==1:
	out = kinesisStream.get_records(ShardIterator=shard_it, Limit=1)
	shard_it = out["NextShardIterator"]
	if len(out["Records"]) > 0:
		data = out["Records"][0]['Data']
		data_json = json.loads(data)
		data_json['Published_Line_Name'] = data_json.pop('PUBLISHEDLINENAME')
		data_json['Short_Trip_Id'] = data_json.pop('SHORTTRIPID')
		data_json['Eecorded_At_Time'] = data_json.pop('RECORDEDATTIME')
		data_json['Nearest_Arrival_Time'] = data_json.pop('NEARESTARRIVALTIME')
		data_json['VehicleLocation_Latitude'] = data_json.pop('VEHICLELOCATIONLATITUDE')
		data_json['VehicleLocation_Longitude'] = data_json.pop('VEHICLELOCATIONLONGITUDE')
		data_json['Velocity'] = data_json.pop('VELOCITY')
		data_json['Nearest_Stop_Id'] = data_json.pop('NEAREST_STOP_ID')
		data_json['Nearest_Stop_Sequence'] = data_json.pop('NEAREST_STOP_SEQUENCE')


		data_json['Eecorded_At_Time'] = datetime.strptime(today + ' ' + data_json['Eecorded_At_Time'], '%Y-%m-%d %H:%M:%S')
		data_json['Nearest_Arrival_Time'] = datetime.strptime(today + ' ' + data_json['Nearest_Arrival_Time'], '%Y-%m-%d %H:%M:%S')

		busAtStation_collection.insert_one(data_json)
	time.sleep(0.2)