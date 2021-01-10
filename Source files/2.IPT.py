import requests
import pandas as dp
import numpy as np
import time
import json
from datetime import datetime , timedelta
import boto3
import time

#Selected routes
routes = ['9872','9857']

MaxRecordTime = datetime.today() - timedelta(hours=1)

STREAM_NAME = "TranOnline"
REGION = "eu-north-1"

kinesis_client = boto3.client('kinesis', region_name=REGION)

stream_url = "http://moran.mot.gov.il:110/Channels/HTTPChannel/SmQuery/2.8/json"
uaer_name = "XXXX123465"

live_params = {
    'Key': uaer_name,
    "MonitoringRef":"AllActiveTripsFilter",
    "StopVisitDetailLevel":"normal"
}


while True:
	#request date from IPT
    json_live = requests.request("GET", stream_url, params=live_params)

	#Get Json
    res_json = json_live.json()

	#Take out the data from the hierarchy json 
    MonitoredStopVisit = res_json["Siri"]["ServiceDelivery"]["StopMonitoringDelivery"]
    stops = [stop for stops in [Monitor["MonitoredStopVisit"] for Monitor in MonitoredStopVisit] for stop in stops]

    lst =[
        {
        "RecordedAtTime":x["RecordedAtTime"],
        "LineRef":x["MonitoredVehicleJourney"]["LineRef"],
        "DirectionRef":x["MonitoredVehicleJourney"]["DirectionRef"],
        "DatedVehicleJourneyRef":x["MonitoredVehicleJourney"]["FramedVehicleJourneyRef"]["DatedVehicleJourneyRef"],
        "DataFrameRef":x["MonitoredVehicleJourney"]["FramedVehicleJourneyRef"]["DataFrameRef"],
        "PublishedLineName":x["MonitoredVehicleJourney"]["PublishedLineName"],
        "OperatorRef":x["MonitoredVehicleJourney"]["OperatorRef"],
        "DestinationRef":x["MonitoredVehicleJourney"]["DestinationRef"],
        "OriginAimedDepartureTime": x["MonitoredVehicleJourney"]["OriginAimedDepartureTime"],
        "VehicleLocation_Longitude":x["MonitoredVehicleJourney"]["VehicleLocation"]["Longitude"],
        "VehicleLocation_Latitude": x["MonitoredVehicleJourney"]["VehicleLocation"]["Latitude"],
        "Velocity":x["MonitoredVehicleJourney"]["Velocity"],
        } 
        for x in stops if x["MonitoredVehicleJourney"]["LineRef"] in routes]

    df = dp.DataFrame(lst)
    

	#Take only the most up-to-date information
    live_data = df[df.groupby(['DatedVehicleJourneyRef'])['RecordedAtTime'].transform(max) == df['RecordedAtTime']]

    live_data.drop_duplicates(inplace = True)

	#Take and format the update time
    live_data["OriginAimedDepartureTime"] = live_data.OriginAimedDepartureTime.map(lambda x : x.split("T")[1].split('+')[0])
    live_data["RecordedAtTime"] = live_data.RecordedAtTime.map(lambda x : x.split("+")[0])
    live_data["RecordedAtTime"] = dp.to_datetime(live_data["RecordedAtTime"], format="%Y-%m-%dT%H:%M:%S")
    live_data = live_data.loc[live_data.RecordedAtTime > MaxRecordTime]

    if live_data.empty == False:
        MaxRecordTime = live_data.RecordedAtTime.max()

	#Send the information to kinesis
    if live_data.empty == False:  
        for index,trip in live_data.iterrows():
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=trip.to_json(),
                PartitionKey="partitionkey")

    time.sleep(15)





