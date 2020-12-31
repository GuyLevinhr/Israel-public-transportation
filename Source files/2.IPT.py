import requests
import pandas as dp
import numpy as np
import time
import json
from datetime import datetime , timedelta
import boto3
import time

routes = ['9872','9857']

MaxRecordTime = datetime.today() - timedelta(hours=1)

STREAM_NAME = "TranOnline"
REGION = "eu-north-1"

kinesis_client = boto3.client('kinesis', region_name=REGION)

stream_url = "http://moran.mot.gov.il:110/Channels/HTTPChannel/SmQuery/2.8/json"
uaer_name = "XXXX123465"

live_params = {
    'Key': uaer_name,
    #'LineRef':'9872',
    #"MonitoringRef":"all",
    "MonitoringRef":"AllActiveTripsFilter",
    "StopVisitDetailLevel":"normal"
}


# In[ ]:


while True:
#####################################################################################################################
#####################################################################################################################

    json_live = requests.request("GET", stream_url, params=live_params)

    res_json = json_live.json()

#####################################################################################################################
#####################################################################################################################

    MonitoredStopVisit = res_json["Siri"]["ServiceDelivery"]["StopMonitoringDelivery"]

    stops = [stop for stops in [Monitor["MonitoredStopVisit"] for Monitor in MonitoredStopVisit] for stop in stops]

#####################################################################################################################
#####################################################################################################################

    lst =[
        {
        "RecordedAtTime":x["RecordedAtTime"],
        #"ItemIdentifier":x["ItemIdentifier"],
        #"MonitoringRef": x["MonitoringRef"],
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
        #"VehicleRef":x["MonitoredVehicleJourney"]["VehicleRef"],
        "Velocity":x["MonitoredVehicleJourney"]["Velocity"],
        #"StopPointRef":x["MonitoredVehicleJourney"]["MonitoredCall"]["StopPointRef"],
        #"Order":x["MonitoredVehicleJourney"]["MonitoredCall"]["Order"],
        #"ExpectedArrivalTime":x["MonitoredVehicleJourney"]["MonitoredCall"]["ExpectedArrivalTime"],
        #"DistanceFromStop":x["MonitoredVehicleJourney"]["MonitoredCall"]["DistanceFromStop"]
        } 
        for x in stops if x["MonitoredVehicleJourney"]["LineRef"] in routes]
        #if x["MonitoredVehicleJourney"]["FramedVehicleJourneyRef"]["DatedVehicleJourneyRef"] == "29669249"]

    df = dp.DataFrame(lst)
    
#####################################################################################################################
#####################################################################################################################

    live_data = df[df.groupby(['DatedVehicleJourneyRef'])['RecordedAtTime'].transform(max) == df['RecordedAtTime']]

    live_data.drop_duplicates(inplace = True)

    live_data["OriginAimedDepartureTime"] = live_data.OriginAimedDepartureTime.map(lambda x : x.split("T")[1].split('+')[0])
    live_data["RecordedAtTime"] = live_data.RecordedAtTime.map(lambda x : x.split("+")[0])
    live_data["RecordedAtTime"] = dp.to_datetime(live_data["RecordedAtTime"], format="%Y-%m-%dT%H:%M:%S")
    live_data = live_data.loc[live_data.RecordedAtTime > MaxRecordTime]

    if live_data.empty == False:
        MaxRecordTime = live_data.RecordedAtTime.max()
        #live_data.columns = map(str.lower, live_data.columns)
        #print(MaxRecordTime)
    
#live_data.sort_values(by=['DatedVehicleJourneyRef','RecordedAtTime'], ascending=[1,1])
#live_data.head()

#####################################################################################################################
#####################################################################################################################

    if live_data.empty == False:  
        for index,trip in live_data.iterrows():
            #print(trip)
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=trip.to_json(),
                PartitionKey="partitionkey")

    time.sleep(15)


# In[ ]:




