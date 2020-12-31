#!/bin/python3.6
#pip install geopandas
import os
import glob
import shutil
import urllib.request as request
from contextlib import closing
import zipfile
import pandas as pd
import numpy as np
import datetime
from datetime import date, timedelta
from xml.etree.ElementTree import parse
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
#import geopandas
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

#pd.set_option('max_rows', 1000)
#pd.set_option('max_colwidth', 400)

download_path = '/home/naya/tranProjects/download/'
download_path_Zip = download_path + 'zip/'
download_path_unZip = download_path + 'unzip/'

fts_server = 'ftp://199.203.58.18/'
tran_zip = 'israel-public-transportation.zip'
zone_zip = 'zones.zip'
trip_zip = 'TripIdToDate.zip'

MongoDbFreeCluster = "mongodb+srv://<UserName>:<Password>@trancluster.3sb01.mongodb.net/?retryWrites=true&w=majority"
MongoDbBICluster =   "mongodb+srv://<UserName>:<Password>@tranmongodbcluster.3sb01.mongodb.net/?retryWrites=true&w=majority"

MongoDb = MongoDbBICluster

today = datetime.datetime.today()

df_agency = []
df_routes = []
df_stop_times = []
df_stops = []
df_trips = []
df_calendar = []
df_shapes = []
df_TripIdToDate = []
document_zones  = []
dow = 0
TelAviv_Polygon = []
trip_first_stops = []
df_trips_TA = []
df_data = []

def set_folders():
    # Delete all Folders
    shutil.rmtree(download_path,ignore_errors=True)

    #Create Folder
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    if not os.path.exists(download_path_Zip):
        os.makedirs(download_path_Zip)

    if not os.path.exists(download_path_unZip):
        os.makedirs(download_path_unZip)

def download_gtfs_files():
    #Download zone zip
    with closing(request.urlopen(fts_server+zone_zip)) as r:
        with open(download_path_Zip+zone_zip, 'wb') as f:
            shutil.copyfileobj(r, f)

    #Downlod tran zip
    with closing(request.urlopen(fts_server+tran_zip)) as r:
        with open(download_path_Zip+tran_zip, 'wb') as f:
            shutil.copyfileobj(r, f)

    #Downlod trip zip
    with closing(request.urlopen(fts_server+trip_zip)) as r:
        with open(download_path_Zip+trip_zip, 'wb') as f:
            shutil.copyfileobj(r, f)

def unzip_gfts_file():
    #unZip
    with zipfile.ZipFile(download_path_Zip+zone_zip, 'r') as zip_ref:
        zip_ref.extractall(download_path_unZip)

    shutil.move(download_path_unZip+'zones/zones.kml', download_path_unZip)

    with zipfile.ZipFile(download_path_Zip+tran_zip, 'r') as zip_ref:
        zip_ref.extractall(download_path_unZip)

    with zipfile.ZipFile(download_path_Zip+trip_zip, 'r') as zip_ref:
        zip_ref.extractall(download_path_unZip)

def delete_unnecessary_files():
    #Remove Files
    shutil.rmtree(download_path_unZip+'/zones/',ignore_errors=True,onerror=None)

    if os.path.exists(download_path_unZip+'fare_attributes.txt'):
      os.remove(download_path_unZip+'fare_attributes.txt')

    if os.path.exists(download_path_unZip+'fare_rules.txt'):
      os.remove(download_path_unZip+'fare_rules.txt')

    if os.path.exists(download_path_unZip+'translations.txt'):
      os.remove(download_path_unZip+'translations.txt')

def Load_files_Clean_Data_Filter_and_Save_to_mongoDB():
    # Load all files to Data Frame
    df_agency = pd.read_csv(download_path_unZip + "agency.txt",
                            index_col='agency_id')

    df_routes = pd.read_csv(download_path_unZip + "routes.txt",
                            index_col='route_id')

    df_stop_times = pd.read_csv(download_path_unZip + "stop_times.txt",
                                index_col='trip_id')

    df_stops = pd.read_csv(download_path_unZip + "stops.txt",
                           index_col='stop_id')

    df_trips = pd.read_csv(download_path_unZip + "trips.txt",
                           index_col=['route_id', 'service_id', 'trip_id'])

    df_calendar = pd.read_csv(download_path_unZip + "calendar.txt",
                              index_col='service_id')

    df_shapes = pd.read_csv(download_path_unZip + "shapes.txt")
    df_shapes.set_index("shape_id", inplace=True)

    #load trip schedule file
    df_TripIdToDate = pd.read_csv(download_path_unZip + "TripIdToDate.txt",skiprows = 1,header = None,index_col=None)

    document_zones = parse(download_path_unZip + "zones.kml")

	#Filter_the_services_work_today
    dow = today.weekday() + 2
    if dow == 8:
        dow = 1

    df_calendar['start_date'] = pd.to_datetime(df_calendar['start_date'], format="%Y%m%d")
    df_calendar['end_date'] = pd.to_datetime(df_calendar['end_date'], format="%Y%m%d")

    df_calendar = df_calendar.loc[(df_calendar.start_date <= today) & (df_calendar.end_date >= today) &
                                          (
                                            ((dow == 1) & (df_calendar.sunday == 1))
                                          | ((dow == 2) & (df_calendar.monday == 1))
                                          | ((dow == 3) & (df_calendar.tuesday == 1))
                                          | ((dow == 4) & (df_calendar.wednesday == 1))
                                          | ((dow == 5) & (df_calendar.thursday == 1))
                                          | ((dow == 6) & (df_calendar.friday == 1))
                                          | ((dow == 7) & (df_calendar.saturday == 1))
                                          )]

	#Get_Dan_and_Eged_agency
    df_agency.drop(["agency_url","agency_timezone","agency_lang","agency_phone","agency_fare_url"], axis=1, inplace=True)

    df_agency = df_agency.loc[(df_agency["agency_name"] == "???") | (df_agency["agency_name"] == "??")]

	#Get_polygon_of_Tel_Aviv
    for item in document_zones.iterfind('Document/Folder/Placemark'):
        s_name = item.findtext("name") if item is not None else None

        if s_name == '5':  # 5 : Tel Aviv
            s_coordinates = item.findtext("Polygon/outerBoundaryIs/LinearRing/coordinates") if item is not None else None
            s_coordinates = s_coordinates.split(',0 ')
            coordinates = [(float(x.split(",")[0]), float(x.split(",")[1])) for x in s_coordinates]
            break;

    TelAviv_Polygon = Polygon(coordinates)


	#Clean_routes_file_data
    df_routes["OfficeLineId"] = df_routes.route_desc.map(lambda x : x.split("-")[0])

    df_routes["Start_Stop"] = df_routes.route_long_name.map(lambda x : x.split("<->")[0].split("-")[0])
    df_routes["Start_City"] = df_routes.route_long_name.map(lambda x : x.split("<->")[0].split("-")[1])

    df_routes["End_Stop"] = df_routes.route_long_name.map(lambda x : x.split("<->")[1].split("-")[0])
    df_routes["End_City"] = df_routes.route_long_name.map(lambda x : x.split("<->")[1].split("-")[1])

    df_routes.drop(["route_desc","route_color"], axis=1, inplace=True)

    #Clean stop_times file data
    df_stop_times.drop(["pickup_type","drop_off_type","shape_dist_traveled"], axis=1, inplace=True)

	#Filer_routes_by_agency_id
    df_routes = df_routes[df_routes.agency_id.isin(df_agency.index.values)]
    df_routes = df_routes[df_routes.route_type == 3]

	#filter_trip_of_today
    df_trips["DepartureDate"] = today.strftime("%Y-%m-%d")

    df_trips = df_trips[df_trips.index.isin(df_calendar.index,level="service_id")]
    df_trips["DayInWeek"] = dow

	#filter_trip_thet_start_from_Tal_Aviv_Polygon
    #Add column of last stop time
    df_stop_times["last_stop_time"] = df_stop_times.groupby(['trip_id'], sort=False)['departure_time'].max()

    #Find the first stop of each trip
    trip_first_stops = df_stop_times.loc[df_stop_times.stop_sequence == 1]

    #Convert lat&lon to Point in geo pandas data frame
    geo_df = geopandas.GeoDataFrame(df_stops, geometry=geopandas.points_from_xy(df_stops.stop_lon, df_stops.stop_lat))

    #filter the stop then in Tel Aviv
    Stop_In_Tel_Aviv = geo_df[geo_df.within(TelAviv_Polygon)]

    #filter the first stop thet start in Tel aviv
    trip_with_first_stops_In_Tel_Aviv = trip_first_stops.loc[trip_first_stops.stop_id.isin(Stop_In_Tel_Aviv.index)]

    #Get all trip thet start in Tel Aviv
    df_trips_TA = df_trips[df_trips.index.isin(trip_with_first_stops_In_Tel_Aviv.index,level="trip_id")]

    # Set Columns
    df_trips_TA.reset_index(inplace=True)
    df_trips_TA.drop('service_id', axis='columns', inplace=True)
    df_trips_TA.set_index('trip_id', inplace=True)

    trip_first_stops.rename(columns = {"arrival_time":"start_time"}, inplace = True)
    trip_first_stops["start_time"] = trip_first_stops["start_time"].apply(lambda st : st[:5])

	#clean_TripIdToDate
    df_TripIdToDate.drop(df_TripIdToDate.columns[9], axis=1, inplace=True)

    #Set column names
    df_TripIdToDate.columns = ["LineDetailRecordId","OfficeLineId","Direction","LineAlternative","FromDate","ToDate","TripId","DayInWeek","DepartureTime"]
    df_TripIdToDate.rename(columns = {'TripId':'trip_id_of_agency'}, inplace = True)
    df_TripIdToDate.rename(columns = {'LineDetailRecordId':'route_id'}, inplace = True)
    df_TripIdToDate.rename(columns = {'DepartureTime':'start_time'}, inplace = True)

    #Set index
    df_TripIdToDate.set_index("route_id", inplace = True)

	#merge_all_DataFrame
    df_data = pd.merge(df_trips_TA, df_routes,  on='route_id',right_index=True, how='inner')

    df_data = pd.merge(df_data, trip_first_stops,  on='trip_id',right_index=True, how='inner')

    df_data = pd.merge(df_data, df_agency,  on='agency_id',right_index=True, how='inner')

    df_data = pd.merge(df_data, df_TripIdToDate,  on=['route_id',"start_time","DayInWeek"],right_index=True, how='inner')

    #Set Number_of_passengers at all trips
    df_data["Number_of_passengers"] = df_data.apply(lambda t : 34 if t.agency_name == "??" else 45, axis=1)

    df_data.drop(["direction_id","OfficeLineId_y","LineAlternative","FromDate","ToDate","departure_time","stop_id","stop_sequence"], axis=1, inplace=True) # ,"Direction"
    df_data.rename(columns = {'trip_id_Clean':'trip_id_short',"OfficeLineId_x":"OfficeLineId"}, inplace = True)

	#merge_stopdate_to_all_DataFrame
    df_stop_times = df_stop_times.loc[df_stop_times.index.isin(df_data.index)]
    df_stop_times = pd.merge(df_stop_times, df_stops,  on='stop_id',right_index=True, how='inner')

    df_stop_times.drop(["departure_time","stop_name","stop_desc","location_type","zone_id","geometry","parent_station"], axis=1, inplace=True) # ,"Direction"

    df_stop_times_json = df_stop_times.groupby('trip_id').apply(lambda x: x.to_json(orient='records')).to_frame(name = 'stop_time_json')

    df_data = pd.merge(df_data, df_stop_times_json,  on='trip_id',right_index=True, how='inner')

	#Filter_shapes_of_Tel_Aviv_trips_only():
    df_shapes = df_shapes.loc[df_shapes.index.isin(df_data.shape_id)]
	df_shapes["date"] = today.strftime("%Y-%m-%d")
	
	#MongoDB
	client = pymongo.MongoClient(MongoDb)
	db = client["TranDB"]

	# Collection name  
	Trips_collection = db["Trips"]  
	shapes_collection = db["Shapes"]

	df_shapes.reset_index(inplace=True)
	shapes_dict = df_shapes.to_dict("records")

	shapes_collection.delete_many({ "date": today.strftime("%Y-%m-%d") })
	# Insert collection
	shapes_collection.insert_many(shapes_dict)

	df_data.reset_index(inplace=True)
	data_dict = df_data.to_dict("records")

	Trips_collection.delete_many({ "DepartureDate": today.strftime("%Y-%m-%d") })

Trips_collection.insert_many(data_dict)

dag = DAG('night_task', description='night process',
           schedule_interval='0 2 * * *',#02:00 every day
           start_date=datetime.datetime(2020, 11, 7), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

set_folders_operator = PythonOperator(task_id='set_folders', python_callable=set_folders, dag=dag)

download_gtfs_files_operator = PythonOperator(task_id='download_gtfs_files', python_callable=download_gtfs_files, dag=dag)

unzip_gfts_file_operator = PythonOperator(task_id='unzip_gfts_file', python_callable=unzip_gfts_file, dag=dag)

delete_unnecessary_files_operator = PythonOperator(task_id='delete_unnecessary_files', python_callable=delete_unnecessary_files, dag=dag)

Load_files_Clean_Data_Filter_and_Save_to_mongoDB_operator = PythonOperator(task_id='Load_files_Clean_Data_Filter_and_Save_to_mongoDB', python_callable=Load_files_Clean_Data_Filter_and_Save_to_mongoDB, dag=dag)


dummy_operator >> set_folders_operator >> download_gtfs_files_operator >> unzip_gfts_file_operator >> delete_unnecessary_files_operator >> Load_files_Clean_Data_Filter_and_Save_to_mongoDB_operator