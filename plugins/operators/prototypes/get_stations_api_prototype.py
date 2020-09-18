import requests
from pandas import DataFrame
# from pyspark.sql import SparkSession
import os

# Set spark environments
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

# spark = SparkSession \
#     .builder \
#     .appName("Get Stations From API") \
#     .getOrCreate()
#
# sc = spark.sparkContext

	# Call REST API:
API_endpoint="https://environment.data.gov.uk/hydrology/id/stations.json?"
response = requests.get(API_endpoint)
stations = response.json()["items"]

# stations_df = spark.read.json(sc.parallelize(stations))
stations_df = DataFrame.from_dict(stations)

columns_to_drop = ["easting", "northing", "notation", "type", "wiskiID", "RLOIid"]

stations_df.drop(columns=columns_to_drop, inplace=True)

# stations_df[stations_df.columns] = stations_df[stations_df.columns].astype('str')

print(stations_df.head(0))
#print(stations_df.columns)

database = ""
table = ""
user = ""
password = ""
