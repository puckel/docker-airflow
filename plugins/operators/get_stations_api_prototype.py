import requests
from pandas import DataFrame

	# Call REST API:
API_endpoint="https://environment.data.gov.uk/hydrology/id/stations.json?"
response = requests.get(API_endpoint)
stations = response.json()["items"]
stations_df = DataFrame.from_dict(stations)

columns_to_drop = ["easting", "northing", "notation", "type", "wiskiID", "RLOIid"]


stations_df.drop(columns=columns_to_drop, inplace=True)
