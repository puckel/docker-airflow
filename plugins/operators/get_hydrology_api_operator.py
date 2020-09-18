import requests
from pandas import DataFrame
from pandas import read_sql_query
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GetHydrologyAPIOperator(BaseOperator):
    ui_color = '#358140'


    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 *args, **kwargs):

        super(GetHydrologyAPIOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):


        #DATA FROM THE COMPOSE FILE
        origin_database = "airflow"
        origin_table = "stations"
        origin_user = "airflow"
        origin_password = "airflow"

        origin_sql_connection = create_engine("postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=origin_user, password=origin_password, database=origin_database))

        # DATA FROM THE COMPOSE FILE
        destination_database = "airflow"
        destination_table = "measures"
        destination_user = "airflow"
        destination_password = "airflow"

        destination_sql_connection = create_engine(
            "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=destination_user, password=destination_password,
                                                                                       database=destination_database))

        station_reference_df = read_sql_query('SELECT label, "stationReference" FROM {table};'.format(table=origin_table), con=origin_sql_connection)
        date = '2020-09-01'
        self.log.info(print(station_reference_df))
        for station_reference in station_reference_df["stationReference"]:
            self.log.info(print(station_reference))
            # Call REST API:
            API_endpoint = "https://environment.data.gov.uk/hydrology/data/readings.json?period={period}&station.stationReference={station_reference}&date={date}".format(period=900, station_reference=station_reference, date=date)

            try:
                response = requests.get(API_endpoint)
                measures = response.json()["items"]
                measures_df = DataFrame.from_dict(measures)
                columns_to_drop = ["measure", "date"]
                self.log.info(print(measures_df))
                measures_df.drop(columns=columns_to_drop, inplace=True)
                measures_df["stationReference"] = station_reference
                measures_df.reset_index(drop=True, inplace=True)
                self.log.info(print(measures_df))
                measures_df.head(0).to_sql(name=destination_table, con=destination_sql_connection, if_exists='append', index=False)

                conn = destination_sql_connection.raw_connection()
                cur = conn.cursor()
                output = StringIO()
                measures_df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, destination_table, null="", sep='\t')
                conn.commit()
            except:
                self.log.info(print("station may not have this type of measure"))
