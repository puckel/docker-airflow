import requests
from pandas import DataFrame
from pandas import read_sql_query
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class GetHydrologyAPIOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ['date']

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 origin_database="airflow",
                 origin_table="stations",
                 origin_user="airflow",
                 origin_password="airflow",
                 destination_database="airflow",
                 destination_table="measures",
                 destination_user="airflow",
                 destination_password="airflow",
                 physical_quantity="waterFlow",
                 date="",
                 aws_conn_id='aws_credentials',
                 s3_key="",
                 *args, **kwargs):

        super(GetHydrologyAPIOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.origin_database = origin_database
        self.origin_table = origin_table
        self.origin_user = origin_user
        self.origin_password = origin_password
        self.destination_database = destination_database
        self.destination_table = destination_table
        self.destination_user = destination_user
        self.destination_password = destination_password
        self.physical_quantity = physical_quantity
        self.date = date
        self.aws_conn_id = aws_conn_id
        self.s3_key = s3_key

    def execute(self, context):

        origin_sql_connection = create_engine(
            "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=self.origin_user,
                                                                                      password=self.origin_password,
                                                                                      database=self.origin_database
                                                                                      )
        )

        # DATA FROM THE COMPOSE FILE
        destination_table = "measures"

        destination_sql_connection = create_engine(
            "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=self.destination_user,
                                                                                      password=self.destination_password,
                                                                                      database=self.destination_database
                                                                                      )
        )

        station_reference_df = read_sql_query(
            'SELECT label, "stationReference", lat, long FROM {table};'.format(
                table=self.origin_table), con=origin_sql_connection
        )


        self.log.info(print(station_reference_df))
        for station_reference, lat, long in zip(station_reference_df["stationReference"], station_reference_df["lat"],
                                                station_reference_df["long"]):
            self.log.info(print(station_reference))
            # Call REST API:
            API_endpoint = "https://environment.data.gov.uk/hydrology/data/readings.json?period={period}&station.stationReference={station_reference}&date={date}".format(period=900, station_reference=station_reference, date=self.date)
            try:
                response = requests.get(API_endpoint)
                measures = response.json()["items"]
                measures_df = DataFrame.from_dict(measures)
                columns_to_drop = ["measure"]#, "date"]
                self.log.info(print(measures_df))
                measures_df.drop(columns=columns_to_drop, inplace=True)
                measures_df["stationReference"] = station_reference
                measures_df["lat"] = lat
                measures_df["long"] = long
                measures_df["physicalQuantity"] = self.physical_quantity
                measures_df.reset_index(drop=True, inplace=True)
                self.log.info(print(measures_df))
                measures_df.head(0).to_sql(name=self.destination_table, con=destination_sql_connection,
                                           if_exists='append', index=False)

                conn = destination_sql_connection.raw_connection()
                cur = conn.cursor()
                output = StringIO()
                measures_df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, self.destination_table, null="", sep='\t')
                conn.commit()
            except:
                self.log.info(print("station may not have this type of measure"))

            try:
                hook = S3Hook(aws_conn_id=self.aws_conn_id)
                credentials = hook.get_credentials()
                bucket = Variable.get('s3_bucket')
                rendered_key = self.s3_key.format(**context)
                s3_path="s3://{}/{}".format(bucket, rendered_key)
                measures_df.write.partitionBy("date", "stationReference").parquet(s3_path)
            except:
                self.log.info(print("Loading to AWS S3 failed"))