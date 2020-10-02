import requests
from pandas import DataFrame
from pandas import read_sql_query
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import boto3
import datetime


class GetHydrologyAPIOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ['date']

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 source_database={},
                 target_database={},
                 origin_sql_connection="",
                 destination_sql_connection="",
                 observed_property="",
                 measures_df=DataFrame([]).empty,
                 general_API_endpoint="",
                 columns_to_drop=[],
                 date="",
                 aws_conn_id='aws_credentials',
                 file_key="",
                 *args, **kwargs):

        super(GetHydrologyAPIOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_database = source_database
        self.target_database = target_database
        self.observed_property = observed_property
        self.date = date
        self.columns_to_drop = columns_to_drop
        self.aws_conn_id = aws_conn_id
        self.file_key = file_key
        self.origin_sql_connection = origin_sql_connection
        self.destination_sql_connection = destination_sql_connection
        self.measures_df = measures_df
        self.general_API_endpoint = general_API_endpoint

    def create_connections(self):
        self.origin_sql_connection = create_engine(
            "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=self.source_database['user'],
                                                                                      password=self.source_database[
                                                                                          'password'],
                                                                                      database=self.source_database[
                                                                                          'database']
                                                                                      )
        )

        self.destination_sql_connection = create_engine(
            "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=self.target_database['user'],
                                                                                      password=self.target_database[
                                                                                          'password'],
                                                                                      database=self.target_database[
                                                                                          'database']
                                                                                      )
        )

    def save_locally(self):
        try:
            self.measures_df.to_parquet(
                fname=self.file_key,
                partition_cols=["date", "stationReference"],
                compression='gzip'
            )
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to save parquet file locally"))
            raise ValueError

    def save_to_s3(self):
        try:
            hook = S3Hook(aws_conn_id=self.aws_conn_id)
            self.log.info(print(hook))
            credentials = hook.get_credentials()
            bucket = Variable.get('s3_bucket')
            client = boto3.client(
                's3',
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
            )
            client.put_object(
                Bucket=bucket,
                Key=self.file_key,
                Body=self.file_key,
            )
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to save parquet file in s3"))
            raise ValueError

    def date_str_to_dateTime(self):
        date_time_date = datetime.datetime.strptime(self.date, '%Y-%m-%d')
        return date_time_date

    def datetime_generator(self):
        """Generates a string with all the datetime values with which to call the API"""
        base_string = "dateTime="
        final_string = ""
        date_time_date = self.date_str_to_dateTime()
        time_delta = datetime.timedelta(minutes=15)
        start_time = datetime.datetime(year=date_time_date.year, month=date_time_date.month, day=date_time_date.day, hour=0, minute=0, second=0, microsecond=0)
        minute_slots = list(range(0, 96))
        for i in range(0, 96):
            minute_slots[i] = minute_slots[i] * time_delta + start_time
            final_string = final_string + base_string + str(minute_slots[i].date()) + "T" + str(
                minute_slots[i].time()) + "Z&"
        return final_string

    def get_api_endpoint(self, station_reference):
        if self.observed_property == "waterFlow":
            API_endpoint = self.general_API_endpoint.format(
                period=900, station_reference=station_reference, date=self.date
            )
        else:
            API_endpoint = self.general_API_endpoint.format(station_reference=station_reference,
                                                            observed_property=self.observed_property
                                                            ) + self.datetime_generator()
        return API_endpoint

    def read_json(self, station_reference):
        try:
            API_endpoint = self.get_api_endpoint(station_reference)
            self.log.info(print(API_endpoint))
            response = requests.get(API_endpoint)
            measures = response.json()["items"]
            self.log.info(print(measures))
            self.measures_df = DataFrame.from_dict(measures)
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to read JSON from the API"))
            raise ValueError

    def process_dataframe(self, station_reference, lat, long):
        try:
            self.log.info(print(self.measures_df.head(0)))
            if self.observed_property!="waterFlow":
                self.measures_df["date"] = self.date_str_to_dateTime()
            else: pass
            self.measures_df.drop(columns=self.columns_to_drop, inplace=True)
            self.measures_df["stationReference"] = station_reference
            self.measures_df["lat"] = lat
            self.measures_df["long"] = long
            self.measures_df["observedProperty"] = self.observed_property
            self.measures_df.reset_index(drop=True, inplace=True)
            self.measures_df = self.measures_df.reindex(sorted(self.measures_df.columns), axis=1)
            self.log.info(print(self.measures_df))
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to process the dataframe"))
            raise ValueError

    def write_to_local_sql(self):
        try:
            self.log.info(print(self.measures_df.head()))
            self.measures_df.head(0).to_sql(name=self.target_database['table'], con=self.destination_sql_connection,
                                            if_exists='append', index=False)
            try:
                self.destination_sql_connection.execute(
                    """ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "dateTime";""".format(table=self.target_database["table"]))
                self.destination_sql_connection.execute(
                    """ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "observedProperty";""".format(
                        table=self.target_database["table"]))
                self.destination_sql_connection.execute(
                    """ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "stationReference";""".format(
                        table=self.target_database["table"]))
                self.destination_sql_connection.execute("""ALTER TABLE {table} ADD PRIMARY KEY ("stationReference", 
                "observedProperty", "dateTime");""".format(table=self.target_database["table"]))
            except Exception as e:
                self.log.info(print(e))
                self.log.info(print("Primary key restriction already exists"))

            try:
                conn = self.destination_sql_connection.raw_connection()
                cur = conn.cursor()
                output = StringIO()
                self.measures_df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, self.target_database['table'], null="", sep='\t')
                conn.commit()
            except Exception as e:
                self.log.info(print(e))
                self.log.info(print("Failure to write to local database"))

        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to write to local database"))
            raise ValueError

    def execute(self, context):

        self.create_connections()

        station_reference_df = read_sql_query(
            'SELECT "stationReference", lat, long FROM {table};'.format(
                table=self.source_database["table"]), con=self.origin_sql_connection
        )
        # label,
        for station_reference, lat, long in zip(station_reference_df["stationReference"], station_reference_df["lat"],
                                                station_reference_df["long"]):

            try:
                self.read_json(station_reference=station_reference)
                self.process_dataframe(station_reference=station_reference, lat=lat, long=long)
            except:
                self.log.info(print("Station may not have this type of measure"))
                continue
            self.file_key = self.target_database['table'] + "/" + self.observed_property + "/" + str(self.date) + "/" +\
                            str(station_reference) + ".parquet.gzip"

            self.write_to_local_sql()
            self.save_locally()
            # self.save_to_s3()
