import requests
from pandas import DataFrame
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import boto3

def create_sql_connection(database):
    """This function creates a SQLalchemy connection from some database information and returns it."""
    sql_connection = create_engine(
        StageStationsAPIOperator.sql_engine.format(
            user=database["user"],
            password=database["password"],
            database=database["database"],
        )
    )
    return sql_connection

class StageStationsAPIOperator(BaseOperator):
    ui_color = "#DD3581"

    sql_engine = "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}"

    @apply_defaults
    def __init__(
        self,
        API_endpoint="",
        observed_property="",
        stations_df={},
        target_database={},
        file_key="",
        limit=0,
        columns_to_drop=[],
        *args,
        **kwargs
    ):

        super(StageStationsAPIOperator, self).__init__(*args, **kwargs)
        self.API_endpoint = API_endpoint.format(observed_property=observed_property)
        self.observed_property = observed_property
        self.stations_df = stations_df
        self.target_database = target_database
        self.target_sql_connection = create_sql_connection(target_database)
        self.file_key = file_key
        self.limit = limit
        self.columns_to_drop = columns_to_drop

    def read_json(self):
        """This method reads a JSON from the API endpoint and creates a dataframe with the data retrieved"""
        try:
            response = requests.get(self.API_endpoint)
            stations = response.json()["items"]
            self.stations_df = DataFrame.from_dict(stations)
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to read JSON from the API"))
            raise ValueError

    def process_dataframe(self):
        """This method processes the raw dataframe obtained from the JSON, by adding the observed property column, by
        dropping the unecesary columns and sorting the rest. This creates a consistency throughout the different
        observedProperties, with the same columns"""
        try:
            self.stations_df["observedProperty"] = self.observed_property
            self.stations_df.drop(columns=self.columns_to_drop, inplace=True)
            self.stations_df = self.stations_df.reindex(
                sorted(self.stations_df.columns), axis=1
            )
            self.stations_df.dropna(axis="index", subset=["stationReference"], inplace=True)
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to process the dataframe"))
            raise ValueError

    def write_to_local_sql(self):
        """This function loads the final clean dataframe to a local SQL database, for later use."""
        try:
            self.stations_df.head(0).to_sql(
                name=self.target_database["table"],
                con=self.target_sql_connection,
                if_exists="append",
                index=False,
            )

            self.log.info(print(self.stations_df.head(0)))
            try:
                conn = self.target_sql_connection.raw_connection()
                cur = conn.cursor()
                output = StringIO()
                self.stations_df.to_csv(output, sep="\t", header=False, index=False)
                output.seek(0)
                cur.copy_from(output, self.target_database["table"], null="", sep="\t")
                conn.commit()
            except Exception as e:
                self.log.info(print(e))
                self.log.info(print("Failure to write to local database"))

        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to write to local database"))
            raise ValueError

    def save_to_s3(self):
        """This method loads the parquet file stored within the container to an S3 bucket"""
        try:
            hook = S3Hook(aws_conn_id=self.aws_conn_id)
            self.log.info(print(hook))
            credentials = hook.get_credentials()
            bucket = Variable.get("s3_bucket")
            client = boto3.client(
                "s3",
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

    def save_locally(self):
        """This function saves the final clean dataframe as a parquet file within the container, prior to its loading
        to s3"""
        try:
            self.stations_df.to_parquet(
                fname=self.file_key,
                partition_cols=["observedProperty"],
                compression="gzip",
            )
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to save parquet file locally"))
            raise ValueError

    def execute(self, context):

        self.file_key = (
            self.target_database["table"]
            + "/"
            + self.observed_property
            + ".parquet.gzip"
        )
        self.read_json()
        self.process_dataframe()
        self.write_to_local_sql()

