import requests
from pandas import DataFrame
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import boto3


class GetStationsAPIOperator(BaseOperator):
    ui_color = "#358140"

    sql_engine = "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}"

    @apply_defaults
    def __init__(
        self,
        aws_conn_id="",
        API_endpoint="",
        observed_property="",
        stations_df={},
        target_database={},
        file_key="",
        columns_to_drop=[],
        *args,
        **kwargs
    ):

        super(GetStationsAPIOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.API_endpoint = API_endpoint.format(observed_property=observed_property)
        self.observed_property = observed_property
        self.stations_df = stations_df
        self.target_database = target_database
        self.file_key = file_key
        self.columns_to_drop = columns_to_drop

    def read_json(self):
        try:
            response = requests.get(self.API_endpoint)
            stations = response.json()["items"]
            self.stations_df = DataFrame.from_dict(stations)
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to read JSON from the API"))
            raise ValueError

    def process_dataframe(self):
        try:
            self.stations_df["observedProperty"] = self.observed_property
            self.stations_df.drop(columns=self.columns_to_drop, inplace=True)
            self.stations_df = self.stations_df.reindex(
                sorted(self.stations_df.columns), axis=1
            )
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to process the dataframe"))
            raise ValueError

    def write_to_local_sql(self):
        try:
            sql_connection = create_engine(
                GetStationsAPIOperator.sql_engine.format(
                    user=self.target_database["user"],
                    password=self.target_database["password"],
                    database=self.target_database["database"],
                )
            )

            self.stations_df.head(0).to_sql(
                name=self.target_database["table"],
                con=sql_connection,
                if_exists="append",
                index=False,
            )
            try:
                sql_connection.execute(
                    """ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "stationReference";""".format(
                        table=self.target_database["table"]
                    )
                )  # if NOT exists (select constraint_name from information_schema.table_constraints where table_name = "{table}" and constraint_type = 'PRIMARY KEY') then end if;
                sql_connection.execute(
                    """ALTER TABLE {table} ADD PRIMARY KEY ("stationReference");""".format(
                        table=self.target_database["table"]
                    )
                )
            except Exception as e:
                self.log.info(print(e))
                self.log.info(print("Primary key restriction already exists"))

            self.log.info(print(self.stations_df.head(0)))
            try:
                conn = sql_connection.raw_connection()
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
        self.save_locally()
        self.save_to_s3()
