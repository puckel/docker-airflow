import requests
import os
from pandas import DataFrame
from sqlalchemy import  create_engine
# from pyspark.sql import SparkSession
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GetStationsAPIOperator(BaseOperator):
    ui_color = '#358140'
    
    # template_fields = ("s3_key",) # Allows to load timestamped files from S3 based on execution time and run backfills
    
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
            IGNOREHEADER {}

            """
#             DELIMITER '{}'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 # aws_credentials_id="",
                 # table="",
                 # s3_bucket="",
                 # s3_key="",
                 # json_path="",
                 # delimiter=",",
                 # ignore_header=1,
                 *args, **kwargs):

        super(GetStationsAPIOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        # self.aws_credentials_id=aws_credentials_id
        # self.table=table
        # self.s3_bucket=s3_bucket
        # self.s3_key=s3_key
        # self.json_path=json_path
        # self.delimiter=delimiter
        # self.ignore_header=ignore_header

    def execute(self, context):

        # S3_hook = S3Hook(self.aws_credentials_id)
        # credentials = S3_hook.get_credentials()
        # redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        #
        # self.log.info("Clearing data from destination Redshift table")
        # redshift.run("DELETE FROM {}".format(self.table))
        #
        # self.log.info("Copying data from S3 to Redshift")
        # rendered_key = self.s3_key.format(**context)
        # s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        #
        # formatted_sql = GetStationsAPIOperator.copy_sql.format(
        #     self.table,
        #     s3_path,
        #     credentials.access_key,
        #     credentials.secret_key,
        #     self.json_path,
        #     self.ignore_header,
        #     self.delimiter
        # )
        #
        # redshift.run(formatted_sql)
        # Call REST API:
        # Set spark environments
        # os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
        # os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
        #
        # spark = SparkSession \
        #     .builder \
        #     .appName("Get Stations From API") \
        #     .getOrCreate()
        #
        # sc = spark.sparkContext

        # Call REST API:
        API_endpoint = "https://environment.data.gov.uk/hydrology/id/stations.json?"
        response = requests.get(API_endpoint)
        stations = response.json()["items"]

        # stations_df = spark.read.json(sc.parallelize(stations))
        stations_df = DataFrame.from_dict(stations)

        columns_to_drop = ["easting", "northing", "notation", "type", "wiskiID", "RLOIid"]

        stations_df.drop(columns=columns_to_drop, inplace=True)

        #DATA FROM THE COMPOSE FILE
        database = "airflow"
        table = "stations"
        user = "airflow"
        password = "airflow"

        sql_connection = create_engine(f"postgresql://{user}:{password}@postgres:5432/{database}".format(user=user, password=password, database=database))

        stations_df.to_sql(name=table, con=sql_connection, schema=None, if_exists='replace')

        # stations_df.write.mode("overwrite") \
        #     .format("jdbc") \
        #     .option("url", f"jdbc:sqlserver://0.0.0.0:5432;databaseName={database};") \
        #     .option("dbtable", table) \
        #     .option("user", user) \
        #     .option("password", password) \
        #     .save()
            # .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \