import requests
from pandas import DataFrame
from pandas import read_sql
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import boto3
from numpy import squeeze
from numpy import asarray
from numpy import matrix
from pandas import get_dummies
from pandas import DataFrame


def categorical_encoder(dataframe, group_col, target_col):
    """
    dataframe: Pandas dataframe containing data to encode
    group_col: column in dataframe to group into single value
    target_col: column with categorical data that will be encoded into a single vector
    """
    # get one-hot encoded vector for each row
    dummy_df = get_dummies(dataframe, columns=[target_col])

    aggregated_vecs = {}
    # group by group_col, and aggregate each group by summing one-hot encoded vectors
    for name, group in dummy_df.groupby(group_col):
        # get all columns that match our dummy variable prefix
        g = group[group.columns[(group.columns).str.startswith(target_col)]]
        # sum columns together
        aggregated_vec = matrix(g).sum(axis=0)
        #print(np.squeeze(np.asarray(phecode_vec)))
        # turn matrix into vector
        aggregated_vecs[name] = squeeze(asarray(aggregated_vec))
    # create dataframe with dictionary mapping group_col values to aggregated vectors
    aggregated_vecs_df = DataFrame.from_dict(aggregated_vecs, orient="index")
    # add back column names
    aggregated_vecs_df.columns = dummy_df.columns.values[dummy_df.columns.str.startswith(target_col)]
    return aggregated_vecs_df

class LoadStationsOperator(BaseOperator):
    ui_color = "#358140"

    sql_engine = "postgresql+psycopg2://{user}:{password}@postgres:5432/{database}"

    @apply_defaults
    def __init__(
        self,
        aws_conn_id="",
        stations_df={},
        source_database={},
        target_database={},
        file_key="",
        limit=0,
        columns_to_drop=[],
        *args,
        **kwargs
    ):

        super(LoadStationsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.stations_df = stations_df
        self.source_database = source_database
        self.target_database = target_database
        self.file_key = file_key
        self.limit = limit
        self.columns_to_drop = columns_to_drop


    def encode(self):
        try:
            stations_df_encoded = categorical_encoder(dataframe=self.stations_df,
                                                   group_col="stationReference", target_col="observedProperty")
            stations_df_encoded["stationReference"]=stations_df_encoded.index
            stations_df_encoded.reset_index(drop=True, inplace=True)
            self.log.info(print(stations_df_encoded))
            self.stations_df = self.stations_df.merge(right=stations_df_encoded, on="stationReference", how="left")
            self.log.info(print(self.stations_df))
            self.stations_df.drop(columns="observedProperty", inplace=True)
            self.stations_df.drop_duplicates(subset=["stationReference"], inplace=True)
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to encode the dataframe"))
            raise ValueError

    def read_from_local_sql(self):
        try:
            sql_connection = create_engine(
                LoadStationsOperator.sql_engine.format(
                    user=self.source_database["user"],
                    password=self.source_database["password"],
                    database=self.source_database["database"],
                )
            )
            self.stations_df = read_sql(sql=self.source_database["table"], con=sql_connection)

        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to read the dataframe"))
            raise ValueError

    def write_to_local_sql(self):
        try:
            sql_connection = create_engine(
                LoadStationsOperator.sql_engine.format(
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
                )
                sql_connection.execute(
                    """ALTER TABLE {table} ADD PRIMARY KEY ("stationReference");""".format(
                        table=self.target_database["table"]
                    )
                )
            except Exception as e:
                self.log.info(print(e))
                self.log.info(print("Primary key restriction already exists"))

            self.stations_df.to_sql(
                name=self.target_database["table"],
                con=sql_connection,
                if_exists="append",
                index=False,
            )
            self.log.info(print(self.stations_df.head(0)))
            # try:
            #     conn = sql_connection.raw_connection()
            #     cur = conn.cursor()
            #     output = StringIO()
            #     self.stations_df.to_csv(output, sep="\t", header=False, index=False)
            #     output.seek(0)
            #     cur.copy_from(output, self.target_database["table"], null="", sep="\t")
            #     conn.commit()
            # except Exception as e:
            #     self.log.info(print(e))
            #     self.log.info(print("Failure to write to local database"))

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
                # partition_cols=["observedProperty"],
                compression="gzip",
            )
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to save parquet file locally"))
            raise ValueError

    def clear_staging_tables(self):
        sql_connection = create_engine(
            LoadStationsOperator.sql_engine.format(
                user=self.source_database["user"],
                password=self.source_database["password"],
                database=self.source_database["database"],
            )
        )
        sql_connection.execute(
            """DROP TABLE {table};""".format(
                table=self.source_database["table"]
            )
        )

    def execute(self, context):

        self.file_key = (
            self.target_database["table"]
            + ".parquet.gzip"
        )

        self.read_from_local_sql()
        self.encode()
        self.write_to_local_sql()
        self.save_locally()
        # self.clear_staging_tables()
        # self.save_to_s3()
#