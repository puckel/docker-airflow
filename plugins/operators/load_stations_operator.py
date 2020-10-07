from pandas import read_sql
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

def create_sql_connection(database):
    """This function creates a SQLalchemy connection from some database information and returns it."""
    sql_connection = create_engine(
        LoadStationsOperator.sql_engine.format(
            user=database["user"],
            password=database["password"],
            database=database["database"],
        )
    )
    return sql_connection


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
        self.source_sql_connection = create_sql_connection(source_database)
        self.target_sql_connection = create_sql_connection(target_database)
        self.file_key = file_key
        self.limit = limit
        self.columns_to_drop = columns_to_drop


    def encode(self):
        """This method one hot encodes the observedProperties from each station, allowing to have only one
        record per stationReference, then it drops the duplicates and the hot-encoded column."""
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
        """This method reads the staging SQL database to load the final stations database."""
        try:
            self.stations_df = read_sql(sql=self.source_database["table"], con=self.source_sql_connection)
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to read the dataframe"))
            raise ValueError

    def write_to_local_sql(self):
        """This function loads the final clean dataframe to a local SQL database, checking the PK constraint.
        It serves as a test that the dataframe is ready to be saved to parquet."""
        try:

            self.stations_df.head(0).to_sql(
                name=self.target_database["table"],
                con=self.target_sql_connection,
                if_exists="append",
                index=False,
            )
            try:
                self.target_sql_connection.execute(
                    """ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "stationReference";""".format(
                        table=self.target_database["table"]
                    )
                )
                self.target_sql_connection.execute(
                    """ALTER TABLE {table} ADD PRIMARY KEY ("stationReference");""".format(
                        table=self.target_database["table"]
                    )
                )
            except Exception as e:
                self.log.info(print(e))
                self.log.info(print("Primary key restriction already exists"))

            self.stations_df.to_sql(
                name=self.target_database["table"],
                con=self.target_sql_connection,
                if_exists="append",
                index=False,
            )
            self.log.info(print(self.stations_df.head(0)))

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
                # partition_cols=["observedProperty"],
                compression="gzip",
            )
        except Exception as e:
            self.log.info(print(e))
            self.log.info(print("Failure to save parquet file locally"))
            raise ValueError

    def clear_staging_tables(self):
        """This method drops the staging_table for stations after the data has been cleaned and loaded to the final
        table"""
        self.source_sql_connection.execute(
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
