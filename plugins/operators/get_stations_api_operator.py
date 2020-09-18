import requests
from pandas import DataFrame
from io import StringIO
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GetStationsAPIOperator(BaseOperator):
    ui_color = '#358140'


    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 *args, **kwargs):

        super(GetStationsAPIOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):

        # Call REST API:
        API_endpoint = "https://environment.data.gov.uk/hydrology/id/stations.json?_limit=5"
        response = requests.get(API_endpoint)
        stations = response.json()["items"]

        stations_df = DataFrame.from_dict(stations)

        columns_to_drop = ["easting", "northing", "notation", "type", "wiskiID", "RLOIid"]

        stations_df.drop(columns=columns_to_drop, inplace=True)

        #DATA FROM THE COMPOSE FILE
        database = "airflow"
        table = "stations"
        user = "airflow"
        password = "airflow"

        sql_connection = create_engine(f"postgresql+psycopg2://{user}:{password}@postgres:5432/{database}".format(user=user, password=password, database=database))

        stations_df.head(0).to_sql(name=table, con=sql_connection, if_exists='replace', index=False)

        conn = sql_connection.raw_connection()
        cur = conn.cursor()
        output = StringIO()
        stations_df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, table, null="", sep='\t')
        conn.commit()
