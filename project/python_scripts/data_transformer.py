import logging
from pathlib import Path
import pandas as pd


logger = logging.getLogger(__name__)


class ETLoader():
    """This Class orchestrate a simple Data pipeline:
    - Load the input csv file in a Pandas DataFrame
    - Transform the data in the Pandas DataFrame
    - Load Pandas DataFrame in a SQLite table
    """

    def __init__(self, csv_filename, downloads_dir, conn_engine, schema, table):
        self.input_csv = csv_filename
        self.downloads_dir = downloads_dir
        self.df = None
        self.output_columns = ['year', 'quarter', 'retailer_country', 'retailer_type',
                               'order_method_type', 'revenue']
        self.conn_engine = conn_engine
        self.db_schema = schema
        self.db_table = table
        logger.info("Initialized ETLoader for file %s", self.input_csv)

    def orchestrate_etl(self):
        # Extract data from csv
        self._extract_data()
        # Transform data in Pandas Dataframe
        self._transform_data()
        # Load data in a SQLite table
        self._load_into_db()

    def _extract_data(self):
        try:
            csv_path = Path(self.downloads_dir) / self.input_csv
            self.df = pd.read_csv(csv_path, index_col=None)
            logger.info("The file %s initially contains %s rows", self.input_csv, len(self.df))
        except Exception as e:
            logger.error("Failed to insert data in %s. Error %s", self.db_table, e)
            raise
        else:
            logger.info("The csv %s has been loaded in a Pandas Dataframe", self.input_csv)

    def _transform_data(self):
        # Remove the year (from Q1 2012 to Q1)
        self.df['Quarter'] = self.df['Quarter'].astype(str).str[:2]
        # Remove space and format in lowercase the column names
        self.df.columns = self.df.columns.str.lower().str.replace(' ', '_')
        # Keep only the columns for saving in the table
        self.df = self.df[self.df.columns[self.df.columns.isin(self.output_columns)]]
        # Sum revenues grouping by self.output_columns
        self.df = self.df[self.output_columns].groupby(
            by=['year', 'quarter', 'retailer_country', 'retailer_type', 'order_method_type'],
            as_index=False).sum()
        # Remove decimals
        self.df['revenue'] = self.df['revenue'].apply(lambda x: '{:.0f}'.format(x))
        logger.info("The data have been transformed and grouped in %s rows.", len(self.df))
        logger.info("Ended data transformation.")

    def _load_into_db(self):
        try:
            self.df.to_sql(self.db_table,
                           schema='revenues',
                           con=self.conn_engine,
                           if_exists='append',
                           index=False)
        except Exception as e:
            logger.error("Failed to insert data in %s table. Error %s", self.db_table, e)
            raise e
        else:
            logger.info("Data inserted in %s table.", self.db_table)

        try:
            num_rows = self.conn_engine.execute('SELECT count(*) FROM revenues;').fetchone()
            logger.info("In total there are %d records in %s table.", num_rows[0], self.db_table)
        except Exception as e:
            logger.error("Failed to count data in the %s table. Error %s", self.db_table, e)
            raise e
        finally:
            self.conn_engine.close()
