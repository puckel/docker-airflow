'''
An operator to run great expecations tests on db tables.
https://great-expectations.readthedocs.io/en/v0.4.4/

currently we need to create some custom expecations get really robust batch-time testing

the best way to write tests right now are to write sql that returns a true or false value,
then the expectation will test for truthiness.

Expectation sql templates should always live in the lib/templates/sql/{dag_name}/ge_dataset_sql/{dataset}.sql
this isn't enforced in any way at this point, but we can decide if we want to do that

Params:
sql_conn (required): string
sqlalchemy connection_id for db

data_set(required): string
name of the dataset - a set of data that a set of expecations can be run on

sql(required for now): sql template file or sql string
the custom sql that gets run to create the dataset. creates a temporary table

validation_config: file
a file location with configuration for expectations on a dataset. Currently all should be
located at lib/ge_validations/{dataset}.json

Fails if any tests in the expectations config fail.
'''

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from calm_logger.logging_helper import setup_logging

import os
import great_expectations as ge


logger = setup_logging(__name__)


class GreatExpectationsSqlContextOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql')

    @apply_defaults
    def __init__(self, sql_conn, data_set, sql, validation_config, *args, **kwargs):
        super(GreatExpectationsSqlContextOperator, self).__init__(*args, **kwargs)
        self.sql_conn = sql_conn
        self.sql = sql
        dags_folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER')
        self.validation_config = f'test/ge_validations/{validation_config}'
        self.data_set_name = data_set

    def execute(self, context):
        logger.info(f'running validations on {self.data_set_name}')
        try:
            self.data_context = self.get_data_context()
            self.data_set = self.get_data_set()
        except Exception as e:
            raise AirflowException(f'error getting dataset {e}')
        self.validate()

    def get_data_context(self):
        return ge.get_data_context('SqlAlchemy', self.sql_conn)

    def get_data_set(self):
        return self.data_context.get_dataset(self.data_set_name, self.sql)

    def validate(self):
        validation = self.data_set.get('validate')(expectations_config=self.validation_config)
        if not validation.get('success'):
            logger.error(f'test failed: {validation.get("results")}')
            raise AirflowException(f'test failed results: {validation}')
        else:
            logger.info(f'validations succeeded')
