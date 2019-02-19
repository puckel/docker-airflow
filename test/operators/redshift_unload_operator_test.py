import pytest
import os
from operators.redshift_unload_operator import RedshiftUnloadOperator
from airflow.operators import PostgresOperator
from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowException


pytest_plugins = ["pytest_mock"]


@pytest.fixture
def set_env():
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = '/dags_folder'
    os.environ['CALM_ENV'] = 'dev'
    os.environ['AWS_ACCESS_KEY_ID'] = 'test_key_id'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret'


@pytest.fixture
def test_dag(set_env):
    return DAG(dag_id='test_dag', start_date=datetime(2019, 1, 1))


@pytest.fixture
def redshift_unload(mocker, test_dag):
    def _redshift_unload(result='success'):
        mocker.patch.object(PostgresOperator,
                            'execute',
                            new=mocker.Mock(side_effect=AirflowException('fail') if result == 'fail' else None))
        operator = RedshiftUnloadOperator(custom_unload_options={'query': 'select * from a_table'},
                                          postgres_conn_id='conn_id', db='stitch', task_id='task', dag=test_dag)

        return operator

    return _redshift_unload


def test_init(redshift_unload):
    assert redshift_unload().custom_opts == {'query': 'select * from a_table'}


def test_execute_success_unload(redshift_unload):
    operator = redshift_unload()
    operator.execute(context={'dag_id': 'test_dag',
                              'task_id': 'fake_task',
                              'execution_date': "some_date"})
    assert operator.custom_opts.get('s3_location') == 's3://calm-redshift-dev/unloads/test_dag/fake_task/some_date/'
    PostgresOperator.execute.assert_called()


def test_execute_fail_unload(redshift_unload):
    operator = redshift_unload(result='fail')

    with pytest.raises(AirflowException):
        operator.execute(context={'dag_id': 'test_dag',
                                  'task_id': 'fake_task',
                                  'execution_date': "some_date"})
