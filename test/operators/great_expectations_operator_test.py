import pytest
import os
from operators.great_expectations_operator import GreatExpectationsSqlContextOperator
from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowException

pytest_plugins = ["pytest_mock"]


class FakeDataSet():
    def __init__(self, data_set, sql_string):
        pass

    def validate(self, expectations_config=None):
        pass


class FakeDataContext():
    def __init__(self, context_name, sql_conn):
        pass

    def get_dataset(self, name, sql):
        return FakeDataSet(name, sql)


@pytest.fixture
def set_env():
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = '/dags_folder'


@pytest.fixture
def ge_operator(mocker, set_env):
    def _ge_operator(result='success'):
        dag = DAG(dag_id='test_dag', start_date=datetime(2019, 1, 1))
        operator = GreatExpectationsSqlContextOperator(sql_conn='sql_conn', data_set='test_data_set',
                                                       sql=None, validation_config='test.json', dag=dag,
                                                       task_id='test_task')
        dataset = FakeDataSet('test_data_set', 'sql')
        mocker.patch.object(dataset, 'validate', new=mocker.Mock(return_value={'success': result == 'success'}))

        mocker.patch.object(operator, 'get_data_context',
                            new=mocker.Mock(return_value=FakeDataContext('test_data_set', 'sql')))
        mocker.patch.object(operator, 'get_data_set', new=mocker.Mock(return_value=dataset))
        return operator

    return _ge_operator


def test_init(ge_operator):
    assert ge_operator().validation_config == 'test/ge_validations/test.json'


def test_execute_success(ge_operator):
    context = {}
    operator = ge_operator()
    operator.execute(context)
    operator.get_data_context.assert_called()
    operator.get_data_set.assert_called()
    operator.data_set.validate.assert_called()


def test_execute_fail(ge_operator):
    context = {}
    operator = ge_operator(result='failed')
    with pytest.raises(AirflowException):
        operator.execute(context)
