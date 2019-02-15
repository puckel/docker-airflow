import pytest
import os
from operators.great_expectations_operator import GreatExpectationsSqlContextOperator
from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowException


pytest_plugins = ["pytest_mock"]


@pytest.fixture
def set_env():
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = '/dags_folder'


@pytest.fixture
def mocked_success_dataset(mocker):
    validate_stub = mocker.Mock(return_value={'success': True})

    mocker.patch('operators.great_expectations_operator.GreatExpectationsSqlContextOperator.get_data_set',
                 return_value={'validate': validate_stub})

    return validate_stub


@pytest.fixture
def mocked_failed_dataset(mocker):
    validate_stub = mocker.Mock(return_value={'success': False})

    mocker.patch('operators.great_expectations_operator.GreatExpectationsSqlContextOperator.get_data_set',
                 return_value={'validate': validate_stub})


@pytest.fixture
def mocked_get_data_context(mocker):
    dataset_stub = mocker.stub(name='dataset_stub')

    mocker.patch('operators.great_expectations_operator.GreatExpectationsSqlContextOperator.get_data_context',
                 return_value={'get_dataset': dataset_stub})

    return dataset_stub


@pytest.fixture
def ge_operator(set_env):
    dag = DAG(dag_id='test_dag', start_date=datetime(2019, 1, 1))
    return GreatExpectationsSqlContextOperator(sql_conn='sql_conn', data_set='test_data_set',
                                               sql=None, validation_config='test.json', dag=dag,
                                               task_id='test_task')


def test_init(ge_operator, mocked_get_data_context):
    assert ge_operator.validation_config == 'test/ge_validations/test.json'
    mocked_get_data_context.assert_called


def test_execute(ge_operator, mocked_get_data_context, mocked_success_dataset):
    context = {}
    ge_operator.execute(context)
    mocked_success_dataset.assert_called()


def test_operator_fail(ge_operator, mocked_get_data_context, mocked_failed_dataset):
    context = {}
    with pytest.raises(AirflowException):
        ge_operator.execute(context)


def test_operator_success(ge_operator, mocked_get_data_context, mocked_success_dataset):
    context = {}
    try:
        ge_operator.execute(context)
    except AirflowException:
        pytest.fail("Task failed even though the success message came back")
