#import the required libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator


#defining the default arguments dictionary
args = {
	'owner': 'airflow',
	'start_date': datetime(2020,12,2), #you can change this start_date
	'retries': 1,
    "retry_delay": timedelta(seconds=10),
}

dag = DAG('Assignment_1', default_args=args)

#task1 is to create a directory 'test_dir' inside dags folder
task1 = BashOperator(task_id='create_directory', bash_command='mkdir -p ~/dags/test_dir', dag=dag)

#task2 is to get the 'shasum' of 'test_dir' directory
task2 = BashOperator(task_id='get_shasum', bash_command='shasum ~/dags/test_dir', dag=dag)

#below we are setting up the operator relationships such that task1 will run first than task2
task2.set_upstream(task1)
