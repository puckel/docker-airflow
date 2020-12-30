from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import zipfile
import random
import pandas as pd

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 30, 18, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False
    #"retries": 1,
    #"retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "treino-04", 
    description="Uma dag com condicionais",
    default_args=default_args, 
    schedule_interval=timedelta(minutes=2)
)

get_data = BashOperator(
    task_id="get-data",
    bash_command='curl https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    trigger_rule="all_done",
    dag=dag
)


def unzip_file():
    with zipfile.ZipFile("/usr/local/airflow/data/microdados_enade_2019.zip", 'r') as zipped:
        zipped.extractall("/usr/local/airflow/data")

unzip_data = PythonOperator(
    task_id='unzip-data',
    python_callable=unzip_file,
    dag=dag
)


def select_student():
    df = pd.read_csv('/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/microdados_enade_2019.txt', sep=';', decimal=',')
    escolha = random.randint(0, df.shape[0]-1)
    aluno = df.iloc[escolha]
    return aluno.TP_SEXO

pick_student = PythonOperator(
    task_id="pick-student",
    python_callable=select_student,
    dag=dag
)

def MouF(**context):
    value = context['task_instance'].xcom_pull(task_ids='pick-student')
    if value == 'M':
        return 'male_branch'
    elif value == 'F':
        return 'female_branch'

male_of_female = BranchPythonOperator(
    task_id='condition-male_or_female',
    python_callable=MouF,
    provide_context=True,
    dag=dag
)


male_branch = BashOperator(
    task_id="male_branch",
    bash_command='echo "Estudante escolhido foi do sexo Masculino"',
    dag=dag
)

female_branch = BashOperator(
    task_id="female_branch",
    bash_command='echo "Estudante escolhido foi do sexo Feminino"',
    dag=dag
)

get_data >> unzip_data >> pick_student >> male_of_female >> [male_branch, female_branch]