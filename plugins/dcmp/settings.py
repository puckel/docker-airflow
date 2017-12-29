# encoding: utf-8

import os
import socket

from airflow import configuration


TASK_TYPES = ["bash", "hql", "python", "short_circuit", "time_sensor", "timedelta_sensor"]

AUTHENTICATE = configuration.getboolean('webserver', 'AUTHENTICATE')
BASE_URL = configuration.get('webserver', 'BASE_URL')

try:
    DAG_CREATION_MANAGER_LINE_INTERPOLATE = configuration.get('dag_creation_manager', 'DAG_CREATION_MANAGER_LINE_INTERPOLATE')
except Exception as e:
    DAG_CREATION_MANAGER_LINE_INTERPOLATE = "basis"

HOSTNAME = socket.gethostname()
AIRFLOW_DAGS_FOLDER = configuration.get('core', 'DAGS_FOLDER')
DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER = os.path.join(AIRFLOW_DAGS_FOLDER, "deployedDags")
DAG_CREATION_MANAGER_QUEUE_POOL_STR = configuration.get('dag_creation_manager', 'DAG_CREATION_MANAGER_QUEUE_POOL')

DAG_CREATION_MANAGER_QUEUE_POOL = []
for queue_pool_str in DAG_CREATION_MANAGER_QUEUE_POOL_STR.split(","):
    key, queue_pool = queue_pool_str.split(":")
    queue, pool = queue_pool.split("|")
    DAG_CREATION_MANAGER_QUEUE_POOL.append((key, (queue, pool)))

DAG_CREATION_MANAGER_QUEUE_POOL_DICT = dict(DAG_CREATION_MANAGER_QUEUE_POOL)

DAG_CREATION_MANAGER_CATEGORY_STR = configuration.get('dag_creation_manager', 'DAG_CREATION_MANAGER_CATEGORY')
DAG_CREATION_MANAGER_CATEGORYS = ["default"]
for category in DAG_CREATION_MANAGER_CATEGORY_STR.split(","):
    if category not in DAG_CREATION_MANAGER_CATEGORYS:
        DAG_CREATION_MANAGER_CATEGORYS.append(category)

DAG_CREATION_MANAGER_TASK_CATEGORY_STR = configuration.get('dag_creation_manager', 'DAG_CREATION_MANAGER_TASK_CATEGORY')
DAG_CREATION_MANAGER_TASK_CATEGORYS = []
for task_category in DAG_CREATION_MANAGER_TASK_CATEGORY_STR.split(","):
    key, color = task_category.split(":")
    if key != "default":
        DAG_CREATION_MANAGER_TASK_CATEGORYS.append((key, color))

DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE_STR = configuration.get('dag_creation_manager', 'DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE')
DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE = [queue_pool_mr_str.split(":") for queue_pool_mr_str in DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE_STR.split(",")]
DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE_DICT = dict(DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE)


DAG_CREATION_MANAGER_DEFAULT_EMAIL_STR = configuration.get('dag_creation_manager', 'DAG_CREATION_MANAGER_DEFAULT_EMAIL')
DAG_CREATION_MANAGER_DEFAULT_EMAILS = [email.strip() for email in DAG_CREATION_MANAGER_DEFAULT_EMAIL_STR.split(",") if email.strip()]

try:
    DAG_CREATION_MANAGER_NEED_APPROVER = configuration.getboolean('dag_creation_manager', 'DAG_CREATION_MANAGER_NEED_APPROVER')
except Exception as e:
    DAG_CREATION_MANAGER_NEED_APPROVER = False

try:
    DAG_CREATION_MANAGER_CAN_APPROVE_SELF = configuration.getboolean('dag_creation_manager', 'DAG_CREATION_MANAGER_CAN_APPROVE_SELF')
except Exception as e:
    DAG_CREATION_MANAGER_CAN_APPROVE_SELF = True