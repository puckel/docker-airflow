# encoding: utf-8

import os
import logging
from StringIO import StringIO

from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow import settings
from airflow.utils.file import TemporaryDirectory
from airflow.models import DagBag, TaskInstance


def get_dag(dag_name, dag_folder=None, include_examples=None):
    kw = {"dag_folder": dag_folder, }
    if include_examples is not None:
        kw.update({"include_examples": include_examples})
    dagbag = DagBag(**kw)
    return dagbag.dags.get(dag_name, None)


def get_task(dag_name, task_name, dag_folder=None, include_examples=None):
    dag = get_dag(dag_name, dag_folder=dag_folder, include_examples=include_examples)
    if not dag:
        return None
    try:
        task = dag.get_task(task_id=task_name)
    except Exception as e:
        return None
    return task


def create_task_instance(dag_name, task_name, execution_date=None, dag_folder=None, include_examples=None):
    if execution_date is None:
        execution_date = datetime.now()
    task = get_task(dag_name, task_name, dag_folder=dag_folder, include_examples=include_examples)
    if not task:
        return None
    ti = TaskInstance(task, execution_date)
    return ti


def create_task_instance_by_dag_code(dag_code, dag_name, task_name, execution_date=None):
    with TemporaryDirectory(prefix='dcmp_dag_') as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir) as f:
            f.write(dag_code.encode('UTF-8'))
            f.flush()
            ti = create_task_instance(dag_name, task_name, execution_date=execution_date, dag_folder=os.path.join(tmp_dir, f.name), include_examples=False)
    return ti


def create_dagbag_by_dag_code(dag_code):
    with TemporaryDirectory(prefix='dcmp_dag_') as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir) as f:
            f.write(dag_code.encode('UTF-8'))
            f.flush()
            dagbag = DagBag(dag_folder=os.path.join(tmp_dir, f.name), include_examples=False)
    return dagbag


def search_conf_iter(search, conf, key=None, task_name=""):
    if isinstance(conf, basestring):
        for line in conf.split("\n"):
            if search in line:
                yield task_name, key, line
    elif isinstance(conf, (tuple, list)):
        for item in conf:
            if isinstance(item, dict) and item.get("task_name"):
                task_name = item.get("task_name")
            for result_task_name, result_key, result_line in search_conf_iter(search, item, key=key, task_name=task_name):
                yield result_task_name, result_key, result_line
    elif isinstance(conf, dict):
        for item_key in sorted(conf.keys()):
            item_value = conf[item_key]
            for result_task_name, result_key, result_line in search_conf_iter(search, item_value, key=item_key, task_name=task_name):
                yield result_task_name, result_key, result_line


class LogStreamContext(object):
    def __init__(self, *args, **kwargs):
        super(LogStreamContext, self).__init__(*args, **kwargs)
        self.stream = StringIO()
        self.logger = logging.getLogger()
        self.handler = logging.StreamHandler(self.stream)
        self.handler.setFormatter(logging.Formatter(settings.SIMPLE_LOG_FORMAT))
    
    def __enter__(self):
        self.logger.addHandler(self.handler)
        return self.stream
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.logger.removeHandler(self.handler)
        self.stream.close()