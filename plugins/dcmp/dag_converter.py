# encoding: utf-8

import logging
import shutil
import re
import os
import json
from copy import deepcopy
from datetime import datetime
from collections import OrderedDict

from croniter import croniter
from airflow import configuration
from airflow.utils.db import provide_session
from airflow.models import TaskInstance

from dcmp import settings as dcmp_settings
from dcmp.models import DcmpDag
from dcmp.utils import create_dagbag_by_dag_code


BASE_DIR = os.path.dirname(os.path.realpath(__file__))
DAG_TEMPLATES_DIR = os.path.join(BASE_DIR, "dag_templates")


get_list = lambda x: x if x else []
get_string = lambda x: x.strip() if x else ""
get_int = lambda x: int(x) if x else 0
get_bool_code_true = lambda x: True if x is not False else False
get_bool_code_false = lambda x: False if x is not True else True


def load_dag_template(template_name):
    logging.info("loading dag template: %s" % template_name)
    with open(os.path.join(DAG_TEMPLATES_DIR, template_name + ".template"), "r") as f:
        res = f.read()
    return res


class DAGConverter(object):
    DAG_ITEMS = (("dag_name", get_string, True), ("cron", get_string, True), ("category", get_string, True),
        ("retries", get_int, False), ("retry_delay_minutes", get_int, False), ("email_on_failure", get_bool_code_true, False),
        ("email_on_retry", get_bool_code_false, False), ("depends_on_past", get_bool_code_false, False),
        ("concurrency", lambda x: int(x) if x else 16, False), ("max_active_runs", lambda x: int(x) if x else 16, False),
        ("add_start_task", get_bool_code_false, False), ("add_end_task", get_bool_code_false, False),
        ("skip_dag_not_latest", get_bool_code_false, False), ("skip_dag_on_prev_running", get_bool_code_false, False),
        ("email_on_skip_dag", get_bool_code_false, False), ("emails", get_string, False), ("start_date", get_string, False),
        ("end_date", get_string, False))
    TASK_ITEMS = (("task_name", get_string, True), ("task_type", get_string, True), ("command", get_string, False),
        ("priority_weight", get_int, False), ("upstreams", get_list, False), ("queue_pool", get_string, False),
        ("task_category", get_string, False), )
    TASK_EXTRA_ITEMS = (("retries", get_int, "retries=%s,"), ("retry_delay_minutes", get_int, "retry_delay=timedelta(minutes=%s),"), )
    
    DAG_CODE_TEMPLATE = load_dag_template("dag_code")
    
    BASE_TASK_CODE_TEMPLATE = r"""%(before_code)s
_["%%(task_name)s"] = %(operator_name)s(
    task_id='%%(task_name)s',
%(operator_code)s
    priority_weight=%%(priority_weight)s,
    queue=%%(queue_code)s,
    pool=%%(pool_code)s,
    dag=dag,
    %%(extra_params)s)

_["%%(task_name)s"].category = {
    "name": r'''%%(task_category)s''',
    "fgcolor": r'''%%(task_category_fgcolor)s''',
    "order": %%(task_category_order)s,
}
"""
    
    DUMMY_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "DummyOperator",
        "operator_code": "", }
    
    BASH_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "BashOperator",
        "operator_code": r"""
    bash_command=r'''%(processed_command)s '''.decode("utf-8"),
""", }

    HQL_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "HiveOperator",
        "operator_code": r"""
    mapred_job_name="%(task_name)s",
    mapred_queue=%(mapred_queue_code)s,
    hql=r'''
%(processed_command)s
'''.decode("utf-8"),
""", }
    
    PYTHON_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": """
def %(task_name)s_worker(ds, **context):
%(processed_command)s
    return None
""",
        "operator_name": "PythonOperator",
        "operator_code": r"""
    provide_context=True,
    python_callable=%(task_name)s_worker,
""", }

    SHORT_CIRCUIT_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": """
def %(task_name)s_worker(ds, **context):
%(processed_command)s
    return None
""",
        "operator_name": "ShortCircuitOperator",
        "operator_code": r"""
    provide_context=True,
    python_callable=%(task_name)s_worker,
""", }

    TIME_SENSOR_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "TimeSensor",
        "operator_code": r"""
    target_time=%(processed_command)s,
""", }

    TIMEDELTA_SENSOR_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "TimeDeltaSensor",
        "operator_code": r"""
    delta=%(processed_command)s,
""", }
    
    STREAM_CODE_TEMPLATE = """
_["%(task_name)s"] << _["%(upstream_name)s"]
"""
    
    TASK_TYPE_TO_TEMPLATE = {
        "bash": BASH_TASK_CODE_TEMPLATE,
        "dummy": DUMMY_TASK_CODE_TEMPLATE,
        "hql": HQL_TASK_CODE_TEMPLATE,
        "python": PYTHON_TASK_CODE_TEMPLATE,
        "short_circuit": SHORT_CIRCUIT_TASK_CODE_TEMPLATE,
        "time_sensor": TIME_SENSOR_TASK_CODE_TEMPLATE,
        "timedelta_sensor": TIMEDELTA_SENSOR_TASK_CODE_TEMPLATE,
    }
    
    JOB_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]+$")

    def check_job_name(self, job_name):
        return bool(self.JOB_NAME_RE.match(job_name))

    def clean_task_dict(self, task_dict, strict=False):
        task_name = task_dict.get("task_name")
        if not task_name:
            raise ValueError("task name required")
        task_name = get_string(task_name)
        if not self.check_job_name(task_name):
            raise ValueError("task %s name invalid" % task_name)

        task_res = {}
        for key, trans_func, required in self.TASK_ITEMS:
            value = task_dict.get(key)
            if required and not value:
                raise ValueError("task %s params %s required" % (task_name, key))
            if strict and key in ["queue_pool"] and not value:
                raise ValueError("task %s params %s required" % (task_name, key))
            value = trans_func(value)
            task_res[key] = value
        for key, trans_func, _ in self.TASK_EXTRA_ITEMS:
            value = task_dict.get(key)
            if value:
                value = trans_func(value)
                task_res[key] = value
        return task_res

    def dict_to_json(self, dag_dict, strict=False):
        if not dag_dict or not isinstance(dag_dict, dict):
            raise ValueError("dags required")
        
        task_dicts = dag_dict.get("tasks", [])
        if not task_dicts or not isinstance(task_dicts, list):
            raise ValueError("tasks required")

        dag_res = {}
        for key, trans_func, required in self.DAG_ITEMS:
            value = dag_dict.get(key)
            if required and not value:
                raise ValueError("dag params %s required" % key)
            value = trans_func(value)
            dag_res[key] = value

        dag_name = dag_res["dag_name"]
        if not self.check_job_name(dag_name):
            raise ValueError("dag name invalid")

        cron = dag_res["cron"]
        if cron == "None":
            pass
        else:
            try:
                croniter(cron)
            except Exception as e:
                raise ValueError("dag params cron invalid")
        
        task_names = []
        tasks_res = []
        for task_dict in task_dicts:
            task_res = self.clean_task_dict(task_dict, strict=strict)
            task_name = task_res["task_name"]
            if task_name in task_names:
                raise ValueError("task %s name duplicated" % task_name)
            task_names.append(task_res["task_name"])
            tasks_res.append(task_res)
        
        for task_res in tasks_res:
            for upstream in task_res["upstreams"]:
                if upstream not in task_names or upstream == task_res["task_name"]:
                    raise ValueError("task %s upstream %s invalid" % (task_res["task_name"], upstream))
        
        dag_res["tasks"] = tasks_res
        return dag_res
    
    def render_confs(self, confs):
        confs = deepcopy(confs)
        now = datetime.now()        
        dag_codes = []
        task_catgorys_dict = {
            "default": {"order": str(0), "fgcolor": "#f0ede4"}
        }
        for i, category_data in enumerate(dcmp_settings.DAG_CREATION_MANAGER_TASK_CATEGORYS):
            key, fgcolor = category_data
            task_catgorys_dict[key] = {"order": str(i + 1), "fgcolor": fgcolor}
        
        for dag_name, conf in confs.iteritems():
            emails = [email.strip() for email in conf["emails"].split(",") if email.strip()] or dcmp_settings.DAG_CREATION_MANAGER_DEFAULT_EMAILS
            conf["email_code"] = json.dumps(emails)

            if not conf.get("owner"):
                conf["owner"] = "airflow"

            task_names = [task["task_name"] for task in conf["tasks"]]
            
            def get_task_name(origin_task_name):
                task_name = origin_task_name
                for i in xrange(10000):
                    if task_name in task_names:
                        task_name = "%s_%s" % (origin_task_name, i)
                    else:
                        break
                else:
                    task_name = None
                return task_name
            
            if conf["add_start_task"]:
                task_name = get_task_name("start")
                if task_name:
                    for task in conf["tasks"]:
                        if not task["upstreams"]:
                            task["upstreams"] = [task_name]
                    conf["tasks"].append(self.clean_task_dict({
                        "task_name": task_name,
                        "task_type": "dummy",
                    }))
            
            if conf["add_end_task"]:
                task_name = get_task_name("end")
                if task_name:
                    root_task_names = set(task_names)
                    for task in conf["tasks"]:
                        root_task_names -= set(task["upstreams"])
                    conf["tasks"].append(self.clean_task_dict({
                        "task_name": task_name,
                        "task_type": "dummy",
                        "upstreams": root_task_names,
                    }))

            if conf["skip_dag_not_latest"] or conf["skip_dag_on_prev_running"]:
                task_name = []
                if conf["skip_dag_not_latest"]:
                    task_name.append("not_latest")
                if conf["skip_dag_on_prev_running"]:
                    task_name.append("when_previous_running")
                task_name = "_or_".join(task_name)
                task_name = "skip_dag_" + task_name
                task_name = get_task_name(task_name)
                if task_name:
                    command = """
skip = False
if context['dag_run'] and context['dag_run'].external_trigger:
    logging.info('Externally triggered DAG_Run: allowing execution to proceed.')
    return True
"""
                    if conf["skip_dag_not_latest"]:
                        command += """
if not skip:
    now = datetime.now()
    left_window = context['dag'].following_schedule(context['execution_date'])
    right_window = context['dag'].following_schedule(left_window)
    logging.info('Checking latest only with left_window: %s right_window: %s now: %s', left_window, right_window, now)
    
    if not left_window < now <= right_window:
        skip = True
"""

                    if conf["skip_dag_on_prev_running"]:
                        command += """
if not skip:
    session = settings.Session()
    count = session.query(DagRun).filter(
        DagRun.dag_id == context['dag'].dag_id,
        DagRun.state.in_(['running']),
    ).count()
    session.close()
    logging.info('Checking running DAG count: %s' % count)
    skip = count > 1
"""

                    if conf["email_on_skip_dag"]:
                        command += """
if skip:
    send_alert_email("SKIP", context)
"""

                    command += """
return not skip
"""

                    for task in conf["tasks"]:
                        if not task["upstreams"]:
                            task["upstreams"] = [task_name]
                    conf["tasks"].append(self.clean_task_dict({
                        "task_name": task_name,
                        "task_type": "short_circuit",
                        "command": command,
                    }))

            for task in conf["tasks"]:
                extra_params = []
                for key, trans_func, template in self.TASK_EXTRA_ITEMS:
                    value = task.get(key)
                    if value is None:
                        continue
                    value = trans_func(value)
                    extra_params.append(template % value)
                task["extra_params"] = "".join(extra_params)

            cron = conf["cron"]
            if cron == "None":
                conf["start_date_code"] = now.strftime('datetime.strptime("%Y-%m-%d %H:%M:%S", "%%Y-%%m-%%d %%H:%%M:%%S")')
                conf["end_date_code"] = "None"
                conf["cron_code"] = "None"
            else:
                cron_instance = croniter(cron, now)
                start_date = cron_instance.get_prev(datetime)
                conf["start_date_code"] = start_date.strftime('datetime.strptime("%Y-%m-%d %H:%M:%S", "%%Y-%%m-%%d %%H:%%M:%%S")')
                conf["end_date_code"] = "None"
                conf["cron_code"] = "'%s'" % cron
            
            if conf["start_date"]:
                conf["start_date_code"] = 'datetime.strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")' % conf["start_date"]

            if conf["end_date"]:
                conf["end_date_code"] = 'datetime.strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")' % conf["end_date"]
            
            dag_code = self.DAG_CODE_TEMPLATE % conf

            task_codes = []
            stream_codes = []
            for task in conf["tasks"]:
                queue_pool = dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL_DICT.get(task["queue_pool"])
                if queue_pool:
                    queue, pool = queue_pool
                    task["queue_code"] = "'%s'" % queue
                    task["pool_code"] = "'%s'" % pool
                else:
                    task["queue_code"] = "'%s'" % configuration.get("celery", "default_queue")
                    task["pool_code"] = "None"

                task["task_category"] = task.get("task_category", "default")
                task_category = task_catgorys_dict.get(task["task_category"], None)
                if not task_category:
                    task["task_category"] = "default"
                    task_category = task_catgorys_dict["default"]
                task["task_category_fgcolor"] = task_category["fgcolor"]
                task["task_category_order"] = task_category["order"]

                if task["task_type"] in ["python", "short_circuit"]:
                    task["processed_command"] = "\n".join(map(lambda x: "    " + x, task["command"].split("\n")))
                else:
                    task["processed_command"] = task["command"]

                if task["task_type"] == "hql":
                    mapred_queue_code = dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE_DICT.get(task["queue_pool"], None)
                    if mapred_queue_code:
                        mapred_queue_code = '"%s"' % mapred_queue_code
                    else:
                        mapred_queue_code = "None"
                    task["mapred_queue_code"] = mapred_queue_code

                task_template = self.TASK_TYPE_TO_TEMPLATE.get(task["task_type"])
                if task_template:
                    task_codes.append(task_template % task)
                else:
                    continue

                for upstream in task["upstreams"]:
                    stream_code = self.STREAM_CODE_TEMPLATE % {
                        "task_name": task["task_name"],
                        "upstream_name": upstream,
                    }
                    stream_codes.append(stream_code)
    
            dag_code = "%s\n%s\n%s" % (dag_code, "\n".join(task_codes), "\n".join(stream_codes))
            dag_codes.append((dag_name, dag_code))
        return dag_codes
    
    @provide_session
    def refresh_dags(self, session=None):
        confs = OrderedDict()
        dcmp_dags = session.query(DcmpDag).all()
        for dcmp_dag in dcmp_dags:
            conf = dcmp_dag.get_approved_conf(session=session)
            if conf:
                confs[dcmp_dag.dag_name] = conf
        
        dag_codes = self.render_confs(confs)
        
        shutil.rmtree(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER, ignore_errors=True)
        os.makedirs(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER)
        for dag_name, dag_code in dag_codes:
            with open(os.path.join(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER, dag_name + ".py"), "w") as f:
                f.write(dag_code.encode("utf-8"))
    
    def create_dagbag_by_conf(self, conf):
        _, dag_code = self.render_confs({conf["dag_name"]: conf})[0]
        return create_dagbag_by_dag_code(dag_code)
    
    def clean_dag_dict(self, dag_dict, strict=False):
        conf = self.dict_to_json(dag_dict, strict=strict)
        dagbag = self.create_dagbag_by_conf(conf)
        if dagbag.import_errors:
            raise ImportError(dagbag.import_errors.items()[0][1])
        return conf
    
    def create_dag_by_conf(self, conf):
        return self.create_dagbag_by_conf(conf).dags[conf["dag_name"]]
    
    def create_task_by_task_conf(self, task_conf, dag_conf=None):
        if not task_conf.get("task_name"):
            task_conf["task_name"] = "tmp_task"
        task_conf["upstreams"] = []
        if not dag_conf:
            dag_conf = {
                "dag_name": "tmp_dag",
                "cron": "0 * * * *",
                "category": "default",
            }
        dag_conf["tasks"] = [task_conf]
        conf = self.dict_to_json(dag_conf)
        dag = self.create_dag_by_conf(conf)
        task = dag.get_task(task_id=task_conf["task_name"])
        if not task:
            raise ValueError("invalid conf")
        return task
        
    def create_task_instance_by_task_conf(self, task_conf, dag_conf=None, execution_date=None):
        if execution_date is None:
            execution_date = datetime.now()
        task = self.create_task_by_task_conf(task_conf, dag_conf=dag_conf)
        ti = TaskInstance(task, execution_date)
        return ti
    
    def render_task_conf(self, task_conf, dag_conf=None, execution_date=None):
        ti = self.create_task_instance_by_task_conf(task_conf, dag_conf=dag_conf, execution_date=execution_date)
        ti.render_templates()
        res = OrderedDict()
        for template_field in ti.task.__class__.template_fields:
            res[template_field] = getattr(ti.task, template_field)
        return res

dag_converter = DAGConverter()