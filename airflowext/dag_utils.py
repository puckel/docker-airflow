# coding:utf-8
import os
import re

from jinja2 import Template
from datetime import datetime as dte


CUR_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(CUR_DIR, "dag.py.template")
TARGET_DIR = os.environ.get("AIRFLOW_HOME", "/tmp")


def trans_interval(text):
    """
    时间文本转换
    """
    time_type_trans = {
        "d": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
    }
    params = dict.fromkeys(time_type_trans.values(), 0)

    matcher = re.compile("^(\d+)([s|h|d|ms])$").match(text)
    if not matcher:
        raise Exception()
    time, time_type = matcher.groups()
    params[time_type_trans[time_type]] = int(time)
    txt = "timedelta(days={days}, hours={hours}, minutes={minutes}, seconds={seconds})"
    return txt.format(**params)


def generate_dag_file(data):
    """
    生成DAG定义py文件
    """
    with open(TEMPLATE_PATH) as f:
        tpl_content = f.read()

    template = Template(tpl_content)
    today = dte.today()

    partent2child = []
    for t in data["tasks"]:
        if not t["pre_task"]:
            continue
        partent2child.append((t["pre_task"], t["name"]))

    content = template.render(
        name=data["name"],
        owner="luke",
        start_date="datetime(%s, %s, %s)" % (today.year, today.month, today.day),
        email="junping.luo@aqara.com",
        retries=1,
        interval=trans_interval(data["interval"]),
        tasks=data["tasks"],
        partent2child=partent2child,
    )

    target_path = os.path.join(TARGET_DIR, "dags/auto_%s.py" % data["name"])
    with open(target_path, "w+") as f:
        f.write(content)
    return target_path


if __name__ == "__main__":
    data = {
        "interval": "20s",
        "name": "test",
        "state": "禁用",
        "sync_type": "全量同步",
        "tasks": [
            {
                "name": "同步表1",
                "pre_task": "SELECT",
                "source": {
                    "conn_id": "src_conn",
                    "query_sql": ""
                },
                "target": {
                    "columns": ["xxx"],
                    "conn_id": "tar_conn",
                    "table": "test",
                }
            }
        ]
    }
    generate_dag_file(data)

