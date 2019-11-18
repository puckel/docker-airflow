# -*- coding:utf-8 -*-
# This is the class you derive to create a plugin

import json
import uuid
import subprocess
import os
import json
import re

from datetime import timedelta
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request
from flask.views import MethodView
from flask_admin import expose
from flask_admin.base import MenuLink
from flask.json import jsonify

# Importing base classes that we need to derive
from sqlalchemy import Column, Integer, String, ForeignKey
from airflow.models.base import ID_LEN, Base
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperatorLink
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.decorators import apply_defaults
from flask_appbuilder import BaseView as AppBuilderBaseView
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Connection, DagModel, DagRun
from airflow.utils.db import create_session, provide_session
from airflow.www_rbac.app import csrf
from airflowext.dag_utils import generate_dag_file
from airflowext.sqlalchemy_utils import dbutil, create_external_session
from airflowext.datax_util import DataXConnectionInfo, RDMS2RDMSDataXJob


SYNC_TYPES = ["增量同步", "全量同步"]


def load_interval(text):
    """
    时间文本转timedelta对象

    eg:
        >> load_interval("10s")
           timedelta(seconds=10)
    """
    time_type_trans = {
        "d": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
    }
    matcher = re.compile("^(\d+)([s|h|d|ms])$").match(text)
    if not matcher:
        raise Exception()
    time, time_type = matcher.groups()
    return timedelta(**{time_type_trans[time_type]: int(time)})


def dump_interval(obj):
    if obj.seconds:
        return "%ss" % obj.seconds
    if obj.days:
        return "%sd" % obj.days
    if obj.hours:
        return "%sh" % obj.hours
    if obj.minutes:
        return "%sm" % obj.minutes


class SyncDAGModel(DagModel):
    __tablename__ = 'sync_dag'
    __mapper_args__ = {'polymorphic_identity': 'sync_dag'}
    sync_dag_id = Column(String(ID_LEN), ForeignKey('dag.dag_id'), primary_key=True)
    sync_type = Column(String(50))
    task_json_str = Column(String(5000), default="[]")

    def to_json(self):
        return {
            "name": self.dag_id,
            "sync_type": self.sync_type,
            "interval": dump_interval(self.schedule_interval),
            "state": self.state,
            "tasks": json.loads(self.task_json_str)
        }

    @property
    def state(self):
        return "启用" if self.is_active else "禁用"

    @provide_session
    def refresh_dag_file(self, session=None):
        path = generate_dag_file(self.to_json())
        self.fileloc = path
        session.commit()

    def delete_dag_file(self):
        if os.path.exists(self.fileloc):
            os.unlink(self.fileloc)


class RDMS2RDMSOperator(BaseOperator):
    template_fields = ('src_query_sql',  'tar_table', 'tar_columns')
    ui_color = '#edd5f1'

    @apply_defaults
    def __init__(self,
                 sync_type,
                 src_conn_id,
                 src_query_sql,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 tar_pre_sql,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        assert sync_type in SYNC_TYPES
        self.sync_type = sync_type
        self.src_conn_id = src_conn_id
        self.src_query_sql = src_query_sql
        self.tar_conn_id = tar_conn_id
        self.tar_table = tar_table
        self.tar_columns = tar_columns
        self.tar_pre_sql = tar_pre_sql

    def execute(self, context):
        """
        Execute
        """
        self.log.info('RDMS2RDMSOperator execute...')

        self.task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id
        self.run_datax_job()

    def run_datax_job(self):
        src_conn = DataXConnectionInfo(
            self.s
        )
        job = RDMS2RDMSDataXJob(self.task_id, )


# Will show up under airflow.sensors.test_plugin.PluginSensorOperator
class PluginSensorOperator(BaseSensorOperator):
    pass


# Will show up under airflow.executors.test_plugin.PluginExecutor
class PluginExecutor(BaseExecutor):
    pass


# Will show up under airflow.macros.test_plugin.plugin_macro
# and in templates through {{ macros.test_plugin.plugin_macro }}
def plugin_macro():
    pass


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "datax", __name__,
    template_folder='templates',    # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/datax')

csrf.exempt(bp)


class SyncDAGListView(MethodView):

    @provide_session
    def post(self, session=None):
        """
        增加syncdag

        Input:
            {
                "name": "xx",
                "sync_type": "增量同步",
                "interval": "10s",
                "tasks": [{
                    "name": "yy",
                    "pre_task": "zz",
                    "source":{
                        "conn_id": "",
                        "query_sql": "",
                    },
                    "target":{
                        "conn_id": "",
                        "columns": [""],
                    }
                }]
            }
        """
        params = json.loads(request.data)
        name = params["name"]

        dag = session.query(DagModel).filter_by(dag_id=name).first()
        if dag:
            return jsonify({
                "code": -1,
                "msg": "名字为%s的DAG已存在!" % name
            })

        try:
            interval = load_interval(params["interval"])
        except Exception as e:
            raise e
            return jsonify({
                "code": -1,
                "msg": "interval数据格式错误! %s" % params["interval"]
            })

        dag = SyncDAGModel(
            dag_id=name,
            sync_type=params["sync_type"],
            owners="luke",
            schedule_interval=interval,
            fileloc="",
            task_json_str=json.dumps(params["tasks"]),
            is_active=True,
        )
        session.add(dag)
        session.commit()

        dag.refresh_dag_file()
        return jsonify({
            "status": 0,
            "msg": "新建成功"
        })


class SyncDAGDetailView(MethodView):

    @provide_session
    def delete(self, dag_id, session=None):
        """
        删除DAG
        """
        dag = session.query(SyncDAGModel).get(dag_id)
        if not dag:
            return jsonify({
                "status": -1,
                "msg": "不存在名为%s的dag" % dag_id
            })
        session.delete(dag)
        session.commit()
        dag.delete_dag_file()

        return jsonify({
            "status": 0,
            "msg": "删除成功"
        })

    @provide_session
    def put(self, dag_id, session=None):
        """
        修改DAG
        """
        dag = session.query(SyncDAGModel).get(dag_id)
        if not dag:
            return jsonify({
                "status": -1,
                "msg": "不存在名为%s的dag" % dag_id
            })
        params = json.loads(request.data)
        dag.sync_type = params["sync_type"]
        dag.schedule_interval = load_interval(params["interval"])
        dag.task_json_str = json.dumps(params["tasks"])
        session.commit()
        dag.refresh_dag_file()

        return jsonify({
            "status": 0,
            "msg": "修改成功"
        })

    @provide_session
    def get(self, dag_id, session=None):
        """
        获取DAG
        """
        dag = session.query(SyncDAGModel).get(dag_id)
        if not dag:
            return jsonify({
                "status": -1,
                "msg": "不存在名为%s的dag" % dag_id
            })
        return jsonify(dag.to_json())


@bp.route("/datax/api/connections", methods=["GET"])
@provide_session
@csrf.exempt
def get_connections(session=None):
    conns = session.query(Connection).all()
    conn_ids = [c.conn_id for c in conns]
    return jsonify({
        "code": 0,
        "msg": "OK",
        "connections": conn_ids,
    })


@bp.route("/datax/api/connection/<conn_id>/tables", methods=["GET"])
@csrf.exempt
@provide_session
def get_tables(conn_id, session=None):
    conn = session.query(Connection).filter_by(conn_id=conn_id).one()
    if not conn:
        return jsonify({
            "code": -1,
            "msg": "不存在名为%s的Connection" % conn_id,
        })

    with create_external_session(conn) as external_session:
        tables = dbutil.get_tables(external_session)
    return jsonify({
        "code": 0,
        "msg": "SUCCESS",
        "tables": tables
    })


@bp.route("/datax/api/connection/<conn_id>/table/<table_name>/columns", methods=["GET"])
@csrf.exempt
@provide_session
def get_columns(conn_id, table_name, session=None):
    conn = session.query(Connection).filter_by(conn_id=conn_id).one()
    if not conn:
        return jsonify({
            "code": -1,
            "msg": "不存在名为%s的Connection" % conn_id,
        })
    with create_external_session(conn) as external_session:
        columns = dbutil.get_cloumns(external_session, table_name)
    return jsonify({
        "code": 0,
        "msg": "SUCCESS",
        "tables": columns
    })

bp.add_url_rule('/datax/api/syncdags', view_func=SyncDAGListView.as_view('syncdaglist'))
bp.add_url_rule('/datax/api/syncdag/<dag_id>', view_func=SyncDAGDetailView.as_view('syncdetaillist'))


class DataXDAGView(AppBuilderBaseView):

    @expose('/')
    @provide_session
    def list(self, session=None):
        currentPage = 1
        pageSize = 10
        allPages = 1
        qs = session.query(SyncDAGModel).all()
        dags = []
        for dag in qs:
            dags.append({
                "name": dag.dag_id,
                "sync_type": dag.sync_type,
                "interval": dump_interval(dag.schedule_interval),
                "state": dag.state,
            })
        return self.render_template("datax/list.html",
                                    dags=dags,
                                    pageSize=pageSize,
                                    allPages=allPages,
                                    currentPage=currentPage)

    @expose('/create')
    def dag_add_page(self):
        return self.render_template("datax/add_task.html",
                                    sync_types=SYNC_TYPES)


datax_view = DataXDAGView()
appbuilder_views = {
    "name": "任务列表",
    "category": "同步任务",
    "view": datax_view
}


class DataXPlugin(AirflowPlugin):
    name = "datax"
    operators = [RDMS2RDMSOperator]
    sensors = [PluginSensorOperator]
    hooks = []
    executors = [PluginExecutor]
    macros = [plugin_macro]
    # admin_views = [datax_view]
    flask_blueprints = [bp]
    # menu_links = [ml]
    appbuilder_views = [appbuilder_views]
    # appbuilder_menu_items = []
    # global_operator_extra_links = []

