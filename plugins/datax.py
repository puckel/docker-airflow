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


SYNC_TYPES = ["增量同步", "全量同步"]


def load_interval(text):
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


class RDMS2RDMSOperator(BaseOperator):
    template_fields = ('src_query_sql',  'tar_table', 'tar_columns')
    ui_color = '#edd5f1'

    @apply_defaults
    def __init__(self,
                 src_conn_id,
                 src_query_sql,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 tar_pre_sql,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
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
        task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        self.hook = RDBMS2RDBMSHook(
                        task_id=task_id,
                        src_conn_id=self.src_conn_id,
                        src_query_sql=self.src_query_sql,
                        tar_conn_id=self.tar_conn_id,
                        tar_table=self.tar_table,
                        tar_columns=self.tar_columns,
                        tar_pre_sql=self.tar_pre_sql,
                    )
        self.hook.execute(context=context)

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.hook.sp.pid), signal.SIGTERM)


class RDBMS2RDBMSHook(BaseHook):
    """
    Datax执行器
    """

    def __init__(self,
                 task_id,
                 src_conn_id,
                 src_query_sql,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 tar_pre_sql):
        self.task_id = task_id
        self.src_conn = self.get_connection(src_conn_id)
        self.src_query_sql = src_query_sql
        self.tar_conn = self.get_connection(tar_conn_id)
        self.tar_table = tar_table
        self.tar_columns = tar_columns
        self.tar_pre_sql = tar_pre_sql

        self.log.info("Source connection: {}:{}/{}".format(self.src_conn.host, self.src_conn.port, self.src_conn.schema))
        self.log.info("Target connection: {}:{}/{}".format(self.tar_conn.host, self.tar_conn.port, self.tar_conn.schema))

    def Popen(self, cmd, **kwargs):
        """
        Remote Popen

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        :return: handle to subprocess
        """
        self.sp = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            **kwargs)

        for line in iter(self.sp.stdout):
            self.log.info(line.strip().decode('utf-8'))

        self.sp.wait()

        self.log.info("Command exited with return code %s", self.sp.returncode)

        if self.sp.returncode:
            raise AirflowException("Execute command failed")

    def generate_setting(self):
        """
         datax速度等设置
        """
        self.setting = {
            "speed": {
                 "byte": 104857600
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        }
        return self.setting

    def generate_reader(self):
        """
        datax reader
        """
        conn = self.src_conn
        conn_type = 'mysql'
        reader_name = 'mysqlreader'
        if(conn.conn_type == 'postgres'):
            conn_type = 'postgresql'
            reader_name = 'postgresqlreader'

        self.src_jdbc_url = "jdbc:"+conn_type+"://"+conn.host.strip()+":" + str(conn.port) + "/" + conn.schema.strip()
        self.reader = {
            "name": reader_name,
            "parameter": {
                "username": conn.login.strip(),
                "password": conn.password.strip(),
                "connection": [
                    {
                        "querySql": [
                            self.src_query_sql
                        ],
                        "jdbcUrl": [
                            self.src_jdbc_url
                        ]
                    }
                ]
            }
        }

        return self.reader

    def generate_writer(self):
        conn = self.tar_conn
        conn_type = 'mysql'
        reader_name = 'mysqlreader'
        if(conn.conn_type == 'postgres'):
            conn_type = 'postgresql'
            reader_name = 'postgresqlreader'

        self.tar_jdbc_url = "jdbc:"+conn_type+"://"+conn.host.strip()+":" + str(conn.port) + "/" + conn.schema.strip()
        self.writer = {
            "name": "postgresqlwriter",
            "parameter": {
                "username": conn.login.strip(),
                "password": conn.password.strip(),
                "column": self.tar_columns,
                "preSql": [
                    self.tar_pre_sql
                ],
                "connection": [{
                    "jdbcUrl": self.tar_jdbc_url,
                    "table": [self.tar_table]
                }]
            }
        }
        return self.writer

    def generate_config(self):
        content = [{
            "reader": self.generate_reader(),
            "writer": self.generate_writer()
        }]

        job = {
            "setting": self.generate_setting(),
            "content": content
        }

        config = {
            "job": job
        }

        self.target_json = json.dumps(config)

        # write json to file
        self.json_file = '/tmp/datax_json_'+self.task_id + uuid.uuid1().hex
        # 打开一个文件
        fo = open(self.json_file, "w")
        fo.write(self.target_json)
        fo.close()
        self.log.info("write config json {}".format(self.json_file))
        return self.json_file

    def execute(self, context):
        self.generate_config()

        # 上传文件
        datax_home = '/opt/datax/bin'
        cmd = ['python', datax_home + '/datax.py', self.json_file]
        self.Popen(cmd)
        # 删除配置文件
        os.remove(self.json_file)


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
        # dag.dag_id = params["name"]
        dag.sync_type = params["sync_type"]
        dag.schedule_interval = load_interval(params["interval"])
        dag.task_json_str = json.dumps(params["tasks"])
        session.commit()
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
    def dag_list_page(self):
        return self.render_template("datax/add_task.html",
                                    sync_types=SYNC_TYPES)

    @expose('/api/add', methods=["POST"])
    def api_add_dag(self):
        """
        {
            "name": "xx",
            "sync_type": "增量同步",
            "interval": "10s",
            "tasks": [
                "name": "yy",
                "pre_task": "zz",
                "source":{

                },
                "target":{
                }
            ]
        }
        """
        return "ADD OK"

    @expose('/api/update')
    def api_update_dag(self):
        pass

    @expose('/api/delete')
    def api_update_dag(self):
        pass


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
    hooks = [RDBMS2RDBMSHook]
    executors = [PluginExecutor]
    macros = [plugin_macro]
    # admin_views = [datax_view]
    flask_blueprints = [bp]
    # menu_links = [ml]
    appbuilder_views = [appbuilder_views]
    # appbuilder_menu_items = []
    # global_operator_extra_links = []

