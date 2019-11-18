# This is the class you derive to create a plugin
import json
import uuid
import subprocess
import os

from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink

# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperatorLink
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.decorators import apply_defaults
from flask_appbuilder import BaseView as AppBuilderBaseView
from airflow.exceptions import AirflowException
from datetime import datetime as dte, timedelta


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

        filter_start_date = dte.utcnow() - timedelta(days=5)
        filter_start_date = filter_start_date.strftime("%Y-%m-%d %H:%M:%S")
        self.src_conn_id = src_conn_id
        self.src_query_sql = src_query_sql + " WHERE create_date > '%s'" % filter_start_date
        self.tar_conn_id = tar_conn_id
        self.tar_table = tar_table
        self.tar_columns = tar_columns
        self.tar_pre_sql = tar_pre_sql + " WHERE create_date > '%s'" % filter_start_date

    def execute(self, context):
        """
        Execute
        """
        self.log.info('src_query_sql: %s', self.src_query_sql)
        self.log.info('tar_pre_sql: %s', self.tar_pre_sql)

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

# Creating a flask admin BaseView
class TestView(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.html
        return self.render("test_plugin/test.html", content="Hello galaxy!")
v = TestView(category="Test Plugin", name="Test View")

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test_plugin", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/test_plugin')

ml = MenuLink(
    category='Test Plugin',
    name='Test Menu Link',
    url='https://airflow.apache.org/')

# Creating a flask appbuilder BaseView
class TestAppBuilderBaseView(AppBuilderBaseView):
    @expose("/")
    def test(self):
        return self.render("test_plugin/test.html", content="Hello galaxy!")

v_appbuilder_view = TestAppBuilderBaseView()
v_appbuilder_package = {"name": "Test View",
                        "category": "Test Plugin",
                        "view": v_appbuilder_view}

# Creating a flask appbuilder Menu Item
appbuilder_mitem = {"name": "Google",
                    "category": "Search",
                    "category_icon": "fa-th",
                    "href": "https://www.google.com"}


# A global operator extra link that redirect you to
# task logs stored in S3
class S3LogLink(BaseOperatorLink):
    name = 'S3'

    def get_link(self, operator, dttm):
        return 'https://s3.amazonaws.com/airflow-logs/{dag_id}/{task_id}/{execution_date}'.format(
            dag_id=operator.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )


# Defining the plugin class
class DataXPlugin(AirflowPlugin):
    name = "datax2"
    operators = [RDMS2RDMSOperator]
    sensors = [PluginSensorOperator]
    hooks = [RDBMS2RDBMSHook]
    executors = [PluginExecutor]
    macros = [plugin_macro]
    # admin_views = [v]
    # flask_blueprints = [bp]
    # menu_links = [ml]
    # appbuilder_views = [v_appbuilder_package]
    # appbuilder_menu_items = [appbuilder_mitem]
    #global_operator_extra_links = [S3LogLink()]

