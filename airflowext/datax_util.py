# -*- coding:utf-8 -*-
import subprocess
import os
import abc
import logging
import json


_logger = logging.getLogger(__name__)


class DataXConnectionInfo(object):

    def __init__(self, db_type, host, port, schema, username, password):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.schema = schema
        self.username = username
        self.password = password

    @property
    def conn_type(self):
        db_type = self.db_type.lower()
        if not hasattr(self, "_conn_type"):
            if db_type in ["mysql"]:
                self._conn_type = "mysql"
            elif db_type in ["postgres", "postgresql", "pg"]:
                self._conn_type = "postgresql"
            else:
                self._conn_type = db_type
        return self._conn_type

    @property
    def reader_name(self):
        name2reader = {
            "mysql": "mysqlreader",
            "postgresql": "postgresqlreader"
        }
        return name2reader[self.conn_type]

    @property
    def writer_name(self):
        name2writer = {
            "mysql": "mysqlwriter",
            "postgresql": "postgresqlwriter"
        }
        return name2writer[self.conn_type]

    @property
    def jdbc_url(self):
        if not hasattr(self, "_jdbc_url"):
            self._jdbc_url = "jdbc:{conn_type}://{host}:{port}/{schema}".format(**dict(
                conn_type=self.conn_type,
                host=self.host,
                port=self.port,
                schema=self.schema
            ))
        return self._jdbc_url


class DataXException(Exception):
    pass


class DataXJob(object):

    __metaclass__ = abc.ABCMeta

    log = _logger

    def __init__(self, job_id):
        self.job_id = job_id

    def on_kill(self):
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)

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

    @abc.abstractmethod
    def generate_reader(self):
        """
        生成DataX reader配置
        """
        pass

    @abc.abstractmethod
    def generate_writer(self):
        """
        生成DataX writer配置
        """

    def generate_config(self):
        """
        生成DataX DAG配置
        """
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
        self.json_file = '/tmp/datax_json_' + self.job_id + uuid.uuid1().hex
        # 打开一个文件
        fo = open(self.json_file, "w")
        fo.write(self.target_json)
        fo.close()
        self.log.info("write config json {}".format(self.json_file))
        return self.json_file

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
            raise DataXException("Execute command failed")

    def execute(self):
        self.generate_config()
        # 上传文件
        datax_home = '/opt/datax/bin'
        cmd = ['python', datax_home + '/datax.py', self.json_file]
        self.Popen(cmd)
        # 删除配置文件
        os.remove(self.json_file)


class RDMS2RDMSDataXJob(DataXJob):

    def __init__(self, job_id, src_conn,
                 tar_conn, src_query_sql,
                 tar_table, tar_columns,
                 tar_pre_sql):
        """
        :param src_conn: `ConnectionInfo`,  source database connection information
        :param tar_conn: `ConnectionInfo`,  target database connection information
        """
        super(RDMS2RDMSDataXJob, self).__init__(job_id)
        self.src_conn = src_conn
        self.tar_conn = tar_conn
        self.src_query_sql = src_query_sql
        self.tar_table = tar_table
        self.tar_columns = tar_columns
        self.tar_pre_sql = tar_pre_sql

    def generate_reader(self):
        """
        datax reader
        """
        conn = self.src_conn
        self.reader = {
            "name": conn.reader_name,
            "parameter": {
                "username": conn.username,
                "password": conn.password,
                "connection": [
                    {
                        "querySql": [
                            self.src_query_sql
                        ],
                        "jdbcUrl": [
                            conn.jdbc_url,
                        ]
                    }
                ]
            }
        }

        return self.reader

    def generate_writer(self):
        """
        生成DataX writer配置
        """
        conn = self.tar_conn
        self.writer = {
            "name": conn.writer_name,
            "parameter": {
                "username": conn.username,
                "password": conn.password,
                "column": self.tar_columns,
                "preSql": [
                    self.tar_pre_sql
                ],
                "connection": [{
                    "jdbcUrl": conn.jdbc_url,
                    "table": [self.tar_table]
                }]
            }
        }
        return self.writer
