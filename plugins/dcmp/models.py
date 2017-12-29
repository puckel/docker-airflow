# encoding: utf-8

import json
from datetime import datetime, timedelta

import airflow
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import synonym

from dcmp import settings as dcmp_settings


Base = declarative_base()
ID_LEN = 250


def get_current_user(raw=True):
    try:
        if raw:
            res = airflow.login.current_user.user
        else:
            res = airflow.login.current_user
    except Exception as e:
        res = None
    return res


def get_current_user_approval_notification_emails_list():
    if not dcmp_settings.AUTHENTICATE:
        return []
    if not dcmp_settings.DAG_CREATION_MANAGER_NEED_APPROVER:
        return []
    current_user = get_current_user(raw=False)
    if not current_user:
        return []
    if not hasattr(current_user, "get_approval_notification_emails_list"):
        return []
    return current_user.get_approval_notification_emails_list()


class DcmpDag(Base):
    __tablename__ = "dcmp_dag"

    id = Column(Integer, primary_key=True)
    dag_name = Column(String(ID_LEN), unique=True, nullable=False)
    category = Column(String(50), index=True, default="default", nullable=False)
    version = Column(Integer, default=1, nullable=False)
    approved_version = Column(Integer, default=0, nullable=False)
    approver_user_id = Column(Integer)
    approver_user_name = Column(String(ID_LEN))
    last_approved_at = Column(DateTime, index=True)
    editing = Column(Boolean, index=True, default=False, nullable=False)
    editing_user_id = Column(Integer)
    editing_user_name = Column(String(ID_LEN))
    editing_start = Column(DateTime, index=True)
    last_editor_user_id = Column(Integer)
    last_editor_user_name = Column(String(ID_LEN))
    last_edited_at = Column(DateTime, index=True)
    updated_at = Column(DateTime, index=True, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return self.dag_name
    
    def set_last_editor(self, user):
        if user:
            self.last_editor_user_id = user.id
            self.last_editor_user_name = user.username
    
    def start_editing(self, user):
        if user:
            if not self.editing or user.id != self.editing_user_id:
                self.editing_start = datetime.now()
            self.editing = True
            self.editing_user_id = user.id
            self.editing_user_name = user.username
    
    def set_approver(self, user):
        if user:
            self.approver_user_id = user.id
            self.approver_user_name = user.username
    
    def end_editing(self):
        self.editing = False
    
    @provide_session
    def get_dcmp_dag_conf(self, version=None, session=None):
        if not version:
            version = self.version
        dcmp_dag_conf = session.query(DcmpDagConf).filter(
            DcmpDagConf.dag_id == self.id,
            DcmpDagConf.version == version,
        ).first()
        return dcmp_dag_conf
    
    @provide_session
    def get_conf(self, pure=False, version=None, session=None):
        dcmp_dag_conf = self.get_dcmp_dag_conf(version=version, session=session)
        if dcmp_dag_conf:
            conf = dcmp_dag_conf.conf
            if not pure:
                conf["owner"] = self.last_editor_user_name if self.last_editor_user_name else "airflow"
        else:
            conf = {}
        return conf
    
    @provide_session
    def get_approved_conf(self, pure=False, session=None):
        if self.approved_version <= 0:
            return {}
        return self.get_conf(pure=pure, version=self.approved_version, session=session)
    
    @provide_session
    def approve_conf(self, version=None, user=None, session=None):
        if not version:
            version = self.version
        self.approved_version = version
        self.last_approved_at = datetime.now()
        self.set_approver(user)
        dcmp_dag_conf = self.get_dcmp_dag_conf(version=self.approved_version, session=session)
        dcmp_dag_conf.approved_at = self.last_approved_at
        dcmp_dag_conf.set_approver(user)
    
    @provide_session
    def update_conf(self, conf, user=None, session=None):
        created = self.id is None
        if not created:
            old_conf = self.get_conf(pure=True, session=session)
            if old_conf == conf:
                return None
        self.set_last_editor(user)
        self.last_edited_at = datetime.now()
        self.category = conf.get("category", "default")
        if not created:
            self.version += 1
        if created:
            self.dag_name = conf["dag_name"]
            session.add(self)
        session.flush()
        dcmp_dag_conf = DcmpDagConf()
        dcmp_dag_conf.dag_id = self.id
        dcmp_dag_conf.dag_name = self.dag_name
        dcmp_dag_conf.action = "create" if created else "update"
        dcmp_dag_conf.version = self.version
        dcmp_dag_conf.conf = conf
        dcmp_dag_conf.set_creator(user)
        session.add(dcmp_dag_conf)
        session.flush()
        if dcmp_settings.DAG_CREATION_MANAGER_NEED_APPROVER:
            approval_notification_emails_list = get_current_user_approval_notification_emails_list()
            if approval_notification_emails_list:
                last_dcmp_dag_conf = session.query(DcmpDagConf).filter(
                    DcmpDagConf.dag_id == self.id,
                    DcmpDagConf.version < self.version,
                ).order_by(DcmpDagConf.version.desc()).first()
                if last_dcmp_dag_conf and not last_dcmp_dag_conf.approved_at and self.last_edited_at - last_dcmp_dag_conf.created_at <= timedelta(minutes=5):
                    pass
                else:
                    title = "[Need Approve] %s" % self.dag_name
                    body = (
                        "Approve: <a href='%(approve_url)s'>Link</a><br>"
                        "Host: %(hostname)s<br>"
                    ) % {
                        "approve_url": dcmp_settings.BASE_URL + "/admin/dagcreationmanager/approve?dag_name=" + self.dag_name,
                        "hostname": dcmp_settings.HOSTNAME,
                    }
                    send_email(approval_notification_emails_list, title, body)
        else:
            self.approve_conf(user=user, session=session)
    
    @provide_session
    def delete_conf(self, user=None, session=None):
        dcmp_dag_conf = DcmpDagConf()
        dcmp_dag_conf.dag_id = self.id
        dcmp_dag_conf.dag_name = self.dag_name
        dcmp_dag_conf.action = "delete"
        dcmp_dag_conf.version = self.version + 1
        dcmp_dag_conf.conf = self.get_conf(session=session)
        dcmp_dag_conf.set_creator(user)
        session.add(dcmp_dag_conf)
        session.delete(self)
    
    @classmethod
    @provide_session
    def create_or_update_conf(cls, conf, user=None, session=None):
        dag_name = conf["dag_name"]
        dcmp_dag = session.query(cls).filter(
            cls.dag_name == dag_name,
        ).first()
        if not dcmp_dag:
            dcmp_dag = cls()
        dcmp_dag.update_conf(conf, user=user, session=session)
        return dcmp_dag


class DcmpDagConf(Base):
    __tablename__ = "dcmp_dag_conf"

    id = Column(Integer, primary_key=True)
    dag_id = Column(Integer, index=True, nullable=False)
    dag_name = Column(String(ID_LEN), index=True, nullable=False)
    action = Column(String(50), index=True, nullable=False) # choices are create, update, delete
    version = Column(Integer, index=True, nullable=False)
    _conf = Column('conf', Text, default="{}", nullable=False)
    approver_user_id = Column(Integer)
    approver_user_name = Column(String(ID_LEN))
    approved_at = Column(DateTime, index=True)
    creator_user_id = Column(Integer, nullable=False)
    creator_user_name = Column(String(ID_LEN), nullable=False)
    created_at = Column(DateTime, index=True, default=datetime.now)

    def __repr__(self):
        return "%s(v%s)" % (self.dag_name, self.version)

    def get_conf(self):
        try:
            res = json.loads(self._conf)
        except Exception as e:
            res = {}
        return res

    def set_conf(self, value):
        if value:
            self._conf = json.dumps(value)

    @declared_attr
    def conf(cls):
        return synonym('_conf',
                       descriptor=property(cls.get_conf, cls.set_conf))
    
    def set_approver(self, user):
        if user:
            self.approver_user_id = user.id
            self.approver_user_name = user.username
    
    def set_creator(self, user):
        if user:
            self.creator_user_id = user.id
            self.creator_user_name = user.username
