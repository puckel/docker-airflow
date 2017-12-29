# encoding: utf-8

import logging
from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker
from airflow import settings, configuration
from airflow.utils.db import provide_session
from airflow.models import User


Base = declarative_base()


class DcmpUserProfile(Base):
    __tablename__ = "dcmp_user_profile"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, nullable=False)
    is_superuser = Column(Boolean, index=True, default=False, nullable=False)
    is_data_profiler = Column(Boolean, index=True, default=False, nullable=False)
    is_approver = Column(Boolean, index=True, default=False, nullable=False)
    approval_notification_emails = Column(Text, default="", nullable=False)
    updated_at = Column(DateTime, index=True, default=datetime.now, onupdate=datetime.now)
    created_at = Column(DateTime, index=True, default=datetime.now)

    def __repr__(self):
        return "<DcmpUserProfile: %s user#%s>" % (self.id, self.user_id)
    
    @property
    @provide_session
    def username(self, session=None):
        user = session.query(User).filter(User.id == self.user_id).first()
        if user:
            return user.username
        return ""
    
    @property
    def approval_notification_emails_list(self):
        return [email.strip() for email in self.approval_notification_emails.split(",") if email.strip()]


def sync_profiles(action=None, target=None):
    session = sessionmaker(autocommit=False, autoflush=False, bind=settings.engine)()
    user_ids = {user.id for user in session.query(User)}
    if action == "insert":
        user_ids.add(target.id)
    elif action == "delete":
        user_ids.remove(target.id)
    profile_user_ids = {profile.user_id for profile in session.query(DcmpUserProfile)}
    no_profile_user_ids = user_ids - profile_user_ids
    no_user_user_ids = profile_user_ids - user_ids
    for user_id in no_profile_user_ids:
        profile = DcmpUserProfile()
        profile.user_id = user_id
        session.add(profile)
    session.query(DcmpUserProfile).filter(DcmpUserProfile.user_id.in_(no_user_user_ids)).delete(synchronize_session=False)
    session.commit()
    session.close()


if __package__:
    try:
        sync_profiles()
    except Exception as e:
        logging.warn("Run python {AIRFLOW_HOME}/plugins/dcmp/tools/upgradedb.py first")
    
    listen(User, 'after_insert', lambda mapper, connection, target: sync_profiles(action="insert", target=target))
    listen(User, 'after_delete', lambda mapper, connection, target: sync_profiles(action="delete", target=target))
    
    if configuration.get('webserver', 'auth_backend').endswith('dcmp.auth.backends.password_auth'):
        from dcmp.auth.backends.password_auth import PasswordUser
        
        listen(PasswordUser, 'after_insert', lambda mapper, connection, target: sync_profiles(action="insert", target=target))
        listen(PasswordUser, 'after_delete', lambda mapper, connection, target: sync_profiles(action="delete", target=target))