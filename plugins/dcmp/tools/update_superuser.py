# encoding: utf-8

from importlib import import_module

import airflow
from airflow import models, settings, configuration

from dcmp.auth.models import DcmpUserProfile


def update_superuser(username):
    if not username:
        raise ValueError("username required")
    session = settings.Session()
    user = session.query(models.User).filter(models.User.username==username).first()
    if not user:
        session.close()
        raise ValueError("user does not exists")
    profile = session.query(DcmpUserProfile).filter(DcmpUserProfile.user_id==user.id).first()
    if not profile:
        session.close()
        raise ValueError("profile does not exists")
    profile.is_superuser = True
    session.commit()
    session.close()


def main():
    username = raw_input("Enter User Name: ")
    update_superuser(username)
    print "Finished."


if __name__ == "__main__":
    main()