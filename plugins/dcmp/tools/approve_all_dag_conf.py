# encoding: utf-8

import os
import sys
sys.path.append( os.path.join(os.path.dirname(__file__), '..', '..') )

import airflow
from dcmp.models import DcmpDag
from dcmp.dag_converter import dag_converter
from airflow.utils.db import provide_session


@provide_session
def main(session=None):
    dcmp_dags = session.query(DcmpDag).order_by(DcmpDag.dag_name).all()
    for dcmp_dag in dcmp_dags:
        if dcmp_dag.version != dcmp_dag.approved_version:
            dcmp_dag.approve_conf(session=session)
    session.commit()
    dag_converter.refresh_dags()


if __name__ == "__main__":
    main()