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
        print "cleaning %s" % dcmp_dag
        dcmp_dag_conf = dcmp_dag.get_dcmp_dag_conf(session=session)
        dcmp_dag_conf.conf = dag_converter.dict_to_json(dcmp_dag_conf.conf)
        session.commit()
        print "%s cleaned" % dcmp_dag


if __name__ == "__main__":
    main()