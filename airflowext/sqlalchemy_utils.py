# coding: utf-8

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

_session_class = {}


@contextmanager
def create_external_session(conn):
    """
    return a sqlalchemy database session.

    usage:
        with open_session(conn) as session:
            pass
    """
    session = _build_session(trans_conn(conn))
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def trans_conn(conn):
    """
    extract connection info from airflow connection object
    """
    conn_type = conn.conn_type
    if conn.conn_type == "postgres":
        conn_type = "postgresql"
    host = conn.host.strip()
    port = str(conn.port)
    username = conn.login.strip()
    password = conn.password.strip()
    schema = conn.schema.strip()
    return {
        "conn_type": conn_type,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "schema": schema
    }


def _build_external_session(conn_params):
    """
    new a sqlalchemy database session.
    """
    conn_info = '{conn_type}://{username}:{password}@{host}:{port}/{schema}'
    conn_info = conn_info.format(**conn_params)
    global _session_class
    if conn_info not in _session_class:
        engine = create_engine(conn_info)
        Session = sessionmaker(bind=engine)
        _session_class[conn_info] = Session
    session = _session_class[conn_info]()
    session.conn_type = conn_params["conn_type"]
    return session


def get_external_tables(session):
    """
    return all tables of given database session

    目前只支持PG库
    """
    assert session.conn_type == "postgresql"
    result = session.execute("select tablename from pg_tables where schemaname='public'")
    tables = []
    for row in result.fetchall():
        tables.append(row[0])
    return tables


def get_external_columns(session, table):
    assert session.conn_type == "postgresql"
    sql = "select column_name,data_type from information_schema.columns where table_name='%s';" % table
    print(sql)
    result = session.execute(sql)
    tables = []
    for row in result.fetchall():
        tables.append({
            "column_name": row[0],
            "data_type": row[1]
        })
    return tables


class ExternalDBUtil(object):

    def get_tables(self, session):
        return get_external_tables(session)

    def get_cloumns(self, session, table):
        return get_external_columns(session, table)


dbutil = ExternalDBUtil()


if __name__ == "__main__":
    data = {
        "conn_type": "postgresql",
        "host": "pg_master",
        "port": 5432,
        "username": "odoo",
        "password": "odoo",
        "schema": "erp"
    }

    sess = _build_external_session(data)
    # print(get_external_tables(sess))
    print(get_external_columns(sess, "sale_order"))
