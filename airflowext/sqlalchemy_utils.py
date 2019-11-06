# coding: utf-8

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

_session_class = {}


@contextmanager
def open_session(conn):
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


def _build_session(conn_params):
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
    return _session_class[conn_info]()

