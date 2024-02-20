# do not import sqlalchemy testing feature in this file, since it's
# run directly, not passing through pytest
from sqlalchemy import create_engine


def _get_version(conn):
    # this is the suggested way of finding the mode, from
    # https://python-oracledb.readthedocs.io/en/latest/user_guide/tracing.html#vsessconinfo
    sql = (
        "SELECT UNIQUE CLIENT_DRIVER "
        "FROM V$SESSION_CONNECT_INFO "
        "WHERE SID = SYS_CONTEXT('USERENV', 'SID')"
    )
    return conn.exec_driver_sql(sql).scalar()


def run_thin_mode(url, queue, **kw):
    e = create_engine(url, **kw)
    with e.connect() as conn:
        res = _get_version(conn)
        queue.put((res, e.dialect.is_thin_mode(conn)))
    e.dispose()


def run_thick_mode(url, queue, **kw):
    e = create_engine(url, **kw)
    with e.connect() as conn:
        res = _get_version(conn)
        queue.put((res, e.dialect.is_thin_mode(conn)))
    e.dispose()
