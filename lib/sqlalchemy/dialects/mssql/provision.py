import logging

from ... import create_engine
from ... import exc
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import run_reap_dbs
from ...testing.provision import update_db_opts


log = logging.getLogger(__name__)


@update_db_opts.for_db("mssql")
def _mssql_update_db_opts(db_url, db_opts):
    db_opts["legacy_schema_aliasing"] = False


@create_db.for_db("mssql")
def _mssql_create_db(cfg, eng, ident):
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute("create database %s" % ident)
        conn.execute(
            "ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION ON" % ident
        )
        conn.execute(
            "ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT ON" % ident
        )
        conn.execute("use %s" % ident)
        conn.execute("create schema test_schema")
        conn.execute("create schema test_schema_2")


@drop_db.for_db("mssql")
def _mssql_drop_db(cfg, eng, ident):
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        _mssql_drop_ignore(conn, ident)


def _mssql_drop_ignore(conn, ident):
    try:
        # typically when this happens, we can't KILL the session anyway,
        # so let the cleanup process drop the DBs
        # for row in conn.execute(
        #     "select session_id from sys.dm_exec_sessions "
        #        "where database_id=db_id('%s')" % ident):
        #    log.info("killing SQL server sesssion %s", row['session_id'])
        #    conn.execute("kill %s" % row['session_id'])

        conn.execute("drop database %s" % ident)
        log.info("Reaped db: %s", ident)
        return True
    except exc.DatabaseError as err:
        log.warning("couldn't drop db: %s", err)
        return False


@run_reap_dbs.for_db("mssql")
def _reap_mssql_dbs(url, idents):
    log.info("db reaper connecting to %r", url)
    eng = create_engine(url)
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:

        log.info("identifiers in file: %s", ", ".join(idents))

        to_reap = conn.execute(
            "select d.name from sys.databases as d where name "
            "like 'TEST_%' and not exists (select session_id "
            "from sys.dm_exec_sessions "
            "where database_id=d.database_id)"
        )
        all_names = {dbname.lower() for (dbname,) in to_reap}
        to_drop = set()
        for name in all_names:
            if name in idents:
                to_drop.add(name)

        dropped = total = 0
        for total, dbname in enumerate(to_drop, 1):
            if _mssql_drop_ignore(conn, dbname):
                dropped += 1
        log.info(
            "Dropped %d out of %d stale databases detected", dropped, total
        )
