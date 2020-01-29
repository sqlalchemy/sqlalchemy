import logging
import time

from ... import exc
from ... import text
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import temp_table_keyword_args


log = logging.getLogger(__name__)


@create_db.for_db("postgresql")
def _pg_create_db(cfg, eng, ident):
    template_db = cfg.options.postgresql_templatedb

    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        try:
            _pg_drop_db(cfg, conn, ident)
        except Exception:
            pass
        if not template_db:
            template_db = conn.scalar("select current_database()")

        attempt = 0
        while True:
            try:
                conn.execute(
                    "CREATE DATABASE %s TEMPLATE %s" % (ident, template_db)
                )
            except exc.OperationalError as err:
                attempt += 1
                if attempt >= 3:
                    raise
                if "accessed by other users" in str(err):
                    log.info(
                        "Waiting to create %s, URI %r, "
                        "template DB %s is in use sleeping for .5",
                        ident,
                        eng.url,
                        template_db,
                    )
                    time.sleep(0.5)
            except:
                raise
            else:
                break


@drop_db.for_db("postgresql")
def _pg_drop_db(cfg, eng, ident):
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(
            text(
                "select pg_terminate_backend(pid) from pg_stat_activity "
                "where usename=current_user and pid != pg_backend_pid() "
                "and datname=:dname"
            ),
            dname=ident,
        )
        conn.execute("DROP DATABASE %s" % ident)


@temp_table_keyword_args.for_db("postgresql")
def _postgresql_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
