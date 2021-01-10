import os

from ...engine import url as sa_url
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import follower_url_from_main
from ...testing.provision import log
from ...testing.provision import post_configure_engine
from ...testing.provision import run_reap_dbs
from ...testing.provision import stop_test_class_outside_fixtures
from ...testing.provision import temp_table_keyword_args


@follower_url_from_main.for_db("sqlite")
def _sqlite_follower_url_from_main(url, ident):
    url = sa_url.make_url(url)
    if not url.database or url.database == ":memory:":
        return url
    else:
        return sa_url.make_url("sqlite:///%s.db" % ident)


@post_configure_engine.for_db("sqlite")
def _sqlite_post_configure_engine(url, engine, follower_ident):
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        # use file DBs in all cases, memory acts kind of strangely
        # as an attached
        if not follower_ident:
            # note this test_schema.db gets created for all test runs.
            # there's not any dedicated cleanup step for it.  it in some
            # ways corresponds to the "test.test_schema" schema that's
            # expected to be already present, so for now it just stays
            # in a given checkout directory.
            dbapi_connection.execute(
                'ATTACH DATABASE "test_schema.db" AS test_schema'
            )
        else:
            dbapi_connection.execute(
                'ATTACH DATABASE "%s_test_schema.db" AS test_schema'
                % follower_ident
            )


@create_db.for_db("sqlite")
def _sqlite_create_db(cfg, eng, ident):
    pass


@drop_db.for_db("sqlite")
def _sqlite_drop_db(cfg, eng, ident):
    for path in ["%s.db" % ident, "%s_test_schema.db" % ident]:
        if os.path.exists(path):
            log.info("deleting SQLite database file: %s" % path)
            os.remove(path)


@stop_test_class_outside_fixtures.for_db("sqlite")
def stop_test_class_outside_fixtures(config, db, cls):
    with db.connect() as conn:
        files = [
            row.file
            for row in conn.exec_driver_sql("PRAGMA database_list")
            if row.file
        ]

    if files:
        db.dispose()

        # some sqlite file tests are not cleaning up well yet, so do this
        # just to make things simple for now
        for file in files:
            if file:
                os.remove(file)


@temp_table_keyword_args.for_db("sqlite")
def _sqlite_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}


@run_reap_dbs.for_db("sqlite")
def _reap_sqlite_dbs(url, idents):
    log.info("db reaper connecting to %r", url)

    log.info("identifiers in file: %s", ", ".join(idents))
    for ident in idents:
        # we don't have a config so we can't call _sqlite_drop_db due to the
        # decorator
        for path in ["%s.db" % ident, "%s_test_schema.db" % ident]:
            if os.path.exists(path):
                log.info("deleting SQLite database file: %s" % path)
                os.remove(path)
