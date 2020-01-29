import os

from ...engine import url as sa_url
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import follower_url_from_main
from ...testing.provision import post_configure_engine
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
    if ident:
        os.remove("%s_test_schema.db" % ident)
    else:
        os.remove("%s.db" % ident)


@temp_table_keyword_args.for_db("sqlite")
def _sqlite_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
