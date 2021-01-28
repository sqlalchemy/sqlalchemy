from ... import create_engine
from ... import exc
from ...engine import url as sa_url
from ...testing.provision import configure_follower
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import follower_url_from_main
from ...testing.provision import log
from ...testing.provision import post_configure_engine
from ...testing.provision import run_reap_dbs
from ...testing.provision import stop_test_class
from ...testing.provision import temp_table_keyword_args
from ...testing.provision import update_db_opts


@create_db.for_db("oracle")
def _oracle_create_db(cfg, eng, ident):
    # NOTE: make sure you've run "ALTER DATABASE default tablespace users" or
    # similar, so that the default tablespace is not "system"; reflection will
    # fail otherwise
    with eng.connect() as conn:
        conn.execute("create user %s identified by xe" % ident)
        conn.execute("create user %s_ts1 identified by xe" % ident)
        conn.execute("create user %s_ts2 identified by xe" % ident)
        conn.execute("grant dba to %s" % (ident,))
        conn.execute("grant unlimited tablespace to %s" % ident)
        conn.execute("grant unlimited tablespace to %s_ts1" % ident)
        conn.execute("grant unlimited tablespace to %s_ts2" % ident)


@configure_follower.for_db("oracle")
def _oracle_configure_follower(config, ident):
    config.test_schema = "%s_ts1" % ident
    config.test_schema_2 = "%s_ts2" % ident


def _ora_drop_ignore(conn, dbname):
    try:
        conn.execute("drop user %s cascade" % dbname)
        log.info("Reaped db: %s", dbname)
        return True
    except exc.DatabaseError as err:
        log.warning("couldn't drop db: %s", err)
        return False


@drop_db.for_db("oracle")
def _oracle_drop_db(cfg, eng, ident):
    with eng.connect() as conn:
        # cx_Oracle seems to occasionally leak open connections when a large
        # suite it run, even if we confirm we have zero references to
        # connection objects.
        # while there is a "kill session" command in Oracle,
        # it unfortunately does not release the connection sufficiently.
        _ora_drop_ignore(conn, ident)
        _ora_drop_ignore(conn, "%s_ts1" % ident)
        _ora_drop_ignore(conn, "%s_ts2" % ident)


@update_db_opts.for_db("oracle")
def _oracle_update_db_opts(db_url, db_opts):
    pass


@stop_test_class.for_db("oracle")
def stop_test_class(config, db, cls):
    """run magic command to get rid of identity sequences

    # https://floo.bar/2019/11/29/drop-the-underlying-sequence-of-an-identity-column/

    """

    with db.begin() as conn:
        conn.execute("purge recyclebin")

    # clear statement cache on all connections that were used
    # https://github.com/oracle/python-cx_Oracle/issues/519

    for cx_oracle_conn in _all_conns:
        try:
            sc = cx_oracle_conn.stmtcachesize
        except db.dialect.dbapi.InterfaceError:
            # connection closed
            pass
        else:
            cx_oracle_conn.stmtcachesize = 0
            cx_oracle_conn.stmtcachesize = sc
    _all_conns.clear()


_all_conns = set()


@post_configure_engine.for_db("oracle")
def _oracle_post_configure_engine(url, engine, follower_ident):
    from sqlalchemy import event

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_con, con_record, con_proxy):
        _all_conns.add(dbapi_con)


@run_reap_dbs.for_db("oracle")
def _reap_oracle_dbs(url, idents):
    log.info("db reaper connecting to %r", url)
    eng = create_engine(url)
    with eng.connect() as conn:

        log.info("identifiers in file: %s", ", ".join(idents))

        to_reap = conn.execute(
            "select u.username from all_users u where username "
            "like 'TEST_%' and not exists (select username "
            "from v$session where username=u.username)"
        )
        all_names = {username.lower() for (username,) in to_reap}
        to_drop = set()
        for name in all_names:
            if name.endswith("_ts1") or name.endswith("_ts2"):
                continue
            elif name in idents:
                to_drop.add(name)
                if "%s_ts1" % name in all_names:
                    to_drop.add("%s_ts1" % name)
                if "%s_ts2" % name in all_names:
                    to_drop.add("%s_ts2" % name)

        dropped = total = 0
        for total, username in enumerate(to_drop, 1):
            if _ora_drop_ignore(conn, username):
                dropped += 1
        log.info(
            "Dropped %d out of %d stale databases detected", dropped, total
        )


@follower_url_from_main.for_db("oracle")
def _oracle_follower_url_from_main(url, ident):
    url = sa_url.make_url(url)
    url.username = ident
    url.password = "xe"
    return url


@temp_table_keyword_args.for_db("oracle")
def _oracle_temp_table_keyword_args(cfg, eng):
    return {
        "prefixes": ["GLOBAL TEMPORARY"],
        "oracle_on_commit": "PRESERVE ROWS",
    }
