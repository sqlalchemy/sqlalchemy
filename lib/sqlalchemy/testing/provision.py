from sqlalchemy.engine import url as sa_url
from sqlalchemy import text
from sqlalchemy import exc
from sqlalchemy.util import compat
from . import config, engines
import time

FOLLOWER_IDENT = None


class register(object):
    def __init__(self):
        self.fns = {}

    @classmethod
    def init(cls, fn):
        return register().for_db("*")(fn)

    def for_db(self, dbname):
        def decorate(fn):
            self.fns[dbname] = fn
            return self
        return decorate

    def __call__(self, cfg, *arg):
        if isinstance(cfg, compat.string_types):
            url = sa_url.make_url(cfg)
        elif isinstance(cfg, sa_url.URL):
            url = cfg
        else:
            url = cfg.db.url
        backend = url.get_backend_name()
        if backend in self.fns:
            return self.fns[backend](cfg, *arg)
        else:
            return self.fns['*'](cfg, *arg)


def create_follower_db(follower_ident):

    for cfg in _configs_for_db_operation():
        _create_db(cfg, cfg.db, follower_ident)


def configure_follower(follower_ident):
    for cfg in config.Config.all_configs():
        _configure_follower(cfg, follower_ident)


def setup_config(db_url, options, file_config, follower_ident):
    if follower_ident:
        db_url = _follower_url_from_main(db_url, follower_ident)
    db_opts = {}
    _update_db_opts(db_url, db_opts)
    eng = engines.testing_engine(db_url, db_opts)
    eng.connect().close()
    cfg = config.Config.register(eng, db_opts, options, file_config)
    if follower_ident:
        _configure_follower(cfg, follower_ident)
    return cfg


def drop_follower_db(follower_ident):
    for cfg in _configs_for_db_operation():
        _drop_db(cfg, cfg.db, follower_ident)


def _configs_for_db_operation():
    hosts = set()

    for cfg in config.Config.all_configs():
        cfg.db.dispose()

    for cfg in config.Config.all_configs():
        url = cfg.db.url
        backend = url.get_backend_name()
        host_conf = (
            backend,
            url.username, url.host, url.database)

        if host_conf not in hosts:
            yield cfg
            hosts.add(host_conf)

    for cfg in config.Config.all_configs():
        cfg.db.dispose()


@register.init
def _create_db(cfg, eng, ident):
    raise NotImplementedError("no DB creation routine for cfg: %s" % eng.url)


@register.init
def _drop_db(cfg, eng, ident):
    raise NotImplementedError("no DB drop routine for cfg: %s" % eng.url)


@register.init
def _update_db_opts(db_url, db_opts):
    pass


@register.init
def _configure_follower(cfg, ident):
    pass


@register.init
def _follower_url_from_main(url, ident):
    url = sa_url.make_url(url)
    url.database = ident
    return url


@_update_db_opts.for_db("mssql")
def _mssql_update_db_opts(db_url, db_opts):
    db_opts['legacy_schema_aliasing'] = False


@_follower_url_from_main.for_db("sqlite")
def _sqlite_follower_url_from_main(url, ident):
    url = sa_url.make_url(url)
    if not url.database or url.database == ':memory:':
        return url
    else:
        return sa_url.make_url("sqlite:///%s.db" % ident)


@_create_db.for_db("postgresql")
def _pg_create_db(cfg, eng, ident):
    with eng.connect().execution_options(
            isolation_level="AUTOCOMMIT") as conn:
        try:
            _pg_drop_db(cfg, conn, ident)
        except Exception:
            pass
        currentdb = conn.scalar("select current_database()")
        for attempt in range(3):
            try:
                conn.execute(
                    "CREATE DATABASE %s TEMPLATE %s" % (ident, currentdb))
            except exc.OperationalError as err:
                if attempt != 2 and "accessed by other users" in str(err):
                    time.sleep(.2)
                    continue
                else:
                    raise
            else:
                break


@_create_db.for_db("mysql")
def _mysql_create_db(cfg, eng, ident):
    with eng.connect() as conn:
        try:
            _mysql_drop_db(cfg, conn, ident)
        except Exception:
            pass
        conn.execute("CREATE DATABASE %s" % ident)
        conn.execute("CREATE DATABASE %s_test_schema" % ident)
        conn.execute("CREATE DATABASE %s_test_schema_2" % ident)


@_configure_follower.for_db("mysql")
def _mysql_configure_follower(config, ident):
    config.test_schema = "%s_test_schema" % ident
    config.test_schema_2 = "%s_test_schema_2" % ident


@_create_db.for_db("sqlite")
def _sqlite_create_db(cfg, eng, ident):
    pass


@_drop_db.for_db("postgresql")
def _pg_drop_db(cfg, eng, ident):
    with eng.connect().execution_options(
            isolation_level="AUTOCOMMIT") as conn:
        conn.execute(
            text(
                "select pg_terminate_backend(pid) from pg_stat_activity "
                "where usename=current_user and pid != pg_backend_pid() "
                "and datname=:dname"
            ), dname=ident)
        conn.execute("DROP DATABASE %s" % ident)


@_drop_db.for_db("sqlite")
def _sqlite_drop_db(cfg, eng, ident):
    pass
    #os.remove("%s.db" % ident)


@_drop_db.for_db("mysql")
def _mysql_drop_db(cfg, eng, ident):
    with eng.connect() as conn:
        try:
            conn.execute("DROP DATABASE %s_test_schema" % ident)
        except Exception:
            pass
        try:
            conn.execute("DROP DATABASE %s_test_schema_2" % ident)
        except Exception:
            pass
        try:
            conn.execute("DROP DATABASE %s" % ident)
        except Exception:
            pass


@_create_db.for_db("oracle")
def _oracle_create_db(cfg, eng, ident):
    # NOTE: make sure you've run "ALTER DATABASE default tablespace users" or
    # similar, so that the default tablespace is not "system"; reflection will
    # fail otherwise
    with eng.connect() as conn:
        conn.execute("create user %s identified by xe" % ident)
        conn.execute("create user %s_ts1 identified by xe" % ident)
        conn.execute("create user %s_ts2 identified by xe" % ident)
        conn.execute("grant dba to %s" % (ident, ))
        conn.execute("grant unlimited tablespace to %s" % ident)
        conn.execute("grant unlimited tablespace to %s_ts1" % ident)
        conn.execute("grant unlimited tablespace to %s_ts2" % ident)

@_configure_follower.for_db("oracle")
def _oracle_configure_follower(config, ident):
    config.test_schema = "%s_ts1" % ident
    config.test_schema_2 = "%s_ts2" % ident


@_drop_db.for_db("oracle")
def _oracle_drop_db(cfg, eng, ident):
    with eng.connect() as conn:
        conn.execute("drop user %s cascade" % ident)
        conn.execute("drop user %s_ts1 cascade" % ident)
        conn.execute("drop user %s_ts2 cascade" % ident)


@_follower_url_from_main.for_db("oracle")
def _oracle_follower_url_from_main(url, ident):
    url = sa_url.make_url(url)
    url.username = ident
    url.password = 'xe'
    return url


