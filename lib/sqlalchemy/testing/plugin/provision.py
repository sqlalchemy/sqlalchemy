from sqlalchemy.engine import url as sa_url

def create_follower_db(follower_ident):
    from .. import config, engines

    follower_ident = "test_%s" % follower_ident

    hosts = set()

    for cfg in config.Config.all_configs():
        url = cfg.db.url
        backend = url.get_backend_name()
        host_conf = (
            backend,
            url.username, url.host, url.database)

        if host_conf not in hosts:
            if backend.startswith("postgresql"):
                _pg_create_db(cfg.db, follower_ident)
            elif backend.startswith("mysql"):
                _mysql_create_db(cfg.db, follower_ident)

            new_url = sa_url.make_url(str(url))

            new_url.database = follower_ident
            eng = engines.testing_engine(new_url, cfg.db_opts)

            if backend.startswith("postgresql"):
                _pg_init_db(eng)
            elif backend.startswith("mysql"):
                _mysql_init_db(eng)

            hosts.add(host_conf)


def _pg_create_db(eng, ident):
    with eng.connect().execution_options(
            isolation_level="AUTOCOMMIT") as conn:
        try:
            conn.execute("DROP DATABASE %s" % ident)
        except:
            pass
        conn.execute("CREATE DATABASE %s" % ident)


def _pg_init_db(eng):
    with eng.connect() as conn:
        conn.execute("CREATE SCHEMA test_schema")
        conn.execute("CREATE SCHEMA test_schema_2")


def _mysql_create_db(eng, ident):
    with eng.connect() as conn:
        try:
            conn.execute("DROP DATABASE %s" % ident)
        except:
            pass
        conn.execute("CREATE DATABASE %s" % ident)


def _mysql_init_db(eng):
    pass
