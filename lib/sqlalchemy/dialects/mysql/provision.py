import copy

from ... import exc
from ...testing.provision import configure_follower
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import generate_driver_url
from ...testing.provision import temp_table_keyword_args


@generate_driver_url.for_db("mysql", "mariadb")
def generate_driver_url(url, driver, query):
    backend = url.get_backend_name()

    if backend == "mysql":
        dialect_cls = url.get_dialect()
        if dialect_cls._is_mariadb_from_url(url):
            backend = "mariadb"

    new_url = copy.copy(url)
    new_url.query = dict(new_url.query)
    new_url.drivername = "%s+%s" % (backend, driver)
    new_url.query.update(query)

    try:
        new_url.get_dialect()
    except exc.NoSuchModuleError:
        return None
    else:
        return new_url


@create_db.for_db("mysql", "mariadb")
def _mysql_create_db(cfg, eng, ident):
    with eng.connect() as conn:
        try:
            _mysql_drop_db(cfg, conn, ident)
        except Exception:
            pass

        conn.exec_driver_sql(
            "CREATE DATABASE %s CHARACTER SET utf8mb4" % ident
        )
        conn.exec_driver_sql(
            "CREATE DATABASE %s_test_schema CHARACTER SET utf8mb4" % ident
        )
        conn.exec_driver_sql(
            "CREATE DATABASE %s_test_schema_2 CHARACTER SET utf8mb4" % ident
        )


@configure_follower.for_db("mysql", "mariadb")
def _mysql_configure_follower(config, ident):
    config.test_schema = "%s_test_schema" % ident
    config.test_schema_2 = "%s_test_schema_2" % ident


@drop_db.for_db("mysql", "mariadb")
def _mysql_drop_db(cfg, eng, ident):
    with eng.connect() as conn:
        conn.exec_driver_sql("DROP DATABASE %s_test_schema" % ident)
        conn.exec_driver_sql("DROP DATABASE %s_test_schema_2" % ident)
        conn.exec_driver_sql("DROP DATABASE %s" % ident)


@temp_table_keyword_args.for_db("mysql", "mariadb")
def _mysql_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
