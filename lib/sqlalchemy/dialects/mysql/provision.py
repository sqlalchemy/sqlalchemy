from ...testing.provision import configure_follower
from ...testing.provision import create_db
from ...testing.provision import drop_db
from ...testing.provision import temp_table_keyword_args


@create_db.for_db("mysql")
def _mysql_create_db(cfg, eng, ident):
    with eng.connect() as conn:
        try:
            _mysql_drop_db(cfg, conn, ident)
        except Exception:
            pass

        conn.execute("CREATE DATABASE %s CHARACTER SET utf8mb4" % ident)
        conn.execute(
            "CREATE DATABASE %s_test_schema CHARACTER SET utf8mb4" % ident
        )
        conn.execute(
            "CREATE DATABASE %s_test_schema_2 CHARACTER SET utf8mb4" % ident
        )


@configure_follower.for_db("mysql")
def _mysql_configure_follower(config, ident):
    config.test_schema = "%s_test_schema" % ident
    config.test_schema_2 = "%s_test_schema_2" % ident


@drop_db.for_db("mysql")
def _mysql_drop_db(cfg, eng, ident):
    with eng.connect() as conn:
        conn.execute("DROP DATABASE %s_test_schema" % ident)
        conn.execute("DROP DATABASE %s_test_schema_2" % ident)
        conn.execute("DROP DATABASE %s" % ident)


@temp_table_keyword_args.for_db("mysql")
def _mysql_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}
