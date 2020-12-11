# coding: utf-8

import datetime

from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.dialects import mysql
from sqlalchemy.engine.url import make_url
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from ...engine import test_deprecations


class BackendDialectTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "mysql", "mariadb"

    def test_no_show_variables(self):
        from sqlalchemy.testing import mock

        engine = engines.testing_engine()

        def my_execute(self, statement, *args, **kw):
            if statement.startswith("SHOW VARIABLES"):
                statement = "SELECT 1 FROM DUAL WHERE 1=0"
            return real_exec(self, statement, *args, **kw)

        real_exec = engine._connection_cls.exec_driver_sql
        with mock.patch.object(
            engine._connection_cls, "exec_driver_sql", my_execute
        ):
            with expect_warnings(
                "Could not retrieve SQL_MODE; please ensure the "
                "MySQL user has permissions to SHOW VARIABLES"
            ):
                engine.connect()

    def test_no_default_isolation_level(self):
        from sqlalchemy.testing import mock

        engine = engines.testing_engine()

        real_isolation_level = testing.db.dialect.get_isolation_level

        def fake_isolation_level(connection):
            connection = mock.Mock(
                cursor=mock.Mock(
                    return_value=mock.Mock(
                        fetchone=mock.Mock(return_value=None)
                    )
                )
            )
            return real_isolation_level(connection)

        with mock.patch.object(
            engine.dialect, "get_isolation_level", fake_isolation_level
        ):
            with expect_warnings(
                "Could not retrieve transaction isolation level for MySQL "
                "connection."
            ):
                engine.connect()

    def test_autocommit_isolation_level(self):
        c = testing.db.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )
        assert c.exec_driver_sql("SELECT @@autocommit;").scalar()

        c = c.execution_options(isolation_level="READ COMMITTED")
        assert not c.exec_driver_sql("SELECT @@autocommit;").scalar()

    def test_isolation_level(self):
        values = [
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "REPEATABLE READ",
            "SERIALIZABLE",
        ]
        for value in values:
            c = testing.db.connect().execution_options(isolation_level=value)
            eq_(testing.db.dialect.get_isolation_level(c.connection), value)


class DialectTest(fixtures.TestBase):
    @testing.combinations(
        (None, "cONnection was kILLEd", "InternalError", "pymysql", True),
        (None, "cONnection aLREady closed", "InternalError", "pymysql", True),
        (None, "something broke", "InternalError", "pymysql", False),
        (1927, "Connection was killed", "OperationalError", "pymysql", True),
        (1927, "Connection was killed", "OperationalError", "mysqldb", True),
        (2006, "foo", "OperationalError", "mysqldb", True),
        (2006, "foo", "OperationalError", "pymysql", True),
        (2007, "foo", "OperationalError", "mysqldb", False),
        (2007, "foo", "OperationalError", "pymysql", False),
    )
    def test_is_disconnect(
        self, arg0, message, exc_cls_name, dialect_name, is_disconnect
    ):
        class Error(Exception):
            pass

        dbapi = mock.Mock()
        dbapi.Error = Error
        dbapi.ProgrammingError = type("ProgrammingError", (Error,), {})
        dbapi.OperationalError = type("OperationalError", (Error,), {})
        dbapi.InterfaceError = type("InterfaceError", (Error,), {})
        dbapi.InternalError = type("InternalError", (Error,), {})

        dialect = getattr(mysql, dialect_name).dialect(dbapi=dbapi)

        error = getattr(dbapi, exc_cls_name)(arg0, message)
        eq_(dialect.is_disconnect(error, None, None), is_disconnect)

    def test_ssl_arguments_mysqldb(self):
        from sqlalchemy.dialects.mysql import mysqldb

        dialect = mysqldb.dialect()
        self._test_ssl_arguments(dialect)

    def test_ssl_arguments_oursql(self):
        from sqlalchemy.dialects.mysql import oursql

        dialect = oursql.dialect()
        self._test_ssl_arguments(dialect)

    def _test_ssl_arguments(self, dialect):
        kwarg = dialect.create_connect_args(
            make_url(
                "mysql://scott:tiger@localhost:3306/test"
                "?ssl_ca=/ca.pem&ssl_cert=/cert.pem&ssl_key=/key.pem"
            )
        )[1]
        # args that differ among mysqldb and oursql
        for k in ("use_unicode", "found_rows", "client_flag"):
            kwarg.pop(k, None)
        eq_(
            kwarg,
            {
                "passwd": "tiger",
                "db": "test",
                "ssl": {
                    "ca": "/ca.pem",
                    "cert": "/cert.pem",
                    "key": "/key.pem",
                },
                "host": "localhost",
                "user": "scott",
                "port": 3306,
            },
        )

    @testing.combinations(
        ("compress", True),
        ("connect_timeout", 30),
        ("read_timeout", 30),
        ("write_timeout", 30),
        ("client_flag", 1234),
        ("local_infile", 1234),
        ("use_unicode", False),
        ("charset", "hello"),
    )
    def test_normal_arguments_mysqldb(self, kwarg, value):
        from sqlalchemy.dialects.mysql import mysqldb

        dialect = mysqldb.dialect()
        connect_args = dialect.create_connect_args(
            make_url(
                "mysql://scott:tiger@localhost:3306/test"
                "?%s=%s" % (kwarg, value)
            )
        )

        eq_(connect_args[1][kwarg], value)

    def test_mysqlconnector_buffered_arg(self):
        from sqlalchemy.dialects.mysql import mysqlconnector

        dialect = mysqlconnector.dialect()
        kw = dialect.create_connect_args(
            make_url("mysql+mysqlconnector://u:p@host/db?buffered=true")
        )[1]
        eq_(kw["buffered"], True)

        kw = dialect.create_connect_args(
            make_url("mysql+mysqlconnector://u:p@host/db?buffered=false")
        )[1]
        eq_(kw["buffered"], False)

        kw = dialect.create_connect_args(
            make_url("mysql+mysqlconnector://u:p@host/db")
        )[1]
        eq_(kw["buffered"], True)

    def test_mysqlconnector_raise_on_warnings_arg(self):
        from sqlalchemy.dialects.mysql import mysqlconnector

        dialect = mysqlconnector.dialect()
        kw = dialect.create_connect_args(
            make_url(
                "mysql+mysqlconnector://u:p@host/db?raise_on_warnings=true"
            )
        )[1]
        eq_(kw["raise_on_warnings"], True)

        kw = dialect.create_connect_args(
            make_url(
                "mysql+mysqlconnector://u:p@host/db?raise_on_warnings=false"
            )
        )[1]
        eq_(kw["raise_on_warnings"], False)

        kw = dialect.create_connect_args(
            make_url("mysql+mysqlconnector://u:p@host/db")
        )[1]
        assert "raise_on_warnings" not in kw

    @testing.only_on(
        [
            "mysql+mysqldb",
            "mysql+pymysql",
            "mariadb+mysqldb",
            "mariadb+pymysql",
        ]
    )
    def test_random_arg(self):
        dialect = testing.db.dialect
        kw = dialect.create_connect_args(
            make_url("mysql://u:p@host/db?foo=true")
        )[1]
        eq_(kw["foo"], "true")

    @testing.only_on(
        [
            "mysql+mysqldb",
            "mysql+pymysql",
            "mariadb+mysqldb",
            "mariadb+pymysql",
        ]
    )
    def test_special_encodings(self):

        for enc in ["utf8mb4", "utf8"]:
            eng = engines.testing_engine(
                options={"connect_args": {"charset": enc, "use_unicode": 0}}
            )
            conn = eng.connect()
            eq_(conn.dialect._connection_charset, enc)


class ParseVersionTest(fixtures.TestBase):
    def test_mariadb_madness(self):
        mysql_dialect = make_url("mysql://").get_dialect()()

        is_(mysql_dialect.is_mariadb, False)

        mysql_dialect = make_url("mysql+pymysql://").get_dialect()()
        is_(mysql_dialect.is_mariadb, False)

        mariadb_dialect = make_url("mariadb://").get_dialect()()

        is_(mariadb_dialect.is_mariadb, True)

        mariadb_dialect = make_url("mariadb+pymysql://").get_dialect()()

        is_(mariadb_dialect.is_mariadb, True)

        assert_raises_message(
            exc.InvalidRequestError,
            "MySQL version 5.7.20 is not a MariaDB variant.",
            mariadb_dialect._parse_server_version,
            "5.7.20",
        )

    def test_502_minimum(self):
        dialect = mysql.dialect()
        assert_raises_message(
            NotImplementedError,
            "the MySQL/MariaDB dialect supports server "
            "version info 5.0.2 and above.",
            dialect._parse_server_version,
            "5.0.1",
        )

    @testing.combinations(
        ((10, 2, 7), "10.2.7-MariaDB", (10, 2, 7), True),
        (
            (10, 2, 7),
            "5.6.15.10.2.7-MariaDB",
            (5, 6, 15, 10, 2, 7),
            True,
        ),
        ((5, 0, 51, 24), "5.0.51a.24+lenny5", (5, 0, 51, 24), False),
        ((10, 2, 10), "10.2.10-MariaDB", (10, 2, 10), True),
        ((5, 7, 20), "5.7.20", (5, 7, 20), False),
        ((5, 6, 15), "5.6.15", (5, 6, 15), False),
        (
            (10, 2, 6),
            "10.2.6.MariaDB.10.2.6+maria~stretch-log",
            (10, 2, 6, 10, 2, 6),
            True,
        ),
        (
            (10, 1, 9),
            "10.1.9-MariaDBV1.0R050D002-20170809-1522",
            (10, 1, 9, 20170809, 1522),
            True,
        ),
    )
    def test_mariadb_normalized_version(
        self, expected, raw_version, version, is_mariadb
    ):
        dialect = mysql.dialect()
        eq_(dialect._parse_server_version(raw_version), version)
        dialect.server_version_info = version
        eq_(dialect._mariadb_normalized_version_info, expected)
        assert dialect._is_mariadb is is_mariadb

    @testing.combinations(
        (True, "10.2.7-MariaDB"),
        (True, "5.6.15-10.2.7-MariaDB"),
        (False, "10.2.10-MariaDB"),
        (False, "5.7.20"),
        (False, "5.6.15"),
        (True, "10.2.6-MariaDB-10.2.6+maria~stretch.log"),
    )
    def test_mariadb_check_warning(self, expect_, version):
        dialect = mysql.dialect(is_mariadb="MariaDB" in version)
        dialect._parse_server_version(version)
        if expect_:
            with expect_warnings(
                ".*before 10.2.9 has known issues regarding "
                "CHECK constraints"
            ):
                dialect._warn_for_known_db_issues()
        else:
            dialect._warn_for_known_db_issues()


class RemoveUTCTimestampTest(fixtures.TablesTest):
    """This test exists because we removed the MySQL dialect's
    override of the UTC_TIMESTAMP() function, where the commit message
    for this feature stated that "it caused problems with executemany()".
    Since no example was provided, we are trying lots of combinations
    here.

    [ticket:3966]

    """

    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("data", DateTime),
        )

        Table(
            "t_default",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("idata", DateTime, default=func.utc_timestamp()),
            Column("udata", DateTime, onupdate=func.utc_timestamp()),
        )

    def test_insert_executemany(self, connection):
        conn = connection
        conn.execute(
            self.tables.t.insert().values(data=func.utc_timestamp()),
            [{"x": 5}, {"x": 6}, {"x": 7}],
        )

    def test_update_executemany(self, connection):
        conn = connection
        timestamp = datetime.datetime(2015, 4, 17, 18, 5, 2)
        conn.execute(
            self.tables.t.insert(),
            [
                {"x": 5, "data": timestamp},
                {"x": 6, "data": timestamp},
                {"x": 7, "data": timestamp},
            ],
        )

        conn.execute(
            self.tables.t.update()
            .values(data=func.utc_timestamp())
            .where(self.tables.t.c.x == bindparam("xval")),
            [{"xval": 5}, {"xval": 6}, {"xval": 7}],
        )

    def test_insert_executemany_w_default(self, connection):
        conn = connection
        conn.execute(
            self.tables.t_default.insert(), [{"x": 5}, {"x": 6}, {"x": 7}]
        )

    def test_update_executemany_w_default(self, connection):
        conn = connection
        timestamp = datetime.datetime(2015, 4, 17, 18, 5, 2)
        conn.execute(
            self.tables.t_default.insert(),
            [
                {"x": 5, "idata": timestamp},
                {"x": 6, "idata": timestamp},
                {"x": 7, "idata": timestamp},
            ],
        )

        conn.execute(
            self.tables.t_default.update()
            .values(idata=func.utc_timestamp())
            .where(self.tables.t_default.c.x == bindparam("xval")),
            [{"xval": 5}, {"xval": 6}, {"xval": 7}],
        )


class SQLModeDetectionTest(fixtures.TestBase):
    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    def _options(self, modes):
        def connect(con, record):
            cursor = con.cursor()
            cursor.execute("set sql_mode='%s'" % (",".join(modes)))

        e = engines.testing_engine(
            options={
                "pool_events": [
                    (connect, "first_connect"),
                    (connect, "connect"),
                ]
            }
        )
        return e

    def test_backslash_escapes(self):
        engine = self._options(["NO_BACKSLASH_ESCAPES"])
        c = engine.connect()
        assert not engine.dialect._backslash_escapes
        c.close()
        engine.dispose()

        engine = self._options([])
        c = engine.connect()
        assert engine.dialect._backslash_escapes
        c.close()
        engine.dispose()

    def test_ansi_quotes(self):
        engine = self._options(["ANSI_QUOTES"])
        c = engine.connect()
        assert engine.dialect._server_ansiquotes
        c.close()
        engine.dispose()

    def test_combination(self):
        engine = self._options(["ANSI_QUOTES,NO_BACKSLASH_ESCAPES"])
        c = engine.connect()
        assert engine.dialect._server_ansiquotes
        assert not engine.dialect._backslash_escapes
        c.close()
        engine.dispose()


class ExecutionTest(fixtures.TestBase):
    """Various MySQL execution special cases."""

    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    def test_charset_caching(self):
        engine = engines.testing_engine()

        cx = engine.connect()
        meta = MetaData()
        charset = engine.dialect._detect_charset(cx)

        meta.reflect(cx)
        eq_(cx.dialect._connection_charset, charset)
        cx.close()

    def test_sysdate(self):
        d = testing.db.scalar(func.sysdate())
        assert isinstance(d, datetime.datetime)


class AutocommitTextTest(
    test_deprecations.AutocommitKeywordFixture, fixtures.TestBase
):
    __only_on__ = "mysql", "mariadb"

    def test_load_data(self):
        self._test_keyword("LOAD DATA STUFF")

    def test_replace(self):
        self._test_keyword("REPLACE THING")
