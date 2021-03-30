# coding: utf-8

import re

from sqlalchemy import bindparam
from sqlalchemy import Computed
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import outparam
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy.dialects.oracle import base as oracle
from sqlalchemy.dialects.oracle import cx_oracle
from sqlalchemy.engine import url
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.suite import test_select
from sqlalchemy.util import u
from sqlalchemy.util import ue


class DialectTest(fixtures.TestBase):
    def test_cx_oracle_version_parse(self):
        dialect = cx_oracle.OracleDialect_cx_oracle()

        eq_(dialect._parse_cx_oracle_ver("5.2"), (5, 2))

        eq_(dialect._parse_cx_oracle_ver("5.0.1"), (5, 0, 1))

        eq_(dialect._parse_cx_oracle_ver("6.0b1"), (6, 0))

    def test_minimum_version(self):
        with mock.patch(
            "sqlalchemy.dialects.oracle.cx_oracle.OracleDialect_cx_oracle."
            "_parse_cx_oracle_ver",
            lambda self, vers: (5, 1, 5),
        ):
            assert_raises_message(
                exc.InvalidRequestError,
                "cx_Oracle version 5.2 and above are supported",
                cx_oracle.OracleDialect_cx_oracle,
                dbapi=Mock(),
            )

        with mock.patch(
            "sqlalchemy.dialects.oracle.cx_oracle.OracleDialect_cx_oracle."
            "_parse_cx_oracle_ver",
            lambda self, vers: (5, 3, 1),
        ):
            cx_oracle.OracleDialect_cx_oracle(dbapi=Mock())


class DialectWBackendTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "oracle"

    def test_hypothetical_not_implemented_isolation_level(self):
        engine = engines.testing_engine()

        def get_isolation_level(connection):
            raise NotImplementedError

        with mock.patch.object(
            engine.dialect, "get_isolation_level", get_isolation_level
        ):
            conn = engine.connect()

            # for NotImplementedError we get back None.  But the
            # cx_Oracle dialect does not raise this.
            eq_(conn.dialect.default_isolation_level, None)

            dbapi_conn = conn.connection.connection

            eq_(
                testing.db.dialect.get_isolation_level(dbapi_conn),
                "READ COMMITTED",
            )

    def test_graceful_failure_isolation_level_not_available(self):
        engine = engines.testing_engine()

        def get_isolation_level(connection):
            raise exc.DBAPIError(
                "get isolation level",
                {},
                engine.dialect.dbapi.Error("isolation level failed"),
            )

        with mock.patch.object(
            engine.dialect, "get_isolation_level", get_isolation_level
        ):
            conn = engine.connect()
            eq_(conn.dialect.default_isolation_level, "READ COMMITTED")

            # test that we can use isolation level setting and that it
            # reverts for "real" back to READ COMMITTED even though we
            # can't read it
            dbapi_conn = conn.connection.connection

            conn = conn.execution_options(isolation_level="SERIALIZABLE")
            eq_(
                testing.db.dialect.get_isolation_level(dbapi_conn),
                "SERIALIZABLE",
            )

            conn.close()
            eq_(
                testing.db.dialect.get_isolation_level(dbapi_conn),
                "READ COMMITTED",
            )

            with engine.connect() as conn:
                assert_raises_message(
                    exc.DBAPIError,
                    r".*isolation level failed.*",
                    conn.get_isolation_level,
                )


class DefaultSchemaNameTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "oracle"

    def test_default_name_is_the_user(self):
        default_schema_name = testing.db.dialect.default_schema_name

        with testing.db.connect() as conn:
            oracles_known_default_schema_name = (
                testing.db.dialect.normalize_name(
                    conn.exec_driver_sql("SELECT USER FROM DUAL").scalar()
                )
            )

        eq_(oracles_known_default_schema_name, default_schema_name)

    def test_default_schema_detected(self):
        default_schema_name = testing.db.dialect.default_schema_name

        eng = engines.testing_engine()

        with eng.connect() as conn:

            trans = conn.begin()
            eq_(
                testing.db.dialect._get_default_schema_name(conn),
                default_schema_name,
            )

            conn.exec_driver_sql(
                "ALTER SESSION SET CURRENT_SCHEMA=%s" % config.test_schema
            )

            eq_(
                testing.db.dialect._get_default_schema_name(conn),
                config.test_schema,
            )

            conn.invalidate()
            trans.rollback()

            eq_(
                testing.db.dialect._get_default_schema_name(conn),
                default_schema_name,
            )


class EncodingErrorsTest(fixtures.TestBase):
    """mock test for encoding_errors.

    While we tried to write a round trip test, I could only reproduce the
    problem on Python 3 and only for STRING/CHAR.  I couldn't get a CLOB to
    come back with broken encoding and also under py2k cx_Oracle would always
    return a bytestring with the correct encoding.    Since the test barely
    worked, it is not included here to avoid future problems.  It's not clear
    what other levels of encode/decode are going on such that explicitly
    selecting for AL16UTF16 is still returning a utf-8 bytestring under py2k or
    for CLOBs, nor is it really  clear that this flag is useful, however, at
    least for the Py3K case, cx_Oracle supports the flag and we did have one
    user reporting that they had a (non-reproducible) database which
    illustrated the problem so we will pass it in.

    """

    # NOTE: these numbers are arbitrary, they are not the actual
    # cx_Oracle constants
    cx_Oracle_NUMBER = 0
    cx_Oracle_STRING = 1
    cx_Oracle_FIXED_CHAR = 2
    cx_Oracle_CLOB = 3
    cx_Oracle_NCLOB = 4

    @testing.fixture
    def cx_Oracle(self):
        return mock.Mock(
            NUMBER=self.cx_Oracle_NUMBER,
            STRING=self.cx_Oracle_STRING,
            FIXED_CHAR=self.cx_Oracle_FIXED_CHAR,
            CLOB=self.cx_Oracle_CLOB,
            NCLOB=self.cx_Oracle_NCLOB,
            version="7.0.1",
            __future__=mock.Mock(),
        )

    _oracle_char_combinations = testing.combinations(
        (
            "STRING",
            cx_Oracle_STRING,
        ),
        (
            "FIXED_CHAR",
            cx_Oracle_FIXED_CHAR,
        ),
        (
            "CLOB",
            cx_Oracle_CLOB,
        ),
        (
            "NCLOB",
            cx_Oracle_NCLOB,
        ),
        argnames="cx_oracle_type",
        id_="ia",
    )

    def _assert_errorhandler(self, outconverter, has_errorhandler):
        data = ue("\uee2c\u9a66")  # this is u"\uee2c\u9a66"

        utf8_w_errors = data.encode("utf-16")

        if has_errorhandler:

            eq_(
                outconverter(utf8_w_errors),
                data.encode("utf-16").decode("utf-8", "ignore"),
            )
        else:
            assert_raises(UnicodeDecodeError, outconverter, utf8_w_errors)

    @_oracle_char_combinations
    @testing.requires.python3
    def test_older_cx_oracle_warning(self, cx_Oracle, cx_oracle_type):
        cx_Oracle.version = "6.3"

        ignore_dialect = cx_oracle.dialect(
            dbapi=cx_Oracle, encoding_errors="ignore"
        )
        ignore_outputhandler = (
            ignore_dialect._generate_connection_outputtype_handler()
        )

        cursor = mock.Mock()

        with testing.expect_warnings(
            r"cx_oracle version \(6, 3\) does not support encodingErrors"
        ):
            ignore_outputhandler(
                cursor, "foo", cx_oracle_type, None, None, None
            )

    @_oracle_char_combinations
    @testing.requires.python2
    def test_encoding_errors_sqla_py2k(
        self,
        cx_Oracle,
        cx_oracle_type,
    ):
        ignore_dialect = cx_oracle.dialect(
            dbapi=cx_Oracle, encoding_errors="ignore"
        )

        ignore_outputhandler = (
            ignore_dialect._generate_connection_outputtype_handler()
        )

        cursor = mock.Mock()
        ignore_outputhandler(cursor, "foo", cx_oracle_type, None, None, None)
        outconverter = cursor.mock_calls[0][2]["outconverter"]
        self._assert_errorhandler(outconverter, True)

    @_oracle_char_combinations
    @testing.requires.python2
    def test_no_encoding_errors_sqla_py2k(
        self,
        cx_Oracle,
        cx_oracle_type,
    ):
        plain_dialect = cx_oracle.dialect(dbapi=cx_Oracle)

        plain_outputhandler = (
            plain_dialect._generate_connection_outputtype_handler()
        )

        cursor = mock.Mock()
        plain_outputhandler(cursor, "foo", cx_oracle_type, None, None, None)
        outconverter = cursor.mock_calls[0][2]["outconverter"]
        self._assert_errorhandler(outconverter, False)

    @_oracle_char_combinations
    @testing.requires.python3
    def test_encoding_errors_cx_oracle_py3k(
        self,
        cx_Oracle,
        cx_oracle_type,
    ):
        ignore_dialect = cx_oracle.dialect(
            dbapi=cx_Oracle, encoding_errors="ignore"
        )

        ignore_outputhandler = (
            ignore_dialect._generate_connection_outputtype_handler()
        )

        cursor = mock.Mock()
        ignore_outputhandler(cursor, "foo", cx_oracle_type, None, None, None)

        eq_(
            cursor.mock_calls,
            [
                mock.call.var(
                    mock.ANY,
                    None,
                    cursor.arraysize,
                    encodingErrors="ignore",
                )
            ],
        )

    @_oracle_char_combinations
    @testing.requires.python3
    def test_no_encoding_errors_cx_oracle_py3k(
        self,
        cx_Oracle,
        cx_oracle_type,
    ):
        plain_dialect = cx_oracle.dialect(dbapi=cx_Oracle)

        plain_outputhandler = (
            plain_dialect._generate_connection_outputtype_handler()
        )

        cursor = mock.Mock()
        plain_outputhandler(cursor, "foo", cx_oracle_type, None, None, None)

        eq_(
            cursor.mock_calls,
            [mock.call.var(mock.ANY, None, cursor.arraysize)],
        )


class ComputedReturningTest(fixtures.TablesTest):
    __only_on__ = "oracle"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer, Computed("foo + 42")),
        )

        Table(
            "test_no_returning",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Column("bar", Integer, Computed("foo + 42")),
            implicit_returning=False,
        )

    def test_computed_insert(self, connection):
        test = self.tables.test
        conn = connection
        result = conn.execute(
            test.insert().return_defaults(), {"id": 1, "foo": 5}
        )

        eq_(result.returned_defaults, (47,))

        eq_(conn.scalar(select(test.c.bar)), 47)

    def test_computed_update_warning(self, connection):
        test = self.tables.test
        conn = connection
        conn.execute(test.insert(), {"id": 1, "foo": 5})

        if testing.db.dialect._supports_update_returning_computed_cols:
            result = conn.execute(
                test.update().values(foo=10).return_defaults()
            )
            eq_(result.returned_defaults, (52,))
        else:
            with testing.expect_warnings(
                "Computed columns don't work with Oracle UPDATE"
            ):
                result = conn.execute(
                    test.update().values(foo=10).return_defaults()
                )

                # returns the *old* value
                eq_(result.returned_defaults, (47,))

        eq_(conn.scalar(select(test.c.bar)), 52)

    def test_computed_update_no_warning(self, connection):
        test = self.tables.test_no_returning
        conn = connection
        conn.execute(test.insert(), {"id": 1, "foo": 5})

        result = conn.execute(test.update().values(foo=10).return_defaults())

        # no returning
        eq_(result.returned_defaults, None)

        eq_(conn.scalar(select(test.c.bar)), 52)


class OutParamTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = "oracle+cx_oracle"
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        with testing.db.begin() as c:
            c.exec_driver_sql(
                """
create or replace procedure foo(x_in IN number, x_out OUT number,
y_out OUT number, z_out OUT varchar) IS
retval number;
begin
    retval := 6;
    x_out := 10;
    y_out := x_in * 15;
    z_out := NULL;
end;
                """
            )

    def test_out_params(self, connection):
        result = connection.execute(
            text(
                "begin foo(:x_in, :x_out, :y_out, " ":z_out); end;"
            ).bindparams(
                bindparam("x_in", Float),
                outparam("x_out", Integer),
                outparam("y_out", Float),
                outparam("z_out", String),
            ),
            dict(x_in=5),
        )
        eq_(result.out_parameters, {"x_out": 10, "y_out": 75, "z_out": None})
        assert isinstance(result.out_parameters["x_out"], int)

    @classmethod
    def teardown_test_class(cls):
        with testing.db.begin() as conn:
            conn.execute(text("DROP PROCEDURE foo"))


class QuotedBindRoundTripTest(fixtures.TestBase):

    __only_on__ = "oracle"
    __backend__ = True

    def test_table_round_trip(self, metadata, connection):
        oracle.RESERVED_WORDS.discard("UNION")

        table = Table(
            "t1",
            metadata,
            Column("option", Integer),
            Column("plain", Integer, quote=True),
            # test that quote works for a reserved word
            # that the dialect isn't aware of when quote
            # is set
            Column("union", Integer, quote=True),
        )
        metadata.create_all(connection)

        connection.execute(
            table.insert(), {"option": 1, "plain": 1, "union": 1}
        )
        eq_(connection.execute(table.select()).first(), (1, 1, 1))
        connection.execute(table.update().values(option=2, plain=2, union=2))
        eq_(connection.execute(table.select()).first(), (2, 2, 2))

    def test_numeric_bind_round_trip(self, connection):
        eq_(
            connection.scalar(
                select(
                    literal_column("2", type_=Integer())
                    + bindparam("2_1", value=2)
                )
            ),
            4,
        )

    def test_numeric_bind_in_crud(self, metadata, connection):
        t = Table("asfd", metadata, Column("100K", Integer))
        t.create(connection)

        connection.execute(t.insert(), {"100K": 10})
        eq_(connection.scalar(t.select()), 10)

    def test_expanding_quote_roundtrip(self, metadata, connection):
        t = Table("asfd", metadata, Column("foo", Integer))
        t.create(connection)

        connection.execute(
            select(t).where(t.c.foo.in_(bindparam("uid", expanding=True))),
            dict(uid=[1, 2, 3]),
        )


class CompatFlagsTest(fixtures.TestBase, AssertsCompiledSQL):
    def _dialect(self, server_version, **kw):
        def server_version_info(conn):
            return server_version

        dialect = oracle.dialect(
            dbapi=Mock(
                version="0.0.0",
                paramstyle="named",
            ),
            **kw
        )

        dialect._get_server_version_info = server_version_info
        dialect.get_isolation_level = Mock()
        dialect._check_unicode_returns = Mock()
        dialect._check_unicode_description = Mock()
        dialect._get_default_schema_name = Mock()
        dialect._detect_decimal_char = Mock()
        dialect.__check_max_identifier_length = Mock()
        dialect._get_compat_server_version_info = Mock()
        return dialect

    def test_ora8_flags(self):
        dialect = self._dialect((8, 2, 5))

        # before connect, assume modern DB
        assert dialect._supports_char_length
        assert dialect.use_ansi
        assert not dialect._use_nchar_for_unicode

        dialect.initialize(Mock())
        assert not dialect.implicit_returning
        assert not dialect._supports_char_length
        assert not dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50)", dialect=dialect)
        self.assert_compile(Unicode(50), "VARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "CLOB", dialect=dialect)

        dialect = self._dialect((8, 2, 5), implicit_returning=True)
        dialect.initialize(testing.db.connect())
        assert dialect.implicit_returning

    def test_default_flags(self):
        """test with no initialization or server version info"""

        dialect = self._dialect(None)

        assert dialect._supports_char_length
        assert not dialect._use_nchar_for_unicode
        assert dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(UnicodeText(), "CLOB", dialect=dialect)

    def test_ora10_flags(self):
        dialect = self._dialect((10, 2, 5))

        dialect.initialize(Mock())
        assert dialect._supports_char_length
        assert not dialect._use_nchar_for_unicode
        assert dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(UnicodeText(), "CLOB", dialect=dialect)

    def test_use_nchar(self):
        dialect = self._dialect((10, 2, 5), use_nchar_for_unicode=True)

        dialect.initialize(Mock())
        assert dialect._use_nchar_for_unicode

        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "NVARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "NCLOB", dialect=dialect)

    def test_ident_length_in_13_is_30(self):
        from sqlalchemy import __version__

        m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", __version__)
        version = tuple(int(x) for x in m.group(1, 2, 3) if x is not None)
        if version >= (1, 4):
            length = 128
        else:
            length = 30

        eq_(oracle.OracleDialect.max_identifier_length, length)

        dialect = self._dialect((12, 2, 0))
        conn = mock.Mock(
            exec_driver_sql=mock.Mock(
                return_value=mock.Mock(scalar=lambda: "12.2.0")
            )
        )
        dialect.initialize(conn)
        eq_(dialect.server_version_info, (12, 2, 0))
        eq_(
            dialect._get_effective_compat_server_version_info(conn), (12, 2, 0)
        )
        eq_(dialect.max_identifier_length, length)

    def test_max_ident_122(self):
        dialect = self._dialect((12, 2, 0))

        conn = mock.Mock(
            exec_driver_sql=mock.Mock(
                return_value=mock.Mock(scalar=lambda: "12.2.0")
            )
        )
        dialect.initialize(conn)
        eq_(dialect.server_version_info, (12, 2, 0))
        eq_(
            dialect._get_effective_compat_server_version_info(conn), (12, 2, 0)
        )
        eq_(
            dialect.max_identifier_length,
            oracle.OracleDialect.max_identifier_length,
        )

    def test_max_ident_112(self):
        dialect = self._dialect((11, 2, 0))

        conn = mock.Mock(
            exec_driver_sql=mock.Mock(return_value=mock.Mock(scalar="11.0.0"))
        )
        dialect.initialize(conn)
        eq_(dialect.server_version_info, (11, 2, 0))
        eq_(
            dialect._get_effective_compat_server_version_info(conn), (11, 2, 0)
        )
        eq_(dialect.max_identifier_length, 30)

    def test_max_ident_122_11compat(self):
        dialect = self._dialect((12, 2, 0))

        conn = mock.Mock(
            exec_driver_sql=mock.Mock(
                return_value=mock.Mock(scalar=lambda: "11.0.0")
            )
        )
        dialect.initialize(conn)
        eq_(dialect.server_version_info, (12, 2, 0))
        eq_(
            dialect._get_effective_compat_server_version_info(conn), (11, 0, 0)
        )
        eq_(dialect.max_identifier_length, 30)

    def test_max_ident_122_11compat_vparam_raises(self):
        dialect = self._dialect((12, 2, 0))

        def c122():
            raise exc.DBAPIError(
                "statement", None, "no such table", None, None
            )

        conn = mock.Mock(
            exec_driver_sql=mock.Mock(return_value=mock.Mock(scalar=c122))
        )
        dialect.initialize(conn)
        eq_(dialect.server_version_info, (12, 2, 0))
        eq_(
            dialect._get_effective_compat_server_version_info(conn), (12, 2, 0)
        )
        eq_(
            dialect.max_identifier_length,
            oracle.OracleDialect.max_identifier_length,
        )

    def test_max_ident_122_11compat_vparam_cant_parse(self):
        dialect = self._dialect((12, 2, 0))

        def c122():
            return "12.thisiscrap.0"

        conn = mock.Mock(
            exec_driver_sql=mock.Mock(return_value=mock.Mock(scalar=c122))
        )
        dialect.initialize(conn)
        eq_(dialect.server_version_info, (12, 2, 0))
        eq_(
            dialect._get_effective_compat_server_version_info(conn), (12, 2, 0)
        )
        eq_(
            dialect.max_identifier_length,
            oracle.OracleDialect.max_identifier_length,
        )


class ExecuteTest(fixtures.TestBase):

    __only_on__ = "oracle"
    __backend__ = True

    def test_basic(self):
        with testing.db.connect() as conn:
            eq_(
                conn.exec_driver_sql(
                    "/*+ this is a comment */ SELECT 1 FROM " "DUAL"
                ).fetchall(),
                [(1,)],
            )

    def test_sequences_are_integers(self, connection):
        seq = Sequence("foo_seq")
        seq.create(connection)
        try:
            val = connection.execute(seq)
            eq_(val, 1)
            assert type(val) is int
        finally:
            seq.drop(connection)

    def test_limit_offset_for_update(self, metadata, connection):
        # oracle can't actually do the ROWNUM thing with FOR UPDATE
        # very well.

        t = Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Integer),
        )
        metadata.create_all(connection)

        connection.execute(
            t.insert(),
            [
                {"id": 1, "data": 1},
                {"id": 2, "data": 7},
                {"id": 3, "data": 12},
                {"id": 4, "data": 15},
                {"id": 5, "data": 32},
            ],
        )

        # here, we can't use ORDER BY.
        eq_(
            connection.execute(
                t.select().with_for_update().limit(2)
            ).fetchall(),
            [(1, 1), (2, 7)],
        )

        # here, its impossible.  But we'd prefer it to raise ORA-02014
        # instead of issuing a syntax error.
        assert_raises_message(
            exc.DatabaseError,
            "ORA-02014",
            connection.execute,
            t.select().with_for_update().limit(2).offset(3),
        )


class UnicodeSchemaTest(fixtures.TestBase):
    __only_on__ = "oracle"
    __backend__ = True

    def test_quoted_column_non_unicode(self, metadata, connection):
        table = Table(
            "atable",
            metadata,
            Column("_underscorecolumn", Unicode(255), primary_key=True),
        )
        metadata.create_all(connection)

        connection.execute(table.insert(), {"_underscorecolumn": u("’é")})
        result = connection.execute(
            table.select().where(table.c._underscorecolumn == u("’é"))
        ).scalar()
        eq_(result, u("’é"))

    def test_quoted_column_unicode(self, metadata, connection):
        table = Table(
            "atable",
            metadata,
            Column(u("méil"), Unicode(255), primary_key=True),
        )
        metadata.create_all(connection)

        connection.execute(table.insert(), {u("méil"): u("’é")})
        result = connection.execute(
            table.select().where(table.c[u("méil")] == u("’é"))
        ).scalar()
        eq_(result, u("’é"))


class CXOracleConnectArgsTest(fixtures.TestBase):
    __only_on__ = "oracle+cx_oracle"
    __backend__ = True

    def test_cx_oracle_service_name(self):
        url_string = "oracle+cx_oracle://scott:tiger@host/?service_name=hr"
        eng = create_engine(url_string, _initialize=False)
        cargs, cparams = eng.dialect.create_connect_args(eng.url)

        assert "SERVICE_NAME=hr" in cparams["dsn"]
        assert "SID=hr" not in cparams["dsn"]

    def test_cx_oracle_service_name_bad(self):
        url_string = "oracle+cx_oracle://scott:tiger@host/hr1?service_name=hr2"
        assert_raises(
            exc.InvalidRequestError,
            create_engine,
            url_string,
            _initialize=False,
        )

    def _test_db_opt(self, url_string, key, value):
        import cx_Oracle

        url_obj = url.make_url(url_string)
        dialect = cx_oracle.dialect(dbapi=cx_Oracle)
        arg, kw = dialect.create_connect_args(url_obj)
        eq_(kw[key], value)

    def _test_db_opt_unpresent(self, url_string, key):
        import cx_Oracle

        url_obj = url.make_url(url_string)
        dialect = cx_oracle.dialect(dbapi=cx_Oracle)
        arg, kw = dialect.create_connect_args(url_obj)
        assert key not in kw

    def _test_dialect_param_from_url(self, url_string, key, value):
        import cx_Oracle

        url_obj = url.make_url(url_string)
        dialect = cx_oracle.dialect(dbapi=cx_Oracle)
        with testing.expect_deprecated(
            "cx_oracle dialect option %r should" % key
        ):
            arg, kw = dialect.create_connect_args(url_obj)
        eq_(getattr(dialect, key), value)

        # test setting it on the dialect normally
        dialect = cx_oracle.dialect(dbapi=cx_Oracle, **{key: value})
        eq_(getattr(dialect, key), value)

    def test_mode(self):
        import cx_Oracle

        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/?mode=sYsDBA",
            "mode",
            cx_Oracle.SYSDBA,
        )

        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/?mode=SYSOPER",
            "mode",
            cx_Oracle.SYSOPER,
        )

    def test_int_mode(self):
        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/?mode=32767", "mode", 32767
        )

    @testing.requires.cxoracle6_or_greater
    def test_purity(self):
        import cx_Oracle

        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/?purity=attr_purity_new",
            "purity",
            cx_Oracle.ATTR_PURITY_NEW,
        )

    def test_encoding(self):
        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/"
            "?encoding=AMERICAN_AMERICA.UTF8",
            "encoding",
            "AMERICAN_AMERICA.UTF8",
        )

    def test_threaded(self):
        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/?threaded=true",
            "threaded",
            True,
        )

        self._test_db_opt_unpresent(
            "oracle+cx_oracle://scott:tiger@host/", "threaded"
        )

    def test_events(self):
        self._test_db_opt(
            "oracle+cx_oracle://scott:tiger@host/?events=true", "events", True
        )

    def test_threaded_deprecated_at_dialect_level(self):
        with testing.expect_deprecated(
            "The 'threaded' parameter to the cx_oracle dialect"
        ):
            dialect = cx_oracle.dialect(threaded=False)
        arg, kw = dialect.create_connect_args(
            url.make_url("oracle+cx_oracle://scott:tiger@dsn")
        )
        eq_(kw["threaded"], False)

    def test_deprecated_use_ansi(self):
        self._test_dialect_param_from_url(
            "oracle+cx_oracle://scott:tiger@host/?use_ansi=False",
            "use_ansi",
            False,
        )

    def test_deprecated_auto_convert_lobs(self):
        self._test_dialect_param_from_url(
            "oracle+cx_oracle://scott:tiger@host/?auto_convert_lobs=False",
            "auto_convert_lobs",
            False,
        )


class TableValuedTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "oracle"

    @testing.fixture
    def scalar_strings(self, connection):
        connection.exec_driver_sql(
            "CREATE OR REPLACE TYPE strings_t IS TABLE OF VARCHAR2 (100)"
        )
        connection.exec_driver_sql(
            r"""
CREATE OR REPLACE FUNCTION scalar_strings (
   count_in IN INTEGER)
   RETURN strings_t
   AUTHID DEFINER
IS
   l_strings   strings_t := strings_t ();
BEGIN
   l_strings.EXTEND (count_in);

   FOR indx IN 1 .. count_in
   LOOP
      l_strings (indx) := 'some string';
   END LOOP;

   RETURN l_strings;
END;
        """
        )
        yield
        connection.exec_driver_sql("DROP FUNCTION scalar_strings")
        connection.exec_driver_sql("DROP TYPE strings_t")

    @testing.fixture
    def two_strings(self, connection):
        connection.exec_driver_sql(
            """
CREATE OR REPLACE TYPE two_strings_ot
   AUTHID DEFINER IS OBJECT
(
   string1 VARCHAR2 (10),
   string2 VARCHAR2 (10)
)"""
        )
        connection.exec_driver_sql(
            """
            CREATE OR REPLACE TYPE two_strings_nt IS TABLE OF two_strings_ot
"""
        )

        connection.exec_driver_sql(
            """
        CREATE OR REPLACE FUNCTION three_pairs
   RETURN two_strings_nt
   AUTHID DEFINER
IS
   l_strings   two_strings_nt;
BEGIN
   RETURN two_strings_nt (two_strings_ot ('a', 'b'),
                          two_strings_ot ('c', 'd'),
                          two_strings_ot ('e', 'f'));
END;
"""
        )
        yield
        connection.exec_driver_sql("DROP FUNCTION three_pairs")
        connection.exec_driver_sql("DROP TYPE two_strings_nt")
        connection.exec_driver_sql("DROP TYPE two_strings_ot")

    def test_scalar_strings_control(self, scalar_strings, connection):
        result = (
            connection.exec_driver_sql(
                "SELECT COLUMN_VALUE my_string FROM TABLE (scalar_strings (5))"
            )
            .scalars()
            .all()
        )
        eq_(result, ["some string"] * 5)

    def test_scalar_strings_named_control(self, scalar_strings, connection):
        result = (
            connection.exec_driver_sql(
                "SELECT COLUMN_VALUE anon_1 "
                "FROM TABLE (scalar_strings (5)) anon_1"
            )
            .scalars()
            .all()
        )
        eq_(result, ["some string"] * 5)

    def test_scalar_strings(self, scalar_strings, connection):
        fn = func.scalar_strings(5)
        result = connection.execute(select(fn.column_valued())).scalars().all()
        eq_(result, ["some string"] * 5)

    def test_two_strings_control(self, two_strings, connection):
        result = connection.exec_driver_sql(
            "SELECT string1, string2 FROM TABLE (three_pairs ())"
        ).all()
        eq_(result, [("a", "b"), ("c", "d"), ("e", "f")])

    def test_two_strings(self, two_strings, connection):
        fn = func.three_pairs().table_valued("string1", "string2")
        result = connection.execute(select(fn.c.string1, fn.c.string2)).all()
        eq_(result, [("a", "b"), ("c", "d"), ("e", "f")])


class OptimizedFetchLimitOffsetTest(test_select.FetchLimitOffsetTest):
    __only_on__ = "oracle"

    @classmethod
    def setup_bind(cls):
        return engines.testing_engine(options={"optimize_limits": True})
