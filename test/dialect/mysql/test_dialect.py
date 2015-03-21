# coding: utf-8

from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy.engine.url import make_url
from sqlalchemy.testing import fixtures
from sqlalchemy import testing
from sqlalchemy.testing import engines
import datetime


class DialectTest(fixtures.TestBase):
    __backend__ = True

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
            make_url("mysql://scott:tiger@localhost:3306/test"
                "?ssl_ca=/ca.pem&ssl_cert=/cert.pem&ssl_key=/key.pem")
        )[1]
        # args that differ among mysqldb and oursql
        for k in ('use_unicode', 'found_rows', 'client_flag'):
            kwarg.pop(k, None)
        eq_(
            kwarg,
            {
                'passwd': 'tiger', 'db': 'test',
                'ssl': {'ca': '/ca.pem', 'cert': '/cert.pem',
                        'key': '/key.pem'},
                'host': 'localhost', 'user': 'scott',
                'port': 3306
            }
        )

    def test_mysqlconnector_buffered_arg(self):
        from sqlalchemy.dialects.mysql import mysqlconnector
        dialect = mysqlconnector.dialect()
        kw = dialect.create_connect_args(
                make_url("mysql+mysqlconnector://u:p@host/db?buffered=true")
            )[1]
        eq_(kw['buffered'], True)

        kw = dialect.create_connect_args(
                make_url("mysql+mysqlconnector://u:p@host/db?buffered=false")
            )[1]
        eq_(kw['buffered'], False)

        kw = dialect.create_connect_args(
                make_url("mysql+mysqlconnector://u:p@host/db")
            )[1]
        eq_(kw['buffered'], True)

    def test_mysqlconnector_raise_on_warnings_arg(self):
        from sqlalchemy.dialects.mysql import mysqlconnector
        dialect = mysqlconnector.dialect()
        kw = dialect.create_connect_args(
                make_url("mysql+mysqlconnector://u:p@host/db?raise_on_warnings=true")
            )[1]
        eq_(kw['raise_on_warnings'], True)

        kw = dialect.create_connect_args(
                make_url("mysql+mysqlconnector://u:p@host/db?raise_on_warnings=false")
            )[1]
        eq_(kw['raise_on_warnings'], False)


        kw = dialect.create_connect_args(
                make_url("mysql+mysqlconnector://u:p@host/db")
            )[1]
        assert "raise_on_warnings" not in kw

    @testing.only_on('mysql')
    def test_random_arg(self):
        dialect = testing.db.dialect
        kw = dialect.create_connect_args(
                make_url("mysql://u:p@host/db?foo=true")
            )[1]
        eq_(kw['foo'], "true")

    @testing.only_on('mysql')
    @testing.skip_if('mysql+mysqlconnector', "totally broken for the moment")
    @testing.fails_on('mysql+oursql', "unsupported")
    def test_special_encodings(self):

        for enc in ['utf8mb4', 'utf8']:
            eng = engines.testing_engine(
                options={"connect_args": {'charset': enc, 'use_unicode': 0}})
            conn = eng.connect()
            eq_(conn.dialect._connection_charset, enc)

class SQLModeDetectionTest(fixtures.TestBase):
    __only_on__ = 'mysql'
    __backend__ = True

    def _options(self, modes):
        def connect(con, record):
            cursor = con.cursor()
            cursor.execute("set sql_mode='%s'" % (",".join(modes)))
        e = engines.testing_engine(options={
            'pool_events':[
                (connect, 'first_connect'),
                (connect, 'connect')
            ]
        })
        return e

    def test_backslash_escapes(self):
        engine = self._options(['NO_BACKSLASH_ESCAPES'])
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
        engine = self._options(['ANSI_QUOTES'])
        c = engine.connect()
        assert engine.dialect._server_ansiquotes
        c.close()
        engine.dispose()

    def test_combination(self):
        engine = self._options(['ANSI_QUOTES,NO_BACKSLASH_ESCAPES'])
        c = engine.connect()
        assert engine.dialect._server_ansiquotes
        assert not engine.dialect._backslash_escapes
        c.close()
        engine.dispose()


class ExecutionTest(fixtures.TestBase):
    """Various MySQL execution special cases."""

    __only_on__ = 'mysql'
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
