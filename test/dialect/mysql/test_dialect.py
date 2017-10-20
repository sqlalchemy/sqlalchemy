# coding: utf-8

from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy.engine.url import make_url
from sqlalchemy.testing import fixtures, expect_warnings
from sqlalchemy import testing
from sqlalchemy.testing import engines
from ...engine import test_execute
import datetime
from sqlalchemy.dialects import mysql


class DialectTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = 'mysql'

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
            make_url(
                "mysql+mysqlconnector://u:p@host/db?raise_on_warnings=true"
            )
        )[1]
        eq_(kw['raise_on_warnings'], True)

        kw = dialect.create_connect_args(
            make_url(
                "mysql+mysqlconnector://u:p@host/db?raise_on_warnings=false"
            )
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

    def test_no_show_variables(self):
        from sqlalchemy.testing import mock
        engine = engines.testing_engine()

        def my_execute(self, statement, *args, **kw):
            if statement.startswith("SHOW VARIABLES"):
                statement = "SELECT 1 FROM DUAL WHERE 1=0"
            return real_exec(self, statement, *args, **kw)

        real_exec = engine._connection_cls._execute_text
        with mock.patch.object(
                engine._connection_cls, "_execute_text", my_execute):
            with expect_warnings(
                "Could not retrieve SQL_MODE; please ensure the "
                "MySQL user has permissions to SHOW VARIABLES"
            ):
                engine.connect()

    def test_autocommit_isolation_level(self):
        c = testing.db.connect().execution_options(
            isolation_level='AUTOCOMMIT'
        )
        assert c.execute('SELECT @@autocommit;').scalar()

        c = c.execution_options(isolation_level='READ COMMITTED')
        assert not c.execute('SELECT @@autocommit;').scalar()

    def test_isolation_level(self):
        values = {
            # sqlalchemy -> mysql
            'READ UNCOMMITTED': 'READ-UNCOMMITTED',
            'READ COMMITTED': 'READ-COMMITTED',
            'REPEATABLE READ': 'REPEATABLE-READ',
            'SERIALIZABLE': 'SERIALIZABLE'
        }
        for sa_value, mysql_value in values.items():
            c = testing.db.connect().execution_options(
                isolation_level=sa_value
            )
            assert c.execute('SELECT @@tx_isolation;').scalar() == mysql_value


class ParseVersionTest(fixtures.TestBase):
    def test_mariadb_normalized_version(self):
        for expected, version in [
            ((10, 2, 7), (10, 2, 7, 'MariaDB')),
            ((10, 2, 7), (5, 6, 15, 10, 2, 7, 'MariaDB')),
            ((10, 2, 10), (10, 2, 10, 'MariaDB')),
            ((5, 7, 20), (5, 7, 20)),
            ((5, 6, 15), (5, 6, 15)),
            ((10, 2, 6),
             (10, 2, 6, 'MariaDB', 10, 2, '6+maria~stretch', 'log')),
        ]:
            dialect = mysql.dialect()
            dialect.server_version_info = version
            eq_(
                dialect._mariadb_normalized_version_info,
                expected
            )

    def test_mariadb_check_warning(self):

        for expect_, version in [
            (True, (10, 2, 7, 'MariaDB')),
            (True, (5, 6, 15, 10, 2, 7, 'MariaDB')),
            (False, (10, 2, 10, 'MariaDB')),
            (False, (5, 7, 20)),
            (False, (5, 6, 15)),
            (True, (10, 2, 6, 'MariaDB', 10, 2, '6+maria~stretch', 'log')),
        ]:
            dialect = mysql.dialect()
            dialect.server_version_info = version
            if expect_:
                with expect_warnings(
                        ".*before 10.2.9 has known issues regarding "
                        "CHECK constraints"):
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
    __only_on__ = 'mysql'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            't', metadata,
            Column('id', Integer, primary_key=True),
            Column('x', Integer),
            Column('data', DateTime)
        )

        Table(
            't_default', metadata,
            Column('id', Integer, primary_key=True),
            Column('x', Integer),
            Column('idata', DateTime, default=func.utc_timestamp()),
            Column('udata', DateTime, onupdate=func.utc_timestamp())
        )

    def test_insert_executemany(self):
        with testing.db.connect() as conn:
            conn.execute(
                self.tables.t.insert().values(data=func.utc_timestamp()),
                [{"x": 5}, {"x": 6}, {"x": 7}]
            )

    def test_update_executemany(self):
        with testing.db.connect() as conn:
            timestamp = datetime.datetime(2015, 4, 17, 18, 5, 2)
            conn.execute(
                self.tables.t.insert(),
                [
                    {"x": 5, "data": timestamp},
                    {"x": 6, "data": timestamp},
                    {"x": 7, "data": timestamp}]
            )

            conn.execute(
                self.tables.t.update().
                values(data=func.utc_timestamp()).
                where(self.tables.t.c.x == bindparam('xval')),
                [{"xval": 5}, {"xval": 6}, {"xval": 7}]
            )

    def test_insert_executemany_w_default(self):
        with testing.db.connect() as conn:
            conn.execute(
                self.tables.t_default.insert(),
                [{"x": 5}, {"x": 6}, {"x": 7}]
            )

    def test_update_executemany_w_default(self):
        with testing.db.connect() as conn:
            timestamp = datetime.datetime(2015, 4, 17, 18, 5, 2)
            conn.execute(
                self.tables.t_default.insert(),
                [
                    {"x": 5, "idata": timestamp},
                    {"x": 6, "idata": timestamp},
                    {"x": 7, "idata": timestamp}]
            )

            conn.execute(
                self.tables.t_default.update().
                values(idata=func.utc_timestamp()).
                where(self.tables.t_default.c.x == bindparam('xval')),
                [{"xval": 5}, {"xval": 6}, {"xval": 7}]
            )


class SQLModeDetectionTest(fixtures.TestBase):
    __only_on__ = 'mysql'
    __backend__ = True

    def _options(self, modes):
        def connect(con, record):
            cursor = con.cursor()
            cursor.execute("set sql_mode='%s'" % (",".join(modes)))
        e = engines.testing_engine(options={
            'pool_events': [
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


class AutocommitTextTest(test_execute.AutocommitTextTest):
    __only_on__ = 'mysql'

    def test_load_data(self):
        self._test_keyword("LOAD DATA STUFF")

    def test_replace(self):
        self._test_keyword("REPLACE THING")
