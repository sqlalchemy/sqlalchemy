# -*- encoding: utf-8
from sqlalchemy.testing import eq_, engines
from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.dialects.mssql import pyodbc, pymssql
from sqlalchemy.engine import url
from sqlalchemy.testing import fixtures
from sqlalchemy import testing
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing.mock import Mock


class ParseConnectTest(fixtures.TestBase):

    def test_pyodbc_connect_dsn_trusted(self):
        dialect = pyodbc.dialect()
        u = url.make_url('mssql://mydsn')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;Trusted_Connection=Yes'], {}], connection)

    def test_pyodbc_connect_old_style_dsn_trusted(self):
        dialect = pyodbc.dialect()
        u = url.make_url('mssql:///?dsn=mydsn')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;Trusted_Connection=Yes'], {}], connection)

    def test_pyodbc_connect_dsn_non_trusted(self):
        dialect = pyodbc.dialect()
        u = url.make_url('mssql://username:password@mydsn')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_dsn_extra(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql://username:password@mydsn/?LANGUAGE=us_'
                         'english&foo=bar')
        connection = dialect.create_connect_args(u)
        dsn_string = connection[0][0]
        assert ";LANGUAGE=us_english" in dsn_string
        assert ";foo=bar" in dsn_string

    def test_pyodbc_connect(self):
        dialect = pyodbc.dialect()
        u = url.make_url('mssql://username:password@hostspec/database')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_comma_port(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql://username:password@hostspec:12345/data'
                         'base')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec,12345;Database=datab'
            'ase;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_config_port(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql://username:password@hostspec/database?p'
                         'ort=12345')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password;port=12345'], {}], connection)

    def test_pyodbc_extra_connect(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql://username:password@hostspec/database?L'
                         'ANGUAGE=us_english&foo=bar')
        connection = dialect.create_connect_args(u)
        eq_(connection[1], {})
        eq_(connection[0][0]
            in ('DRIVER={SQL Server};Server=hostspec;Database=database;'
            'UID=username;PWD=password;foo=bar;LANGUAGE=us_english',
            'DRIVER={SQL Server};Server=hostspec;Database=database;UID='
            'username;PWD=password;LANGUAGE=us_english;foo=bar'), True)

    def test_pyodbc_odbc_connect(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql:///?odbc_connect=DRIVER%3D%7BSQL+Server'
                         '%7D%3BServer%3Dhostspec%3BDatabase%3Ddatabase'
                         '%3BUID%3Dusername%3BPWD%3Dpassword')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password'], {}], connection)

    def test_pyodbc_odbc_connect_with_dsn(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql:///?odbc_connect=dsn%3Dmydsn%3BDatabase'
                         '%3Ddatabase%3BUID%3Dusername%3BPWD%3Dpassword'
                         )
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;Database=database;UID=username;PWD=password'],
            {}], connection)

    def test_pyodbc_odbc_connect_ignores_other_values(self):
        dialect = pyodbc.dialect()
        u = \
            url.make_url('mssql://userdiff:passdiff@localhost/dbdiff?od'
                         'bc_connect=DRIVER%3D%7BSQL+Server%7D%3BServer'
                         '%3Dhostspec%3BDatabase%3Ddatabase%3BUID%3Duse'
                         'rname%3BPWD%3Dpassword')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password'], {}], connection)

    def test_pymssql_port_setting(self):
        dialect = pymssql.dialect()

        u = \
            url.make_url('mssql+pymssql://scott:tiger@somehost/test')
        connection = dialect.create_connect_args(u)
        eq_(
            [[], {'host': 'somehost', 'password': 'tiger',
                    'user': 'scott', 'database': 'test'}], connection
        )

        u = \
            url.make_url('mssql+pymssql://scott:tiger@somehost:5000/test')
        connection = dialect.create_connect_args(u)
        eq_(
            [[], {'host': 'somehost:5000', 'password': 'tiger',
                    'user': 'scott', 'database': 'test'}], connection
        )

    def test_pymssql_disconnect(self):
        dialect = pymssql.dialect()

        for error in [
                'Adaptive Server connection timed out',
                'Net-Lib error during Connection reset by peer',
                'message 20003',
                'Error 10054',
                'Not connected to any MS SQL server',
                'Connection is closed'
                ]:
            eq_(dialect.is_disconnect(error, None, None), True)

        eq_(dialect.is_disconnect("not an error", None, None), False)

    @testing.only_on(['mssql+pyodbc', 'mssql+pymssql'],
                            "FreeTDS specific test")
    def test_bad_freetds_warning(self):
        engine = engines.testing_engine()

        def _bad_version(connection):
            return 95, 10, 255

        engine.dialect._get_server_version_info = _bad_version
        assert_raises_message(exc.SAWarning,
                              'Unrecognized server version info',
                              engine.connect)


class VersionDetectionTest(fixtures.TestBase):
    def test_pymssql_version(self):
        dialect = pymssql.MSDialect_pymssql()

        for vers in [
            "Microsoft SQL Server Blah - 11.0.9216.62",
            "Microsoft SQL Server (XYZ) - 11.0.9216.62 \n"
            "Jul 18 2014 22:00:21 \nCopyright (c) Microsoft Corporation",
            "Microsoft SQL Azure (RTM) - 11.0.9216.62 \n"
            "Jul 18 2014 22:00:21 \nCopyright (c) Microsoft Corporation"
        ]:
            conn = Mock(scalar=Mock(return_value=vers))
            eq_(
                dialect._get_server_version_info(conn),
                (11, 0, 9216, 62)
            )
