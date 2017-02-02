from sqlalchemy.testing import assert_raises, eq_, assert_raises_message
import sqlalchemy.engine.url as url
from sqlalchemy import create_engine, engine_from_config, exc, pool
from sqlalchemy.engine.default import DefaultDialect
import sqlalchemy as tsa
from sqlalchemy.testing import fixtures
from sqlalchemy import testing
from sqlalchemy.testing.mock import Mock, MagicMock, call
from sqlalchemy.testing import mock
from sqlalchemy.dialects import registry
from sqlalchemy.dialects import plugins

dialect = None


class ParseConnectTest(fixtures.TestBase):
    def test_rfc1738(self):
        for text in (
            'dbtype://username:password@hostspec:110//usr/db_file.db',
            'dbtype://username:password@hostspec/database',
            'dbtype+apitype://username:password@hostspec/database',
            'dbtype://username:password@hostspec',
            'dbtype://username:password@/database',
            'dbtype://username@hostspec',
            'dbtype://username:password@127.0.0.1:1521',
            'dbtype://hostspec/database',
            'dbtype://hostspec',
            'dbtype://hostspec/?arg1=val1&arg2=val2',
            'dbtype+apitype:///database',
            'dbtype:///:memory:',
            'dbtype:///foo/bar/im/a/file',
            'dbtype:///E:/work/src/LEM/db/hello.db',
            'dbtype:///E:/work/src/LEM/db/hello.db?foo=bar&hoho=lala',
            'dbtype://',
            'dbtype://username:password@/database',
            'dbtype:////usr/local/_xtest@example.com/members.db',
            'dbtype://username:apples%2Foranges@hostspec/database',
            'dbtype://username:password@[2001:da8:2004:1000:202:116:160:90]'
            '/database?foo=bar',
            'dbtype://username:password@[2001:da8:2004:1000:202:116:160:90]:80'
            '/database?foo=bar'
        ):
            u = url.make_url(text)

            assert u.drivername in ('dbtype', 'dbtype+apitype')
            assert u.username in ('username', None)
            assert u.password in ('password', 'apples/oranges', None)
            assert u.host in (
                'hostspec', '127.0.0.1',
                '2001:da8:2004:1000:202:116:160:90', '', None), u.host
            assert u.database in (
                'database',
                '/usr/local/_xtest@example.com/members.db',
                '/usr/db_file.db', ':memory:', '',
                'foo/bar/im/a/file',
                'E:/work/src/LEM/db/hello.db', None), u.database
            eq_(str(u), text)

    def test_rfc1738_password(self):
        u = url.make_url("dbtype://user:pass word + other%3Awords@host/dbname")
        eq_(u.password, "pass word + other:words")
        eq_(str(u), "dbtype://user:pass word + other%3Awords@host/dbname")

        u = url.make_url(
            'dbtype://username:apples%2Foranges@hostspec/database')
        eq_(u.password, "apples/oranges")
        eq_(str(u), 'dbtype://username:apples%2Foranges@hostspec/database')

        u = url.make_url(
            'dbtype://username:apples%40oranges%40%40@hostspec/database')
        eq_(u.password, "apples@oranges@@")
        eq_(
            str(u),
            'dbtype://username:apples%40oranges%40%40@hostspec/database')

        u = url.make_url('dbtype://username%40:@hostspec/database')
        eq_(u.password, '')
        eq_(u.username, "username@")
        eq_(str(u), 'dbtype://username%40:@hostspec/database')

        u = url.make_url('dbtype://username:pass%2Fword@hostspec/database')
        eq_(u.password, 'pass/word')
        eq_(str(u), 'dbtype://username:pass%2Fword@hostspec/database')


class DialectImportTest(fixtures.TestBase):
    def test_import_base_dialects(self):
        # the globals() somehow makes it for the exec() + nose3.

        for name in (
                'mysql',
                'firebird',
                'postgresql',
                'sqlite',
                'oracle',
                'mssql'):
            exec('from sqlalchemy.dialects import %s\ndialect = '
                 '%s.dialect()' % (name, name), globals())
            eq_(dialect.name, name)


class CreateEngineTest(fixtures.TestBase):
    """test that create_engine arguments of different types get
    propagated properly"""

    def test_connect_query(self):
        dbapi = MockDBAPI(foober='12', lala='18', fooz='somevalue')
        e = \
            create_engine('postgresql://scott:tiger@somehost/test?foobe'
                          'r=12&lala=18&fooz=somevalue', module=dbapi,
                          _initialize=False)
        e.connect()

    def test_kwargs(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this': 'dict'},
                          fooz='somevalue')
        e = \
            create_engine(
                'postgresql://scott:tiger@somehost/test?fooz='
                'somevalue', connect_args={
                    'foober': 12,
                    'lala': 18, 'hoho': {'this': 'dict'}},
                module=dbapi, _initialize=False)
        e.connect()

    def test_engine_from_config(self):
        dbapi = mock_dbapi

        config = {
            'sqlalchemy.url': 'postgresql://scott:tiger@somehost/test'
            '?fooz=somevalue',
            'sqlalchemy.pool_recycle': '50',
            'sqlalchemy.echo': 'true'}

        e = engine_from_config(config, module=dbapi, _initialize=False)
        assert e.pool._recycle == 50
        assert e.url \
            == url.make_url('postgresql://scott:tiger@somehost/test?foo'
                            'z=somevalue')
        assert e.echo is True

    def test_pool_threadlocal_from_config(self):
        dbapi = mock_dbapi

        config = {
            'sqlalchemy.url': 'postgresql://scott:tiger@somehost/test',
            'sqlalchemy.pool_threadlocal': "false"}

        e = engine_from_config(config, module=dbapi, _initialize=False)
        eq_(e.pool._use_threadlocal, False)

        config = {
            'sqlalchemy.url': 'postgresql://scott:tiger@somehost/test',
            'sqlalchemy.pool_threadlocal': "true"}

        e = engine_from_config(config, module=dbapi, _initialize=False)
        eq_(e.pool._use_threadlocal, True)

    def test_pool_reset_on_return_from_config(self):
        dbapi = mock_dbapi

        for value, expected in [
            ("rollback", pool.reset_rollback),
            ("commit", pool.reset_commit),
            ("none", pool.reset_none)
        ]:
            config = {
                'sqlalchemy.url': 'postgresql://scott:tiger@somehost/test',
                'sqlalchemy.pool_reset_on_return': value}

            e = engine_from_config(config, module=dbapi, _initialize=False)
            eq_(e.pool._reset_on_return, expected)

    def test_engine_from_config_custom(self):
        from sqlalchemy import util
        tokens = __name__.split(".")

        class MyDialect(MockDialect):
            engine_config_types = {
                "foobar": int,
                "bathoho": util.bool_or_str('force')
            }

            def __init__(self, foobar=None, bathoho=None, **kw):
                self.foobar = foobar
                self.bathoho = bathoho

        global dialect
        dialect = MyDialect
        registry.register(
            "mockdialect.barb",
            ".".join(tokens[0:-1]), tokens[-1])

        config = {
            "sqlalchemy.url": "mockdialect+barb://",
            "sqlalchemy.foobar": "5",
            "sqlalchemy.bathoho": "false"
        }
        e = engine_from_config(config, _initialize=False)
        eq_(e.dialect.foobar, 5)
        eq_(e.dialect.bathoho, False)

    def test_custom(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this': 'dict'},
                          fooz='somevalue')

        def connect():
            return dbapi.connect(foober=12, lala=18, fooz='somevalue',
                                 hoho={'this': 'dict'})

        # start the postgresql dialect, but put our mock DBAPI as the
        # module instead of psycopg

        e = create_engine('postgresql://', creator=connect,
                          module=dbapi, _initialize=False)
        e.connect()

    def test_recycle(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this': 'dict'},
                          fooz='somevalue')
        e = create_engine('postgresql://', pool_recycle=472,
                          module=dbapi, _initialize=False)
        assert e.pool._recycle == 472

    def test_reset_on_return(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this': 'dict'},
                          fooz='somevalue')
        for (value, expected) in [
            ('rollback', pool.reset_rollback),
            ('commit', pool.reset_commit),
            (None, pool.reset_none),
            (True, pool.reset_rollback),
            (False, pool.reset_none),
        ]:
            e = create_engine(
                'postgresql://', pool_reset_on_return=value,
                module=dbapi, _initialize=False)
            assert e.pool._reset_on_return is expected

        assert_raises(
            exc.ArgumentError,
            create_engine, "postgresql://",
            pool_reset_on_return='hi', module=dbapi,
            _initialize=False
        )

    def test_bad_args(self):
        assert_raises(exc.ArgumentError, create_engine, 'foobar://',
                      module=mock_dbapi)

        # bad arg

        assert_raises(TypeError, create_engine, 'postgresql://',
                      use_ansi=True, module=mock_dbapi)

        # bad arg

        assert_raises(
            TypeError,
            create_engine,
            'oracle://',
            lala=5,
            use_ansi=True,
            module=mock_dbapi,
        )
        assert_raises(TypeError, create_engine, 'postgresql://',
                      lala=5, module=mock_dbapi)
        assert_raises(TypeError, create_engine, 'sqlite://', lala=5,
                      module=mock_sqlite_dbapi)
        assert_raises(TypeError, create_engine, 'mysql+mysqldb://',
                      use_unicode=True, module=mock_dbapi)

    def test_urlattr(self):
        """test the url attribute on ``Engine``."""

        e = create_engine('mysql://scott:tiger@localhost/test',
                          module=mock_dbapi, _initialize=False)
        u = url.make_url('mysql://scott:tiger@localhost/test')
        e2 = create_engine(u, module=mock_dbapi, _initialize=False)
        assert e.url.drivername == e2.url.drivername == 'mysql'
        assert e.url.username == e2.url.username == 'scott'
        assert e2.url is u
        assert str(u) == 'mysql://scott:tiger@localhost/test'
        assert repr(u) == 'mysql://scott:***@localhost/test'
        assert repr(e) == 'Engine(mysql://scott:***@localhost/test)'
        assert repr(e2) == 'Engine(mysql://scott:***@localhost/test)'

    def test_poolargs(self):
        """test that connection pool args make it thru"""

        e = create_engine(
            'postgresql://',
            creator=None,
            pool_recycle=50,
            echo_pool=None,
            module=mock_dbapi,
            _initialize=False,
        )
        assert e.pool._recycle == 50

        # these args work for QueuePool

        e = create_engine(
            'postgresql://',
            max_overflow=8,
            pool_timeout=60,
            poolclass=tsa.pool.QueuePool,
            module=mock_dbapi,
            _initialize=False,
        )

        # but not SingletonThreadPool

        assert_raises(
            TypeError,
            create_engine,
            'sqlite://',
            max_overflow=8,
            pool_timeout=60,
            poolclass=tsa.pool.SingletonThreadPool,
            module=mock_sqlite_dbapi,
            _initialize=False,
        )


class TestRegNewDBAPI(fixtures.TestBase):
    def test_register_base(self):
        registry.register("mockdialect", __name__, "MockDialect")

        e = create_engine("mockdialect://")
        assert isinstance(e.dialect, MockDialect)

    def test_register_dotted(self):
        registry.register("mockdialect.foob", __name__, "MockDialect")

        e = create_engine("mockdialect+foob://")
        assert isinstance(e.dialect, MockDialect)

    def test_register_legacy(self):
        tokens = __name__.split(".")

        global dialect
        dialect = MockDialect
        registry.register(
            "mockdialect.foob", ".".join(tokens[0:-1]), tokens[-1])

        e = create_engine("mockdialect+foob://")
        assert isinstance(e.dialect, MockDialect)

    def test_register_per_dbapi(self):
        registry.register("mysql.my_mock_dialect", __name__, "MockDialect")

        e = create_engine("mysql+my_mock_dialect://")
        assert isinstance(e.dialect, MockDialect)

    @testing.requires.sqlite
    def test_wrapper_hooks(self):
        def get_dialect_cls(url):
            url.drivername = "sqlite"
            return url.get_dialect()

        global WrapperFactory
        WrapperFactory = Mock()
        WrapperFactory.get_dialect_cls.side_effect = get_dialect_cls

        registry.register("wrapperdialect", __name__, "WrapperFactory")

        from sqlalchemy.dialects import sqlite
        e = create_engine("wrapperdialect://")

        eq_(e.dialect.name, "sqlite")
        assert isinstance(e.dialect, sqlite.dialect)

        eq_(
            WrapperFactory.mock_calls,
            [
                call.get_dialect_cls(url.make_url("sqlite://")),
                call.engine_created(e)
            ]
        )

    @testing.requires.sqlite
    def test_plugin_registration(self):
        from sqlalchemy.dialects import sqlite

        global MyEnginePlugin

        def side_effect(url, kw):
            eq_(kw, {"logging_name": "foob"})
            kw['logging_name'] = 'bar'
            return MyEnginePlugin

        MyEnginePlugin = Mock(side_effect=side_effect)

        plugins.register("engineplugin", __name__, "MyEnginePlugin")

        e = create_engine(
            "sqlite:///?plugin=engineplugin&foo=bar", logging_name='foob')
        eq_(e.dialect.name, "sqlite")
        eq_(e.logging_name, "bar")
        assert isinstance(e.dialect, sqlite.dialect)

        eq_(
            MyEnginePlugin.mock_calls,
            [
                call(e.url, {}),
                call.handle_dialect_kwargs(sqlite.dialect, mock.ANY),
                call.handle_pool_kwargs(mock.ANY, {"dialect": e.dialect}),
                call.engine_created(e)
            ]
        )
        eq_(
            str(MyEnginePlugin.mock_calls[0][1][0]),
            "sqlite:///?foo=bar"
        )


class MockDialect(DefaultDialect):
    @classmethod
    def dbapi(cls, **kw):
        return MockDBAPI()


def MockDBAPI(**assert_kwargs):
    connection = Mock(get_server_version_info=Mock(return_value='5.0'))

    def connect(*args, **kwargs):
        for k in assert_kwargs:
            assert k in kwargs, 'key %s not present in dictionary' % k
            eq_(
                kwargs[k], assert_kwargs[k]
            )
        return connection

    return MagicMock(
        sqlite_version_info=(99, 9, 9,),
        version_info=(99, 9, 9,),
        sqlite_version='99.9.9',
        paramstyle='named',
        connect=Mock(side_effect=connect)
    )


mock_dbapi = MockDBAPI()
mock_sqlite_dbapi = msd = MockDBAPI()
