from testbase import PersistTest
import testbase
import sqlalchemy.engine.url as url
from sqlalchemy import *
import unittest

        
class ParseConnectTest(PersistTest):
    def testrfc1738(self):
        for text in (
            'dbtype://username:password@hostspec:110//usr/db_file.db',
            'dbtype://username:password@hostspec/database',
            'dbtype://username:password@hostspec',
            'dbtype://username:password@/database',
            'dbtype://username@hostspec',
            'dbtype://username:password@127.0.0.1:1521',
            'dbtype://hostspec/database',
            'dbtype://hostspec',
            'dbtype://hostspec/?arg1=val1&arg2=val2',
            'dbtype:///database',
            'dbtype:///:memory:',
            'dbtype:///foo/bar/im/a/file',
            'dbtype:///E:/work/src/LEM/db/hello.db',
            'dbtype:///E:/work/src/LEM/db/hello.db?foo=bar&hoho=lala',
            'dbtype://',
            'dbtype://username:password@/db',
            'dbtype:////usr/local/mailman/lists/_xtest@example.com/members.db',
            'dbtype://username:apples%2Foranges@hostspec/mydatabase',
        ):
            u = url.make_url(text)
            print u, text
            print "username=", u.username, "password=", u.password,  "database=", u.database, "host=", u.host
            assert u.drivername == 'dbtype'
            assert u.username == 'username' or u.username is None
            assert u.password == 'password' or u.password == 'apples/oranges' or u.password is None
            assert u.host == 'hostspec' or u.host == '127.0.0.1' or (not u.host)
            assert str(u) == text

class CreateEngineTest(PersistTest):
    """test that create_engine arguments of different types get propigated properly"""
    def testconnectquery(self):
        dbapi = MockDBAPI(foober='12', lala='18', fooz='somevalue')
        
        # start the postgres dialect, but put our mock DBAPI as the module instead of psycopg
        e = create_engine('postgres://scott:tiger@somehost/test?foober=12&lala=18&fooz=somevalue', module=dbapi)
        c = e.connect()

    def testkwargs(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this':'dict'}, fooz='somevalue')

        # start the postgres dialect, but put our mock DBAPI as the module instead of psycopg
        e = create_engine('postgres://scott:tiger@somehost/test?fooz=somevalue', connect_args={'foober':12, 'lala':18, 'hoho':{'this':'dict'}}, module=dbapi)
        c = e.connect()

    def testcustom(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this':'dict'}, fooz='somevalue')

        def connect():
            return dbapi.connect(foober=12, lala=18, fooz='somevalue', hoho={'this':'dict'})
            
        # start the postgres dialect, but put our mock DBAPI as the module instead of psycopg
        e = create_engine('postgres://', creator=connect, module=dbapi)
        c = e.connect()
    
    def testrecycle(self):
        dbapi = MockDBAPI(foober=12, lala=18, hoho={'this':'dict'}, fooz='somevalue')
        e = create_engine('postgres://', pool_recycle=472, module=dbapi)
        assert e.connection_provider._pool._recycle == 472
        
    def testbadargs(self):
        # good arg, use MockDBAPI to prevent oracle import errors
        e = create_engine('oracle://', use_ansi=True, module=MockDBAPI())

        # bad arg
        try:
            e = create_engine('postgres://', use_ansi=True, module=MockDBAPI())
            assert False
        except TypeError:
            assert True
        
        # bad arg
        try:
            e = create_engine('oracle://', lala=5, use_ansi=True, module=MockDBAPI())
            assert False
        except TypeError:
            assert True
            
        try:
            e = create_engine('postgres://', lala=5, module=MockDBAPI())
            assert False
        except TypeError:
            assert True
        
        try:
            e = create_engine('sqlite://', lala=5)
            assert False
        except TypeError:
            assert True

        try:
            e = create_engine('mysql://', use_unicode=True)
            assert False
        except TypeError:
            assert True

        try:
            # sqlite uses SingletonThreadPool which doesnt have max_overflow
            e = create_engine('sqlite://', max_overflow=5)
            assert False
        except TypeError:
            assert True
            
        e = create_engine('sqlite://', echo=True)
        e = create_engine('mysql://', module=MockDBAPI(), connect_args={'use_unicode':True}, convert_unicode=True)
        
        e = create_engine('sqlite://', connect_args={'use_unicode':True}, convert_unicode=True)
        try:
            c = e.connect()
            assert False
        except exceptions.DBAPIError:
            assert True
            
    def testpoolargs(self):
        """test that connection pool args make it thru"""
        e = create_engine('postgres://', creator=None, pool_recycle=-1, echo_pool=None, auto_close_cursors=False, disallow_open_cursors=True, module=MockDBAPI())
        assert e.connection_provider._pool.auto_close_cursors is False
        assert e.connection_provider._pool.disallow_open_cursors is True

        # these args work for QueuePool
        e = create_engine('postgres://', max_overflow=8, pool_timeout=60, poolclass=pool.QueuePool, module=MockDBAPI())

        try:
            # but not SingletonThreadPool
            e = create_engine('sqlite://', max_overflow=8, pool_timeout=60, poolclass=pool.SingletonThreadPool)
            assert False
        except TypeError:
            assert True

class MockDBAPI(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.paramstyle = 'named'
    def connect(self, **kwargs):
        print kwargs, self.kwargs
        for k in self.kwargs:
            assert k in kwargs, "key %s not present in dictionary" % k
            assert kwargs[k]==self.kwargs[k], "value %s does not match %s" % (kwargs[k], self.kwargs[k])
        return MockConnection()
class MockConnection(object):
    def close(self):
        pass
    def cursor(self):
        return MockCursor()
class MockCursor(object):
    def close(self):
        pass
mock_dbapi = MockDBAPI()
            
if __name__ == "__main__":
    testbase.main()
        