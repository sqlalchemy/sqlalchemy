import testenv; testenv.configure_for_tests()
import sys, weakref
from sqlalchemy import create_engine, exceptions, select, MetaData, Table, Column, Integer, String
from testlib import *


class MockDisconnect(Exception):
    pass

class MockDBAPI(object):
    def __init__(self):
        self.paramstyle = 'named'
        self.connections = weakref.WeakKeyDictionary()
    def connect(self, *args, **kwargs):
        return MockConnection(self)
    def shutdown(self):
        for c in self.connections:
            c.explode[0] = True
    Error = MockDisconnect

class MockConnection(object):
    def __init__(self, dbapi):
        dbapi.connections[self] = True
        self.explode = [False]
    def rollback(self):
        pass
    def commit(self):
        pass
    def cursor(self):
        return MockCursor(self)
    def close(self):
        pass

class MockCursor(object):
    def __init__(self, parent):
        self.explode = parent.explode
        self.description = None
    def execute(self, *args, **kwargs):
        if self.explode[0]:
            raise MockDisconnect("Lost the DB connection")
        else:
            return
    def close(self):
        pass

class MockReconnectTest(TestBase):
    def setUp(self):
        global db, dbapi
        dbapi = MockDBAPI()

        # create engine using our current dburi
        db = create_engine('postgres://foo:bar@localhost/test', module=dbapi)

        # monkeypatch disconnect checker
        db.dialect.is_disconnect = lambda e: isinstance(e, MockDisconnect)

    def test_reconnect(self):
        """test that an 'is_disconnect' condition will invalidate the connection, and additionally
        dispose the previous connection pool and recreate."""


        pid = id(db.pool)

        # make a connection
        conn = db.connect()

        # connection works
        conn.execute(select([1]))

        # create a second connection within the pool, which we'll ensure also goes away
        conn2 = db.connect()
        conn2.close()

        # two connections opened total now
        assert len(dbapi.connections) == 2

        # set it to fail
        dbapi.shutdown()

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError:
            pass

        # assert was invalidated
        assert not conn.closed
        assert conn.invalidated

        # close shouldnt break
        conn.close()

        assert id(db.pool) != pid

        # ensure all connections closed (pool was recycled)
        assert len(dbapi.connections) == 0

        conn =db.connect()
        conn.execute(select([1]))
        conn.close()
        assert len(dbapi.connections) == 1

    def test_invalidate_trans(self):
        conn = db.connect()
        trans = conn.begin()
        dbapi.shutdown()

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError:
            pass

        # assert was invalidated
        assert len(dbapi.connections) == 0
        assert not conn.closed
        assert conn.invalidated
        assert trans.is_active

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Can't reconnect until invalid transaction is rolled back"

        assert trans.is_active

        try:
            trans.commit()
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Can't reconnect until invalid transaction is rolled back"

        assert trans.is_active

        trans.rollback()
        assert not trans.is_active

        conn.execute(select([1]))
        assert not conn.invalidated

        assert len(dbapi.connections) == 1

    def test_conn_reusable(self):
        conn = db.connect()

        conn.execute(select([1]))

        assert len(dbapi.connections) == 1

        dbapi.shutdown()

        # raises error
        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError:
            pass

        assert not conn.closed
        assert conn.invalidated

        # ensure all connections closed (pool was recycled)
        assert len(dbapi.connections) == 0

        # test reconnects
        conn.execute(select([1]))
        assert not conn.invalidated
        assert len(dbapi.connections) == 1


class RealReconnectTest(TestBase):
    def setUp(self):
        global engine
        engine = engines.reconnecting_engine()

    def tearDown(self):
        engine.dispose()

    def test_reconnect(self):
        conn = engine.connect()

        self.assertEquals(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed

        engine.test_shutdown()

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError, e:
            if not e.connection_invalidated:
                raise

        assert not conn.closed
        assert conn.invalidated

        assert conn.invalidated
        self.assertEquals(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated

        # one more time
        engine.test_shutdown()
        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError, e:
            if not e.connection_invalidated:
                raise
        assert conn.invalidated
        self.assertEquals(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated

        conn.close()
    
    def test_close(self):
        conn = engine.connect()
        self.assertEquals(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed

        engine.test_shutdown()

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError, e:
            if not e.connection_invalidated:
                raise

        conn.close()
        conn = engine.connect()
        self.assertEquals(conn.execute(select([1])).scalar(), 1)

    def test_with_transaction(self):
        conn = engine.connect()

        trans = conn.begin()

        self.assertEquals(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed

        engine.test_shutdown()

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.DBAPIError, e:
            if not e.connection_invalidated:
                raise

        assert not conn.closed
        assert conn.invalidated
        assert trans.is_active

        try:
            conn.execute(select([1]))
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Can't reconnect until invalid transaction is rolled back"

        assert trans.is_active

        try:
            trans.commit()
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Can't reconnect until invalid transaction is rolled back"

        assert trans.is_active

        trans.rollback()
        assert not trans.is_active

        assert conn.invalidated
        self.assertEquals(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated

class InvalidateDuringResultTest(TestBase):
    def setUp(self):
        global meta, table, engine
        engine = engines.reconnecting_engine()
        meta = MetaData(engine)
        table = Table('sometable', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)))
        meta.create_all()
        table.insert().execute(
            [{'id':i, 'name':'row %d' % i} for i in range(1, 100)]
        )
        
    def tearDown(self):
        meta.drop_all()
        engine.dispose()
    
    @testing.fails_on('mysql')    
    def test_invalidate_on_results(self):
        conn = engine.connect()
        
        result = conn.execute("select * from sometable")
        for x in xrange(20):
            result.fetchone()
        
        engine.test_shutdown()
        try:
            result.fetchone()
            assert False
        except exceptions.DBAPIError, e:
            if not e.connection_invalidated:
                raise

        assert conn.invalidated
        
if __name__ == '__main__':
    testenv.main()
