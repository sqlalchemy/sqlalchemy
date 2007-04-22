import testbase
from sqlalchemy import create_engine, exceptions
import gc, weakref, sys

class MockDisconnect(Exception):
    pass

class MockDBAPI(object):
    def __init__(self):
        self.paramstyle = 'named'
        self.connections = weakref.WeakKeyDictionary()
    def connect(self, *args, **kwargs):
        return MockConnection(self)
        
class MockConnection(object):
    def __init__(self, dbapi):
        self.explode = False
        dbapi.connections[self] = True
    def rollback(self):
        pass
    def commit(self):
        pass
    def cursor(self):
        return MockCursor(explode=self.explode)
    def close(self):
        pass
            
class MockCursor(object):
    def __init__(self, explode):
        self.explode = explode
        self.description = None
    def execute(self, *args, **kwargs):
        if self.explode:
            raise MockDisconnect("Lost the DB connection")
        else:
            return
    def close(self):
        pass
        
class ReconnectTest(testbase.PersistTest):
    def test_reconnect(self):
        """test that an 'is_disconnect' condition will invalidate the connection, and additionally
        dispose the previous connection pool and recreate."""
        
        dbapi = MockDBAPI()
        
        # create engine using our current dburi
        db = create_engine('postgres://foo:bar@localhost/test', module=dbapi)
        
        # monkeypatch disconnect checker
        db.dialect.is_disconnect = lambda e: isinstance(e, MockDisconnect)
        
        pid = id(db.connection_provider._pool)
        
        # make a connection
        conn = db.connect()
        
        # connection works
        conn.execute("SELECT 1")
        
        # create a second connection within the pool, which we'll ensure also goes away
        conn2 = db.connect()
        conn2.close()

        # two connections opened total now
        assert len(dbapi.connections) == 2

        # set it to fail
        conn.connection.connection.explode = True
        
        try:
            # execute should fail
            conn.execute("SELECT 1")
            assert False
        except exceptions.SQLAlchemyError, e:
            pass
        
        # assert was invalidated
        assert conn.connection.connection is None
        
        # close shouldnt break
        conn.close()

        assert id(db.connection_provider._pool) != pid
        
        # ensure all connections closed (pool was recycled)
        assert len(dbapi.connections) == 0
        
        conn =db.connect()
        conn.execute("SELECT 1")
        conn.close()
        assert len(dbapi.connections) == 1
        
if __name__ == '__main__':
    testbase.main()