from sqlalchemy import *
from test.lib.testing import eq_
from test.lib import engines
from test.lib import fixtures

# TODO: we should probably build mock bases for
# these to share with test_reconnect, test_parseconnect
class MockDBAPI(object):
    paramstyle = 'qmark'
    def __init__(self):
        self.log = []
    def connect(self, *args, **kwargs):
        return MockConnection(self)

class MockConnection(object):
    def __init__(self, parent):
        self.parent = parent
    def cursor(self):
        return MockCursor(self)
    def close(self):
        pass
    def rollback(self):
        pass
    def commit(self):
        pass

class MockCursor(object):
    description = None
    rowcount = None
    def __init__(self, parent):
        self.parent = parent
    def execute(self, *args, **kwargs):
        self.parent.parent.log.append('execute')
    def executedirect(self, *args, **kwargs):
        self.parent.parent.log.append('executedirect')
    def close(self):
        pass

class MxODBCTest(fixtures.TestBase):

    def test_native_odbc_execute(self):
        t1 = Table('t1', MetaData(), Column('c1', Integer))
        dbapi = MockDBAPI()
        engine = engines.testing_engine('mssql+mxodbc://localhost',
                options={'module': dbapi, '_initialize': False})
        conn = engine.connect()

        # crud: uses execute

        conn.execute(t1.insert().values(c1='foo'))
        conn.execute(t1.delete().where(t1.c.c1 == 'foo'))
        conn.execute(t1.update().where(t1.c.c1 == 'foo').values(c1='bar'
                     ))

        # select: uses executedirect

        conn.execute(t1.select())

        # manual flagging

        conn.execution_options(native_odbc_execute=True).\
                execute(t1.select())
        conn.execution_options(native_odbc_execute=False).\
                execute(t1.insert().values(c1='foo'
                ))
        eq_(dbapi.log, [
            'execute',
            'execute',
            'execute',
            'executedirect',
            'execute',
            'executedirect',
            ])
