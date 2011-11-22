from sqlalchemy import *
from test.lib import *
from sqlalchemy.engine import default

class CompileTest(fixtures.TestBase, AssertsExecutionResults):
    __requires__ = 'cpython',

    @classmethod
    def setup_class(cls):

        global t1, t2, metadata
        metadata = MetaData()
        t1 = Table('t1', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)))

        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)))

        # do a "compile" ahead of time to load
        # deferred imports
        t1.insert().compile()

        # go through all the TypeEngine
        # objects in use and pre-load their _type_affinity
        # entries.
        for t in (t1, t2):
            for c in t.c:
                c.type._type_affinity
        from sqlalchemy import types
        for t in types._type_map.values():
            t._type_affinity

        cls.dialect = default.DefaultDialect()

    @profiling.function_call_count(versions={'2.7':62, '2.6':62,
                                            '3':68})
    def test_insert(self):
        t1.insert().compile(dialect=self.dialect)

    @profiling.function_call_count(versions={'2.6':56, '2.7':56})
    def test_update(self):
        t1.update().compile(dialect=self.dialect)

    @profiling.function_call_count(versions={'2.6':117, '2.7':117, '3':118})
    def test_update_whereclause(self):
        t1.update().where(t1.c.c2==12).compile(dialect=self.dialect)

    @profiling.function_call_count(versions={'2.7':148, '2.6':148,
                                                '3':161})
    def test_select(self):
        s = select([t1], t1.c.c2==t2.c.c1)
        s.compile(dialect=self.dialect)

