from sqlalchemy import *
from sqlalchemy.test import *


class CompileTest(TestBase, AssertsExecutionResults):
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

    @profiling.function_call_count(72, {'2.4': 49, '3.0':77, '3.1':77})
    def test_insert(self):
        t1.insert().compile()

    @profiling.function_call_count(72, {'2.4': 50})
    def test_update(self):
        t1.update().compile()

    @profiling.function_call_count(195, versions={'2.4':118, '3.0':208, '3.1':208})
    def test_select(self):
        s = select([t1], t1.c.c2==t2.c.c1)
        s.compile()

