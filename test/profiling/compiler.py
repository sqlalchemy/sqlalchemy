import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from testlib import *


class CompileTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global t1, t2, metadata
        metadata = MetaData()
        t1 = Table('t1', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)))

        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)))

    @profiling.function_call_count(42, {'2.3': 44})
    def test_insert(self):
        t1.insert().compile()

    @profiling.function_call_count(42, {'2.3': 47})
    def test_update(self):
        t1.update().compile()

    # TODO: this is alittle high
    @profiling.function_call_count(125, versions={'2.3': 180})
    def test_select(self):
        s = select([t1], t1.c.c2==t2.c.c1)
        s.compile()


if __name__ == '__main__':
    testenv.main()
