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

    def test_insert(self):
        t1.insert().compile()
    test_insert = profiling.function_call_count(74, {'2.3': 44, '2.4': 42})(test_insert)

    def test_update(self):
        t1.update().compile()
    test_update = profiling.function_call_count(75, {'2.3': 47, '2.4': 42})(test_update)

    def test_select(self):
        s = select([t1], t1.c.c2==t2.c.c1)
        s.compile()
    test_select = profiling.function_call_count(228, versions={'2.3': 153, '2.4':116})(test_select)


if __name__ == '__main__':
    testenv.main()
