import testbase
from sqlalchemy import *
from testlib import *


class CompileTest(AssertMixin):
    def setUpAll(self):
        global t1, t2, metadata
        metadata = MetaData()
        t1 = Table('t1', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)))

        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)))

    @profiling.profiled('ctest_insert', call_range=(40, 50), always=True)        
    def test_insert(self):
        t1.insert().compile()

    @profiling.profiled('ctest_update', call_range=(40, 50), always=True)        
    def test_update(self):
        t1.update().compile()

    # TODO: this is alittle high
    @profiling.profiled('ctest_select', call_range=(190, 210), always=True)        
    def test_select(self):
        s = select([t1], t1.c.c2==t2.c.c1)
        s.compile()
        
        
if __name__ == '__main__':
    testbase.main()        
