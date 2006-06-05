from testbase import PersistTest
import testbase

from sqlalchemy import *

from sqlalchemy.ext.selectresults import SelectResultsExt

class Foo(object):
    pass

class SelectResultsTest(PersistTest):
    def setUpAll(self):
        self.install_threadlocal()
        global foo
        foo = Table('foo', testbase.db,
                    Column('id', Integer, Sequence('foo_id_seq'), primary_key=True),
                    Column('bar', Integer),
                    Column('range', Integer))
        
        assign_mapper(Foo, foo, extension=SelectResultsExt())
        foo.create()
        for i in range(100):
            Foo(bar=i, range=i%10)
        objectstore.flush()
    
    def setUp(self):
        self.query = Foo.mapper.query()
        self.orig = self.query.select_whereclause()
        self.res = self.query.select()
        
    def tearDownAll(self):
        global foo
        foo.drop()
        self.uninstall_threadlocal()
    
    def test_selectby(self):
        res = self.query.select_by(range=5)
        assert res.order_by([Foo.c.bar])[0].bar == 5
        assert res.order_by([desc(Foo.c.bar)])[0].bar == 95
        
    def test_slice(self):
        assert self.res[1] == self.orig[1]
        assert list(self.res[10:20]) == self.orig[10:20]
        assert list(self.res[10:]) == self.orig[10:]
        assert list(self.res[:10]) == self.orig[:10]
        assert list(self.res[:10]) == self.orig[:10]
        assert list(self.res[10:40:3]) == self.orig[10:40:3]
        assert list(self.res[-5:]) == self.orig[-5:]
        assert self.res[10:20][5] == self.orig[10:20][5]

    def test_aggregate(self):
        assert self.res.count() == 100
        assert self.res.filter(foo.c.bar<30).min(foo.c.bar) == 0
        assert self.res.filter(foo.c.bar<30).max(foo.c.bar) == 29

    @testbase.unsupported('mysql')
    def test_aggregate_1(self):
        # this one fails in mysql as the result comes back as a string
        assert self.res.filter(foo.c.bar<30).sum(foo.c.bar) == 435

    @testbase.unsupported('postgres', 'mysql', 'firebird')
    def test_aggregate_2(self):
        # this one fails with postgres, the floating point comparison fails
        assert self.res.filter(foo.c.bar<30).avg(foo.c.bar) == 14.5

    def test_filter(self):
        assert self.res.count() == 100
        assert self.res.filter(Foo.c.bar < 30).count() == 30
        res2 = self.res.filter(Foo.c.bar < 30).filter(Foo.c.bar > 10)
        assert res2.count() == 19
        
    def test_order_by(self):
        assert self.res.order_by([Foo.c.bar])[0].bar == 0
        assert self.res.order_by([desc(Foo.c.bar)])[0].bar == 99

    def test_offset(self):
        assert list(self.res.order_by([Foo.c.bar]).offset(10))[0].bar == 10
        
    def test_offset(self):
        assert len(list(self.res.limit(10))) == 10


if __name__ == "__main__":
    testbase.main()        
