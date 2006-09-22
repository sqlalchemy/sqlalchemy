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
        clear_mappers()
    
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

class Obj1(object):
    pass
class Obj2(object):
    pass

class SelectResultsTest2(PersistTest):
    def setUpAll(self):
        self.install_threadlocal()
        global metadata, table1, table2
        metadata = BoundMetaData(testbase.db)
        table1 = Table('Table1', metadata,
            Column('id', Integer, primary_key=True),
            )
        table2 = Table('Table2', metadata,
            Column('t1id', Integer, ForeignKey("Table1.id"), primary_key=True),
            Column('num', Integer, primary_key=True),
            )
        assign_mapper(Obj1, table1, extension=SelectResultsExt())
        assign_mapper(Obj2, table2, extension=SelectResultsExt())
        metadata.create_all()
        table1.insert().execute({'id':1},{'id':2},{'id':3},{'id':4})
        table2.insert().execute({'num':1,'t1id':1},{'num':2,'t1id':1},{'num':3,'t1id':1},\
{'num':4,'t1id':2},{'num':5,'t1id':2},{'num':6,'t1id':3})

    def setUp(self):
        self.query = Obj1.mapper.query()
        #self.orig = self.query.select_whereclause()
        #self.res = self.query.select()

    def tearDownAll(self):
        metadata.drop_all()
        self.uninstall_threadlocal()
        clear_mappers()

    def test_distinctcount(self):
        res = self.query.select()
        assert res.count() == 4
        res = self.query.select(and_(table1.c.id==table2.c.t1id,table2.c.t1id==1))
        assert res.count() == 3
        res = self.query.select(and_(table1.c.id==table2.c.t1id,table2.c.t1id==1), distinct=True)
        self.assertEqual(res.count(), 1)

class SelectResultsTest3(PersistTest):
    def setUpAll(self):
        self.install_threadlocal()
        global metadata, table1, table2
        metadata = BoundMetaData(testbase.db)
        table1 = Table('Table1', metadata,
            Column('ID', Integer, primary_key=True),
            )
        table2 = Table('Table2', metadata,
            Column('T1ID', Integer, ForeignKey("Table1.ID"), primary_key=True),
            Column('NUM', Integer, primary_key=True),
            )
        assign_mapper(Obj1, table1, extension=SelectResultsExt())
        assign_mapper(Obj2, table2, extension=SelectResultsExt())
        metadata.create_all()
        table1.insert().execute({'ID':1},{'ID':2},{'ID':3},{'ID':4})
        table2.insert().execute({'NUM':1,'T1ID':1},{'NUM':2,'T1ID':1},{'NUM':3,'T1ID':1},\
{'NUM':4,'T1ID':2},{'NUM':5,'T1ID':2},{'NUM':6,'T1ID':3})

    def setUp(self):
        self.query = Obj1.mapper.query()
        #self.orig = self.query.select_whereclause()
        #self.res = self.query.select()

    def tearDownAll(self):
        metadata.drop_all()
        self.uninstall_threadlocal()
        clear_mappers()
        
    def test_distinctcount(self):
        res = self.query.select()
        assert res.count() == 4
        res = self.query.select(and_(table1.c.ID==table2.c.T1ID,table2.c.T1ID==1))
        assert res.count() == 3
        res = self.query.select(and_(table1.c.ID==table2.c.T1ID,table2.c.T1ID==1), distinct=True)
        self.assertEqual(res.count(), 1)


if __name__ == "__main__":
    testbase.main()        
