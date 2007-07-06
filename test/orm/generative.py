from testbase import PersistTest, AssertMixin, ORMTest
import testbase
import tables

from sqlalchemy import *
from sqlalchemy import exceptions

class Foo(object):
    pass

class GenerativeQueryTest(PersistTest):
    def setUpAll(self):
        self.install_threadlocal()
        global foo, metadata
        metadata = MetaData(testbase.db)
        foo = Table('foo', metadata,
                    Column('id', Integer, Sequence('foo_id_seq'), primary_key=True),
                    Column('bar', Integer),
                    Column('range', Integer))
        
        assign_mapper(Foo, foo)
        metadata.create_all()
        for i in range(100):
            Foo(bar=i, range=i%10)
        objectstore.flush()
    
    def setUp(self):
        self.query = Foo.query()
        self.orig = self.query.select_whereclause()
        self.res = self.query
        
    def tearDownAll(self):
        metadata.drop_all()
        self.uninstall_threadlocal()
        clear_mappers()
    
    def test_selectby(self):
        res = self.query.filter_by(range=5)
        assert res.order_by([Foo.c.bar])[0].bar == 5
        assert res.order_by([desc(Foo.c.bar)])[0].bar == 95
        
    @testbase.unsupported('mssql')
    def test_slice(self):
        assert self.query[1] == self.orig[1]
        assert list(self.query[10:20]) == self.orig[10:20]
        assert list(self.query[10:]) == self.orig[10:]
        assert list(self.query[:10]) == self.orig[:10]
        assert list(self.query[:10]) == self.orig[:10]
        assert list(self.query[10:40:3]) == self.orig[10:40:3]
        assert list(self.query[-5:]) == self.orig[-5:]
        assert self.query[10:20][5] == self.orig[10:20][5]

    @testbase.supported('mssql')
    def test_slice_mssql(self):
        assert list(self.query[:10]) == self.orig[:10]
        assert list(self.query[:10]) == self.orig[:10]

    def test_aggregate(self):
        assert self.query.count() == 100
        assert self.query.filter(foo.c.bar<30).min(foo.c.bar) == 0
        assert self.query.filter(foo.c.bar<30).max(foo.c.bar) == 29
        assert self.query.filter(foo.c.bar<30).apply_max(foo.c.bar).scalar() == 29

    @testbase.unsupported('mysql')
    def test_aggregate_1(self):
        # this one fails in mysql as the result comes back as a string
        assert self.query.filter(foo.c.bar<30).sum(foo.c.bar) == 435

    @testbase.unsupported('postgres', 'mysql', 'firebird', 'mssql')
    def test_aggregate_2(self):
        assert self.res.filter(foo.c.bar<30).avg(foo.c.bar) == 14.5

    @testbase.supported('postgres', 'mysql', 'firebird', 'mssql')
    def test_aggregate_2_int(self):
        assert int(self.res.filter(foo.c.bar<30).avg(foo.c.bar)) == 14

    @testbase.unsupported('postgres', 'mysql', 'firebird', 'mssql')
    def test_aggregate_3(self):
        assert self.res.filter(foo.c.bar<30).apply_avg(foo.c.bar).scalar() == 14.5
        
    def test_filter(self):
        assert self.query.count() == 100
        assert self.query.filter(Foo.c.bar < 30).count() == 30
        res2 = self.query.filter(Foo.c.bar < 30).filter(Foo.c.bar > 10)
        assert res2.count() == 19
    
    def test_options(self):
        class ext1(MapperExtension):
            def populate_instance(self, mapper, selectcontext, row, instance, identitykey, isnew):
                instance.TEST = "hello world"
                return EXT_PASS
        objectstore.clear()
        assert self.res.options(extension(ext1()))[0].TEST == "hello world"
        
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

class GenerativeTest2(PersistTest):
    def setUpAll(self):
        self.install_threadlocal()
        global metadata, table1, table2
        metadata = MetaData(testbase.db)
        table1 = Table('Table1', metadata,
            Column('id', Integer, primary_key=True),
            )
        table2 = Table('Table2', metadata,
            Column('t1id', Integer, ForeignKey("Table1.id"), primary_key=True),
            Column('num', Integer, primary_key=True),
            )
        assign_mapper(Obj1, table1)
        assign_mapper(Obj2, table2)
        metadata.create_all()
        table1.insert().execute({'id':1},{'id':2},{'id':3},{'id':4})
        table2.insert().execute({'num':1,'t1id':1},{'num':2,'t1id':1},{'num':3,'t1id':1},\
{'num':4,'t1id':2},{'num':5,'t1id':2},{'num':6,'t1id':3})

    def setUp(self):
        self.query = Query(Obj1)
        #self.orig = self.query.select_whereclause()
        #self.res = self.query.select()

    def tearDownAll(self):
        metadata.drop_all()
        self.uninstall_threadlocal()
        clear_mappers()

    def test_distinctcount(self):
        res = self.query
        assert res.count() == 4
        res = self.query.filter(and_(table1.c.id==table2.c.t1id,table2.c.t1id==1))
        assert res.count() == 3
        res = self.query.filter(and_(table1.c.id==table2.c.t1id,table2.c.t1id==1)).distinct()
        self.assertEqual(res.count(), 1)

class RelationsTest(AssertMixin):
    def setUpAll(self):
        tables.create()
        tables.data()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        clear_mappers()
    def test_jointo(self):
        """test the join and outerjoin functions on Query"""
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session()
        query = session.query(tables.User)
        x = query.join('orders').join('items').filter(tables.Item.c.item_id==2)
        print x.compile()
        self.assert_result(list(x), tables.User, tables.user_result[2])
    def test_outerjointo(self):
        """test the join and outerjoin functions on Query"""
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session()
        query = session.query(tables.User)
        x = query.outerjoin('orders').outerjoin('items').filter(or_(tables.Order.c.order_id==None,tables.Item.c.item_id==2))
        print x.compile()
        self.assert_result(list(x), tables.User, *tables.user_result[1:3])
    def test_outerjointo_count(self):
        """test the join and outerjoin functions on Query"""
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session()
        query = session.query(tables.User)
        x = query.outerjoin('orders').outerjoin('items').filter(or_(tables.Order.c.order_id==None,tables.Item.c.item_id==2)).count()
        assert x==2
    def test_from(self):
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session()
        query = session.query(tables.User)
        x = query.select_from([tables.users.outerjoin(tables.orders).outerjoin(tables.orderitems)]).\
            filter(or_(tables.Order.c.order_id==None,tables.Item.c.item_id==2))
        print x.compile()
        self.assert_result(list(x), tables.User, *tables.user_result[1:3])
        

class CaseSensitiveTest(PersistTest):
    def setUpAll(self):
        self.install_threadlocal()
        global metadata, table1, table2
        metadata = MetaData(testbase.db)
        table1 = Table('Table1', metadata,
            Column('ID', Integer, primary_key=True),
            )
        table2 = Table('Table2', metadata,
            Column('T1ID', Integer, ForeignKey("Table1.ID"), primary_key=True),
            Column('NUM', Integer, primary_key=True),
            )
        assign_mapper(Obj1, table1)
        assign_mapper(Obj2, table2)
        metadata.create_all()
        table1.insert().execute({'ID':1},{'ID':2},{'ID':3},{'ID':4})
        table2.insert().execute({'NUM':1,'T1ID':1},{'NUM':2,'T1ID':1},{'NUM':3,'T1ID':1},\
{'NUM':4,'T1ID':2},{'NUM':5,'T1ID':2},{'NUM':6,'T1ID':3})

    def setUp(self):
        self.query = Query(Obj1)
        #self.orig = self.query.select_whereclause()
        #self.res = self.query.select()

    def tearDownAll(self):
        metadata.drop_all()
        self.uninstall_threadlocal()
        clear_mappers()
        
    def test_distinctcount(self):
        res = self.query
        assert res.count() == 4
        res = self.query.filter(and_(table1.c.ID==table2.c.T1ID,table2.c.T1ID==1))
        assert res.count() == 3
        res = self.query.filter(and_(table1.c.ID==table2.c.T1ID,table2.c.T1ID==1)).distinct()
        self.assertEqual(res.count(), 1)

class SelfRefTest(ORMTest):
    def define_tables(self, metadata):
        global t1
        t1 = Table('t1', metadata, 
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('t1.id'))
            )
    def test_noautojoin(self):
        class T(object):pass
        mapper(T, t1, properties={'children':relation(T)})
        sess = create_session()
        try:
            sess.query(T).join('children').select_by(id=7)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Self-referential query on 'T.children (T)' property must be constructed manually using an Alias object for the related table.", str(e)

        try:
            sess.query(T).join(['children']).select_by(id=7)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Self-referential query on 'T.children (T)' property must be constructed manually using an Alias object for the related table.", str(e)
        
            
            
if __name__ == "__main__":
    testbase.main()        
