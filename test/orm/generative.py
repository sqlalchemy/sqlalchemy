import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exceptions
from testlib import *
import testlib.tables as tables

# TODO: these are more tests that should be updated to be part of test/orm/query.py

class Foo(object):
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

class GenerativeQueryTest(PersistTest):
    def setUpAll(self):
        global foo, metadata
        metadata = MetaData(testbase.db)
        foo = Table('foo', metadata,
                    Column('id', Integer, Sequence('foo_id_seq'), primary_key=True),
                    Column('bar', Integer),
                    Column('range', Integer))
        
        mapper(Foo, foo)
        metadata.create_all()
        
        sess = create_session(bind=testbase.db)
        for i in range(100):
            sess.save(Foo(bar=i, range=i%10))
        sess.flush()
    
    def tearDownAll(self):
        metadata.drop_all()
        clear_mappers()
    
    def test_selectby(self):
        res = create_session(bind=testbase.db).query(Foo).filter_by(range=5)
        assert res.order_by([Foo.c.bar])[0].bar == 5
        assert res.order_by([desc(Foo.c.bar)])[0].bar == 95
        
    @testing.unsupported('mssql')
    def test_slice(self):
        sess = create_session(bind=testbase.db)
        query = sess.query(Foo)
        orig = query.all()
        assert query[1] == orig[1]
        assert list(query[10:20]) == orig[10:20]
        assert list(query[10:]) == orig[10:]
        assert list(query[:10]) == orig[:10]
        assert list(query[:10]) == orig[:10]
        assert list(query[10:40:3]) == orig[10:40:3]
        assert list(query[-5:]) == orig[-5:]
        assert query[10:20][5] == orig[10:20][5]

    @testing.supported('mssql')
    def test_slice_mssql(self):
        sess = create_session(bind=testbase.db)
        query = sess.query(Foo)
        orig = query.all()
        assert list(query[:10]) == orig[:10]
        assert list(query[:10]) == orig[:10]

    def test_aggregate(self):
        sess = create_session(bind=testbase.db)
        query = sess.query(Foo)
        assert query.count() == 100
        assert query.filter(foo.c.bar<30).min(foo.c.bar) == 0
        assert query.filter(foo.c.bar<30).max(foo.c.bar) == 29
        assert query.filter(foo.c.bar<30).apply_max(foo.c.bar).first() == 29
        assert query.filter(foo.c.bar<30).apply_max(foo.c.bar).one() == 29

    @testing.unsupported('mysql')
    def test_aggregate_1(self):
        # this one fails in mysql as the result comes back as a string
        query = create_session(bind=testbase.db).query(Foo)
        assert query.filter(foo.c.bar<30).sum(foo.c.bar) == 435

    @testing.unsupported('postgres', 'mysql', 'firebird', 'mssql')
    def test_aggregate_2(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert query.filter(foo.c.bar<30).avg(foo.c.bar) == 14.5

    @testing.supported('postgres', 'mysql', 'firebird', 'mssql')
    def test_aggregate_2_int(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert int(query.filter(foo.c.bar<30).avg(foo.c.bar)) == 14

    @testing.unsupported('postgres', 'mysql', 'firebird', 'mssql')
    def test_aggregate_3(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert query.filter(foo.c.bar<30).apply_avg(foo.c.bar).first() == 14.5
        assert query.filter(foo.c.bar<30).apply_avg(foo.c.bar).one() == 14.5
        
    def test_filter(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert query.count() == 100
        assert query.filter(Foo.c.bar < 30).count() == 30
        res2 = query.filter(Foo.c.bar < 30).filter(Foo.c.bar > 10)
        assert res2.count() == 19
    
    def test_options(self):
        query = create_session(bind=testbase.db).query(Foo)
        class ext1(MapperExtension):
            def populate_instance(self, mapper, selectcontext, row, instance, **flags):
                instance.TEST = "hello world"
                return EXT_CONTINUE
        assert query.options(extension(ext1()))[0].TEST == "hello world"
        
    def test_order_by(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert query.order_by([Foo.c.bar])[0].bar == 0
        assert query.order_by([desc(Foo.c.bar)])[0].bar == 99

    def test_offset(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert list(query.order_by([Foo.c.bar]).offset(10))[0].bar == 10
        
    def test_offset(self):
        query = create_session(bind=testbase.db).query(Foo)
        assert len(list(query.limit(10))) == 10

class Obj1(object):
    pass
class Obj2(object):
    pass

class GenerativeTest2(PersistTest):
    def setUpAll(self):
        global metadata, table1, table2
        metadata = MetaData()
        table1 = Table('Table1', metadata,
            Column('id', Integer, primary_key=True),
            )
        table2 = Table('Table2', metadata,
            Column('t1id', Integer, ForeignKey("Table1.id"), primary_key=True),
            Column('num', Integer, primary_key=True),
            )
        mapper(Obj1, table1)
        mapper(Obj2, table2)
        metadata.create_all(bind=testbase.db)
        testbase.db.execute(table1.insert(), {'id':1},{'id':2},{'id':3},{'id':4})
        testbase.db.execute(table2.insert(), {'num':1,'t1id':1},{'num':2,'t1id':1},{'num':3,'t1id':1},\
{'num':4,'t1id':2},{'num':5,'t1id':2},{'num':6,'t1id':3})

    def tearDownAll(self):
        metadata.drop_all(bind=testbase.db)
        clear_mappers()

    def test_distinctcount(self):
        query = create_session(bind=testbase.db).query(Obj1)
        assert query.count() == 4
        res = query.filter(and_(table1.c.id==table2.c.t1id,table2.c.t1id==1))
        assert res.count() == 3
        res = query.filter(and_(table1.c.id==table2.c.t1id,table2.c.t1id==1)).distinct()
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
        session = create_session(bind=testbase.db)
        query = session.query(tables.User)
        x = query.join(['orders', 'items']).filter(tables.Item.c.item_id==2)
        print x.compile()
        self.assert_result(list(x), tables.User, tables.user_result[2])
    def test_outerjointo(self):
        """test the join and outerjoin functions on Query"""
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session(bind=testbase.db)
        query = session.query(tables.User)
        x = query.outerjoin(['orders', 'items']).filter(or_(tables.Order.c.order_id==None,tables.Item.c.item_id==2))
        print x.compile()
        self.assert_result(list(x), tables.User, *tables.user_result[1:3])
    def test_outerjointo_count(self):
        """test the join and outerjoin functions on Query"""
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session(bind=testbase.db)
        query = session.query(tables.User)
        x = query.outerjoin(['orders', 'items']).filter(or_(tables.Order.c.order_id==None,tables.Item.c.item_id==2)).count()
        assert x==2
    def test_from(self):
        mapper(tables.User, tables.users, properties={
            'orders':relation(mapper(tables.Order, tables.orders, properties={
                'items':relation(mapper(tables.Item, tables.orderitems))
            }))
        })
        session = create_session(bind=testbase.db)
        query = session.query(tables.User)
        x = query.select_from([tables.users.outerjoin(tables.orders).outerjoin(tables.orderitems)]).\
            filter(or_(tables.Order.c.order_id==None,tables.Item.c.item_id==2))
        print x.compile()
        self.assert_result(list(x), tables.User, *tables.user_result[1:3])
        

class CaseSensitiveTest(PersistTest):
    def setUpAll(self):
        global metadata, table1, table2
        metadata = MetaData(testbase.db)
        table1 = Table('Table1', metadata,
            Column('ID', Integer, primary_key=True),
            )
        table2 = Table('Table2', metadata,
            Column('T1ID', Integer, ForeignKey("Table1.ID"), primary_key=True),
            Column('NUM', Integer, primary_key=True),
            )
        mapper(Obj1, table1)
        mapper(Obj2, table2)
        metadata.create_all()
        table1.insert().execute({'ID':1},{'ID':2},{'ID':3},{'ID':4})
        table2.insert().execute({'NUM':1,'T1ID':1},{'NUM':2,'T1ID':1},{'NUM':3,'T1ID':1},\
{'NUM':4,'T1ID':2},{'NUM':5,'T1ID':2},{'NUM':6,'T1ID':3})

    def tearDownAll(self):
        metadata.drop_all()
        clear_mappers()
        
    def test_distinctcount(self):
        q = create_session(bind=testbase.db).query(Obj1)
        assert q.count() == 4
        res = q.filter(and_(table1.c.ID==table2.c.T1ID,table2.c.T1ID==1))
        assert res.count() == 3
        res = q.filter(and_(table1.c.ID==table2.c.T1ID,table2.c.T1ID==1)).distinct()
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
        sess = create_session(bind=testbase.db)
        try:
            sess.query(T).join('children').select_by(id=7)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Self-referential query on 'T.children (T)' property requires create_aliases=True argument.", str(e)

        try:
            sess.query(T).join(['children']).select_by(id=7)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Self-referential query on 'T.children (T)' property requires create_aliases=True argument.", str(e)
        
            
            
if __name__ == "__main__":
    testbase.main()        
