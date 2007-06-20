from sqlalchemy import *
from sqlalchemy.orm import *
import testbase
from testbase import Table, Column
from fixtures import *

class Base(object):
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __eq__(self, other):
        """'passively' compare this object to another.
        
        only look at attributes that are present on the source object.
        
        """
        # use __dict__ to avoid instrumented properties
        for attr in self.__dict__.keys():
            if attr[0] == '_':
                continue
            value = getattr(self, attr)
            if hasattr(value, '__iter__') and not isinstance(value, basestring):
                if len(value) == 0:
                    continue
                for (us, them) in zip(value, getattr(other, attr)):
                    if us != them:
                        return False
                else:
                    continue
            else:
                if value is not None:
                    if value != getattr(other, attr):
                        return False
        else:
            return True

class QueryTest(testbase.ORMTest):
    keep_mappers = True
    keep_data = True
    
    def setUpAll(self):
        super(QueryTest, self).setUpAll()
        install_fixture_data()
        self.setup_mappers()
    
    def tearDownAll(self):
        clear_mappers()
        super(QueryTest, self).tearDownAll()
          
    def define_tables(self, meta):
        # a slight dirty trick here. 
        meta.tables = metadata.tables
        metadata.connect(meta.engine)
        
    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address),
            'orders':relation(Order, backref='user'), # o2m, m2o
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items),  #m2m
            'address':relation(Address),  # m2o
        })
        mapper(Item, items, properties={
            'keywords':relation(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

    @property
    def user_address_result(self):
        return [
            User(id=7, addresses=[
                Address(id=1)
            ]), 
            User(id=8, addresses=[
                Address(id=2),
                Address(id=3),
                Address(id=4)
            ]), 
            User(id=9, addresses=[
                Address(id=5)
            ]), 
            User(id=10, addresses=[])
        ]

    @property
    def user_all_result(self):
        return [
            User(id=7, addresses=[
                Address(id=1)
            ], orders=[
                Order(description='order 1', items=[Item(description='item 1'), Item(description='item 2'), Item(description='item 3')]),
                Order(description='order 3'),
                Order(description='order 5'),
            ]), 
            User(id=8, addresses=[
                Address(id=2),
                Address(id=3),
                Address(id=4)
            ]), 
            User(id=9, addresses=[
                Address(id=5)
            ], orders=[
                Order(description='order 2', items=[Item(description='item 1'), Item(description='item 2'), Item(description='item 3')]),
                Order(description='order 4', items=[Item(description='item 1'), Item(description='item 5')]),
            ]), 
            User(id=10, addresses=[])
        ]
        
class GetTest(QueryTest):
    def test_get(self):
        s = create_session()
        assert s.query(User).get(19) is None
        u = s.query(User).get(7)
        u2 = s.query(User).get(7)
        assert u is u2
        s.clear()
        u2 = s.query(User).get(7)
        assert u is not u2

    def test_unicode(self):
        """test that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail 
        on postgres, mysql and oracle unless it is converted to an encoded string"""
        
        table = Table('unicode_data', users.metadata, 
            Column('id', Unicode(40), primary_key=True),
            Column('data', Unicode(40)))
        table.create()
        ustring = 'petit voix m\xe2\x80\x99a '.decode('utf-8')
        table.insert().execute(id=ustring, data=ustring)
        class LocalFoo(Base):pass
        mapper(LocalFoo, table)
        assert create_session().query(LocalFoo).get(ustring) == LocalFoo(id=ustring, data=ustring)

class CompileTest(QueryTest):
    def test_deferred(self):
        session = create_session()
        s = session.query(User).filter(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.id)).compile()
        
        l = session.query(User).instances(s.execute(emailad = 'jack@bean.com'))
        assert [User(id=7)] == l
    
class SliceTest(QueryTest):
    def test_first(self):
        assert  User(id=7) == create_session().query(User).first()
        
        assert create_session().query(User).filter(users.c.id==27).first() is None
        
class FilterTest(QueryTest):
    def test_basic(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).all()

    def test_onefilter(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter(users.c.name.endswith('ed')).all()

    def test_typecheck(self):
        try:
            create_session().query(User).filter(User.name==5)
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "filter() argument must be of type sqlalchemy.sql.ClauseElement"
        
class ParentTest(QueryTest):
    def test_o2m(self):
        sess = create_session()
        q = sess.query(User)
        
        u1 = q.filter_by(name='jack').one()

        # test auto-lookup of property
        o = sess.query(Order).with_parent(u1).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test with explicit property
        o = sess.query(Order).with_parent(u1, property='orders').all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test static method
        o = Query.query_from_parent(u1, property='orders', session=sess).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test generative criterion
        o = sess.query(Order).with_parent(u1).filter(orders.c.id>2).all()
        assert [Order(description="order 3"), Order(description="order 5")] == o

    def test_noparent(self):
        sess = create_session()
        q = sess.query(User)
        
        u1 = q.filter_by(name='jack').one()

        try:
            q = sess.query(Item).with_parent(u1)
            assert False
        except exceptions.InvalidRequestError, e:
            assert str(e) == "Could not locate a property which relates instances of class 'Item' to instances of class 'User'"

    def test_m2m(self):
        sess = create_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        k = sess.query(Keyword).with_parent(i1).all()
        assert [Keyword(name='red'), Keyword(name='small'), Keyword(name='square')] == k
        
    
class JoinTest(QueryTest):
    def test_overlapping_paths(self):
        # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
        result = create_session().query(User).join(['orders', 'items']).filter_by(id=3).join(['orders','address']).filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result

    def test_overlapping_paths_outerjoin(self):
        result = create_session().query(User).outerjoin(['orders', 'items']).filter_by(id=3).outerjoin(['orders','address']).filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result
    
    def test_overlap_with_aliases(self):
        oalias = orders.alias('oalias')

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_("order 1", "order 2", "order 3")).join(['orders', 'items']).all()
        assert [User(id=7, name='jack'), User(id=9, name='fred')] == result
        
        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_("order 1", "order 2", "order 3")).join(['orders', 'items']).filter_by(id=4).all()
        assert [User(id=7, name='jack')] == result

class InstancesTest(QueryTest):

    def test_from_alias(self):

        query = users.select(users.c.id==7).union(users.select(users.c.id>7)).alias('ulist').outerjoin(addresses).select(use_labels=True,order_by=['ulist.id', addresses.c.id])
        q = create_session().query(User)

        def go():
            l = q.options(contains_alias('ulist'), contains_eager('addresses')).instances(query.execute())
            assert self.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_contains_eager(self):

        selectquery = users.outerjoin(addresses).select(use_labels=True, order_by=[users.c.id, addresses.c.id])
        q = create_session().query(User)

        def go():
            l = q.options(contains_eager('addresses')).instances(selectquery.execute())
            assert self.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_contains_eager_alias(self):
        adalias = addresses.alias('adalias')
        selectquery = users.outerjoin(adalias).select(use_labels=True, order_by=[users.c.id, adalias.c.id])
        q = create_session().query(User)

        def go():
            # test using a string alias name
            l = q.options(contains_eager('addresses', alias="adalias")).instances(selectquery.execute())
            assert self.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

        def go():
            # test using the Alias object itself
            l = q.options(contains_eager('addresses', alias=adalias)).instances(selectquery.execute())
            assert self.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

        def decorate(row):
            d = {}
            for c in addresses.columns:
                d[c] = row[adalias.corresponding_column(c)]
            return d

        def go():
            # test using a custom 'decorate' function
            l = q.options(contains_eager('addresses', decorator=decorate)).instances(selectquery.execute())
            assert self.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_multi_mappers(self):
        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(Address).all()

        # note the result is a cartesian product
        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]
        
        selectquery = users.outerjoin(addresses).select(use_labels=True, order_by=[users.c.id, addresses.c.id])
        q = sess.query(User)
        l = q.instances(selectquery.execute(), Address)
        assert l == expected

        q = sess.query(User)
        q = q.add_entity(Address).outerjoin('addresses')
        l = q.all()
        assert l == expected

    def test_multi_columns(self):
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [(user7, 1),
            (user8, 3),
            (user9, 1),
            (user10, 0)
            ]
            
        q = sess.query(User)
        q = q.group_by([c for c in users.c]).order_by(User.c.id).outerjoin('addresses').add_column(func.count(addresses.c.id).label('count'))
        l = q.all()
        assert l == expected

        s = select([users, func.count(addresses.c.id).label('count')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=[users.c.id])
        q = sess.query(User)
        l = q.instances(s.execute(), "count")
        assert l == expected

    @testbase.unsupported('mysql') # only because of "+" operator requiring "concat" in mysql (fix #475)
    def test_two_columns(self):
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [
            (user7, 1, "Name:jack"),
            (user8, 3, "Name:ed"),
            (user9, 1, "Name:fred"),
            (user10, 0, "Name:chuck")]

        s = select([users, func.count(addresses.c.id).label('count'), ("Name:" + users.c.name).label('concat')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=[users.c.id])
        q = create_session().query(User)
        l = q.instances(s.execute(), "count", "concat")
        assert l == expected

class FilterByTest(QueryTest):
    def test_aliased(self):
        """test automatic generation of aliased joins using filter_by()."""
        
        sess = create_session()
        
        # test a basic aliasized path
        q = sess.query(User).filter_by(['addresses'], email_address='jack@bean.com')
        assert [User(id=7)] == q.all()

        # test two aliasized paths, one to 'orders' and the other to 'orders','items'.
        # one row is returned because user 7 has order 3 and also has order 1 which has item 1
        # this tests a o2m join and a m2m join.
        q = sess.query(User).filter_by(['orders'], description="order 3").filter_by(['orders', 'items'], description="item 1")
        assert q.count() == 1
        assert [User(id=7)] == q.all()
        
        # test the control version - same joins but not aliased.  rows are not returned because order 3 does not have item 1
        # addtionally by placing this test after the previous one, test that the "aliasing" step does not corrupt the
        # join clauses that are cached by the relationship.
        q = sess.query(User).join('orders').filter_by(description="order 3").join(['orders', 'items']).filter_by(description="item 1")
        assert [] == q.all()
        assert q.count() == 0
        
        
        

if __name__ == '__main__':
    testbase.main()


