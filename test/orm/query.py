from sqlalchemy import *
from sqlalchemy.orm import *
import testbase

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
        self.install_fixture_data()
        self.setup_mappers()
        
    def define_tables(self, metadata):
        global users, orders, addresses, items, order_items, item_keywords, keywords
        
        users = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30), nullable=False))

        orders = Table('orders', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', None, ForeignKey('users.id')),
            Column('address_id', None, ForeignKey('addresses.id')),
            Column('description', String(30)),
            Column('isopen', Integer)
            )

        addresses = Table('addresses', metadata, 
            Column('id', Integer, primary_key=True),
            Column('user_id', None, ForeignKey('users.id')),
            Column('email_address', String(50), nullable=False))
            
        items = Table('items', metadata, 
            Column('id', Integer, primary_key=True),
            Column('description', String(30), nullable=False)
            )

        order_items = Table('order_items', metadata,
            Column('item_id', None, ForeignKey('items.id')),
            Column('order_id', None, ForeignKey('orders.id')))

        item_keywords = Table('item_keywords', metadata, 
            Column('item_id', None, ForeignKey('items.id')),
            Column('keyword_id', None, ForeignKey('keywords.id')))

        keywords = Table('keywords', metadata, 
            Column('id', Integer, primary_key=True),
            Column('name', String(30), nullable=False)
            )
        
    def install_fixture_data(self):
        users.insert().execute(
            dict(id = 7, name = 'jack'),
            dict(id = 8, name = 'ed'),
            dict(id = 9, name = 'fred'),
            dict(id = 10, name = 'chuck'),
            
        )
        addresses.insert().execute(
            dict(id = 1, user_id = 7, email_address = "jack@bean.com"),
            dict(id = 2, user_id = 8, email_address = "ed@wood.com"),
            dict(id = 3, user_id = 8, email_address = "ed@bettyboop.com"),
            dict(id = 4, user_id = 8, email_address = "ed@lala.com"),
            dict(id = 5, user_id = 9, email_address = "fred@fred.com"),
        )
        orders.insert().execute(
            dict(id = 1, user_id = 7, description = 'order 1', isopen=0, address_id=1),
            dict(id = 2, user_id = 9, description = 'order 2', isopen=0, address_id=4),
            dict(id = 3, user_id = 7, description = 'order 3', isopen=1, address_id=1),
            dict(id = 4, user_id = 9, description = 'order 4', isopen=1, address_id=4),
            dict(id = 5, user_id = 7, description = 'order 5', isopen=0, address_id=1)
        )
        items.insert().execute(
            dict(id=1, description='item 1'),
            dict(id=2, description='item 2'),
            dict(id=3, description='item 3'),
            dict(id=4, description='item 4'),
            dict(id=5, description='item 5'),
        )
        order_items.insert().execute(
            dict(item_id=1, order_id=1),
            dict(item_id=2, order_id=1),
            dict(item_id=3, order_id=1),

            dict(item_id=1, order_id=2),
            dict(item_id=2, order_id=2),
            dict(item_id=3, order_id=2),
            dict(item_id=2, order_id=2),

            dict(item_id=3, order_id=3),
            dict(item_id=4, order_id=3),
            dict(item_id=5, order_id=3),
            
            dict(item_id=5, order_id=4),
            dict(item_id=1, order_id=4),
            
            dict(item_id=5, order_id=5),
        )
        keywords.insert().execute(
            dict(id=1, name='blue'),
            dict(id=2, name='red'),
            dict(id=3, name='green'),
            dict(id=4, name='big'),
            dict(id=5, name='small'),
            dict(id=6, name='round'),
            dict(id=7, name='square')
        )

        # this many-to-many table has the keywords inserted
        # in primary key order, to appease the unit tests.
        # this is because postgres, oracle, and sqlite all support 
        # true insert-order row id, but of course our pal MySQL does not,
        # so the best it can do is order by, well something, so there you go.
        item_keywords.insert().execute(
            dict(keyword_id=2, item_id=1),
            dict(keyword_id=2, item_id=2),
            dict(keyword_id=4, item_id=1),
            dict(keyword_id=6, item_id=1),
            dict(keyword_id=5, item_id=2),
            dict(keyword_id=3, item_id=3),
            dict(keyword_id=4, item_id=3),
            dict(keyword_id=7, item_id=2),
            dict(keyword_id=6, item_id=3)
        )

    
    def setup_mappers(self):
        global User, Order, Item, Keyword, Address
        
        class User(Base):pass
        class Order(Base):pass
        class Item(Base):pass
        class Keyword(Base):pass
        class Address(Base):pass

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
        result = create_session().query(User).join(['orders', 'items']).filter_by(id=3).join(['orders','address']).filter_by(id=1).all()
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
        (user7, user8, user9, user10) = sess.query(User).select()
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
        (user7, user8, user9, user10) = sess.query(User).select()
        expected = [
            (user7, 1, "Name:jack"),
            (user8, 3, "Name:ed"),
            (user9, 1, "Name:fred"),
            (user10, 0, "Name:chuck")]

        s = select([users, func.count(addresses.c.id).label('count'), ("Name:" + users.c.name).label('concat')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=[users.c.id])
        q = create_session().query(User)
        l = q.instances(s.execute(), "count", "concat")
        assert l == expected



if __name__ == '__main__':
    testbase.main()


