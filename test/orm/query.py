from sqlalchemy import *
from sqlalchemy.orm import *
import testbase

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
        global User, Order, Item, Keyword, Address, Base
        
        class Base(object):
            def __init__(self, **kwargs):
                for k in kwargs:
                    setattr(self, k, kwargs[k])
            def __eq__(self, other):
                for attr in dir(self):
                    if attr[0] == '_':
                        continue
                    value = getattr(self, attr)
                    if isinstance(value, list):
                        for (us, them) in zip(value, getattr(other, attr)):
                            if us != them:
                                return False
                        else:
                            return True
                    else:
                        if value is not None:
                            return value == getattr(other, attr)
                    
        class User(Base):pass
        class Order(Base):pass
        class Item(Base):pass
        class Keyword(Base):pass
        class Address(Base):pass

        mapper(User, users, properties={
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

class SliceTest(QueryTest):
    def test_first(self):
        assert create_session().query(User).first() == User(id=7)
        
        assert create_session().query(User).filter(users.c.id==27).first() is None
        
class FilterTest(QueryTest):
    def test_basic(self):
        assert create_session().query(User).all() == [User(id=7), User(id=8), User(id=9),User(id=10)]

    def test_onefilter(self):
        assert create_session().query(User).filter(users.c.name.endswith('ed')).all() == [User(id=8), User(id=9)]

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




if __name__ == '__main__':
    testbase.main()


