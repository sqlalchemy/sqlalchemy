# can't be imported until the path is setup; be sure to configure
# first if covering.
from sqlalchemy import *
from sqlalchemy import util
from testlib import *

__all__ = ['keywords', 'addresses', 'Base', 'Keyword', 'FixtureTest', 'Dingaling', 'item_keywords', 
            'dingalings', 'User', 'items', 'Fixtures', 'orders', 'install_fixture_data', 'Address', 'users', 
            'order_items', 'Item', 'Order', 'fixtures']
            
_recursion_stack = util.Set()
class Base(object):
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

    # TODO: add recursion checks to this
    def __repr__(self):
        return "%s(%s)" % (
            (self.__class__.__name__),
            ','.join(["%s=%s" % (key, repr(getattr(self, key))) for key in self.__dict__ if not key.startswith('_')])
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        """'passively' compare this object to another.

        only look at attributes that are present on the source object.

        """

        if self in _recursion_stack:
            return True
        _recursion_stack.add(self)
        try:
            # pick the entity thats not SA persisted as the source
            if other is None:
                a = self
                b = other
            elif hasattr(self, '_instance_key'):
                a = other
                b = self
            else:
                a = self
                b = other

            for attr in a.__dict__.keys():
                if attr[0] == '_':
                    continue
                value = getattr(a, attr)
                #print "looking at attr:", attr, "start value:", value
                if hasattr(value, '__iter__') and not isinstance(value, basestring):
                    try:
                        # catch AttributeError so that lazy loaders trigger
                        battr = getattr(b, attr)
                    except AttributeError:
                        #print "b class does not have attribute named '%s'" % attr
                        return False
                    
                    if list(value) == list(battr):
                        continue
                    else:
                        return False
                else:
                    if value is not None:
                        if value != getattr(b, attr, None):
                            #print "2. Attribute named '%s' does not match that of b" % attr
                            return False
            else:
                return True
        finally:
            _recursion_stack.remove(self)

class User(Base):pass
class Order(Base):pass
class Item(Base):pass
class Keyword(Base):pass
class Address(Base):pass
class Dingaling(Base):pass

metadata = MetaData()

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

dingalings = Table("dingalings", metadata,
    Column('id', Integer, primary_key=True),
    Column('address_id', None, ForeignKey('addresses.id')),
    Column('data', String(30))
    )

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

def install_fixture_data():
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
    dingalings.insert().execute(
        dict(id=1, address_id=2, data='ding 1/2'),
        dict(id=2, address_id=5, data='ding 2/5'),
    )
    orders.insert().execute(
        dict(id = 1, user_id = 7, description = 'order 1', isopen=0, address_id=1),
        dict(id = 2, user_id = 9, description = 'order 2', isopen=0, address_id=4),
        dict(id = 3, user_id = 7, description = 'order 3', isopen=1, address_id=1),
        dict(id = 4, user_id = 9, description = 'order 4', isopen=1, address_id=4),
        dict(id = 5, user_id = 7, description = 'order 5', isopen=0, address_id=None)
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

        dict(item_id=3, order_id=3),
        dict(item_id=4, order_id=3),
        dict(item_id=5, order_id=3),

        dict(item_id=1, order_id=4),
        dict(item_id=5, order_id=4),

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

class FixtureTest(ORMTest):
    refresh_data = False

    def setUpAll(self):
        super(FixtureTest, self).setUpAll()
        if self.keep_data:
            install_fixture_data()

    def setUp(self):
        if self.refresh_data:
            install_fixture_data()

    def define_tables(self, meta):
        pass
FixtureTest.metadata = metadata

class Fixtures(object):
    @property
    def user_address_result(self):
        return [
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
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

    @property
    def user_order_result(self):
        return [
            User(id=7, orders=[
                Order(id=1, items=[Item(id=1), Item(id=2), Item(id=3)]),
                Order(id=3, items=[Item(id=3), Item(id=4), Item(id=5)]),
                Order(id=5, items=[Item(id=5)]),
            ]),
            User(id=8, orders=[]),
            User(id=9, orders=[
                Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)]),
                Order(id=4, items=[Item(id=1), Item(id=5)]),
            ]),
            User(id=10)
        ]

    @property
    def item_keyword_result(self):
        return [
            Item(id=1, keywords=[Keyword(name='red'), Keyword(name='big'), Keyword(name='round')]),
            Item(id=2, keywords=[Keyword(name='red'), Keyword(name='small'), Keyword(name='square')]),
            Item(id=3, keywords=[Keyword(name='green'), Keyword(name='big'), Keyword(name='round')]),
            Item(id=4, keywords=[]),
            Item(id=5, keywords=[]),
        ]
fixtures = Fixtures()
