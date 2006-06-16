
from sqlalchemy import *
import os
import testbase

__all__ = ['db', 'users', 'addresses', 'orders', 'orderitems', 'keywords', 'itemkeywords', 'userkeywords',
            'User', 'Address', 'Order', 'Item', 'Keyword'
        ]

ECHO = testbase.echo
db = testbase.db
metadata = BoundMetaData(db)

users = Table('users', metadata,
    Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
    Column('user_name', String(40)),
    mysql_engine='innodb'
)

addresses = Table('email_addresses', metadata,
    Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('email_address', String(40)),
    
)

orders = Table('orders', metadata,
    Column('order_id', Integer, Sequence('order_id_seq', optional=True), primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('description', String(50)),
    Column('isopen', Integer),
    
)

orderitems = Table('items', metadata,
    Column('item_id', INT, Sequence('items_id_seq', optional=True), primary_key = True),
    Column('order_id', INT, ForeignKey("orders")),
    Column('item_name', VARCHAR(50)),
    
)

keywords = Table('keywords', metadata,
    Column('keyword_id', Integer, Sequence('keyword_id_seq', optional=True), primary_key = True),
    Column('name', VARCHAR(50)),
    
)

userkeywords = Table('userkeywords', metadata, 
    Column('user_id', INT, ForeignKey("users")),
    Column('keyword_id', INT, ForeignKey("keywords")),
)

itemkeywords = Table('itemkeywords', metadata,
    Column('item_id', INT, ForeignKey("items")),
    Column('keyword_id', INT, ForeignKey("keywords")),
#    Column('foo', Boolean, default=True)
)

def create():
    metadata.create_all()
def drop():
    metadata.drop_all()
def delete():
    for t in metadata.table_iterator(reverse=True):
        t.delete().execute()
def user_data():
    users.insert().execute(
        dict(user_id = 7, user_name = 'jack'),
        dict(user_id = 8, user_name = 'ed'),
        dict(user_id = 9, user_name = 'fred')
    )
def delete_user_data():
    users.delete().execute()
        
def data():
    delete()
    
    # with SQLITE, the OID column of a table defaults to the primary key, if it has one.
    # so to database-neutrally get rows back in "insert order" based on OID, we
    # have to also put the primary keys in order for the purpose of these tests
    users.insert().execute(
        dict(user_id = 7, user_name = 'jack'),
        dict(user_id = 8, user_name = 'ed'),
        dict(user_id = 9, user_name = 'fred')
    )
    addresses.insert().execute(
        dict(address_id = 1, user_id = 7, email_address = "jack@bean.com"),
        dict(address_id = 2, user_id = 8, email_address = "ed@wood.com"),
        dict(address_id = 3, user_id = 8, email_address = "ed@bettyboop.com"),
        dict(address_id = 4, user_id = 8, email_address = "ed@lala.com")
    )
    orders.insert().execute(
        dict(order_id = 1, user_id = 7, description = 'order 1', isopen=0),
        dict(order_id = 2, user_id = 9, description = 'order 2', isopen=0),
        dict(order_id = 3, user_id = 7, description = 'order 3', isopen=1),
        dict(order_id = 4, user_id = 9, description = 'order 4', isopen=1),
        dict(order_id = 5, user_id = 7, description = 'order 5', isopen=0)
    )
    orderitems.insert().execute(
        dict(item_id=1, order_id=2, item_name='item 1'),
        dict(item_id=2, order_id=2, item_name='item 2'),
        dict(item_id=3, order_id=3, item_name='item 3'),
        dict(item_id=4, order_id=3, item_name='item 4'),
        dict(item_id=5, order_id=3, item_name='item 5'),
    )
    keywords.insert().execute(
        dict(keyword_id=1, name='blue'),
        dict(keyword_id=2, name='red'),
        dict(keyword_id=3, name='green'),
        dict(keyword_id=4, name='big'),
        dict(keyword_id=5, name='small'),
        dict(keyword_id=6, name='round'),
        dict(keyword_id=7, name='square')
    )
    
    # this many-to-many table has the keywords inserted
    # in primary key order, to appease the unit tests.
    # this is because postgres, oracle, and sqlite all support 
    # true insert-order row id, but of course our pal MySQL does not,
    # so the best it can do is order by, well something, so there you go.
    itemkeywords.insert().execute(
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
    
class User(object):
    def __init__(self):
        self.user_id = None
    def __repr__(self):
        return (
"""
objid: %d
User ID: %s
User Name: %s
email address ?: %s
Addresses: %s
Orders: %s
Open Orders %s
Closed Orderss %s
------------------
""" % tuple([id(self), self.user_id, repr(self.user_name), repr(getattr(self, 'email_address', None))] + [repr(getattr(self, attr, None)) for attr in ('addresses', 'orders', 'open_orders', 'closed_orders')])
)

class Address(object):
    def __repr__(self):
        return "Address: " + repr(getattr(self, 'address_id', None)) + " " + repr(getattr(self, 'user_id', None)) + " " + repr(self.email_address)

class Order(object):
    def __init__(self):
        self.isopen=0
    def __repr__(self):
        return "Order: " + repr(self.description) + " " + repr(self.isopen) + " " + repr(getattr(self, 'items', None))

class Item(object):
    def __repr__(self):
#        return repr(self.__dict__)
        return "Item: " + repr(self.item_name) + " " + repr(getattr(self, 'keywords', None))
    
class Keyword(object):
    def __repr__(self):
        return "Keyword: %s/%s" % (repr(getattr(self, 'keyword_id', None)),repr(self.name))



#db.echo = True
