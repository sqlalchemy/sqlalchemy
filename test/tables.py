
from sqlalchemy.sql import *
from sqlalchemy.schema import *
from sqlalchemy.mapper import *
import os

DBTYPE = 'sqlite_memory'

if DBTYPE == 'sqlite_memory':
    import sqlalchemy.databases.sqlite as sqllite
    db = sqllite.engine(':memory:', {}, echo = ECHO)
elif DBTYPE == 'sqlite_file':
    import sqlalchemy.databases.sqlite as sqllite
    if os.access('querytest.db', os.F_OK):
        os.remove('querytest.db')
    db = sqllite.engine('querytest.db', opts = {}, echo = ECHO)
elif DBTYPE == 'postgres':
    pass

users = Table('users', db,
    Column('user_id', INT, primary_key = True),
    Column('user_name', VARCHAR(20)),
)

addresses = Table('email_addresses', db,
    Column('address_id', INT, primary_key = True),
    Column('user_id', INT, ForeignKey(users.c.user_id)),
    Column('email_address', VARCHAR(20)),
)

orders = Table('orders', db,
    Column('order_id', INT, primary_key = True),
    Column('user_id', INT, ForeignKey(users.c.user_id)),
    Column('description', VARCHAR(50)),
    Column('isopen', INT)
)

orderitems = Table('items', db,
    Column('item_id', INT, primary_key = True),
    Column('order_id', INT, ForeignKey("orders")),
    Column('item_name', VARCHAR(50))
)

keywords = Table('keywords', db,
    Column('keyword_id', INT, primary_key = True),
    Column('name', VARCHAR(50))
)

itemkeywords = Table('itemkeywords', db,
    Column('item_id', INT, ForeignKey("items")),
    Column('keyword_id', INT, ForeignKey("keywords"))
)

users.create()

if DATA:
    users.insert().execute(
        dict(user_id = 7, user_name = 'jack'),
        dict(user_id = 8, user_name = 'ed'),
        dict(user_id = 9, user_name = 'fred')
    )

addresses.create()
if DATA:
    addresses.insert().execute(
        dict(address_id = 1, user_id = 7, email_address = "jack@bean.com"),
        dict(address_id = 2, user_id = 8, email_address = "ed@wood.com"),
        dict(address_id = 3, user_id = 8, email_address = "ed@lala.com")
    )

orders.create()
if DATA:
    orders.insert().execute(
        dict(order_id = 1, user_id = 7, description = 'order 1', isopen=0),
        dict(order_id = 2, user_id = 9, description = 'order 2', isopen=0),
        dict(order_id = 3, user_id = 7, description = 'order 3', isopen=1),
        dict(order_id = 4, user_id = 9, description = 'order 4', isopen=1),
        dict(order_id = 5, user_id = 7, description = 'order 5', isopen=0)
    )

orderitems.create()
if DATA:
    orderitems.insert().execute(
        dict(item_id=1, order_id=2, item_name='item 1'),
        dict(item_id=3, order_id=3, item_name='item 3'),
        dict(item_id=2, order_id=2, item_name='item 2'),
        dict(item_id=5, order_id=3, item_name='item 5'),
        dict(item_id=4, order_id=3, item_name='item 4')
    )

keywords.create()
if DATA:
    keywords.insert().execute(
        dict(keyword_id=1, name='blue'),
        dict(keyword_id=2, name='red'),
        dict(keyword_id=3, name='green'),
        dict(keyword_id=4, name='big'),
        dict(keyword_id=5, name='small'),
        dict(keyword_id=6, name='round'),
        dict(keyword_id=7, name='square')
    )

itemkeywords.create()
if DATA:
    itemkeywords.insert().execute(
        dict(keyword_id=2, item_id=1),
        dict(keyword_id=2, item_id=2),
        dict(keyword_id=4, item_id=1),
        dict(keyword_id=6, item_id=1),
        dict(keyword_id=7, item_id=2),
        dict(keyword_id=6, item_id=3),
        dict(keyword_id=3, item_id=3),
        dict(keyword_id=5, item_id=2),
        dict(keyword_id=4, item_id=3)
    )
db.connection().commit()


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
    def __repr__(self):
        return "Order: " + repr(self.description) + " " + repr(self.isopen) + " " + repr(getattr(self, 'items', None))

class Item(object):
    def __repr__(self):
        return "Item: " + repr(self.item_name) + " " +repr(getattr(self, 'keywords', None))
    
class Keyword(object):
    def __repr__(self):
        return "Keyword: %s/%s" % (repr(getattr(self, 'keyword_id', None)),repr(self.name))

