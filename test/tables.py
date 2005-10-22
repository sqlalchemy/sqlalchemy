
from sqlalchemy.sql import *
from sqlalchemy.schema import *
from sqlalchemy.mapper import *
import sqlalchemy
import os
import testbase

__ALL__ = ['db', 'users', 'addresses', 'orders', 'orderitems', 'keywords', 'itemkeywords']

ECHO = testbase.echo
DATA = False
CREATE = False
#CREATE = True
#DBTYPE = 'sqlite_memory'
DBTYPE = 'postgres'
#DBTYPE = 'sqlite_file'

if DBTYPE == 'sqlite_memory':
    db = sqlalchemy.engine.create_engine('sqlite', ':memory:', {}, echo = testbase.echo)
elif DBTYPE == 'sqlite_file':
    import sqlalchemy.databases.sqlite as sqllite
#    if os.access('querytest.db', os.F_OK):
 #       os.remove('querytest.db')
    db = sqlalchemy.engine.create_engine('sqlite', 'querytest.db', {}, echo = testbase.echo)
elif DBTYPE == 'postgres':
    db = sqlalchemy.engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=testbase.echo)

db = testbase.EngineAssert(db)

users = Table('users', db,
    Column('user_id', Integer, primary_key = True),
    Column('user_name', String(40)),
)

addresses = Table('email_addresses', db,
    Column('address_id', Integer, primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('email_address', String(40)),
)

orders = Table('orders', db,
    Column('order_id', Integer, primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('description', String(50)),
    Column('isopen', Integer)
)

orderitems = Table('items', db,
    Column('item_id', INT, primary_key = True),
    Column('order_id', INT, ForeignKey("orders")),
    Column('item_name', VARCHAR(50))
)

keywords = Table('keywords', db,
    Column('keyword_id', Integer, primary_key = True),
    Column('name', VARCHAR(50))
)

itemkeywords = Table('itemkeywords', db,
    Column('item_id', INT, ForeignKey("items")),
    Column('keyword_id', INT, ForeignKey("keywords"))
)

if CREATE:
    users.create()
    addresses.create()
    orders.create()
    orderitems.create()
    keywords.create()
    itemkeywords.create()


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



#db.echo = True
