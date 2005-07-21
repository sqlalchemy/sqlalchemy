from testbase import PersistTest
import unittest, sys, os

import sqlalchemy.databases.sqlite as sqllite

if os.access('querytest.db', os.F_OK):
    os.remove('querytest.db')
db = sqllite.engine('querytest.db', echo = True)

from sqlalchemy.sql import *
from sqlalchemy.schema import *
from sqlalchemy.mapper import *

users = Table('users', db,
    Column('user_id', INT, primary_key = True),
    Column('user_name', VARCHAR(20)),
)

addresses = Table('email_addresses', db,
    Column('address_id', INT, primary_key = True),
    Column('user_id', INT),
    Column('email_address', VARCHAR(20)),
)

orders = Table('orders', db,
    Column('order_id', INT, primary_key = True),
    Column('user_id', INT),
    Column('description', VARCHAR(50)),
    Column('isopen', INT)
)

orderitems = Table('items', db,
    Column('item_id', INT, primary_key = True),
    Column('order_id', INT),
    Column('item_name', VARCHAR(50))
)

keywords = Table('keywords', db,
    Column('keyword_id', INT, primary_key = True),
    Column('name', VARCHAR(50))
)

itemkeywords = Table('itemkeywords', db,
    Column('item_id', INT),
    Column('keyword_id', INT)
)

users.build()
users.insert().execute(user_id = 7, user_name = 'jack')
users.insert().execute(user_id = 8, user_name = 'ed')
users.insert().execute(user_id = 9, user_name = 'fred')

addresses.build()
addresses.insert().execute(address_id = 1, user_id = 7, email_address = "jack@bean.com")
addresses.insert().execute(address_id = 2, user_id = 8, email_address = "ed@wood.com")
addresses.insert().execute(address_id = 3, user_id = 8, email_address = "ed@lala.com")

orders.build()
orders.insert().execute(order_id = 1, user_id = 7, description = 'order 1', isopen=0)
orders.insert().execute(order_id = 2, user_id = 9, description = 'order 2', isopen=0)
orders.insert().execute(order_id = 3, user_id = 7, description = 'order 3', isopen=1)
orders.insert().execute(order_id = 4, user_id = 9, description = 'order 4', isopen=1)
orders.insert().execute(order_id = 5, user_id = 7, description = 'order 5', isopen=0)

orderitems.build()
orderitems.insert().execute(item_id=1, order_id=2, item_name='item 1')
orderitems.insert().execute(item_id=3, order_id=3, item_name='item 3')
orderitems.insert().execute(item_id=2, order_id=2, item_name='item 2')
orderitems.insert().execute(item_id=5, order_id=3, item_name='item 5')
orderitems.insert().execute(item_id=4, order_id=3, item_name='item 4')

keywords.build()
keywords.insert().execute(keyword_id=1, name='blue')
keywords.insert().execute(keyword_id=2, name='red')
keywords.insert().execute(keyword_id=3, name='green')
keywords.insert().execute(keyword_id=4, name='big')
keywords.insert().execute(keyword_id=5, name='small')
keywords.insert().execute(keyword_id=6, name='round')
keywords.insert().execute(keyword_id=7, name='square')

itemkeywords.build()
itemkeywords.insert().execute(keyword_id=2, item_id=1)
itemkeywords.insert().execute(keyword_id=2, item_id=2)
itemkeywords.insert().execute(keyword_id=4, item_id=1)
itemkeywords.insert().execute(keyword_id=6, item_id=1)
itemkeywords.insert().execute(keyword_id=7, item_id=2)
itemkeywords.insert().execute(keyword_id=6, item_id=3)
itemkeywords.insert().execute(keyword_id=3, item_id=3)
itemkeywords.insert().execute(keyword_id=5, item_id=2)
itemkeywords.insert().execute(keyword_id=4, item_id=3)

class User:
    def __repr__(self):
        return (
"""
User ID: %s
User Name: %s
Addresses: %s
Orders: %s
Open Orders %s
Closed Orders %s
------------------
""" % tuple([self.user_id, repr(self.user_name)] + [repr(getattr(self, attr, None)) for attr in ('addresses', 'orders', 'orders_open', 'orders_closed')])
)

            
class Address:
    def __repr__(self):
        return "Address: " + repr(self.user_id) + " " + repr(self.email_address)

class Order:
    def __repr__(self):
        return "Order: " + repr(self.description) + " " + repr(self.isopen) + " " + repr(getattr(self, 'items', None))

class Item:
    def __repr__(self):
        return "Item: " + repr(self.item_name) + " " +repr(getattr(self, 'keywords', None))
    
class Keyword:
    def __repr__(self):
        return "Keyword: " + repr(self.name)
        
class MapperTest(PersistTest):
    
    def setUp(self):
        globalidentity().clear()
    
        
    def testmapper(self):
        m = mapper(User, users)
        l = m.select()
        print repr(l)
        l = m.select("users.user_name LIKE '%ed%'")
        print repr(l)
        
    def testeager(self):
        m = mapper(User, users, properties = dict(
            addresses = lazymapper(Address, addresses, users.c.user_id==addresses.c.user_id)
        ))
        #l = m.options(eagerload('addresses')).select()
        l = m.select()
        print repr(l)

    def testeagerwithrepeat(self):
        m = mapper(User, users, properties = dict(
            addresses = eagermapper(Address, addresses, users.c.user_id==addresses.c.user_id)
        ))
        l = m.select(and_(addresses.c.email_address == 'ed@lala.com', addresses.c.user_id==users.c.user_id))
        print repr(l)

    def testcompile(self):
        m = mapper(User, users, properties = dict(
            addresses = eagermapper(Address, addresses, users.c.user_id==addresses.c.user_id)
        ))
        s = m.compile(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.user_id))
        c = s.compile()
        print "\n" + str(c) + repr(c.get_params())
        
        l = m.instances(s.execute(emailad = 'jack@bean.com'))
        print repr(l)
        
    def testmultieager(self):
        m = mapper(User, users, properties = dict(
            addresses = eagermapper(Address, addresses, users.c.user_id==addresses.c.user_id),
            orders = eagermapper(Order, orders, users.c.user_id==orders.c.user_id),
        ), identitymap = identitymap())
        l = m.select()
        print repr(l)

    def testdoubleeager(self):
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')
        m = mapper(User, users, properties = dict(
            orders_open = eagermapper(Order, openorders, and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id)),
            orders_closed = eagermapper(Order, closedorders, and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id))
        ), identitymap = identitymap())
        l = m.select()
        print repr(l)

    def testnestedeager(self):
        ordermapper = mapper(Order, orders, properties = dict(
                items = eagermapper(Item, orderitems, orders.c.order_id == orderitems.c.order_id)
            ))

        m = mapper(User, users, properties = dict(
            addresses = eagermapper(Address, addresses, users.c.user_id==addresses.c.user_id),
            orders = eagerloader(ordermapper, users.c.user_id==orders.c.user_id),
        ))
        l = m.select()
        print repr(l)
    
    def testmanytomanyeager(self):
        items = orderitems
        
        m = mapper(Item, items, properties = dict(
                keywords = eagermapper(Keyword, keywords,
                    and_(items.c.item_id == itemkeywords.c.item_id, keywords.c.keyword_id == itemkeywords.c.keyword_id))
            ))
        l = m.select()
        print repr(l)
        
        l = m.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
            
        
if __name__ == "__main__":
    unittest.main()        
