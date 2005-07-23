from testbase import PersistTest
import unittest, sys, os

import sqlalchemy.databases.sqlite as sqllite

if os.access('querytest.db', os.F_OK):
    os.remove('querytest.db')
db = sqllite.engine('querytest.db', opts = {'isolation_level':None}, echo = True)

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


sys.exit()
class User(object):
    def __repr__(self):
        return (
"""
User ID: %s
User Name: %s
Addresses: %s
Orders: %s
Open Orders %s
Closed Orderss %s
------------------
""" % tuple([self.user_id, repr(self.user_name)] + [repr(getattr(self, attr, None)) for attr in ('addresses', 'orders', 'orders_open', 'orders_closed')])
)

            
class Address(object):
    def __repr__(self):
        return "Address: " + repr(self.user_id) + " " + repr(self.email_address)

class Order(object):
    def __repr__(self):
        return "Order: " + repr(self.description) + " " + repr(self.isopen) + " " + repr(getattr(self, 'items', None))

class Item(object):
    def __repr__(self):
        return "Item: " + repr(self.item_name) + " " +repr(getattr(self, 'keywords', None))
    
class Keyword(object):
    def __repr__(self):
        return "Keyword: " + repr(self.name)
        
class MapperTest(PersistTest):
    
    def setUp(self):
        globalidentity().clear()
    
        
    def testload(self):
        """tests loading rows with a mapper and producing object instances"""
        m = mapper(User, users)
        l = m.select()
        print repr(l)
        l = m.select(users.c.user_name.endswith('ed'))
        print repr(l)

    def testoptions(self):
        """tests that a lazy relation can be upgraded to an eager relation via the options method"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = True)
        ))
        l = m.options(eagerload('addresses')).select()
        print repr(l)
    
class LazyTest(PersistTest):
    def setUp(self):
        globalidentity().clear()

    def testbasic(self):
        """tests a basic one-to-many lazy load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = True)
        ))
        l = m.select(users.c.user_id == 7)
        user = l[0]
        a = user.addresses
        print repr(user)

    def testmanytomany(self):
        """tests a many-to-many lazy load"""
        items = orderitems

        m = mapper(Item, items, properties = dict(
                keywords = relation(Keyword, keywords,
                    and_(items.c.item_id == itemkeywords.c.item_id, keywords.c.keyword_id == itemkeywords.c.keyword_id), lazy = True),
            ))
        l = m.select()
        print repr(l)

        l = m.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
        print repr(l)            

        
        

class EagerTest(PersistTest):
    
    def setUp(self):
        globalidentity().clear()

    def testbasic(self):
        """tests a basic one-to-many eager load"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = False)
        ))
        l = m.select()
        print repr(l)

    def testeagerwithrepeat(self):
        """tests a one-to-many eager load where we also query on joined criterion, where the joined
        criterion is using the same tables that are used within the eager load.  the mapper must insure that the 
        criterion doesnt interfere with the eager load criterion."""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = False)
        ))
        l = m.select(and_(addresses.c.email_address == 'ed@lala.com', addresses.c.user_id==users.c.user_id))
        print repr(l)

    def testcompile(self):
        """tests deferred operation of a pre-compiled mapper statement"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = False)
        ))
        s = m.compile(and_(addresses.c.email_address == bindparam('emailad'), addresses.c.user_id==users.c.user_id))
        c = s.compile()
        print "\n" + str(c) + repr(c.get_params())
        
        l = m.instances(s.execute(emailad = 'jack@bean.com'))
        print repr(l)
        
    def testmultieager(self):
        """tests eager loading with two relations simultaneously"""
        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = False),
            orders = relation(Order, orders, users.c.user_id==orders.c.user_id, lazy = False),
        ), identitymap = identitymap())
        l = m.select()
        print repr(l)

    def testdoubleeager(self):
        """tests eager loading with two relations simulatneously, from the same table.  you
        have to use aliases for this less frequent type of operation."""
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')
        m = mapper(User, users, properties = dict(
            orders_open = relation(Order, openorders, and_(openorders.c.isopen == 1, users.c.user_id==openorders.c.user_id), lazy = False),
            orders_closed = relation(Order, closedorders, and_(closedorders.c.isopen == 0, users.c.user_id==closedorders.c.user_id), lazy = False)
        ), identitymap = identitymap())
        l = m.select()
        print repr(l)

    def testnestedeager(self):
        """tests eager loading, where one of the eager loaded items also eager loads its own 
        child items."""
        ordermapper = mapper(Order, orders, properties = dict(
                items = relation(Item, orderitems, orders.c.order_id == orderitems.c.order_id, lazy = False)
            ))

        m = mapper(User, users, properties = dict(
            addresses = relation(Address, addresses, users.c.user_id==addresses.c.user_id, lazy = False),
            orders = relation(ordermapper, users.c.user_id==orders.c.user_id, lazy = False),
        ))
        l = m.select()
        print repr(l)
    
    def testmanytomanyeager(self):
        items = orderitems
        
        m = mapper(Item, items, properties = dict(
                keywords = relation(Keyword, keywords,
                    and_(items.c.item_id == itemkeywords.c.item_id, keywords.c.keyword_id == itemkeywords.c.keyword_id), lazy = False),
            ))
        l = m.select()
        print repr(l)
        
        l = m.select(and_(keywords.c.name == 'red', keywords.c.keyword_id == itemkeywords.c.keyword_id, items.c.item_id==itemkeywords.c.item_id))
        print repr(l)            
        
if __name__ == "__main__":
    unittest.main()        
