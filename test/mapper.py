from testbase import PersistTest
import unittest, sys

import sqlalchemy.databases.sqlite as sqllite

db = sqllite.engine('querytest.db', echo = True)

from sqlalchemy.sql import *
from sqlalchemy.schema import *

import sqlalchemy.mapper as mapper

class User:
    def __repr__(self):
        return ("User: " + repr(self.user_id) + " " + self.user_name + repr(getattr(self, 'addresses', None)) +
            repr(getattr(self, 'orders', None))
            )
            
class Address:
    def __repr__(self):
        return "Address: " + repr(self.user_id) + " " + repr(self.email_address)

class Order:
    def __repr__(self):
        return "Order: " + repr(self.description) + repr(self.isopen)
        
class MapperTest(PersistTest):
    
    def setUp(self):
        self.users = Table('users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )

        self.addresses = Table('email_addresses', db,
            Column('address_id', INT, primary_key = True),
            Column('user_id', INT),
            Column('email_address', VARCHAR(20)),
        )
        
        self.orders = Table('orders', db,
            Column('order_id', INT, primary_key = True),
            Column('user_id', INT),
            Column('description', VARCHAR(50)),
            Column('isopen', INT)
        )
        
        self.users.build()
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'ed')
        self.users.insert().execute(user_id = 9, user_name = 'fred')
        
        self.addresses.build()
        self.addresses.insert().execute(address_id = 1, user_id = 7, email_address = "jack@bean.com")
        self.addresses.insert().execute(address_id = 2, user_id = 8, email_address = "ed@wood.com")
        self.addresses.insert().execute(address_id = 3, user_id = 8, email_address = "ed@lala.com")
        
        self.orders.build()
        self.orders.insert().execute(order_id = 1, user_id = 7, description = 'order 1', isopen=0)
        self.orders.insert().execute(order_id = 2, user_id = 7, description = 'order 2', isopen=0)
        self.orders.insert().execute(order_id = 3, user_id = 7, description = 'order 3', isopen=1)
        self.orders.insert().execute(order_id = 4, user_id = 7, description = 'order 4', isopen=1)
        self.orders.insert().execute(order_id = 5, user_id = 7, description = 'order 5', isopen=0)
        
        
    def testmapper(self):
        m = mapper.Mapper(User, self.users)
        l = m.select()
        print repr(l)
        print repr(m.identitymap.map)

    def testeager(self):
        m = mapper.Mapper(User, self.users, properties = dict(
            addresses = mapper.EagerLoader(mapper.Mapper(Address, self.addresses), self.users.c.user_id==self.addresses.c.user_id)
        ))
        l = m.select()
        print repr(l)

    def testmultieager(self):
        openorders = alias(self.orders, 'openorders')
        closedorders = alias(self.orders, 'closedorders')
        m = mapper.Mapper(User, self.users, properties = dict(
            orders_open = mapper.EagerLoader(mapper.Mapper(Order, openorders), and_(openorders.c.isopen == 1, self.users.c.user_id==openorders.c.user_id)),
            orders_closed = mapper.EagerLoader(mapper.Mapper(Order, closedorders), and_(closedorders.c.isopen == 0, self.users.c.user_id==closedorders.c.user_id))
        ))
        l = m.select()
        print repr(l)

        
    def tearDown(self):
        self.users.drop()
        self.addresses.drop()
        self.orders.drop()
    	pass    
        
if __name__ == "__main__":
    unittest.main()        
