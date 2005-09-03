import unittest, sys, os

import sqlalchemy.databases.sqlite as sqllite

db_type = 'memory'

if db_type == 'memory':
    db = sqllite.engine(':memory:', {})
elif db_type =='file':
    if os.access('querytest.db', os.F_OK):
        os.remove('querytest.db')
    db = sqllite.engine('querytest.db', {}, echo = True)

from sqlalchemy.sql import *
from sqlalchemy.schema import *


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
db.connection().commit()

addresses.build()
addresses.insert().execute(address_id = 1, user_id = 7, email_address = "jack@bean.com")
addresses.insert().execute(address_id = 2, user_id = 8, email_address = "ed@wood.com")
addresses.insert().execute(address_id = 3, user_id = 8, email_address = "ed@lala.com")
db.connection().commit()

orders.build()
orders.insert().execute(order_id = 1, user_id = 7, description = 'order 1', isopen=0)
orders.insert().execute(order_id = 2, user_id = 9, description = 'order 2', isopen=0)
orders.insert().execute(order_id = 3, user_id = 7, description = 'order 3', isopen=1)
orders.insert().execute(order_id = 4, user_id = 9, description = 'order 4', isopen=1)
orders.insert().execute(order_id = 5, user_id = 7, description = 'order 5', isopen=0)
db.connection().commit()

orderitems.build()
orderitems.insert().execute(item_id=1, order_id=2, item_name='item 1')
orderitems.insert().execute(item_id=3, order_id=3, item_name='item 3')
orderitems.insert().execute(item_id=2, order_id=2, item_name='item 2')
orderitems.insert().execute(item_id=5, order_id=3, item_name='item 5')
orderitems.insert().execute(item_id=4, order_id=3, item_name='item 4')
db.connection().commit()

keywords.build()
keywords.insert().execute(keyword_id=1, name='blue')
keywords.insert().execute(keyword_id=2, name='red')
keywords.insert().execute(keyword_id=3, name='green')
keywords.insert().execute(keyword_id=4, name='big')
keywords.insert().execute(keyword_id=5, name='small')
keywords.insert().execute(keyword_id=6, name='round')
keywords.insert().execute(keyword_id=7, name='square')
db.connection().commit()

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
db.connection().commit()

