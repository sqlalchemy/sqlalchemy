"""basic example of using the association object pattern, which is
a richer form of a many-to-many relationship."""


# the model will be an ecommerce example.  We will have an
# Order, which represents a set of Items purchased by a user.
# each Item has a price.  however, the Order must store its own price for
# each Item, representing the price paid by the user for that particular order, which 
# is independent of the price on each Item (since those can change).

from sqlalchemy import *
from sqlalchemy.ext.selectresults import SelectResults
from datetime import datetime

import logging
logging.basicConfig(format='%(message)s')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

engine = create_engine('sqlite://')
metadata = MetaData(engine)

orders = Table('orders', metadata, 
    Column('order_id', Integer, primary_key=True),
    Column('customer_name', String(30), nullable=False),
    Column('order_date', DateTime, nullable=False, default=datetime.now()),
    )

items = Table('items', metadata,
    Column('item_id', Integer, primary_key=True),
    Column('description', String(30), nullable=False),
    Column('price', Float, nullable=False)
    )

orderitems = Table('orderitems', metadata,
    Column('order_id', Integer, ForeignKey('orders.order_id'), primary_key=True),
    Column('item_id', Integer, ForeignKey('items.item_id'), primary_key=True),
    Column('price', Float, nullable=False)
    )
metadata.create_all()

class Order(object):
    def __init__(self, customer_name):
        self.customer_name = customer_name

class Item(object):
    def __init__(self, description, price):
        self.description = description
        self.price = price

class OrderItem(object):
    def __init__(self, item, price=None):
        self.item = item
        self.price = price or item.price
        
mapper(Order, orders, properties={
    'items':relation(OrderItem, cascade="all, delete-orphan", lazy=False)
})
mapper(Item, items)
mapper(OrderItem, orderitems, properties={
    'item':relation(Item, lazy=False)
})

session = create_session()

# create our catalog
session.save(Item('SA T-Shirt', 10.99))
session.save(Item('SA Mug', 6.50))
session.save(Item('SA Hat', 8.99))
session.save(Item('MySQL Crowbar', 16.99))
session.flush()

# function to return items from the DB
def item(name):
    return session.query(Item).get_by(description=name)
    
# create an order
order = Order('john smith')

# add three OrderItem associations to the Order and save
order.items.append(OrderItem(item('SA Mug')))
order.items.append(OrderItem(item('MySQL Crowbar'), 10.99))
order.items.append(OrderItem(item('SA Hat')))
session.save(order)
session.flush()

session.clear()

# query the order, print items
order = session.query(Order).get_by(customer_name='john smith')
print [(item.item.description, item.price) for item in order.items]

# print customers who bought 'MySQL Crowbar' on sale
result = SelectResults(session.query(Order)).join_to('item').select(and_(items.c.description=='MySQL Crowbar', items.c.price>orderitems.c.price))
print [order.customer_name for order in result]










