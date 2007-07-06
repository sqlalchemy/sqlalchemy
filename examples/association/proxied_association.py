"""this is a modified version of the basic association example, which illustrates 
the usage of the associationproxy extension."""

from sqlalchemy import *
from sqlalchemy.ext.selectresults import SelectResults
from sqlalchemy.ext.associationproxy import AssociationProxy
from datetime import datetime

import logging
logging.basicConfig(format='%(message)s')
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

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
    items = AssociationProxy('itemassociations', 'item', creator=lambda x, **kw:OrderItem(x, **kw))
    
class Item(object):
    def __init__(self, description, price):
        self.description = description
        self.price = price

class OrderItem(object):
    def __init__(self, item, price=None):
        self.item = item
        self.price = price or item.price
        
mapper(Order, orders, properties={
    'itemassociations':relation(OrderItem, cascade="all, delete-orphan", lazy=False)
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

# function to return items
def item(name):
    return session.query(Item).get_by(description=name)
    
# create an order
order = Order('john smith')

# append an OrderItem association via the "itemassociations" collection
order.itemassociations.append(OrderItem(item('MySQL Crowbar'), 10.99))

# append two more Items via the transparent "items" proxy, which
# will create OrderItems automatically
order.items.append(item('SA Mug'))
order.items.append(item('SA Hat'))

# now append one more item, overriding the price
order.items.append(item('SA T-Shirt'), price=2.99)

session.save(order)
session.flush()

session.clear()

# query the order, print items
order = session.query(Order).get_by(customer_name='john smith')

# print items based on the OrderItem collection directly
print [(item.item.description, item.price) for item in order.itemassociations]

# print items based on the "proxied" items collection
print [(item.description, item.price) for item in order.items]

# print customers who bought 'MySQL Crowbar' on sale
result = session.query(Order).join('item').filter(and_(items.c.description=='MySQL Crowbar', items.c.price>orderitems.c.price))
print [order.customer_name for order in result]

# print customers who got the special T-shirt discount
result = session.query(Order).join('item').filter(and_(items.c.description=='SA T-Shirt', items.c.price>orderitems.c.price))
print [order.customer_name for order in result]










