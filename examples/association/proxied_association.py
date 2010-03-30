"""this is a modified version of the basic association example, which illustrates
the usage of the associationproxy extension."""

from datetime import datetime
from sqlalchemy import (create_engine, MetaData, Table, Column, Integer,
    String, DateTime, Float, ForeignKey, and_)
from sqlalchemy.orm import mapper, relationship, create_session
from sqlalchemy.ext.associationproxy import AssociationProxy

engine = create_engine('sqlite://')
#engine = create_engine('sqlite://', echo=True)
metadata = MetaData(engine)

orders = Table('orders', metadata,
    Column('order_id', Integer, primary_key=True),
    Column('customer_name', String(30), nullable=False),
    Column('order_date', DateTime, nullable=False, default=datetime.now))

items = Table('items', metadata,
    Column('item_id', Integer, primary_key=True),
    Column('description', String(30), nullable=False),
    Column('price', Float, nullable=False))

orderitems = Table('orderitems', metadata,
    Column('order_id', Integer, ForeignKey('orders.order_id'),
           primary_key=True),
    Column('item_id', Integer, ForeignKey('items.item_id'),
           primary_key=True),
    Column('price', Float, nullable=False))

metadata.create_all()

class OrderItem(object):
    def __init__(self, item, price=None):
        self.item = item
        self.price = price is None and item.price or price

class Order(object):
    def __init__(self, customer_name):
        self.customer_name = customer_name
    items = AssociationProxy('itemassociations', 'item',
                             creator=OrderItem)

class Item(object):
    def __init__(self, description, price):
        self.description = description
        self.price = price


mapper(Order, orders, properties={
    'itemassociations':relationship(OrderItem, cascade="all, delete-orphan", lazy='joined')
})
mapper(Item, items)
mapper(OrderItem, orderitems, properties={
    'item':relationship(Item, lazy='joined')
})

session = create_session()

# create our catalog
session.add_all([Item('SA T-Shirt', 10.99),
                 Item('SA Mug', 6.50),
                 Item('SA Hat', 8.99),
                 Item('MySQL Crowbar', 16.99)])
session.flush()

# function to return items
def item(name):
    return session.query(Item).filter_by(description=name).one()

# create an order
order = Order('john smith')

# append an OrderItem association via the "itemassociations"
# collection with a custom price.
order.itemassociations.append(OrderItem(item('MySQL Crowbar'), 10.99))

# append two more Items via the transparent "items" proxy, which
# will create OrderItems automatically using the default price.
order.items.append(item('SA Mug'))
order.items.append(item('SA Hat'))

session.add(order)
session.flush()

session.expunge_all()

# query the order, print items
order = session.query(Order).filter_by(customer_name='john smith').one()

print "Order #%s:\n%s\n%s\n%s items.\n" % (
    order.order_id, order.customer_name, order.order_date, len(order.items))

# print items based on the OrderItem collection directly
print [(assoc.item.description, assoc.price, assoc.item.price)
       for assoc in order.itemassociations]

# print items based on the "proxied" items collection
print [(item.description, item.price)
       for item in order.items]

# print customers who bought 'MySQL Crowbar' on sale
orders = session.query(Order).join('itemassociations', 'item').filter(
    and_(Item.description=='MySQL Crowbar', Item.price > OrderItem.price))
print [order.customer_name for order in orders]
