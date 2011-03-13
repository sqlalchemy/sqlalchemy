"""A basic example of using the association object pattern.

The association object pattern is a form of many-to-many which
associates additional data with each association between parent/child.

The example illustrates an "order", referencing a collection 
of "items", with a particular price paid associated with each "item".

"""

from datetime import datetime

from sqlalchemy import (create_engine, MetaData, Table, Column, Integer,
    String, DateTime, Float, ForeignKey, and_)
from sqlalchemy.orm import mapper, relationship, Session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Order(Base):
    __tablename__ = 'order'

    order_id = Column(Integer, primary_key=True)
    customer_name = Column(String(30), nullable=False)
    order_date = Column(DateTime, nullable=False, default=datetime.now())
    order_items = relationship("OrderItem", cascade="all, delete-orphan",
                            backref='order')

    def __init__(self, customer_name):
        self.customer_name = customer_name

class Item(Base):
    __tablename__ = 'item'
    item_id = Column(Integer, primary_key=True)
    description = Column(String(30), nullable=False)
    price = Column(Float, nullable=False)

    def __init__(self, description, price):
        self.description = description
        self.price = price

    def __repr__(self):
        return 'Item(%r, %r)' % (
                    self.description, self.price
                )

class OrderItem(Base):
    __tablename__ = 'orderitem'
    order_id = Column(Integer, ForeignKey('order.order_id'), primary_key=True)
    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    price = Column(Float, nullable=False)

    def __init__(self, item, price=None):
        self.item = item
        self.price = price or item.price
    item = relationship(Item, lazy='joined')

if __name__ == '__main__':
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)

    session = Session(engine)

    # create catalog
    tshirt, mug, hat, crowbar = (
        Item('SA T-Shirt', 10.99),
        Item('SA Mug', 6.50),
        Item('SA Hat', 8.99),
        Item('MySQL Crowbar', 16.99)
    )
    session.add_all([tshirt, mug, hat, crowbar])
    session.commit()

    # create an order
    order = Order('john smith')

    # add three OrderItem associations to the Order and save
    order.order_items.append(OrderItem(mug))
    order.order_items.append(OrderItem(crowbar, 10.99))
    order.order_items.append(OrderItem(hat))
    session.add(order)
    session.commit()

    # query the order, print items
    order = session.query(Order).filter_by(customer_name='john smith').one()
    print [(order_item.item.description, order_item.price) 
           for order_item in order.order_items]

    # print customers who bought 'MySQL Crowbar' on sale
    q = session.query(Order).join('order_items', 'item')
    q = q.filter(and_(Item.description == 'MySQL Crowbar',
                      Item.price > OrderItem.price))

    print [order.customer_name for order in q]
