"""proxied_association.py

same example as basic_association, adding in
usage of :mod:`sqlalchemy.ext.associationproxy` to make explicit references
to ``OrderItem`` optional.


"""

from datetime import datetime

from sqlalchemy import (create_engine, Column, Integer, String, DateTime,
                        Float, ForeignKey)
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy

Base = declarative_base()


class Order(Base):
    __tablename__ = 'order'

    order_id = Column(Integer, primary_key=True)
    customer_name = Column(String(30), nullable=False)
    order_date = Column(DateTime, nullable=False, default=datetime.now())
    order_items = relationship("OrderItem", cascade="all, delete-orphan",
                               backref='order')
    items = association_proxy("order_items", "item")

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
        return 'Item(%r, %r)' % (self.description, self.price)


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

    # add items via the association proxy.
    # the OrderItem is created automatically.
    order.items.append(mug)
    order.items.append(hat)

    # add an OrderItem explicitly.
    order.order_items.append(OrderItem(crowbar, 10.99))

    session.add(order)
    session.commit()

    # query the order, print items
    order = session.query(Order).filter_by(customer_name='john smith').one()

    # print items based on the OrderItem collection directly
    print([(assoc.item.description, assoc.price, assoc.item.price)
           for assoc in order.order_items])

    # print items based on the "proxied" items collection
    print([(item.description, item.price)
           for item in order.items])

    # print customers who bought 'MySQL Crowbar' on sale
    orders = session.query(Order).\
        join('order_items', 'item').\
        filter(Item.description == 'MySQL Crowbar').\
        filter(Item.price > OrderItem.price)
    print([o.customer_name for o in orders])
