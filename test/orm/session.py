from testbase import AssertMixin
import testbase
import unittest, sys, datetime

import tables
from tables import *

db = testbase.db
from sqlalchemy import *


class OrphanDeletionTest(AssertMixin):

    def setUpAll(self):
        db.echo = False
        tables.create()
        tables.data()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        tables.drop()
        db.echo = testbase.echo
    def tearDown(self):
        clear_mappers()
    def setUp(self):
        pass

    def test_orphan(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()
        a = Address()
        s.save(a)
        try:
            s.flush()
        except exceptions.FlushError, e:
            pass
        assert a.address_id is None, "Error: address should not be persistent"
        
    def test_delete_new_object(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all,delete-orphan", backref="user")
        ))
        s = create_session()

        u = User()
        s.save(u)
        a = Address()
        assert a not in s.new
        u.addresses.append(a)
        u.addresses.remove(a)
        s.delete(u)
        try:
            s.flush() # (erroneously) causes "a" to be persisted
            assert False
        except exceptions.FlushError:
            assert True
        assert u.user_id is None, "Error: user should not be persistent"
        assert a.address_id is None, "Error: address should not be persistent"


class CascadingOrphanDeletionTest(AssertMixin):
    def setUpAll(self):
        global meta, orders, items, attributes
        meta = BoundMetaData(db)

        orders = Table('orders', meta,
            Column('id', Integer, Sequence('order_id_seq'), primary_key = True),
            Column('name', VARCHAR(50)),

        )
        items = Table('items', meta,
            Column('id', Integer, Sequence('item_id_seq'), primary_key = True),
            Column('order_id', Integer, ForeignKey(orders.c.id), nullable=False),
            Column('name', VARCHAR(50)),

        )
        attributes = Table('attributes', meta,
            Column('id', Integer, Sequence('attribute_id_seq'), primary_key = True),
            Column('item_id', Integer, ForeignKey(items.c.id), nullable=False),
            Column('name', VARCHAR(50)),

        )

    def setUp(self):
        meta.create_all()
    def tearDown(self):
        meta.drop_all()

    def testdeletechildwithchild(self):
        class Order(object): pass
        class Item(object): pass
        class Attribute(object): pass

        attrMapper = mapper(Attribute, attributes)
        itemMapper = mapper(Item, items, properties=dict(
            attributes=relation(attrMapper, cascade="all,delete-orphan", backref="item")
        ))
        orderMapper = mapper(Order, orders, properties=dict(
            items=relation(itemMapper, cascade="all,delete-orphan", backref="order")
        ))

        s = create_session(echo_uow=True)
        order = Order()
        s.save(order)

        item = Item()
        attr = Attribute()
        item.attributes.append(attr)

        order.items.append(item)
        order.items.remove(item) # item is an orphan, but attr is not so flush() tries to save attr
        try:
            s.flush()
            assert False
        except exceptions.FlushError:
            assert True

        assert item.id is None
        assert attr.id is None






if __name__ == "__main__":    
    testbase.main()
