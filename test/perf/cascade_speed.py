import testenv; testenv.simple_setup()
from sqlalchemy import *
from sqlalchemy.orm import *
from timeit import Timer
import sys


meta = MetaData()

orders = Table('orders', meta,
    Column('id', Integer, Sequence('order_id_seq'), primary_key = True),
)
items = Table('items', meta,
    Column('id', Integer, Sequence('item_id_seq'), primary_key = True),
    Column('order_id', Integer, ForeignKey(orders.c.id), nullable=False),
)
attributes = Table('attributes', meta,
    Column('id', Integer, Sequence('attribute_id_seq'), primary_key = True),
    Column('item_id', Integer, ForeignKey(items.c.id), nullable=False),
)
values = Table('values', meta,
    Column('id', Integer, Sequence('value_id_seq'), primary_key = True),
    Column('attribute_id', Integer, ForeignKey(attributes.c.id), nullable=False),
)

class Order(object): pass
class Item(object): pass
class Attribute(object): pass
class Value(object): pass

valueMapper = mapper(Value, values)
attrMapper = mapper(Attribute, attributes, properties=dict(
    values=relationship(valueMapper, cascade="save-update", backref="attribute")
))
itemMapper = mapper(Item, items, properties=dict(
    attributes=relationship(attrMapper, cascade="save-update", backref="item")
))
orderMapper = mapper(Order, orders, properties=dict(
    items=relationship(itemMapper, cascade="save-update", backref="order")
))



class TimeTrial(object):

    def create_fwd_assoc(self):
        item = Item()
        self.order.items.append(item)
        for attrid in range(10):
            attr = Attribute()
            item.attributes.append(attr)
            for valueid in range(5):
                val = Value()
                attr.values.append(val)

    def create_back_assoc(self):
        item = Item()
        item.order = self.order
        for attrid in range(10):
            attr = Attribute()
            attr.item = item
            for valueid in range(5):
                val = Value()
                val.attribute = attr

    def run(self, number):
        s = create_session()
        self.order = order = Order()
        s.save(order)

        ctime = 0.0
        timer = Timer("create_it()", "from __main__ import create_it")
        for n in xrange(number):
            t = timer.timeit(1)
            print "Time to create item %i: %.5f sec" % (n, t)
            ctime += t

        assert len(order.items) == 10
        assert sum(len(item.attributes) for item in order.items) == 100
        assert sum(len(attr.values) for item in order.items for attr in item.attributes) == 500
        assert len(s.new) == 611
        print "Created 610 objects in %.5f sec" % ctime

if __name__ == "__main__":
    tt = TimeTrial()

    print "\nCreate forward associations"
    create_it = tt.create_fwd_assoc
    tt.run(10)

    print "\nCreate backward associations"
    create_it = tt.create_back_assoc
    tt.run(10)
