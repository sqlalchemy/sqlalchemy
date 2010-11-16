import time
from datetime import datetime

from sqlalchemy import *
from sqlalchemy.orm import *
from test.lib import *
from test.lib.profiling import profiled

class Item(object):
    def __repr__(self):
        return 'Item<#%s "%s">' % (self.id, self.name)
class SubItem(object):
    def __repr__(self):
        return 'SubItem<#%s "%s">' % (self.id, self.name)
class Customer(object):
    def __repr__(self):
        return 'Customer<#%s "%s">' % (self.id, self.name)
class Purchase(object):
    def __repr__(self):
        return 'Purchase<#%s "%s">' % (self.id, self.purchase_date)

items, subitems, customers, purchases, purchaseitems = \
    None, None, None, None, None

metadata = MetaData()

@profiled('table')
def define_tables():
    global items, subitems, customers, purchases, purchaseitems
    items = Table('items', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('name', String(100)),
                  test_needs_acid=True)
    subitems = Table('subitems', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('item_id', Integer, ForeignKey('items.id'),
                            nullable=False),
                     Column('name', String(100), server_default='no name'),
                     test_needs_acid=True)
    customers = Table('customers', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String(100)),
                      *[Column("col_%s" % chr(i), String(64), default=str(i))
                        for i in range(97,117)],
                      **dict(test_needs_acid=True))
    purchases = Table('purchases', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('customer_id', Integer,
                             ForeignKey('customers.id'), nullable=False),
                      Column('purchase_date', DateTime,
                             default=datetime.now),
                      test_needs_acid=True)
    purchaseitems = Table('purchaseitems', metadata,
                      Column('purchase_id', Integer,
                             ForeignKey('purchases.id'),
                             nullable=False, primary_key=True),
                      Column('item_id', Integer, ForeignKey('items.id'),
                             nullable=False, primary_key=True),
                      test_needs_acid=True)

@profiled('mapper')
def setup_mappers():
    mapper(Item, items, properties={
            'subitems': relationship(SubItem, backref='item', lazy='select')
            })
    mapper(SubItem, subitems)
    mapper(Customer, customers, properties={
            'purchases': relationship(Purchase, lazy='select', backref='customer')
            })
    mapper(Purchase, purchases, properties={
            'items': relationship(Item, lazy='select', secondary=purchaseitems)
            })

@profiled('inserts')
def insert_data():
    q_items = 1000
    q_sub_per_item = 10
    q_customers = 1000

    con = testing.db.connect()

    transaction = con.begin()
    data, subdata = [], []
    for item_id in xrange(1, q_items + 1):
        data.append({'name': "item number %s" % item_id})
        for subitem_id in xrange(1, (item_id % q_sub_per_item) + 1):
            subdata.append({'item_id': item_id,
                         'name': "subitem number %s" % subitem_id})
        if item_id % 100 == 0:
            items.insert().execute(*data)
            subitems.insert().execute(*subdata)
            del data[:]
            del subdata[:]
    if data:
        items.insert().execute(*data)
    if subdata:
        subitems.insert().execute(*subdata)
    transaction.commit()

    transaction = con.begin()
    data = []
    for customer_id in xrange(1, q_customers):
        data.append({'name': "customer number %s" % customer_id})
        if customer_id % 100 == 0:
            customers.insert().execute(*data)
            del data[:]
    if data:
        customers.insert().execute(*data)
    transaction.commit()

    transaction = con.begin()
    data, subdata = [], []
    order_t = int(time.time()) - (5000 * 5 * 60)
    current = xrange(1, q_customers)
    step, purchase_id = 1, 0
    while current:
        next = []
        for customer_id in current:
            order_t += 300
            data.append({'customer_id': customer_id,
                         'purchase_date': datetime.fromtimestamp(order_t)})
            purchase_id += 1
            for item_id in range(customer_id % 200, customer_id + 1, 200):
                if item_id != 0:
                    subdata.append({'purchase_id': purchase_id,
                                    'item_id': item_id})
            if customer_id % 10 > step:
                next.append(customer_id)

            if len(data) >= 100:
                purchases.insert().execute(*data)
                if subdata:
                    purchaseitems.insert().execute(*subdata)
                del data[:]
                del subdata[:]
        step, current = step + 1, next

    if data:
        purchases.insert().execute(*data)
    if subdata:
        purchaseitems.insert().execute(*subdata)
    transaction.commit()

@profiled('queries')
def run_queries():
    session = create_session()
    # no explicit transaction here.

    # build a report of summarizing the last 50 purchases and
    # the top 20 items from all purchases

    q = session.query(Purchase). \
        order_by(desc(Purchase.purchase_date)). \
        limit(50).\
        options(joinedload('items'), joinedload('items.subitems'),
                joinedload('customer'))

    report = []
    # "write" the report.  pretend it's going to a web template or something,
    # the point is to actually pull data through attributes and collections.
    for purchase in q:
        report.append(purchase.customer.name)
        report.append(purchase.customer.col_a)
        report.append(purchase.purchase_date)
        for item in purchase.items:
            report.append(item.name)
            report.extend([s.name for s in item.subitems])

    # mix a little low-level with orm
    # pull a report of the top 20 items of all time
    _item_id = purchaseitems.c.item_id
    top_20_q = select([func.distinct(_item_id).label('id')],
                      group_by=[purchaseitems.c.purchase_id, _item_id],
                      order_by=[desc(func.count(_item_id)), _item_id],
                      limit=20)
    ids = [r.id for r in top_20_q.execute().fetchall()]
    q2 = session.query(Item).filter(Item.id.in_(ids))

    for num, item in enumerate(q2):
        report.append("number %s: %s" % (num + 1, item.name))

@profiled('creating')
def create_purchase():
    # commit a purchase
    customer_id = 100
    item_ids = (10,22,34,46,58)

    session = create_session()
    session.begin()

    customer = session.query(Customer).get(customer_id)
    items = session.query(Item).filter(Item.id.in_(item_ids))

    purchase = Purchase()
    purchase.customer = customer
    purchase.items.extend(items)

    session.flush()
    session.commit()
    session.expire(customer)

def setup_db():
    metadata.drop_all()
    metadata.create_all()
def cleanup_db():
    metadata.drop_all()

@profiled('default')
def default():
    run_queries()
    create_purchase()

@profiled('all')
def main():
    metadata.bind = testing.db
    try:
        define_tables()
        setup_mappers()
        setup_db()
        insert_data()
        default()
    finally:
        cleanup_db()

main()
