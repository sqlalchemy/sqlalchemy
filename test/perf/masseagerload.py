from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.test import profiling

NUM = 500
DIVISOR = 50

engine = create_engine('sqlite://')
meta = MetaData(engine)
items = Table('items', meta,
              Column('item_id', Integer, primary_key=True),
              Column('value', String(100)))
subitems = Table('subitems', meta,
                 Column('sub_id', Integer, primary_key=True),
                 Column('parent_id', Integer, ForeignKey('items.item_id')),
                 Column('value', String(100)))

class Item(object):pass
class SubItem(object):pass
mapper(Item, items, properties={'subs':relationship(SubItem, lazy='joined')})
mapper(SubItem, subitems)

def load():
    global l
    l = []
    for x in range(1,NUM/DIVISOR + 1):
        l.append({'item_id':x, 'value':'this is item #%d' % x})
    #print l
    items.insert().execute(*l)
    for x in range(1, NUM/DIVISOR + 1):
        l = []
        for y in range(1, DIVISOR + 1):
            z = ((x-1) * DIVISOR) + y
            l.append({'sub_id':z,'value':'this is item #%d' % z, 'parent_id':x})
        #print l
        subitems.insert().execute(*l)

@profiling.profiled('massjoinedload', always=True, sort=['cumulative'])
def massjoinedload(session):
    session.begin()
    query = session.query(Item)
    l = query.all()
    print "loaded ", len(l), " items each with ", len(l[0].subs), "subitems"

def all():
    meta.create_all()
    try:
        load()
        massjoinedload(create_session())
    finally:
        meta.drop_all()

if __name__ == '__main__':
    all()
