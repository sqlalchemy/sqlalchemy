import time
#import sqlalchemy.orm.attributes as attributes
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.test import *

"""

we are testing session.expunge() here, also that the attributes and unitofwork
packages dont keep dereferenced stuff hanging around.

for best results, dont run with sqlite :memory: database, and keep an eye on
top while it runs
"""

NUM = 2500

class LoadTest(TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global items, meta
        meta = MetaData(testing.db)
        items = Table('items', meta,
            Column('item_id', Integer, primary_key=True),
            Column('value', String(100)))
        items.create()
    @classmethod
    def teardown_class(cls):
        items.drop()
    def setup(self):
        for x in range(1,NUM/500+1):
            l = []
            for y in range(x*500-500 + 1, x*500 + 1):
                l.append({'item_id':y, 'value':'this is item #%d' % y})
            items.insert().execute(*l)

    def testload(self):
        class Item(object):pass

        m = mapper(Item, items)
        sess = create_session()
        now = time.time()
        query = sess.query(Item)
        for x in range (1,NUM/100):
            # this is not needed with cpython which clears non-circular refs immediately
            #gc_collect()
            l = query.filter(items.c.item_id.between(x*100 - 100 + 1, x*100)).all()
            assert len(l) == 100
            print "loaded ", len(l), " items "
            # modifying each object will ensure that the objects get placed in the "dirty" list
            # and will hang around until expunged
            #for a in l:
            #    a.value = 'changed...'
            #assert len(objectstore.get_session().dirty) == len(l)
            #assert len(objectstore.get_session().identity_map) == len(l)
            #assert len(attributes.managed_attributes) == len(l)
            #print len(objectstore.get_session().dirty)
            #print len(objectstore.get_session().identity_map)
            #objectstore.expunge(*l)
        total = time.time() -now
        print "total time ", total


