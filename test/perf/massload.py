from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import sqlalchemy.attributes as attributes
import StringIO
import testbase
import gc

db = testbase.db

NUM = 25000

"""
we are testing session.expunge() here, also that the attributes and unitofwork packages dont keep dereferenced
stuff hanging around.

for best results, dont run with sqlite :memory: database, and keep an eye on top while it runs"""

class LoadTest(AssertMixin):
    def setUpAll(self):
        db.echo = False
        global items
        items = Table('items', db, 
            Column('item_id', Integer, primary_key=True),
            Column('value', String(100)))
        items.create()
        db.echo = testbase.echo
    def tearDownAll(self):
        db.echo = False
        items.drop()
        items.deregister()
        db.echo = testbase.echo
    def setUp(self):
        objectstore.clear()
        clear_mappers()
        for x in range(1,NUM/500+1):
            l = []
            for y in range(x*500-500 + 1, x*500 + 1):
                l.append({'item_id':y, 'value':'this is item #%d' % y})
            items.insert().execute(*l)
            
    def testload(self):
        class Item(object):pass
            
        m = mapper(Item, items)
        sess = create_session()
        query = sess.query(Item)
        for x in range (1,NUM/100):
            # this is not needed with cpython which clears non-circular refs immediately
            #gc.collect()
            l = query.select(items.c.item_id.between(x*100 - 100 + 1, x*100))
            assert len(l) == 100
            print "loaded ", len(l), " items "
            # modifying each object will insure that the objects get placed in the "dirty" list
            # and will hang around until expunged
            #for a in l:
            #    a.value = 'changed...'
            #assert len(objectstore.get_session().dirty) == len(l)
            #assert len(objectstore.get_session().identity_map) == len(l)
            #assert len(attributes.managed_attributes) == len(l)
            #print len(objectstore.get_session().dirty)
            #print len(objectstore.get_session().identity_map)
            #objectstore.expunge(*l)

if __name__ == "__main__":
    testbase.main()        
