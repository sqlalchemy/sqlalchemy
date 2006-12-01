from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase
import gc
import time

db = testbase.db

NUM = 25000

class LoadTest(AssertMixin):
    def setUpAll(self):
        global items, meta,subitems
        meta = BoundMetaData(db)
        items = Table('items', meta, 
            Column('item_id', Integer, primary_key=True),
            Column('value', String(100)))
        subitems = Table('subitems', meta, 
            Column('sub_id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('items.item_id')),
            Column('value', String(100)))
        meta.create_all()
    def tearDownAll(self):
        meta.drop_all()
    def setUp(self):
        clear_mappers()
        l = []
        for x in range(1,NUM/500):
            l.append({'item_id':x, 'value':'this is item #%d' % x})
        items.insert().execute(*l)
        for x in range(1, NUM/500):
            l = []
            for y in range(1, NUM/(NUM/500)):
                z = ((x-1) * NUM/(NUM/500)) + y
                l.append({'sub_id':z,'value':'this is iteim #%d' % z, 'parent_id':x})
            #print l
            subitems.insert().execute(*l)    
    def testload(self):
        class Item(object):pass
        class SubItem(object):pass
        mapper(Item, items, properties={'subs':relation(SubItem, lazy=False)})
        mapper(SubItem, subitems)
        sess = create_session()
        now = time.time()
        query = sess.query(Item)
        l = query.select()
        print "loaded ", len(l), " items each with ", len(l[0].subs), "subitems"
        total = time.time() -now
        print "total time ", total
        
if __name__ == "__main__":
    testbase.main()        
