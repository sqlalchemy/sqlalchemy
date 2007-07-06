from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase
import gc
import time
import hotshot
import hotshot.stats

db = testbase.db

NUM = 500
DIVISOR = 50

class LoadTest(AssertMixin):
    def setUpAll(self):
        global items, meta,subitems
        meta = MetaData(db)
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
        for x in range(1,NUM/DIVISOR + 1):
            l.append({'item_id':x, 'value':'this is item #%d' % x})
        #print l
        items.insert().execute(*l)
        for x in range(1, NUM/DIVISOR + 1):
            l = []
            for y in range(1, DIVISOR + 1):
                z = ((x-1) * DIVISOR) + y
                l.append({'sub_id':z,'value':'this is iteim #%d' % z, 'parent_id':x})
            #print l
            subitems.insert().execute(*l)    
    def testload(self):
        class Item(object):pass
        class SubItem(object):pass
        mapper(Item, items, properties={'subs':relation(SubItem, lazy=False)})
        mapper(SubItem, subitems)
        sess = create_session()
        prof = hotshot.Profile("masseagerload.prof")
        prof.start()
        query = sess.query(Item)
        l = query.select()
        print "loaded ", len(l), " items each with ", len(l[0].subs), "subitems"
        prof.stop()
        prof.close()
        stats = hotshot.stats.load("masseagerload.prof")
        stats.sort_stats('time', 'calls')
        stats.print_stats()
        
if __name__ == "__main__":
    testbase.main()        
