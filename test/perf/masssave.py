from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import sqlalchemy.attributes as attributes
import StringIO
import testbase
import gc

db = testbase.db

NUM = 25000

class SaveTest(AssertMixin):
    def setUpAll(self):
        global items, metadata
        metadata = BoundMetaData(db)
        items = Table('items', metadata, 
            Column('item_id', Integer, primary_key=True),
            Column('value', String(100)))
        items.create()
    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()
        
    def testsave(self):
        class Item(object):pass
            
        m = mapper(Item, items)
        
        for x in range(0,100):
            sess = create_session()
            query = sess.query(Item)
            for y in range (0,50):
                print "x,y", (x,y)
                sess.save(Item())
            sess.flush()

if __name__ == "__main__":
    testbase.main()        
