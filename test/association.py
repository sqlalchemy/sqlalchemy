import testbase

from sqlalchemy import *


class AssociationTest(testbase.PersistTest):
    def setUpAll(self):
        global items, item_keywords, keywords, metadata, Item, Keyword, KeywordAssociation
        metadata = BoundMetaData(testbase.db)
        items = Table('items', metadata, 
            Column('item_id', Integer, primary_key=True),
            Column('name', String(40)),
            )
        item_keywords = Table('item_keywords', metadata,
            Column('item_id', Integer, ForeignKey('items.item_id')),
            Column('keyword_id', Integer, ForeignKey('keywords.keyword_id')),
            Column('data', String(40))
            )
        keywords = Table('keywords', metadata,
            Column('keyword_id', Integer, primary_key=True),
            Column('name', String(40))
            )
        metadata.create_all()
        
        class Item(object):
            def __init__(self, name):
                self.name = name
            def __repr__(self):
                return "Item id=%d name=%s keywordassoc=%s" % (self.item_id, self.name, repr(self.keywords))
        class Keyword(object):
            def __init__(self, name):
                self.name = name
            def __repr__(self):
                return "Keyword id=%d name=%s" % (self.keyword_id, self.name)
        class KeywordAssociation(object):
            def __init__(self, keyword, data):
                self.keyword = keyword
                self.data = data
            def __repr__(self):
                return "KeywordAssociation itemid=%d keyword=%s data=%s" % (self.item_id, repr(self.keyword), self.data)
        
        mapper(Keyword, keywords)
        mapper(KeywordAssociation, item_keywords, properties={
            'keyword':relation(Keyword, lazy=False)
        }, primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id], order_by=[item_keywords.c.data])
        mapper(Item, items, properties={
            'keywords' : relation(KeywordAssociation, association=Keyword)
        })
        
    def tearDown(self):
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()
        
    def testinsert(self):
        sess = create_session()
        item1 = Item('item1')
        item2 = Item('item2')
        item1.keywords.append(KeywordAssociation(Keyword('blue'), 'blue_assoc'))
        item1.keywords.append(KeywordAssociation(Keyword('red'), 'red_assoc'))
        item2.keywords.append(KeywordAssociation(Keyword('green'), 'green_assoc'))
        sess.save(item1)
        sess.save(item2)
        sess.flush()
        saved = repr([item1, item2])
        sess.clear()
        l = sess.query(Item).select()
        loaded = repr(l)
        print saved
        print loaded
        self.assert_(saved == loaded)

    def testreplace(self):
        sess = create_session()
        item1 = Item('item1')
        item1.keywords.append(KeywordAssociation(Keyword('blue'), 'blue_assoc'))
        item1.keywords.append(KeywordAssociation(Keyword('red'), 'red_assoc'))
        sess.save(item1)
        sess.flush()
        
        red_keyword = item1.keywords[1].keyword
        del item1.keywords[1]
        item1.keywords.append(KeywordAssociation(red_keyword, 'new_red_assoc'))
        sess.flush()
        saved = repr([item1])
        sess.clear()
        l = sess.query(Item).select()
        loaded = repr(l)
        print saved
        print loaded
        self.assert_(saved == loaded)

    def testmodify(self):
        sess = create_session()
        item1 = Item('item1')
        item2 = Item('item2')
        item1.keywords.append(KeywordAssociation(Keyword('blue'), 'blue_assoc'))
        item1.keywords.append(KeywordAssociation(Keyword('red'), 'red_assoc'))
        item2.keywords.append(KeywordAssociation(Keyword('green'), 'green_assoc'))
        sess.save(item1)
        sess.save(item2)
        sess.flush()
        
        red_keyword = item1.keywords[1].keyword
        del item1.keywords[0]
        del item1.keywords[0]
        purple_keyword = Keyword('purple')
        item1.keywords.append(KeywordAssociation(red_keyword, 'new_red_assoc'))
        item2.keywords.append(KeywordAssociation(purple_keyword, 'purple_item2_assoc'))
        item1.keywords.append(KeywordAssociation(purple_keyword, 'purple_item1_assoc'))
        item1.keywords.append(KeywordAssociation(Keyword('yellow'), 'yellow_assoc'))
        
        sess.flush()
        saved = repr([item1, item2])
        sess.clear()
        l = sess.query(Item).select()
        loaded = repr(l)
        print saved
        print loaded
        self.assert_(saved == loaded)

    def testdelete(self):
        sess = create_session()
        item1 = Item('item1')
        item2 = Item('item2')
        item1.keywords.append(KeywordAssociation(Keyword('blue'), 'blue_assoc'))
        item1.keywords.append(KeywordAssociation(Keyword('red'), 'red_assoc'))
        item2.keywords.append(KeywordAssociation(Keyword('green'), 'green_assoc'))
        sess.save(item1)
        sess.save(item2)
        sess.flush()
        self.assert_(item_keywords.count().scalar() == 3)

        sess.delete(item1)
        sess.delete(item2)
        sess.flush()
        self.assert_(item_keywords.count().scalar() == 0)

        
if __name__ == "__main__":
    testbase.main()        
