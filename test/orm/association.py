import testenv; testenv.configure_for_tests()

from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *

class AssociationTest(TestBase):
    @testing.uses_deprecated('association option')
    def setUpAll(self):
        global items, item_keywords, keywords, metadata, Item, Keyword, KeywordAssociation
        metadata = MetaData(testing.db)
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
        l = sess.query(Item).all()
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
        l = sess.query(Item).all()
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
        l = sess.query(Item).all()
        loaded = repr(l)
        print saved
        print loaded
        self.assert_(saved == loaded)

    @testing.uses_deprecated('association option')
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

class AssociationTest2(TestBase):
    def setUpAll(self):
        global table_originals, table_people, table_isauthor, metadata, Originals, People, IsAuthor
        metadata = MetaData(testing.db)
        table_originals = Table('Originals', metadata,
            Column('ID',        Integer,        primary_key=True),
            Column('Title',     String(200),    nullable=False),
            Column('Date',      Date            ),
            )
        table_people = Table('People', metadata,
            Column('ID',        Integer,        primary_key=True),
            Column('Name',      String(140),    nullable=False),
            Column('Country',   CHAR(2),        default='es'),
            )
        table_isauthor = Table('IsAuthor', metadata,
            Column('OriginalsID', Integer,      ForeignKey('Originals.ID'),
default=None),
            Column('PeopleID', Integer, ForeignKey('People.ID'),
default=None),
            Column('Kind',      CHAR(1),        default='A'),
            )
        metadata.create_all()

        class Base(object):
            def __init__(self, **kw):
                for k,v in kw.iteritems():
                    setattr(self, k, v)
            def display(self):
                c = [ "%s=%s" % (col.key, repr(getattr(self, col.key))) for col
in self.c ]
                return "%s(%s)" % (self.__class__.__name__, ', '.join(c))
            def __repr__(self):
                return self.display()
            def __str__(self):
                return self.display()
        class Originals(Base):
            order = [table_originals.c.Title, table_originals.c.Date]
        class People(Base):
            order = [table_people.c.Name]
        class IsAuthor(Base):
            pass

        mapper(Originals, table_originals, order_by=Originals.order,
            properties={
                'people': relation(IsAuthor, association=People),
                'authors': relation(People, secondary=table_isauthor, backref='written',
                            primaryjoin=and_(table_originals.c.ID==table_isauthor.c.OriginalsID,
                            table_isauthor.c.Kind=='A')),
                'title': table_originals.c.Title,
                'date': table_originals.c.Date,
            })
        mapper(People, table_people, order_by=People.order, properties=    {
                'originals':        relation(IsAuthor, association=Originals),
                'name':             table_people.c.Name,
                'country':          table_people.c.Country,
            })
        mapper(IsAuthor, table_isauthor,
            primary_key=[table_isauthor.c.OriginalsID, table_isauthor.c.PeopleID,
table_isauthor.c.Kind],
            properties={
               'original':  relation(Originals, lazy=False),
               'person':    relation(People, lazy=False),
               'kind':      table_isauthor.c.Kind,
            })

    def tearDown(self):
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()

    def testinsert(self):
        # this test is sure to get more complex...
        p = People(name='name', country='es')
        sess = create_session()
        sess.save(p)
        sess.flush()



if __name__ == "__main__":
    testenv.main()
