import testbase
from sqlalchemy import *

class BaseObject(object):
    def __init__(self, *args, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
class Publication(BaseObject):
    pass

class Issue(BaseObject):
    pass

class Location(BaseObject):
    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, str(getattr(self, 'issue_id', None)), repr(str(self._name.name)))

    def _get_name(self):
        return self._name

    def _set_name(self, name):
        session = create_session()
        s = session.query(LocationName).selectfirst(location_name_table.c.name==name)
        session.clear()
        if s is not None:
            self._name = s

            return

        found = False

        for i in session.new:
            if isinstance(i, LocationName) and i.name == name:
                self._name = i
                found = True

                break

        if found == False:
            self._name = LocationName(name=name)

    name = property(_get_name, _set_name)

class LocationName(BaseObject):
    def __repr__(self):
        return "%s()" % (self.__class__.__name__)

class PageSize(BaseObject):
    def __repr__(self):
        return "%s(%sx%s, %s)" % (self.__class__.__name__, self.width, self.height, self.name)
        
class Magazine(BaseObject):
    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, repr(self.location), repr(self.size))

class Page(BaseObject):
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, str(self.page_no))

class MagazinePage(Page):
    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, str(self.page_no), repr(self.magazine))

class ClassifiedPage(MagazinePage):
    pass

class InheritTest(testbase.AssertMixin):
    """tests a large polymorphic relationship"""
    def setUpAll(self):
        global metadata, publication_table, issue_table, location_table, location_name_table, magazine_table, \
        page_table, magazine_page_table, classified_page_table, page_size_table
        
        metadata = BoundMetaData(testbase.db)

        zerodefault = {} #{'default':0}
        publication_table = Table('publication', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('name', String(45), default=''),
        )
        issue_table = Table('issue', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('publication_id', Integer, ForeignKey('publication.id'), **zerodefault),
            Column('issue', Integer, **zerodefault),
        )
        location_table = Table('location', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('issue_id', Integer, ForeignKey('issue.id'), **zerodefault),
            Column('ref', CHAR(3), default=''),
            Column('location_name_id', Integer, ForeignKey('location_name.id'), **zerodefault),
        )
        location_name_table = Table('location_name', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('name', String(45), default=''),
        )
        magazine_table = Table('magazine', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('location_id', Integer, ForeignKey('location.id'), **zerodefault),
            Column('page_size_id', Integer, ForeignKey('page_size.id'), **zerodefault),
        )
        page_table = Table('page', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('page_no', Integer, **zerodefault),
            Column('type', CHAR(1), default='p'),
        )
        magazine_page_table = Table('magazine_page', metadata,
            Column('page_id', Integer, ForeignKey('page.id'), primary_key=True, **zerodefault),
            Column('magazine_id', Integer, ForeignKey('magazine.id'), **zerodefault),
            Column('orders', TEXT, default=''),
        )
        classified_page_table = Table('classified_page', metadata,
            Column('magazine_page_id', Integer, ForeignKey('magazine_page.page_id'), primary_key=True, **zerodefault),
            Column('titles', String(45), default=''),
        )
        page_size_table = Table('page_size', metadata,
            Column('id', Integer, primary_key=True, default=None),
            Column('width', Integer, **zerodefault),
            Column('height', Integer, **zerodefault),
            Column('name', String(45), default=''),
        )

        metadata.create_all()
        
        publication_mapper = mapper(Publication, publication_table)

        issue_mapper = mapper(Issue, issue_table, properties = {
            'publication': relation(Publication, backref=backref('issues', cascade="all, delete-orphan")),
        })

        location_name_mapper = mapper(LocationName, location_name_table)

        location_mapper = mapper(Location, location_table, properties = {
            'issue': relation(Issue, backref='locations'),
            '_name': relation(LocationName),
        })

        issue_mapper.add_property('locations', relation(Location, lazy=False, private=True, backref='issue'))

        page_size_mapper = mapper(PageSize, page_size_table)

        page_join = polymorphic_union(
            {
                'm': page_table.join(magazine_page_table),
                'c': page_table.join(magazine_page_table).join(classified_page_table),
                'p': page_table.select(page_table.c.type=='p'),
            }, None, 'page_join')

        magazine_join = polymorphic_union(
            {
                'm': page_table.join(magazine_page_table),
                'c': page_table.join(magazine_page_table).join(classified_page_table),
            }, None, 'page_join')

        magazine_mapper = mapper(Magazine, magazine_table, properties = {
            'location': relation(Location, backref=backref('magazine', uselist=False)),
            'size': relation(PageSize),
        })

        page_mapper = mapper(Page, page_table, select_table=page_join, polymorphic_on=page_join.c.type, polymorphic_identity='p')

        magazine_page_mapper = mapper(MagazinePage, magazine_page_table, select_table=magazine_join, inherits=page_mapper, polymorphic_identity='m', properties={
            'magazine': relation(Magazine, backref=backref('pages', order_by=magazine_join.c.page_no))
        })

        classified_page_mapper = mapper(ClassifiedPage, classified_page_table, inherits=magazine_page_mapper, polymorphic_identity='c')

    def tearDown(self):
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def tearDownAll(self):
        metadata.drop_all()
        clear_mappers()
        
    def testone(self):
        session = create_session()

        pub = Publication(name='Test')
        issue = Issue(issue=46,publication=pub)

        location = Location(ref='ABC',name='London',issue=issue)

        page_size = PageSize(name='A4',width=210,height=297)

        magazine = Magazine(location=location,size=page_size)
        page = ClassifiedPage(magazine=magazine,page_no=1)
        page2 = MagazinePage(magazine=magazine,page_no=2)
        page3 = ClassifiedPage(magazine=magazine,page_no=3)
        session.save(pub)
        
        session.flush()
        print [x for x in session]
        session.clear()

        session.echo_uow=True
        session.flush()
        session.clear()
        p = session.query(Publication).selectone_by(name='Test')

        print p.issues[0].locations[0].magazine.pages
        print [page, page2, page3]
        assert repr(p.issues[0].locations[0].magazine.pages) == repr([page, page2, page3])
        
if __name__ == '__main__':
    testbase.main()
