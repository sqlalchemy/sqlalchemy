from sqlalchemy import *
from sqlalchemy.orm import *

from test.lib import testing
from test.lib.util import function_named
from test.lib import fixtures
from test.lib.schema import Table, Column

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
        s = session.query(LocationName).filter(LocationName.name==name).first()
        session.expunge_all()
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


class MagazineTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global publication_table, issue_table, location_table, location_name_table, magazine_table, \
        page_table, magazine_page_table, classified_page_table, page_size_table

        publication_table = Table('publication', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(45), default=''),
        )
        issue_table = Table('issue', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('publication_id', Integer, ForeignKey('publication.id')),
            Column('issue', Integer),
        )
        location_table = Table('location', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('issue_id', Integer, ForeignKey('issue.id')),
            Column('ref', CHAR(3), default=''),
            Column('location_name_id', Integer, ForeignKey('location_name.id')),
        )
        location_name_table = Table('location_name', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(45), default=''),
        )
        magazine_table = Table('magazine', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('location_id', Integer, ForeignKey('location.id')),
            Column('page_size_id', Integer, ForeignKey('page_size.id')),
        )
        page_table = Table('page', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('page_no', Integer),
            Column('type', CHAR(1), default='p'),
        )
        magazine_page_table = Table('magazine_page', metadata,
            Column('page_id', Integer, ForeignKey('page.id'), primary_key=True),
            Column('magazine_id', Integer, ForeignKey('magazine.id')),
            Column('orders', Text, default=''),
        )
        classified_page_table = Table('classified_page', metadata,
            Column('magazine_page_id', Integer, ForeignKey('magazine_page.page_id'), primary_key=True),
            Column('titles', String(45), default=''),
        )
        page_size_table = Table('page_size', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('width', Integer),
            Column('height', Integer),
            Column('name', String(45), default=''),
        )

def _generate_round_trip_test(use_unions=False, use_joins=False):
    def test_roundtrip(self):
        publication_mapper = mapper(Publication, publication_table)

        issue_mapper = mapper(Issue, issue_table, properties = {
            'publication': relationship(Publication, backref=backref('issues', cascade="all, delete-orphan")),
        })

        location_name_mapper = mapper(LocationName, location_name_table)

        location_mapper = mapper(Location, location_table, properties = {
            'issue': relationship(Issue, backref=backref('locations', lazy='joined', cascade="all, delete-orphan")),
            '_name': relationship(LocationName),
        })

        page_size_mapper = mapper(PageSize, page_size_table)

        magazine_mapper = mapper(Magazine, magazine_table, properties = {
            'location': relationship(Location, backref=backref('magazine', uselist=False)),
            'size': relationship(PageSize),
        })

        if use_unions:
            page_join = polymorphic_union(
                {
                    'm': page_table.join(magazine_page_table),
                    'c': page_table.join(magazine_page_table).join(classified_page_table),
                    'p': page_table.select(page_table.c.type=='p'),
                }, None, 'page_join')
            page_mapper = mapper(Page, page_table, with_polymorphic=('*', page_join), polymorphic_on=page_join.c.type, polymorphic_identity='p')
        elif use_joins:
            page_join = page_table.outerjoin(magazine_page_table).outerjoin(classified_page_table)
            page_mapper = mapper(Page, page_table, with_polymorphic=('*', page_join), polymorphic_on=page_table.c.type, polymorphic_identity='p')
        else:
            page_mapper = mapper(Page, page_table, polymorphic_on=page_table.c.type, polymorphic_identity='p')

        if use_unions:
            magazine_join = polymorphic_union(
                {
                    'm': page_table.join(magazine_page_table),
                    'c': page_table.join(magazine_page_table).join(classified_page_table),
                }, None, 'page_join')
            magazine_page_mapper = mapper(MagazinePage, magazine_page_table, with_polymorphic=('*', magazine_join), inherits=page_mapper, polymorphic_identity='m', properties={
                'magazine': relationship(Magazine, backref=backref('pages', order_by=magazine_join.c.page_no))
            })
        elif use_joins:
            magazine_join = page_table.join(magazine_page_table).outerjoin(classified_page_table)
            magazine_page_mapper = mapper(MagazinePage, magazine_page_table, with_polymorphic=('*', magazine_join), inherits=page_mapper, polymorphic_identity='m', properties={
                'magazine': relationship(Magazine, backref=backref('pages', order_by=page_table.c.page_no))
            })
        else:
            magazine_page_mapper = mapper(MagazinePage, magazine_page_table, inherits=page_mapper, polymorphic_identity='m', properties={
                'magazine': relationship(Magazine, backref=backref('pages', order_by=page_table.c.page_no))
            })

        classified_page_mapper = mapper(ClassifiedPage, 
                                    classified_page_table, 
                                    inherits=magazine_page_mapper, 
                                    polymorphic_identity='c', 
                                    primary_key=[page_table.c.id])


        session = create_session()

        pub = Publication(name='Test')
        issue = Issue(issue=46,publication=pub)
        location = Location(ref='ABC',name='London',issue=issue)

        page_size = PageSize(name='A4',width=210,height=297)

        magazine = Magazine(location=location,size=page_size)

        page = ClassifiedPage(magazine=magazine,page_no=1)
        page2 = MagazinePage(magazine=magazine,page_no=2)
        page3 = ClassifiedPage(magazine=magazine,page_no=3)
        session.add(pub)


        session.flush()
        print [x for x in session]
        session.expunge_all()

        session.flush()
        session.expunge_all()
        p = session.query(Publication).filter(Publication.name=="Test").one()

        print p.issues[0].locations[0].magazine.pages
        print [page, page2, page3]
        assert repr(p.issues[0].locations[0].magazine.pages) == repr([page, page2, page3]), repr(p.issues[0].locations[0].magazine.pages)

    test_roundtrip = function_named(
        test_roundtrip, "test_%s" % (not use_union and (use_joins and "joins" or "select") or "unions"))
    setattr(MagazineTest, test_roundtrip.__name__, test_roundtrip)

for (use_union, use_join) in [(True, False), (False, True), (False, False)]:
    _generate_round_trip_test(use_union, use_join)


