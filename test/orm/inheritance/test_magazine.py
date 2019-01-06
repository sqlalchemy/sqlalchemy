from sqlalchemy import CHAR
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.orm import backref
from sqlalchemy.orm import create_session
from sqlalchemy.orm import mapper
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import function_named


class BaseObject(object):
    def __init__(self, *args, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Publication(BaseObject):
    pass


class Issue(BaseObject):
    pass


class Location(BaseObject):
    def __repr__(self):
        return "%s(%s, %s)" % (
            self.__class__.__name__,
            str(getattr(self, "issue_id", None)),
            repr(str(self._name.name)),
        )

    def _get_name(self):
        return self._name

    def _set_name(self, name):
        session = create_session()
        s = (
            session.query(LocationName)
            .filter(LocationName.name == name)
            .first()
        )
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

        if found is False:
            self._name = LocationName(name=name)

    name = property(_get_name, _set_name)


class LocationName(BaseObject):
    def __repr__(self):
        return "%s()" % (self.__class__.__name__)


class PageSize(BaseObject):
    def __repr__(self):
        return "%s(%sx%s, %s)" % (
            self.__class__.__name__,
            self.width,
            self.height,
            self.name,
        )


class Magazine(BaseObject):
    def __repr__(self):
        return "%s(%s, %s)" % (
            self.__class__.__name__,
            repr(self.location),
            repr(self.size),
        )


class Page(BaseObject):
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, str(self.page_no))


class MagazinePage(Page):
    def __repr__(self):
        return "%s(%s, %s)" % (
            self.__class__.__name__,
            str(self.page_no),
            repr(self.magazine),
        )


class ClassifiedPage(MagazinePage):
    pass


class MagazineTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "publication",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(45), default=""),
        )
        Table(
            "issue",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("publication_id", Integer, ForeignKey("publication.id")),
            Column("issue", Integer),
        )
        Table(
            "location",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("issue_id", Integer, ForeignKey("issue.id")),
            Column("ref", CHAR(3), default=""),
            Column(
                "location_name_id", Integer, ForeignKey("location_name.id")
            ),
        )
        Table(
            "location_name",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(45), default=""),
        )
        Table(
            "magazine",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("location_id", Integer, ForeignKey("location.id")),
            Column("page_size_id", Integer, ForeignKey("page_size.id")),
        )
        Table(
            "page",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("page_no", Integer),
            Column("type", CHAR(1), default="p"),
        )
        Table(
            "magazine_page",
            metadata,
            Column(
                "page_id", Integer, ForeignKey("page.id"), primary_key=True
            ),
            Column("magazine_id", Integer, ForeignKey("magazine.id")),
            Column("orders", Text, default=""),
        )
        Table(
            "classified_page",
            metadata,
            Column(
                "magazine_page_id",
                Integer,
                ForeignKey("magazine_page.page_id"),
                primary_key=True,
            ),
            Column("titles", String(45), default=""),
        )
        Table(
            "page_size",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("width", Integer),
            Column("height", Integer),
            Column("name", String(45), default=""),
        )


def _generate_round_trip_test(use_unions=False, use_joins=False):
    def test_roundtrip(self):
        publication_mapper = mapper(Publication, self.tables.publication)

        issue_mapper = mapper(
            Issue,
            self.tables.issue,
            properties={
                "publication": relationship(
                    Publication,
                    backref=backref("issues", cascade="all, delete-orphan"),
                )
            },
        )

        location_name_mapper = mapper(LocationName, self.tables.location_name)

        location_mapper = mapper(
            Location,
            self.tables.location,
            properties={
                "issue": relationship(
                    Issue,
                    backref=backref(
                        "locations",
                        lazy="joined",
                        cascade="all, delete-orphan",
                    ),
                ),
                "_name": relationship(LocationName),
            },
        )

        page_size_mapper = mapper(PageSize, self.tables.page_size)

        magazine_mapper = mapper(
            Magazine,
            self.tables.magazine,
            properties={
                "location": relationship(
                    Location, backref=backref("magazine", uselist=False)
                ),
                "size": relationship(PageSize),
            },
        )

        if use_unions:
            page_join = polymorphic_union(
                {
                    "m": self.tables.page.join(self.tables.magazine_page),
                    "c": self.tables.page.join(self.tables.magazine_page).join(
                        self.tables.classified_page
                    ),
                    "p": self.tables.page.select(
                        self.tables.page.c.type == "p"
                    ),
                },
                None,
                "page_join",
            )
            page_mapper = mapper(
                Page,
                self.tables.page,
                with_polymorphic=("*", page_join),
                polymorphic_on=page_join.c.type,
                polymorphic_identity="p",
            )
        elif use_joins:
            page_join = self.tables.page.outerjoin(
                self.tables.magazine_page
            ).outerjoin(self.tables.classified_page)
            page_mapper = mapper(
                Page,
                self.tables.page,
                with_polymorphic=("*", page_join),
                polymorphic_on=self.tables.page.c.type,
                polymorphic_identity="p",
            )
        else:
            page_mapper = mapper(
                Page,
                self.tables.page,
                polymorphic_on=self.tables.page.c.type,
                polymorphic_identity="p",
            )

        if use_unions:
            magazine_join = polymorphic_union(
                {
                    "m": self.tables.page.join(self.tables.magazine_page),
                    "c": self.tables.page.join(self.tables.magazine_page).join(
                        self.tables.classified_page
                    ),
                },
                None,
                "page_join",
            )
            magazine_page_mapper = mapper(
                MagazinePage,
                self.tables.magazine_page,
                with_polymorphic=("*", magazine_join),
                inherits=page_mapper,
                polymorphic_identity="m",
                properties={
                    "magazine": relationship(
                        Magazine,
                        backref=backref(
                            "pages", order_by=magazine_join.c.page_no
                        ),
                    )
                },
            )
        elif use_joins:
            magazine_join = self.tables.page.join(
                self.tables.magazine_page
            ).outerjoin(self.tables.classified_page)
            magazine_page_mapper = mapper(
                MagazinePage,
                self.tables.magazine_page,
                with_polymorphic=("*", magazine_join),
                inherits=page_mapper,
                polymorphic_identity="m",
                properties={
                    "magazine": relationship(
                        Magazine,
                        backref=backref(
                            "pages", order_by=self.tables.page.c.page_no
                        ),
                    )
                },
            )
        else:
            magazine_page_mapper = mapper(
                MagazinePage,
                self.tables.magazine_page,
                inherits=page_mapper,
                polymorphic_identity="m",
                properties={
                    "magazine": relationship(
                        Magazine,
                        backref=backref(
                            "pages", order_by=self.tables.page.c.page_no
                        ),
                    )
                },
            )

        classified_page_mapper = mapper(
            ClassifiedPage,
            self.tables.classified_page,
            inherits=magazine_page_mapper,
            polymorphic_identity="c",
            primary_key=[self.tables.page.c.id],
        )

        session = create_session()

        pub = Publication(name="Test")
        issue = Issue(issue=46, publication=pub)
        location = Location(ref="ABC", name="London", issue=issue)

        page_size = PageSize(name="A4", width=210, height=297)

        magazine = Magazine(location=location, size=page_size)

        page = ClassifiedPage(magazine=magazine, page_no=1)
        page2 = MagazinePage(magazine=magazine, page_no=2)
        page3 = ClassifiedPage(magazine=magazine, page_no=3)
        session.add(pub)

        session.flush()
        print([x for x in session])
        session.expunge_all()

        session.flush()
        session.expunge_all()
        p = session.query(Publication).filter(Publication.name == "Test").one()

        print(p.issues[0].locations[0].magazine.pages)
        print([page, page2, page3])
        assert repr(p.issues[0].locations[0].magazine.pages) == repr(
            [page, page2, page3]
        ), repr(p.issues[0].locations[0].magazine.pages)

    test_roundtrip = function_named(
        test_roundtrip,
        "test_%s"
        % (not use_union and (use_joins and "joins" or "select") or "unions"),
    )
    setattr(MagazineTest, test_roundtrip.__name__, test_roundtrip)


for (use_union, use_join) in [(True, False), (False, True), (False, False)]:
    _generate_round_trip_test(use_union, use_join)
