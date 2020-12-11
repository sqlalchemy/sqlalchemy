# coding: utf-8

from sqlalchemy import all_
from sqlalchemy import and_
from sqlalchemy import any_
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import false
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_


class IdiosyncrasyTest(fixtures.TestBase):
    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    @testing.emits_warning()
    def test_is_boolean_symbols_despite_no_native(self, connection):

        is_(
            connection.scalar(select(cast(true().is_(true()), Boolean))),
            True,
        )

        is_(
            connection.scalar(select(cast(true().is_not(true()), Boolean))),
            False,
        )

        is_(
            connection.scalar(select(cast(false().is_(false()), Boolean))),
            True,
        )


class MatchTest(fixtures.TablesTest):
    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "cattable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("description", String(50)),
            mysql_engine="MyISAM",
            mariadb_engine="MyISAM",
        )
        Table(
            "matchtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("title", String(200)),
            Column("category_id", Integer, ForeignKey("cattable.id")),
            mysql_engine="MyISAM",
            mariadb_engine="MyISAM",
        )

    @classmethod
    def insert_data(cls, connection):
        cattable, matchtable = cls.tables("cattable", "matchtable")

        connection.execute(
            cattable.insert(),
            [
                {"id": 1, "description": "Python"},
                {"id": 2, "description": "Ruby"},
            ],
        )
        connection.execute(
            matchtable.insert(),
            [
                {
                    "id": 1,
                    "title": "Agile Web Development with Ruby On Rails",
                    "category_id": 2,
                },
                {"id": 2, "title": "Dive Into Python", "category_id": 1},
                {
                    "id": 3,
                    "title": "Programming Matz's Ruby",
                    "category_id": 2,
                },
                {
                    "id": 4,
                    "title": "The Definitive Guide to Django",
                    "category_id": 1,
                },
                {"id": 5, "title": "Python in a Nutshell", "category_id": 1},
            ],
        )

    def test_simple_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_not_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select()
            .where(~matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        )
        eq_([1, 3, 4], [r.id for r in results])

    def test_simple_match_with_apostrophe(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match("Matz's"))
        ).fetchall()
        eq_([3], [r.id for r in results])

    def test_return_value(self, connection):
        matchtable = self.tables.matchtable
        # test [ticket:3263]
        result = connection.execute(
            select(
                matchtable.c.title.match("Agile Ruby Programming").label(
                    "ruby"
                ),
                matchtable.c.title.match("Dive Python").label("python"),
                matchtable.c.title,
            ).order_by(matchtable.c.id)
        ).fetchall()
        eq_(
            result,
            [
                (2.0, 0.0, "Agile Web Development with Ruby On Rails"),
                (0.0, 2.0, "Dive Into Python"),
                (2.0, 0.0, "Programming Matz's Ruby"),
                (0.0, 0.0, "The Definitive Guide to Django"),
                (0.0, 1.0, "Python in a Nutshell"),
            ],
        )

    def test_or_match(self, connection):
        matchtable = self.tables.matchtable
        results1 = connection.execute(
            matchtable.select()
            .where(
                or_(
                    matchtable.c.title.match("nutshell"),
                    matchtable.c.title.match("ruby"),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3, 5], [r.id for r in results1])
        results2 = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("nutshell ruby"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3, 5], [r.id for r in results2])

    def test_and_match(self, connection):
        matchtable = self.tables.matchtable
        results1 = connection.execute(
            matchtable.select().where(
                and_(
                    matchtable.c.title.match("python"),
                    matchtable.c.title.match("nutshell"),
                )
            )
        ).fetchall()
        eq_([5], [r.id for r in results1])
        results2 = connection.execute(
            matchtable.select().where(
                matchtable.c.title.match("+python +nutshell")
            )
        ).fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self, connection):
        matchtable = self.tables.matchtable
        cattable = self.tables.cattable
        results = connection.execute(
            matchtable.select()
            .where(
                and_(
                    cattable.c.id == matchtable.c.category_id,
                    or_(
                        cattable.c.description.match("Ruby"),
                        matchtable.c.title.match("nutshell"),
                    ),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3, 5], [r.id for r in results])


class AnyAllTest(fixtures.TablesTest):
    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "stuff",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", Integer),
        )

    @classmethod
    def insert_data(cls, connection):
        stuff = cls.tables.stuff
        connection.execute(
            stuff.insert(),
            [
                {"id": 1, "value": 1},
                {"id": 2, "value": 2},
                {"id": 3, "value": 3},
                {"id": 4, "value": 4},
                {"id": 5, "value": 5},
            ],
        )

    def test_any_w_comparator(self, connection):
        stuff = self.tables.stuff
        stmt = select(stuff.c.id).where(
            stuff.c.value > any_(select(stuff.c.value).scalar_subquery())
        )

        eq_(connection.execute(stmt).fetchall(), [(2,), (3,), (4,), (5,)])

    def test_all_w_comparator(self, connection):
        stuff = self.tables.stuff
        stmt = select(stuff.c.id).where(
            stuff.c.value >= all_(select(stuff.c.value).scalar_subquery())
        )

        eq_(connection.execute(stmt).fetchall(), [(5,)])

    def test_any_literal(self, connection):
        stuff = self.tables.stuff
        stmt = select(4 == any_(select(stuff.c.value).scalar_subquery()))

        is_(connection.execute(stmt).scalar(), True)
