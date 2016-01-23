# coding: utf-8

from sqlalchemy.testing import eq_, is_
from sqlalchemy import *
from sqlalchemy.testing import fixtures
from sqlalchemy import testing


class IdiosyncrasyTest(fixtures.TestBase):
    __only_on__ = 'mysql'
    __backend__ = True

    @testing.emits_warning()
    def test_is_boolean_symbols_despite_no_native(self):
        is_(
            testing.db.scalar(select([cast(true().is_(true()), Boolean)])),
            True
        )

        is_(
            testing.db.scalar(select([cast(true().isnot(true()), Boolean)])),
            False
        )

        is_(
            testing.db.scalar(select([cast(false().is_(false()), Boolean)])),
            True
        )


class MatchTest(fixtures.TestBase):
    __only_on__ = 'mysql'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)

        cattable = Table('cattable', metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(50)),
            mysql_engine='MyISAM'
        )
        matchtable = Table('matchtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id')),
            mysql_engine='MyISAM'
        )
        metadata.create_all()

        cattable.insert().execute([
            {'id': 1, 'description': 'Python'},
            {'id': 2, 'description': 'Ruby'},
        ])
        matchtable.insert().execute([
            {'id': 1,
             'title': 'Agile Web Development with Ruby On Rails',
             'category_id': 2},
            {'id': 2,
             'title': 'Dive Into Python',
             'category_id': 1},
            {'id': 3,
             'title': "Programming Matz's Ruby",
             'category_id': 2},
            {'id': 4,
             'title': 'The Definitive Guide to Django',
             'category_id': 1},
            {'id': 5,
             'title': 'Python in a Nutshell',
             'category_id': 1}
        ])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_simple_match(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match('python')).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([2, 5], [r.id for r in results])

    def test_not_match(self):
        results = (matchtable.select().
                   where(~matchtable.c.title.match('python')).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([1, 3, 4], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match("Matz's")).
                   execute().
                   fetchall())
        eq_([3], [r.id for r in results])

    def test_return_value(self):
        # test [ticket:3263]
        result = testing.db.execute(
            select([
                matchtable.c.title.match('Agile Ruby Programming').label('ruby'),
                matchtable.c.title.match('Dive Python').label('python'),
                matchtable.c.title
            ]).order_by(matchtable.c.id)
        ).fetchall()
        eq_(
            result,
            [
                (2.0, 0.0, 'Agile Web Development with Ruby On Rails'),
                (0.0, 2.0, 'Dive Into Python'),
                (2.0, 0.0, "Programming Matz's Ruby"),
                (0.0, 0.0, 'The Definitive Guide to Django'),
                (0.0, 1.0, 'Python in a Nutshell')
            ]
        )

    def test_or_match(self):
        results1 = (matchtable.select().
                    where(or_(matchtable.c.title.match('nutshell'),
                              matchtable.c.title.match('ruby'))).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([1, 3, 5], [r.id for r in results1])
        results2 = (matchtable.select().
                    where(matchtable.c.title.match('nutshell ruby')).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([1, 3, 5], [r.id for r in results2])

    def test_and_match(self):
        results1 = (matchtable.select().
                    where(and_(matchtable.c.title.match('python'),
                               matchtable.c.title.match('nutshell'))).
                    execute().
                    fetchall())
        eq_([5], [r.id for r in results1])
        results2 = (matchtable.select().
                    where(matchtable.c.title.match('+python +nutshell')).
                    execute().
                    fetchall())
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = (matchtable.select().
                   where(and_(cattable.c.id==matchtable.c.category_id,
                              or_(cattable.c.description.match('Ruby'),
                                  matchtable.c.title.match('nutshell')))).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([1, 3, 5], [r.id for r in results])


class AnyAllTest(fixtures.TablesTest):
    __only_on__ = 'mysql'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'stuff', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', Integer)
        )

    @classmethod
    def insert_data(cls):
        stuff = cls.tables.stuff
        testing.db.execute(
            stuff.insert(),
            [
                {'id': 1, 'value': 1},
                {'id': 2, 'value': 2},
                {'id': 3, 'value': 3},
                {'id': 4, 'value': 4},
                {'id': 5, 'value': 5},
            ]
        )

    def test_any_w_comparator(self):
        stuff = self.tables.stuff
        stmt = select([stuff.c.id]).where(
            stuff.c.value > any_(select([stuff.c.value])))

        eq_(
            testing.db.execute(stmt).fetchall(),
            [(2,), (3,), (4,), (5,)]
        )

    def test_all_w_comparator(self):
        stuff = self.tables.stuff
        stmt = select([stuff.c.id]).where(
            stuff.c.value >= all_(select([stuff.c.value])))

        eq_(
            testing.db.execute(stmt).fetchall(),
            [(5,)]
        )

    def test_any_literal(self):
        stuff = self.tables.stuff
        stmt = select([4 == any_(select([stuff.c.value]))])

        is_(
            testing.db.execute(stmt).scalar(), True
        )

