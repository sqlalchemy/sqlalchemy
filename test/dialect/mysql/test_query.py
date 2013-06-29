# coding: utf-8

from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import testing


class MatchTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = 'mysql'

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
             'title': 'Agile Web Development with Rails',
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

    @testing.fails_on('mysql+mysqlconnector', 'uses pyformat')
    def test_expression(self):
        format = testing.db.dialect.paramstyle == 'format' and '%s' or '?'
        self.assert_compile(
            matchtable.c.title.match('somstr'),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)" % format)

    @testing.fails_on('mysql+mysqldb', 'uses format')
    @testing.fails_on('mysql+pymysql', 'uses format')
    @testing.fails_on('mysql+cymysql', 'uses format')
    @testing.fails_on('mysql+oursql', 'uses format')
    @testing.fails_on('mysql+pyodbc', 'uses format')
    @testing.fails_on('mysql+zxjdbc', 'uses format')
    def test_expression(self):
        format = '%(title_1)s'
        self.assert_compile(
            matchtable.c.title.match('somstr'),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)" % format)

    def test_simple_match(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match('python')).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match("Matz's")).
                   execute().
                   fetchall())
        eq_([3], [r.id for r in results])

    def test_or_match(self):
        results1 = (matchtable.select().
                    where(or_(matchtable.c.title.match('nutshell'),
                              matchtable.c.title.match('ruby'))).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([3, 5], [r.id for r in results1])
        results2 = (matchtable.select().
                    where(matchtable.c.title.match('nutshell ruby')).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([3, 5], [r.id for r in results2])


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


