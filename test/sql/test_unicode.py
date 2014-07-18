# coding: utf-8
"""verrrrry basic unicode column name testing"""

from sqlalchemy import *
from sqlalchemy.testing import fixtures, engines, eq_
from sqlalchemy import testing
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.util import u, ue


class UnicodeSchemaTest(fixtures.TestBase):
    __requires__ = ('unicode_ddl',)
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata, t1, t2, t3

        metadata = MetaData(testing.db)
        t1 = Table(u('unitable1'), metadata,
                   Column(u('méil'), Integer, primary_key=True),
                   Column(ue('\u6e2c\u8a66'), Integer),
                   test_needs_fk=True,
                   )
        t2 = Table(
            u('Unitéble2'),
            metadata,
            Column(
                u('méil'),
                Integer,
                primary_key=True,
                key="a"),
            Column(
                ue('\u6e2c\u8a66'),
                Integer,
                ForeignKey(
                    u('unitable1.méil')),
                key="b"),
            test_needs_fk=True,
        )

        # Few DBs support Unicode foreign keys
        if testing.against('sqlite'):
            t3 = Table(ue('\u6e2c\u8a66'), metadata,
                       Column(ue('\u6e2c\u8a66_id'), Integer, primary_key=True,
                              autoincrement=False),
                       Column(ue('unitable1_\u6e2c\u8a66'), Integer,
                              ForeignKey(ue('unitable1.\u6e2c\u8a66'))
                              ),
                       Column(u('Unitéble2_b'), Integer,
                              ForeignKey(u('Unitéble2.b'))
                              ),
                       Column(ue('\u6e2c\u8a66_self'), Integer,
                              ForeignKey(ue('\u6e2c\u8a66.\u6e2c\u8a66_id'))
                              ),
                       test_needs_fk=True,
                       )
        else:
            t3 = Table(ue('\u6e2c\u8a66'), metadata,
                       Column(ue('\u6e2c\u8a66_id'), Integer, primary_key=True,
                              autoincrement=False),
                       Column(ue('unitable1_\u6e2c\u8a66'), Integer),
                       Column(u('Unitéble2_b'), Integer),
                       Column(ue('\u6e2c\u8a66_self'), Integer),
                       test_needs_fk=True,
                       )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        if metadata.tables:
            t3.delete().execute()
            t2.delete().execute()
            t1.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_insert(self):
        t1.insert().execute({u('méil'): 1, ue('\u6e2c\u8a66'): 5})
        t2.insert().execute({u('a'): 1, u('b'): 1})
        t3.insert().execute({ue('\u6e2c\u8a66_id'): 1,
                             ue('unitable1_\u6e2c\u8a66'): 5,
                             u('Unitéble2_b'): 1,
                             ue('\u6e2c\u8a66_self'): 1})

        assert t1.select().execute().fetchall() == [(1, 5)]
        assert t2.select().execute().fetchall() == [(1, 1)]
        assert t3.select().execute().fetchall() == [(1, 5, 1, 1)]

    def test_col_targeting(self):
        t1.insert().execute({u('méil'): 1, ue('\u6e2c\u8a66'): 5})
        t2.insert().execute({u('a'): 1, u('b'): 1})
        t3.insert().execute({ue('\u6e2c\u8a66_id'): 1,
                             ue('unitable1_\u6e2c\u8a66'): 5,
                             u('Unitéble2_b'): 1,
                             ue('\u6e2c\u8a66_self'): 1})

        row = t1.select().execute().first()
        eq_(row[t1.c[u('méil')]], 1)
        eq_(row[t1.c[ue('\u6e2c\u8a66')]], 5)

        row = t2.select().execute().first()
        eq_(row[t2.c[u('a')]], 1)
        eq_(row[t2.c[u('b')]], 1)

        row = t3.select().execute().first()
        eq_(row[t3.c[ue('\u6e2c\u8a66_id')]], 1)
        eq_(row[t3.c[ue('unitable1_\u6e2c\u8a66')]], 5)
        eq_(row[t3.c[u('Unitéble2_b')]], 1)
        eq_(row[t3.c[ue('\u6e2c\u8a66_self')]], 1)

    def test_reflect(self):
        t1.insert().execute({u('méil'): 2, ue('\u6e2c\u8a66'): 7})
        t2.insert().execute({u('a'): 2, u('b'): 2})
        t3.insert().execute({ue('\u6e2c\u8a66_id'): 2,
                             ue('unitable1_\u6e2c\u8a66'): 7,
                             u('Unitéble2_b'): 2,
                             ue('\u6e2c\u8a66_self'): 2})

        meta = MetaData(testing.db)
        tt1 = Table(t1.name, meta, autoload=True)
        tt2 = Table(t2.name, meta, autoload=True)
        tt3 = Table(t3.name, meta, autoload=True)

        tt1.insert().execute({u('méil'): 1, ue('\u6e2c\u8a66'): 5})
        tt2.insert().execute({u('méil'): 1, ue('\u6e2c\u8a66'): 1})
        tt3.insert().execute({ue('\u6e2c\u8a66_id'): 1,
                              ue('unitable1_\u6e2c\u8a66'): 5,
                              u('Unitéble2_b'): 1,
                              ue('\u6e2c\u8a66_self'): 1})

        self.assert_(
            tt1.select(
                order_by=desc(
                    u('méil'))).execute().fetchall() == [
                (2, 7), (1, 5)])
        self.assert_(
            tt2.select(
                order_by=desc(
                    u('méil'))).execute().fetchall() == [
                (2, 2), (1, 1)])
        self.assert_(tt3.select(order_by=desc(ue('\u6e2c\u8a66_id'))).
                     execute().fetchall() ==
                     [(2, 7, 2, 2), (1, 5, 1, 1)])

    def test_repr(self):

        m = MetaData()
        t = Table(
            ue('\u6e2c\u8a66'),
            m,
            Column(
                ue('\u6e2c\u8a66_id'),
                Integer))

        # I hardly understand what's going on with the backslashes in
        # this one on py2k vs. py3k
        eq_(repr(t),
            ("Table('\\u6e2c\\u8a66', MetaData(bind=None), "
             "Column('\\u6e2c\\u8a66_id', Integer(), table=<\u6e2c\u8a66>), "
             "schema=None)"))
