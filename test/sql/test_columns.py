from sqlalchemy.test.testing import assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc, sql
from sqlalchemy.test import *
from sqlalchemy import Table, Column  # don't use testlib's wrappers


class ColumnDefinitionTest(TestBase):
    """Test Column() construction."""

    # flesh this out with explicit coverage...

    def columns(self):
        return [ Column(),
                 Column('b'),
                 Column(Integer),
                 Column('d', Integer),
                 Column(name='e'),
                 Column(type_=Integer),
                 Column(Integer()),
                 Column('h', Integer()),
                 Column(type_=Integer()) ]

    def test_basic(self):
        c = self.columns()

        for i, v in ((0, 'a'), (2, 'c'), (5, 'f'), (6, 'g'), (8, 'i')):
            c[i].name = v
            c[i].key = v
        del i, v

        tbl = Table('table', MetaData(), *c)

        for i, col in enumerate(tbl.c):
            assert col.name == c[i].name

    def test_incomplete(self):
        c = self.columns()

        assert_raises(exc.ArgumentError, Table, 't', MetaData(), *c)

    def test_incomplete_key(self):
        c = Column(Integer)
        assert c.name is None
        assert c.key is None

        c.name = 'named'
        t = Table('t', MetaData(), c)

        assert c.name == 'named'
        assert c.name == c.key


    def test_bogus(self):
        assert_raises(exc.ArgumentError, Column, 'foo', name='bar')
        assert_raises(exc.ArgumentError, Column, 'foo', Integer,
                          type_=Integer())

