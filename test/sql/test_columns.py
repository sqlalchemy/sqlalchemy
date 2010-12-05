from sqlalchemy.test.testing import assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc, sql
from sqlalchemy.test import *
from sqlalchemy import Table, Column  # don't use testlib's wrappers


class ColumnDefinitionTest(TestBase):
    """Test Column() construction."""

    # flesh this out with explicit coverage...

    def columns(self):
        return [ Column(Integer),
                 Column('b', Integer),
                 Column(Integer),
                 Column('d', Integer),
                 Column(Integer, name='e'),
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


class ColumnOptionsTest(TestBase):

    def test_default_generators(self):
        g1, g2 = Sequence('foo_id_seq'), ColumnDefault('f5')
        assert Column(String, default=g1).default is g1
        assert Column(String, onupdate=g1).onupdate is g1
        assert Column(String, default=g2).default is g2
        assert Column(String, onupdate=g2).onupdate is g2

    def test_type_required(self):
        assert_raises(exc.ArgumentError, Column)
        assert_raises(exc.ArgumentError, Column, "foo")
        assert_raises(exc.ArgumentError, Column, default="foo")
        assert_raises(exc.ArgumentError, Column, Sequence("a"))
        assert_raises(exc.ArgumentError, Column, "foo", default="foo")
        assert_raises(exc.ArgumentError, Column, "foo", Sequence("a"))
        Column(ForeignKey('bar.id'))
        Column("foo", ForeignKey('bar.id'))
        Column(ForeignKey('bar.id'), default="foo")
        Column(ForeignKey('bar.id'), Sequence("a"))
        Column("foo", ForeignKey('bar.id'), default="foo")
        Column("foo", ForeignKey('bar.id'), Sequence("a"))

    def test_column_info(self):

        c1 = Column('foo', String, info={'x':'y'})
        c2 = Column('bar', String, info={})
        c3 = Column('bat', String)
        assert c1.info == {'x':'y'}
        assert c2.info == {}
        assert c3.info == {}

        for c in (c1, c2, c3):
            c.info['bar'] = 'zip'
            assert c.info['bar'] == 'zip'

