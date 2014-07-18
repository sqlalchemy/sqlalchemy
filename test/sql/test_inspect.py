"""test the inspection registry system."""

from sqlalchemy import inspect
from sqlalchemy import Table, Column, Integer, MetaData
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_


class TestCoreInspection(fixtures.TestBase):

    def test_table(self):
        t = Table('t', MetaData(),
                  Column('x', Integer)
                  )

        is_(inspect(t), t)
        assert t.is_selectable
        is_(t.selectable, t)

    def test_select(self):
        t = Table('t', MetaData(),
                  Column('x', Integer)
                  )
        s = t.select()

        is_(inspect(s), s)
        assert s.is_selectable
        is_(s.selectable, s)

    def test_column_expr(self):
        c = Column('x', Integer)
        is_(inspect(c), c)
        assert not c.is_selectable
        assert not hasattr(c, 'selectable')
