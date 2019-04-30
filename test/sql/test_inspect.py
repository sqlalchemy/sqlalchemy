"""test the inspection registry system."""

from sqlalchemy import Column
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.sql import ClauseElement
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_


class TestCoreInspection(fixtures.TestBase):
    def test_table(self):
        t = Table("t", MetaData(), Column("x", Integer))

        is_(inspect(t), t)
        assert t.is_selectable
        is_(t.selectable, t)

    def test_select(self):
        t = Table("t", MetaData(), Column("x", Integer))
        s = t.select()

        is_(inspect(s), s)
        assert s.is_selectable
        is_(s.selectable, s)

    def test_column_expr(self):
        c = Column("x", Integer)
        is_(inspect(c), c)
        assert not c.is_selectable
        assert not hasattr(c, "selectable")

    def test_no_clause_element_on_clauseelement(self):
        # re [ticket:3802], there are in the wild examples
        # of looping over __clause_element__, therefore the
        # absence of __clause_element__ as a test for "this is the clause
        # element" must be maintained

        class Foo(ClauseElement):
            pass

        assert not hasattr(Foo(), "__clause_element__")

    def test_col_now_has_a_clauseelement(self):

        x = Column("foo", Integer)

        assert hasattr(x, "__clause_element__")
