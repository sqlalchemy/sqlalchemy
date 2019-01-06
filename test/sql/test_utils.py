from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.sql import operators
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true


class CompareClausesTest(fixtures.TestBase):
    def setup(self):
        m = MetaData()
        self.a = Table("a", m, Column("x", Integer), Column("y", Integer))

        self.b = Table("b", m, Column("y", Integer), Column("z", Integer))

    def test_compare_clauselist_associative(self):

        l1 = and_(self.a.c.x == self.b.c.y, self.a.c.y == self.b.c.z)

        l2 = and_(self.a.c.y == self.b.c.z, self.a.c.x == self.b.c.y)

        l3 = and_(self.a.c.x == self.b.c.z, self.a.c.y == self.b.c.y)

        is_true(l1.compare(l1))
        is_true(l1.compare(l2))
        is_false(l1.compare(l3))

    def test_compare_clauselist_not_associative(self):

        l1 = ClauseList(
            self.a.c.x, self.a.c.y, self.b.c.y, operator=operators.sub
        )

        l2 = ClauseList(
            self.b.c.y, self.a.c.x, self.a.c.y, operator=operators.sub
        )

        is_true(l1.compare(l1))
        is_false(l1.compare(l2))

    def test_compare_clauselist_assoc_different_operator(self):

        l1 = and_(self.a.c.x == self.b.c.y, self.a.c.y == self.b.c.z)

        l2 = or_(self.a.c.y == self.b.c.z, self.a.c.x == self.b.c.y)

        is_false(l1.compare(l2))

    def test_compare_clauselist_not_assoc_different_operator(self):

        l1 = ClauseList(
            self.a.c.x, self.a.c.y, self.b.c.y, operator=operators.sub
        )

        l2 = ClauseList(
            self.a.c.x, self.a.c.y, self.b.c.y, operator=operators.div
        )

        is_false(l1.compare(l2))

    def test_compare_binds(self):
        b1 = bindparam("foo", type_=Integer())
        b2 = bindparam("foo", type_=Integer())
        b3 = bindparam("bar", type_=Integer())
        b4 = bindparam("foo", type_=String())

        def c1():
            return 5

        def c2():
            return 6

        b5 = bindparam("foo", type_=Integer(), callable_=c1)
        b6 = bindparam("foo", type_=Integer(), callable_=c2)
        b7 = bindparam("foo", type_=Integer(), callable_=c1)

        b8 = bindparam("foo", type_=Integer, value=5)
        b9 = bindparam("foo", type_=Integer, value=6)

        is_false(b1.compare(b5))
        is_true(b5.compare(b7))
        is_false(b5.compare(b6))
        is_true(b1.compare(b2))

        # currently not comparing "key", as we often have to compare
        # anonymous names.  however we should really check for that
        is_true(b1.compare(b3))

        is_false(b1.compare(b4))
        is_false(b1.compare(b8))
        is_false(b8.compare(b9))
        is_true(b8.compare(b8))


class MiscTest(fixtures.TestBase):
    def test_column_element_no_visit(self):
        class MyElement(ColumnElement):
            pass

        eq_(sql_util.find_tables(MyElement(), check_columns=True), [])
