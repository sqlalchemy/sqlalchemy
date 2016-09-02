from sqlalchemy.testing import fixtures, is_true, is_false
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy import and_, or_
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.sql import operators


class CompareClausesTest(fixtures.TestBase):
    def setup(self):
        m = MetaData()
        self.a = Table(
            'a', m,
            Column('x', Integer),
            Column('y', Integer)
        )

        self.b = Table(
            'b', m,
            Column('y', Integer),
            Column('z', Integer)
        )

    def test_compare_clauselist_associative(self):

        l1 = and_(
            self.a.c.x == self.b.c.y,
            self.a.c.y == self.b.c.z
        )

        l2 = and_(
            self.a.c.y == self.b.c.z,
            self.a.c.x == self.b.c.y,
        )

        l3 = and_(
            self.a.c.x == self.b.c.z,
            self.a.c.y == self.b.c.y
        )

        is_true(l1.compare(l1))
        is_true(l1.compare(l2))
        is_false(l1.compare(l3))

    def test_compare_clauselist_not_associative(self):

        l1 = ClauseList(
            self.a.c.x, self.a.c.y, self.b.c.y, operator=operators.sub)

        l2 = ClauseList(
            self.b.c.y, self.a.c.x, self.a.c.y, operator=operators.sub)

        is_true(l1.compare(l1))
        is_false(l1.compare(l2))

    def test_compare_clauselist_assoc_different_operator(self):

        l1 = and_(
            self.a.c.x == self.b.c.y,
            self.a.c.y == self.b.c.z
        )

        l2 = or_(
            self.a.c.y == self.b.c.z,
            self.a.c.x == self.b.c.y,
        )

        is_false(l1.compare(l2))

    def test_compare_clauselist_not_assoc_different_operator(self):

        l1 = ClauseList(
            self.a.c.x, self.a.c.y, self.b.c.y, operator=operators.sub)

        l2 = ClauseList(
            self.a.c.x, self.a.c.y, self.b.c.y, operator=operators.div)

        is_false(l1.compare(l2))

