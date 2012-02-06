from test.lib.testing import assert_raises, assert_raises_message, eq_, AssertsCompiledSQL
from test.lib import fixtures
from sqlalchemy.orm import relationships
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, select

class JoinCondTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def _join_fixture_one(self):
        m = MetaData()
        left = Table('lft', m,
            Column('id', Integer, primary_key=True),
        )
        right = Table('rgt', m,
            Column('id', Integer, primary_key=True),
            Column('lid', Integer, ForeignKey('lft.id'))
        )
        return relationships.JoinCondition(
                    left, right, left, right,
                )

    def test_determine_join(self):
        joincond = self._join_fixture_one()
        self.assert_compile(
                joincond.primaryjoin,
                "lft.id = rgt.lid"
        )

    def test_join_targets_plain(self):
        joincond = self._join_fixture_one()
        pj, sj, sec, adapter = joincond.join_targets(
                                    joincond.parent_selectable, 
                                    joincond.child_selectable, 
                                    False)
        self.assert_compile(
            pj, "lft.id = rgt.lid"
        )

    def test_join_targets_left_aliased(self):
        joincond = self._join_fixture_one()
        left = select([joincond.parent_selectable]).alias('pj')
        pj, sj, sec, adapter = joincond.join_targets(
                                    left, 
                                    joincond.child_selectable, 
                                    True)
        self.assert_compile(
            pj, "pj.id = rgt.lid"
        )

    def test_join_targets_right_aliased(self):
        joincond = self._join_fixture_one()
        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter = joincond.join_targets(
                                    joincond.parent_selectable, 
                                    right, 
                                    True)
        self.assert_compile(
            pj, "lft.id = pj.lid"
        )

    def _test_lazy_clause_o2m(self):
        joincond = self._join_fixture_one()
        self.assert_compile(
            relationships.create_lazy_clause(joincond),
            ""
        )

    def _test_lazy_clause_o2m_reverse(self):
        joincond = self._join_fixture_one()
        self.assert_compile(
            relationships.create_lazy_clause(joincond, 
                                reverse_direction=True),
            ""
        )

