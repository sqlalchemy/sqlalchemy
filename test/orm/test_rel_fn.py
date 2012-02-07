from test.lib.testing import assert_raises, assert_raises_message, eq_, AssertsCompiledSQL, is_
from test.lib import fixtures
from sqlalchemy.orm import relationships
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, select
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY

class JoinCondTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        m = MetaData()
        cls.left = Table('lft', m,
            Column('id', Integer, primary_key=True),
        )
        cls.right = Table('rgt', m,
            Column('id', Integer, primary_key=True),
            Column('lid', Integer, ForeignKey('lft.id'))
        )
        cls.selfref = Table('selfref', m,
            Column('id', Integer, primary_key=True),
            Column('sid', Integer, ForeignKey('selfref.id'))
        )

    def _join_fixture_o2m(self, **kw):
        return relationships.JoinCondition(
                    self.left, 
                    self.right, 
                    self.left, 
                    self.right,
                    **kw
                )

    def _join_fixture_m2o(self, **kw):
        return relationships.JoinCondition(
                    self.right, 
                    self.left, 
                    self.right,
                    self.left,
                    **kw
                )

    def _join_fixture_o2m_selfref(self, **kw):
        return relationships.JoinCondition(
            self.selfref,
            self.selfref,
            self.selfref,
            self.selfref,
            **kw
        )

    def _join_fixture_m2o_selfref(self, **kw):
        return relationships.JoinCondition(
            self.selfref,
            self.selfref,
            self.selfref,
            self.selfref,
            remote_side=set([self.selfref.c.id]),
            **kw
        )

    def test_determine_join_o2m(self):
        joincond = self._join_fixture_o2m()
        self.assert_compile(
                joincond.primaryjoin,
                "lft.id = rgt.lid"
        )

    def test_determine_direction_o2m(self):
        joincond = self._join_fixture_o2m()
        is_(joincond.direction, ONETOMANY)

    def test_determine_remote_side_o2m(self):
        joincond = self._join_fixture_o2m()
        eq_(
            joincond.remote_side,
            set([self.right.c.lid])
        )

    def test_determine_join_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "selfref.id = selfref.sid"
        )

    def test_determine_direction_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        is_(joincond.direction, ONETOMANY)

    def test_determine_remote_side_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        eq_(
            joincond.remote_side,
            set([self.selfref.c.sid])
        )

    def test_determine_join_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "selfref.id = selfref.sid"
        )

    def test_determine_direction_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        is_(joincond.direction, MANYTOONE)

    def test_determine_remote_side_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        eq_(
            joincond.remote_side,
            set([self.selfref.c.id])
        )

    def test_determine_join_m2o(self):
        joincond = self._join_fixture_m2o()
        self.assert_compile(
                joincond.primaryjoin,
                "lft.id = rgt.lid"
        )

    def test_determine_direction_m2o(self):
        joincond = self._join_fixture_m2o()
        is_(joincond.direction, MANYTOONE)

    def test_determine_remote_side_m2o(self):
        joincond = self._join_fixture_m2o()
        eq_(
            joincond.remote_side,
            set([self.left.c.id])
        )

    def test_determine_local_remote_pairs_o2m(self):
        joincond = self._join_fixture_o2m()
        eq_(
            joincond.local_remote_pairs,
            [(self.left.c.id, self.right.c.lid)]
        )

    def test_join_targets_o2m_plain(self):
        joincond = self._join_fixture_o2m()
        pj, sj, sec, adapter = joincond.join_targets(
                                    joincond.parent_selectable, 
                                    joincond.child_selectable, 
                                    False)
        self.assert_compile(
            pj, "lft.id = rgt.lid"
        )

    def test_join_targets_o2m_left_aliased(self):
        joincond = self._join_fixture_o2m()
        left = select([joincond.parent_selectable]).alias('pj')
        pj, sj, sec, adapter = joincond.join_targets(
                                    left, 
                                    joincond.child_selectable, 
                                    True)
        self.assert_compile(
            pj, "pj.id = rgt.lid"
        )

    def test_join_targets_o2m_right_aliased(self):
        joincond = self._join_fixture_o2m()
        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter = joincond.join_targets(
                                    joincond.parent_selectable, 
                                    right, 
                                    True)
        self.assert_compile(
            pj, "lft.id = pj.lid"
        )

    def _test_lazy_clause_o2m(self):
        joincond = self._join_fixture_o2m()
        self.assert_compile(
            relationships.create_lazy_clause(joincond),
            ""
        )

    def _test_lazy_clause_o2m_reverse(self):
        joincond = self._join_fixture_o2m()
        self.assert_compile(
            relationships.create_lazy_clause(joincond, 
                                reverse_direction=True),
            ""
        )

