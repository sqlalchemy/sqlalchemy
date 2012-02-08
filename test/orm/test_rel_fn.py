from test.lib.testing import assert_raises, assert_raises_message, eq_, \
    AssertsCompiledSQL, is_
from test.lib import fixtures
from sqlalchemy.orm import relationships
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, \
    select, ForeignKeyConstraint, exc
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY

class JoinCondTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        m = MetaData()
        cls.left = Table('lft', m,
            Column('id', Integer, primary_key=True),
            Column('x', Integer),
            Column('y', Integer),
        )
        cls.right = Table('rgt', m,
            Column('id', Integer, primary_key=True),
            Column('lid', Integer, ForeignKey('lft.id')),
            Column('x', Integer),
            Column('y', Integer),
        )
        cls.selfref = Table('selfref', m,
            Column('id', Integer, primary_key=True),
            Column('sid', Integer, ForeignKey('selfref.id'))
        )
        cls.composite_selfref = Table('composite_selfref', m,
            Column('id', Integer, primary_key=True),
            Column('group_id', Integer, primary_key=True),
            Column('parent_id', Integer),
            ForeignKeyConstraint(
                ['parent_id', 'group_id'],
                ['composite_selfref.id', 'composite_selfref.group_id']
            )
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

    def _join_fixture_o2m_composite_selfref(self, **kw):
        return relationships.JoinCondition(
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            **kw
        )

    def _join_fixture_m2o_composite_selfref(self, **kw):
        return relationships.JoinCondition(
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            remote_side=set([self.composite_selfref.c.id, 
                            self.composite_selfref.c.group_id]),
            **kw
        )

    def _join_fixture_compound_expression_1(self, **kw):
        return relationships.JoinCondition(
            self.left,
            self.right,
            self.left,
            self.right,
            primaryjoin=(self.left.c.x + self.left.c.y) == \
                            relationships.remote_foreign(
                                self.right.c.x * self.right.c.y
                            ),
            **kw
        )

    def _join_fixture_compound_expression_2(self, **kw):
        return relationships.JoinCondition(
            self.left,
            self.right,
            self.left,
            self.right,
            primaryjoin=(self.left.c.x + self.left.c.y) == \
                            relationships.foreign(
                                self.right.c.x * self.right.c.y
                            ),
            **kw
        )

    def test_determine_remote_columns_compound_1(self):
        joincond = self._join_fixture_compound_expression_1(
                                support_sync=False)
        eq_(
            joincond.remote_columns,
            set([self.right.c.x, self.right.c.y])
        )

    def test_determine_local_remote_compound_1(self):
        joincond = self._join_fixture_compound_expression_1(
                                support_sync=False)
        eq_(
            joincond.local_remote_pairs,
            []
        )

    def test_err_local_remote_compound_1(self):
        assert_raises_message(
            exc.ArgumentError,
            r"Could not locate any simple equality "
            "expressions involving foreign key "
            "columns for primaryjoin join "
            r"condition 'lft.x \+ lft.y = rgt.x \* rgt.y' "
            "on relationship None.  Ensure that referencing "
            "columns are associated with a ForeignKey or "
            "ForeignKeyConstraint, or are annotated in the "
            r"join condition with the foreign\(\) annotation. "
            "To allow comparison operators other "
            "than '==', the relationship can be marked as viewonly=True.",
            self._join_fixture_compound_expression_1
        )

    def test_determine_remote_columns_compound_2(self):
        joincond = self._join_fixture_compound_expression_2(
                                support_sync=False)
        eq_(
            joincond.remote_columns,
            set([self.right.c.x, self.right.c.y])
        )

    def test_determine_local_remote_compound_2(self):
        joincond = self._join_fixture_compound_expression_2(
                                support_sync=False)
        eq_(
            joincond.local_remote_pairs,
            []
        )

    def test_determine_direction_compound_2(self):
        joincond = self._join_fixture_compound_expression_2(
                                support_sync=False)
        is_(
            joincond.direction,
            ONETOMANY
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

    def test_determine_remote_columns_o2m(self):
        joincond = self._join_fixture_o2m()
        eq_(
            joincond.remote_columns,
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

    def test_determine_remote_columns_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        eq_(
            joincond.remote_columns,
            set([self.selfref.c.sid])
        )

    def test_join_targets_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        left = select([joincond.parent_selectable]).alias('pj')
        pj, sj, sec, adapter = joincond.join_targets(
                                    left, 
                                    joincond.child_selectable, 
                                    True)
        self.assert_compile(
            pj, "pj.id = selfref.sid"
        )

        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter = joincond.join_targets(
                                    joincond.parent_selectable, 
                                    right, 
                                    True)
        self.assert_compile(
            pj, "selfref.id = pj.sid"
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

    def test_determine_remote_columns_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        eq_(
            joincond.remote_columns,
            set([self.selfref.c.id])
        )

    def test_determine_join_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "composite_selfref.group_id = composite_selfref.group_id "
                "AND composite_selfref.id = composite_selfref.parent_id"
        )

    def test_determine_direction_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        is_(joincond.direction, ONETOMANY)

    def test_determine_remote_columns_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        eq_(
            joincond.remote_columns,
            set([self.composite_selfref.c.parent_id, 
                self.composite_selfref.c.group_id])
        )

    def test_determine_join_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "composite_selfref.group_id = composite_selfref.group_id "
                "AND composite_selfref.id = composite_selfref.parent_id"
        )

    def test_determine_direction_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        is_(joincond.direction, MANYTOONE)

    def test_determine_remote_columns_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        eq_(
            joincond.remote_columns,
            set([self.composite_selfref.c.id, 
                self.composite_selfref.c.group_id])
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

    def test_determine_remote_columns_m2o(self):
        joincond = self._join_fixture_m2o()
        eq_(
            joincond.remote_columns,
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

