from sqlalchemy.testing import assert_raises_message, eq_, \
    AssertsCompiledSQL, is_
from sqlalchemy.testing import fixtures
from sqlalchemy.orm import relationships, foreign, remote
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, \
    select, ForeignKeyConstraint, exc, func, and_, String, Boolean
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY
from sqlalchemy.testing import mock

class _JoinFixtures(object):
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
        cls.right_multi_fk = Table('rgt_multi_fk', m,
            Column('id', Integer, primary_key=True),
            Column('lid1', Integer, ForeignKey('lft.id')),
            Column('lid2', Integer, ForeignKey('lft.id')),
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
        cls.m2mleft = Table('m2mlft', m,
            Column('id', Integer, primary_key=True),
        )
        cls.m2mright = Table('m2mrgt', m,
            Column('id', Integer, primary_key=True),
        )
        cls.m2msecondary = Table('m2msecondary', m,
            Column('lid', Integer, ForeignKey('m2mlft.id'), primary_key=True),
            Column('rid', Integer, ForeignKey('m2mrgt.id'), primary_key=True),
        )
        cls.m2msecondary_no_fks = Table('m2msecondary_no_fks', m,
            Column('lid', Integer, primary_key=True),
            Column('rid', Integer, primary_key=True),
        )
        cls.m2msecondary_ambig_fks = Table('m2msecondary_ambig_fks', m,
            Column('lid1', Integer, ForeignKey('m2mlft.id'), primary_key=True),
            Column('rid1', Integer, ForeignKey('m2mrgt.id'), primary_key=True),
            Column('lid2', Integer, ForeignKey('m2mlft.id'), primary_key=True),
            Column('rid2', Integer, ForeignKey('m2mrgt.id'), primary_key=True),
        )
        cls.base_w_sub_rel = Table('base_w_sub_rel', m,
            Column('id', Integer, primary_key=True),
            Column('sub_id', Integer, ForeignKey('rel_sub.id'))
        )
        cls.rel_sub = Table('rel_sub', m,
            Column('id', Integer, ForeignKey('base_w_sub_rel.id'),
                                primary_key=True)
        )
        cls.base = Table('base', m,
            Column('id', Integer, primary_key=True),
            Column('flag', Boolean)
        )
        cls.sub = Table('sub', m,
            Column('id', Integer, ForeignKey('base.id'),
                                primary_key=True),
        )
        cls.sub_w_base_rel = Table('sub_w_base_rel', m,
            Column('id', Integer, ForeignKey('base.id'),
                                primary_key=True),
            Column('base_id', Integer, ForeignKey('base.id'))
        )
        cls.sub_w_sub_rel = Table('sub_w_sub_rel', m,
            Column('id', Integer, ForeignKey('base.id'),
                                primary_key=True),
            Column('sub_id', Integer, ForeignKey('sub.id'))
        )
        cls.right_w_base_rel = Table('right_w_base_rel', m,
            Column('id', Integer, primary_key=True),
            Column('base_id', Integer, ForeignKey('base.id'))
        )

        cls.three_tab_a = Table('three_tab_a', m,
            Column('id', Integer, primary_key=True),
        )
        cls.three_tab_b = Table('three_tab_b', m,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('three_tab_a.id'))
        )
        cls.three_tab_c = Table('three_tab_c', m,
            Column('id', Integer, primary_key=True),
            Column('aid', Integer, ForeignKey('three_tab_a.id')),
            Column('bid', Integer, ForeignKey('three_tab_b.id'))
        )

        cls.composite_target = Table('composite_target', m,
            Column('uid', Integer, primary_key=True),
            Column('oid', Integer, primary_key=True),
        )

        cls.composite_multi_ref = Table('composite_multi_ref', m,
            Column('uid1', Integer),
            Column('uid2', Integer),
            Column('oid', Integer),
            ForeignKeyConstraint(("uid1", "oid"),
                        ("composite_target.uid", "composite_target.oid")),
            ForeignKeyConstraint(("uid2", "oid"),
                        ("composite_target.uid", "composite_target.oid")),
            )

        cls.purely_single_col = Table('purely_single_col', m,
            Column('path', String)
            )

    def _join_fixture_overlapping_three_tables(self, **kw):
        def _can_sync(*cols):
            for c in cols:
                if self.three_tab_c.c.contains_column(c):
                    return False
            else:
                return True
        return relationships.JoinCondition(
            self.three_tab_a,
            self.three_tab_b,
            self.three_tab_a,
            self.three_tab_b,
            support_sync=False,
            can_be_synced_fn=_can_sync,
            primaryjoin=and_(
                self.three_tab_a.c.id == self.three_tab_b.c.aid,
                self.three_tab_c.c.bid == self.three_tab_b.c.id,
                self.three_tab_c.c.aid == self.three_tab_a.c.id
            )
        )

    def _join_fixture_m2m(self, **kw):
        return relationships.JoinCondition(
                    self.m2mleft,
                    self.m2mright,
                    self.m2mleft,
                    self.m2mright,
                    secondary=self.m2msecondary,
                    **kw
                )

    def _join_fixture_m2m_backref(self, **kw):
        """return JoinCondition in the same way RelationshipProperty
        calls it for a backref on an m2m.

        """
        j1 = self._join_fixture_m2m()
        return j1, relationships.JoinCondition(
                    self.m2mright,
                    self.m2mleft,
                    self.m2mright,
                    self.m2mleft,
                    secondary=self.m2msecondary,
                    primaryjoin=j1.secondaryjoin_minus_local,
                    secondaryjoin=j1.primaryjoin_minus_local
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

    def _join_fixture_o2m_composite_selfref_func(self, **kw):
        return relationships.JoinCondition(
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            primaryjoin=and_(
                self.composite_selfref.c.group_id ==
                    func.foo(self.composite_selfref.c.group_id),
                self.composite_selfref.c.parent_id ==
                    self.composite_selfref.c.id
            ),
            **kw
        )

    def _join_fixture_o2m_composite_selfref_func_remote_side(self, **kw):
        return relationships.JoinCondition(
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            primaryjoin=and_(
                self.composite_selfref.c.group_id ==
                    func.foo(self.composite_selfref.c.group_id),
                self.composite_selfref.c.parent_id ==
                    self.composite_selfref.c.id
            ),
            remote_side=set([self.composite_selfref.c.parent_id]),
            **kw
        )

    def _join_fixture_o2m_composite_selfref_func_annotated(self, **kw):
        return relationships.JoinCondition(
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            self.composite_selfref,
            primaryjoin=and_(
                remote(self.composite_selfref.c.group_id) ==
                    func.foo(self.composite_selfref.c.group_id),
                remote(self.composite_selfref.c.parent_id) ==
                    self.composite_selfref.c.id
            ),
            **kw
        )

    def _join_fixture_compound_expression_1(self, **kw):
        return relationships.JoinCondition(
            self.left,
            self.right,
            self.left,
            self.right,
            primaryjoin=(self.left.c.x + self.left.c.y) == \
                            relationships.remote(relationships.foreign(
                                self.right.c.x * self.right.c.y
                            )),
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

    def _join_fixture_compound_expression_1_non_annotated(self, **kw):
        return relationships.JoinCondition(
            self.left,
            self.right,
            self.left,
            self.right,
            primaryjoin=(self.left.c.x + self.left.c.y) == \
                            (
                                self.right.c.x * self.right.c.y
                            ),
            **kw
        )

    def _join_fixture_base_to_joined_sub(self, **kw):
        # see test/orm/inheritance/test_abc_inheritance:TestaTobM2O
        # and others there
        right = self.base_w_sub_rel.join(self.rel_sub,
            self.base_w_sub_rel.c.id == self.rel_sub.c.id
        )
        return relationships.JoinCondition(
            self.base_w_sub_rel,
            right,
            self.base_w_sub_rel,
            self.rel_sub,
            primaryjoin=self.base_w_sub_rel.c.sub_id == \
                        self.rel_sub.c.id,
            **kw
        )

    def _join_fixture_o2m_joined_sub_to_base(self, **kw):
        left = self.base.join(self.sub_w_base_rel,
                        self.base.c.id == self.sub_w_base_rel.c.id)
        return relationships.JoinCondition(
            left,
            self.base,
            self.sub_w_base_rel,
            self.base,
            primaryjoin=self.sub_w_base_rel.c.base_id == self.base.c.id
        )

    def _join_fixture_m2o_joined_sub_to_sub_on_base(self, **kw):
        # this is a late add - a variant of the test case
        # in #2491 where we join on the base cols instead.  only
        # m2o has a problem at the time of this test.
        left = self.base.join(self.sub, self.base.c.id == self.sub.c.id)
        right = self.base.join(self.sub_w_base_rel,
                        self.base.c.id == self.sub_w_base_rel.c.id)
        return relationships.JoinCondition(
            left,
            right,
            self.sub,
            self.sub_w_base_rel,
            primaryjoin=self.sub_w_base_rel.c.base_id == self.base.c.id,
        )

    def _join_fixture_o2m_joined_sub_to_sub(self, **kw):
        left = self.base.join(self.sub, self.base.c.id == self.sub.c.id)
        right = self.base.join(self.sub_w_sub_rel,
                        self.base.c.id == self.sub_w_sub_rel.c.id)
        return relationships.JoinCondition(
            left,
            right,
            self.sub,
            self.sub_w_sub_rel,
            primaryjoin=self.sub.c.id == self.sub_w_sub_rel.c.sub_id
        )

    def _join_fixture_m2o_sub_to_joined_sub(self, **kw):
        # see test.orm.test_mapper:MapperTest.test_add_column_prop_deannotate,
        right = self.base.join(self.right_w_base_rel,
                        self.base.c.id == self.right_w_base_rel.c.id)
        return relationships.JoinCondition(
            self.right_w_base_rel,
            right,
            self.right_w_base_rel,
            self.right_w_base_rel,
        )

    def _join_fixture_m2o_sub_to_joined_sub_func(self, **kw):
        # see test.orm.test_mapper:MapperTest.test_add_column_prop_deannotate,
        right = self.base.join(self.right_w_base_rel,
                        self.base.c.id == self.right_w_base_rel.c.id)
        return relationships.JoinCondition(
            self.right_w_base_rel,
            right,
            self.right_w_base_rel,
            self.right_w_base_rel,
            primaryjoin=self.right_w_base_rel.c.base_id == \
                func.foo(self.base.c.id)
        )

    def _join_fixture_o2o_joined_sub_to_base(self, **kw):
        left = self.base.join(self.sub,
                        self.base.c.id == self.sub.c.id)

        # see test_relationships->AmbiguousJoinInterpretedAsSelfRef
        return relationships.JoinCondition(
            left,
            self.sub,
            left,
            self.sub,
        )

    def _join_fixture_o2m_to_annotated_func(self, **kw):
        return relationships.JoinCondition(
                    self.left,
                    self.right,
                    self.left,
                    self.right,
                    primaryjoin=self.left.c.id ==
                        foreign(func.foo(self.right.c.lid)),
                    **kw
                )

    def _join_fixture_o2m_to_oldstyle_func(self, **kw):
        return relationships.JoinCondition(
                    self.left,
                    self.right,
                    self.left,
                    self.right,
                    primaryjoin=self.left.c.id ==
                        func.foo(self.right.c.lid),
                    consider_as_foreign_keys=[self.right.c.lid],
                    **kw
                )

    def _join_fixture_overlapping_composite_fks(self, **kw):
        return relationships.JoinCondition(
                    self.composite_target,
                    self.composite_multi_ref,
                    self.composite_target,
                    self.composite_multi_ref,
                    consider_as_foreign_keys=[self.composite_multi_ref.c.uid2,
                                    self.composite_multi_ref.c.oid],
                    **kw
                )


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

    def _join_fixture_o2m_o_side_none(self, **kw):
        return relationships.JoinCondition(
                    self.left,
                    self.right,
                    self.left,
                    self.right,
                    primaryjoin=and_(self.left.c.id == self.right.c.lid,
                                        self.left.c.x == 5),
                    **kw
                    )

    def _join_fixture_purely_single_o2m(self, **kw):
        return relationships.JoinCondition(
                    self.purely_single_col,
                    self.purely_single_col,
                    self.purely_single_col,
                    self.purely_single_col,
                    support_sync=False,
                    primaryjoin=
                        self.purely_single_col.c.path.like(
                            remote(
                                foreign(
                                    self.purely_single_col.c.path.concat('%')
                                )
                            )
                        )
                )

    def _join_fixture_purely_single_m2o(self, **kw):
        return relationships.JoinCondition(
                    self.purely_single_col,
                    self.purely_single_col,
                    self.purely_single_col,
                    self.purely_single_col,
                    support_sync=False,
                    primaryjoin=
                        remote(self.purely_single_col.c.path).like(
                            foreign(self.purely_single_col.c.path.concat('%'))
                        )
                )

    def _join_fixture_remote_local_multiple_ref(self, **kw):
        fn = lambda a, b: ((a == b) | (b == a))
        return relationships.JoinCondition(
            self.selfref, self.selfref,
            self.selfref, self.selfref,
            support_sync=False,
            primaryjoin=fn(
                # we're putting a do-nothing annotation on
                # "a" so that the left/right is preserved;
                # annotation vs. non seems to affect __eq__ behavior
                self.selfref.c.sid._annotate({"foo": "bar"}),
                foreign(remote(self.selfref.c.sid)))
        )

    def _join_fixture_inh_selfref_w_entity(self, **kw):
        fake_logger = mock.Mock(info=lambda *arg, **kw: None)
        prop = mock.Mock(
            parent=mock.Mock(),
            mapper=mock.Mock(),
            logger=fake_logger
        )
        local_selectable = self.base.join(self.sub)
        remote_selectable = self.base.join(self.sub_w_sub_rel)

        sub_w_sub_rel__sub_id = self.sub_w_sub_rel.c.sub_id._annotate(
            {'parentmapper': prop.mapper})
        sub__id = self.sub.c.id._annotate({'parentmapper': prop.parent})
        sub_w_sub_rel__flag = self.base.c.flag._annotate(
            {"parentmapper": prop.mapper})
        return relationships.JoinCondition(
            local_selectable, remote_selectable,
            local_selectable, remote_selectable,
            primaryjoin=and_(
                sub_w_sub_rel__sub_id == sub__id,
                sub_w_sub_rel__flag == True
            ),
            prop=prop
        )

    def _assert_non_simple_warning(self, fn):
        assert_raises_message(
            exc.SAWarning,
            "Non-simple column elements in "
            "primary join condition for property "
            r"None - consider using remote\(\) "
            "annotations to mark the remote side.",
            fn
        )

    def _assert_raises_no_relevant_fks(self, fn, expr, relname,
                primary, *arg, **kw):
        assert_raises_message(
            exc.ArgumentError,
            r"Could not locate any relevant foreign key columns "
            r"for %s join condition '%s' on relationship %s.  "
            r"Ensure that referencing columns are associated with "
            r"a ForeignKey or ForeignKeyConstraint, or are annotated "
            r"in the join condition with the foreign\(\) annotation."
            % (
                primary, expr, relname
            ),
            fn, *arg, **kw
        )

    def _assert_raises_no_equality(self, fn, expr, relname,
                    primary, *arg, **kw):
        assert_raises_message(
            exc.ArgumentError,
            "Could not locate any simple equality expressions "
            "involving locally mapped foreign key columns for %s join "
            "condition '%s' on relationship %s.  "
            "Ensure that referencing columns are associated with a "
            "ForeignKey or ForeignKeyConstraint, or are annotated in "
            r"the join condition with the foreign\(\) annotation. "
            "To allow comparison operators other than '==', "
            "the relationship can be marked as viewonly=True." % (
                primary, expr, relname
            ),
            fn, *arg, **kw
        )

    def _assert_raises_ambig_join(self, fn, relname, secondary_arg,
                    *arg, **kw):
        if secondary_arg is not None:
            assert_raises_message(
                exc.AmbiguousForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are multiple foreign key paths linking the "
                "tables via secondary table '%s'.  "
                "Specify the 'foreign_keys' argument, providing a list "
                "of those columns which should be counted as "
                "containing a foreign key reference from the "
                "secondary table to each of the parent and child tables."
                % (relname, secondary_arg),
                fn, *arg, **kw)
        else:
            assert_raises_message(
                exc.AmbiguousForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables.  "
                % (relname,),
                fn, *arg, **kw)

    def _assert_raises_no_join(self, fn, relname, secondary_arg,
                    *arg, **kw):
        if secondary_arg is not None:
            assert_raises_message(
                exc.NoForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables "
                "via secondary table '%s'.  "
                "Ensure that referencing columns are associated "
                "with a ForeignKey "
                "or ForeignKeyConstraint, or specify 'primaryjoin' and "
                "'secondaryjoin' expressions"
                % (relname, secondary_arg),
                fn, *arg, **kw)
        else:
            assert_raises_message(
                exc.NoForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables.  "
                "Ensure that referencing columns are associated "
                "with a ForeignKey "
                "or ForeignKeyConstraint, or specify a 'primaryjoin' "
                "expression."
                % (relname,),
                fn, *arg, **kw)


class ColumnCollectionsTest(_JoinFixtures, fixtures.TestBase,
                                                        AssertsCompiledSQL):
    def test_determine_local_remote_pairs_o2o_joined_sub_to_base(self):
        joincond = self._join_fixture_o2o_joined_sub_to_base()
        eq_(
            joincond.local_remote_pairs,
            [(self.base.c.id, self.sub.c.id)]
        )

    def test_determine_synchronize_pairs_o2m_to_annotated_func(self):
        joincond = self._join_fixture_o2m_to_annotated_func()
        eq_(
            joincond.synchronize_pairs,
            [(self.left.c.id, self.right.c.lid)]
        )

    def test_determine_synchronize_pairs_o2m_to_oldstyle_func(self):
        joincond = self._join_fixture_o2m_to_oldstyle_func()
        eq_(
            joincond.synchronize_pairs,
            [(self.left.c.id, self.right.c.lid)]
        )

    def test_determinelocal_remote_m2o_joined_sub_to_sub_on_base(self):
        joincond = self._join_fixture_m2o_joined_sub_to_sub_on_base()
        eq_(
            joincond.local_remote_pairs,
            [(self.base.c.id, self.sub_w_base_rel.c.base_id)]
        )

    def test_determine_local_remote_base_to_joined_sub(self):
        joincond = self._join_fixture_base_to_joined_sub()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.base_w_sub_rel.c.sub_id, self.rel_sub.c.id)
            ]
        )

    def test_determine_local_remote_o2m_joined_sub_to_base(self):
        joincond = self._join_fixture_o2m_joined_sub_to_base()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.sub_w_base_rel.c.base_id, self.base.c.id)
            ]
        )

    def test_determine_local_remote_m2o_sub_to_joined_sub(self):
        joincond = self._join_fixture_m2o_sub_to_joined_sub()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.right_w_base_rel.c.base_id, self.base.c.id)
            ]
        )

    def test_determine_remote_columns_o2m_joined_sub_to_sub(self):
        joincond = self._join_fixture_o2m_joined_sub_to_sub()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.sub.c.id, self.sub_w_sub_rel.c.sub_id)
            ]
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
            [
                (self.left.c.x, self.right.c.x),
                (self.left.c.x, self.right.c.y),
                (self.left.c.y, self.right.c.x),
                (self.left.c.y, self.right.c.y)
            ]
        )

    def test_determine_local_remote_compound_2(self):
        joincond = self._join_fixture_compound_expression_2(
                                support_sync=False)
        eq_(
            joincond.local_remote_pairs,
            [
                (self.left.c.x, self.right.c.x),
                (self.left.c.x, self.right.c.y),
                (self.left.c.y, self.right.c.x),
                (self.left.c.y, self.right.c.y)
            ]
        )

    def test_determine_local_remote_compound_3(self):
        joincond = self._join_fixture_compound_expression_1()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.left.c.x, self.right.c.x),
                (self.left.c.x, self.right.c.y),
                (self.left.c.y, self.right.c.x),
                (self.left.c.y, self.right.c.y),
            ]
        )

    def test_err_local_remote_compound_1(self):
        self._assert_raises_no_relevant_fks(
            self._join_fixture_compound_expression_1_non_annotated,
            r'lft.x \+ lft.y = rgt.x \* rgt.y',
            "None", "primary"
        )

    def test_determine_remote_columns_compound_2(self):
        joincond = self._join_fixture_compound_expression_2(
                                support_sync=False)
        eq_(
            joincond.remote_columns,
            set([self.right.c.x, self.right.c.y])
        )


    def test_determine_remote_columns_o2m(self):
        joincond = self._join_fixture_o2m()
        eq_(
            joincond.remote_columns,
            set([self.right.c.lid])
        )

    def test_determine_remote_columns_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        eq_(
            joincond.remote_columns,
            set([self.selfref.c.sid])
        )

    def test_determine_local_remote_pairs_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.composite_selfref.c.group_id,
                                    self.composite_selfref.c.group_id),
                (self.composite_selfref.c.id,
                                    self.composite_selfref.c.parent_id),
            ]
        )

    def test_determine_local_remote_pairs_o2m_composite_selfref_func_warning(self):
        self._assert_non_simple_warning(
            self._join_fixture_o2m_composite_selfref_func
        )

    def test_determine_local_remote_pairs_o2m_composite_selfref_func_rs(self):
        # no warning
        self._join_fixture_o2m_composite_selfref_func_remote_side()

    def test_determine_local_remote_pairs_o2m_overlap_func_warning(self):
        self._assert_non_simple_warning(
            self._join_fixture_m2o_sub_to_joined_sub_func
        )

    def test_determine_local_remote_pairs_o2m_composite_selfref_func_annotated(self):
        joincond = self._join_fixture_o2m_composite_selfref_func_annotated()
        eq_(
            joincond.local_remote_pairs,
            [
                (self.composite_selfref.c.group_id,
                                    self.composite_selfref.c.group_id),
                (self.composite_selfref.c.id,
                                    self.composite_selfref.c.parent_id),
            ]
        )

    def test_determine_remote_columns_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        eq_(
            joincond.remote_columns,
            set([self.composite_selfref.c.id,
                self.composite_selfref.c.group_id])
        )

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

    def test_determine_synchronize_pairs_m2m(self):
        joincond = self._join_fixture_m2m()
        eq_(
            joincond.synchronize_pairs,
            [(self.m2mleft.c.id, self.m2msecondary.c.lid)]
        )
        eq_(
            joincond.secondary_synchronize_pairs,
            [(self.m2mright.c.id, self.m2msecondary.c.rid)]
        )

    def test_determine_local_remote_pairs_o2m_backref(self):
        joincond = self._join_fixture_o2m()
        joincond2 = self._join_fixture_m2o(
            primaryjoin=joincond.primaryjoin_reverse_remote,
        )
        eq_(
            joincond2.local_remote_pairs,
            [(self.right.c.lid, self.left.c.id)]
        )

    def test_determine_local_remote_pairs_m2m(self):
        joincond = self._join_fixture_m2m()
        eq_(
            joincond.local_remote_pairs,
            [(self.m2mleft.c.id, self.m2msecondary.c.lid),
            (self.m2mright.c.id, self.m2msecondary.c.rid)]
        )

    def test_determine_local_remote_pairs_m2m_backref(self):
        j1, j2 = self._join_fixture_m2m_backref()
        eq_(
            j1.local_remote_pairs,
            [(self.m2mleft.c.id, self.m2msecondary.c.lid),
            (self.m2mright.c.id, self.m2msecondary.c.rid)]
        )
        eq_(
            j2.local_remote_pairs,
            [
                (self.m2mright.c.id, self.m2msecondary.c.rid),
                (self.m2mleft.c.id, self.m2msecondary.c.lid),
            ]
        )

    def test_determine_local_columns_m2m_backref(self):
        j1, j2 = self._join_fixture_m2m_backref()
        eq_(
            j1.local_columns,
            set([self.m2mleft.c.id])
        )
        eq_(
            j2.local_columns,
            set([self.m2mright.c.id])
        )

    def test_determine_remote_columns_m2m_backref(self):
        j1, j2 = self._join_fixture_m2m_backref()
        eq_(
            j1.remote_columns,
            set([self.m2msecondary.c.lid, self.m2msecondary.c.rid])
        )
        eq_(
            j2.remote_columns,
            set([self.m2msecondary.c.lid, self.m2msecondary.c.rid])
        )


    def test_determine_remote_columns_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        eq_(
            joincond.remote_columns,
            set([self.selfref.c.id])
        )

    def test_determine_local_remote_cols_three_tab_viewonly(self):
        joincond = self._join_fixture_overlapping_three_tables()
        eq_(
            joincond.local_remote_pairs,
            [(self.three_tab_a.c.id, self.three_tab_b.c.aid)]
        )
        eq_(
            joincond.remote_columns,
            set([self.three_tab_b.c.id, self.three_tab_b.c.aid])
        )

    def test_determine_local_remote_overlapping_composite_fks(self):
        joincond = self._join_fixture_overlapping_composite_fks()

        eq_(
            joincond.local_remote_pairs,
            [
                (self.composite_target.c.uid, self.composite_multi_ref.c.uid2,),
                (self.composite_target.c.oid, self.composite_multi_ref.c.oid,)
            ]
        )

    def test_determine_local_remote_pairs_purely_single_col_o2m(self):
        joincond = self._join_fixture_purely_single_o2m()
        eq_(
            joincond.local_remote_pairs,
            [(self.purely_single_col.c.path, self.purely_single_col.c.path)]
        )

    def test_determine_local_remote_pairs_inh_selfref_w_entities(self):
        joincond = self._join_fixture_inh_selfref_w_entity()
        eq_(
            joincond.local_remote_pairs,
            [(self.sub.c.id, self.sub_w_sub_rel.c.sub_id)]
        )
        eq_(
            joincond.remote_columns,
            set([self.base.c.flag, self.sub_w_sub_rel.c.sub_id])
        )

class DirectionTest(_JoinFixtures, fixtures.TestBase, AssertsCompiledSQL):
    def test_determine_direction_compound_2(self):
        joincond = self._join_fixture_compound_expression_2(
                                support_sync=False)
        is_(
            joincond.direction,
            ONETOMANY
        )

    def test_determine_direction_o2m(self):
        joincond = self._join_fixture_o2m()
        is_(joincond.direction, ONETOMANY)

    def test_determine_direction_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        is_(joincond.direction, ONETOMANY)

    def test_determine_direction_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        is_(joincond.direction, MANYTOONE)

    def test_determine_direction_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        is_(joincond.direction, ONETOMANY)

    def test_determine_direction_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        is_(joincond.direction, MANYTOONE)

    def test_determine_direction_m2o(self):
        joincond = self._join_fixture_m2o()
        is_(joincond.direction, MANYTOONE)

    def test_determine_direction_purely_single_o2m(self):
        joincond = self._join_fixture_purely_single_o2m()
        is_(joincond.direction, ONETOMANY)

    def test_determine_direction_purely_single_m2o(self):
        joincond = self._join_fixture_purely_single_m2o()
        is_(joincond.direction, MANYTOONE)

class DetermineJoinTest(_JoinFixtures, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_determine_join_o2m(self):
        joincond = self._join_fixture_o2m()
        self.assert_compile(
                joincond.primaryjoin,
                "lft.id = rgt.lid"
        )

    def test_determine_join_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "selfref.id = selfref.sid"
        )

    def test_determine_join_m2o_selfref(self):
        joincond = self._join_fixture_m2o_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "selfref.id = selfref.sid"
        )

    def test_determine_join_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "composite_selfref.group_id = composite_selfref.group_id "
                "AND composite_selfref.id = composite_selfref.parent_id"
        )

    def test_determine_join_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        self.assert_compile(
                joincond.primaryjoin,
                "composite_selfref.group_id = composite_selfref.group_id "
                "AND composite_selfref.id = composite_selfref.parent_id"
        )

    def test_determine_join_m2o(self):
        joincond = self._join_fixture_m2o()
        self.assert_compile(
                joincond.primaryjoin,
                "lft.id = rgt.lid"
        )

    def test_determine_join_ambiguous_fks_o2m(self):
        assert_raises_message(
            exc.AmbiguousForeignKeysError,
            "Could not determine join condition between "
            "parent/child tables on relationship None - "
            "there are multiple foreign key paths linking "
            "the tables.  Specify the 'foreign_keys' argument, "
            "providing a list of those columns which "
            "should be counted as containing a foreign "
            "key reference to the parent table.",
            relationships.JoinCondition,
                    self.left,
                    self.right_multi_fk,
                    self.left,
                    self.right_multi_fk,
        )

    def test_determine_join_no_fks_o2m(self):
        self._assert_raises_no_join(
            relationships.JoinCondition,
            "None", None,
                    self.left,
                    self.selfref,
                    self.left,
                    self.selfref,
        )


    def test_determine_join_ambiguous_fks_m2m(self):

        self._assert_raises_ambig_join(
            relationships.JoinCondition,
            "None", self.m2msecondary_ambig_fks,
            self.m2mleft,
            self.m2mright,
            self.m2mleft,
            self.m2mright,
            secondary=self.m2msecondary_ambig_fks
        )

    def test_determine_join_no_fks_m2m(self):
        self._assert_raises_no_join(
            relationships.JoinCondition,
            "None", self.m2msecondary_no_fks,
                    self.m2mleft,
                    self.m2mright,
                    self.m2mleft,
                    self.m2mright,
                    secondary=self.m2msecondary_no_fks
        )

    def _join_fixture_fks_ambig_m2m(self):
        return relationships.JoinCondition(
                    self.m2mleft,
                    self.m2mright,
                    self.m2mleft,
                    self.m2mright,
                    secondary=self.m2msecondary_ambig_fks,
                    consider_as_foreign_keys=[
                        self.m2msecondary_ambig_fks.c.lid1,
                        self.m2msecondary_ambig_fks.c.rid1]
        )

    def test_determine_join_w_fks_ambig_m2m(self):
        joincond = self._join_fixture_fks_ambig_m2m()
        self.assert_compile(
                joincond.primaryjoin,
                "m2mlft.id = m2msecondary_ambig_fks.lid1"
        )
        self.assert_compile(
                joincond.secondaryjoin,
                "m2mrgt.id = m2msecondary_ambig_fks.rid1"
        )

class AdaptedJoinTest(_JoinFixtures, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_join_targets_o2m_selfref(self):
        joincond = self._join_fixture_o2m_selfref()
        left = select([joincond.parent_selectable]).alias('pj')
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    left,
                                    joincond.child_selectable,
                                    True)
        self.assert_compile(
            pj, "pj.id = selfref.sid"
        )

        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    joincond.parent_selectable,
                                    right,
                                    True)
        self.assert_compile(
            pj, "selfref.id = pj.sid"
        )


    def test_join_targets_o2m_plain(self):
        joincond = self._join_fixture_o2m()
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    joincond.parent_selectable,
                                    joincond.child_selectable,
                                    False)
        self.assert_compile(
            pj, "lft.id = rgt.lid"
        )

    def test_join_targets_o2m_left_aliased(self):
        joincond = self._join_fixture_o2m()
        left = select([joincond.parent_selectable]).alias('pj')
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    left,
                                    joincond.child_selectable,
                                    True)
        self.assert_compile(
            pj, "pj.id = rgt.lid"
        )

    def test_join_targets_o2m_right_aliased(self):
        joincond = self._join_fixture_o2m()
        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    joincond.parent_selectable,
                                    right,
                                    True)
        self.assert_compile(
            pj, "lft.id = pj.lid"
        )

    def test_join_targets_o2m_composite_selfref(self):
        joincond = self._join_fixture_o2m_composite_selfref()
        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    joincond.parent_selectable,
                                    right,
                                    True)
        self.assert_compile(
            pj,
            "pj.group_id = composite_selfref.group_id "
            "AND composite_selfref.id = pj.parent_id"
        )

    def test_join_targets_m2o_composite_selfref(self):
        joincond = self._join_fixture_m2o_composite_selfref()
        right = select([joincond.child_selectable]).alias('pj')
        pj, sj, sec, adapter, ds = joincond.join_targets(
                                    joincond.parent_selectable,
                                    right,
                                    True)
        self.assert_compile(
            pj,
            "pj.group_id = composite_selfref.group_id "
            "AND pj.id = composite_selfref.parent_id"
        )

class LazyClauseTest(_JoinFixtures, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_lazy_clause_o2m(self):
        joincond = self._join_fixture_o2m()
        lazywhere, bind_to_col, equated_columns = joincond.create_lazy_clause()
        self.assert_compile(
            lazywhere,
            ":param_1 = rgt.lid"
        )

    def test_lazy_clause_o2m_reverse(self):
        joincond = self._join_fixture_o2m()
        lazywhere, bind_to_col, equated_columns =\
            joincond.create_lazy_clause(reverse_direction=True)
        self.assert_compile(
            lazywhere,
            "lft.id = :param_1"
        )

    def test_lazy_clause_o2m_o_side_none(self):
        # test for #2948.  When the join is "o.id == m.oid AND o.something == something",
        # we don't want 'o' brought into the lazy load for 'm'
        joincond = self._join_fixture_o2m_o_side_none()
        lazywhere, bind_to_col, equated_columns = joincond.create_lazy_clause()
        self.assert_compile(
            lazywhere,
            ":param_1 = rgt.lid AND :param_2 = :x_1",
            checkparams={'param_1': None, 'param_2': None, 'x_1': 5}
        )

    def test_lazy_clause_o2m_o_side_none_reverse(self):
        # continued test for #2948.
        joincond = self._join_fixture_o2m_o_side_none()
        lazywhere, bind_to_col, equated_columns = joincond.create_lazy_clause(reverse_direction=True)
        self.assert_compile(
            lazywhere,
            "lft.id = :param_1 AND lft.x = :x_1",
            checkparams= {'param_1': None, 'x_1': 5}
        )

    def test_lazy_clause_remote_local_multiple_ref(self):
        joincond = self._join_fixture_remote_local_multiple_ref()
        lazywhere, bind_to_col, equated_columns = joincond.create_lazy_clause()

        self.assert_compile(
            lazywhere,
            ":param_1 = selfref.sid OR selfref.sid = :param_1",
            checkparams={'param_1': None}
        )
