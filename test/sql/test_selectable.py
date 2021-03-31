"""Test various algorithmic properties of selectables."""

from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import outerjoin
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import type_coerce
from sqlalchemy import TypeDecorator
from sqlalchemy import union
from sqlalchemy import util
from sqlalchemy.sql import Alias
from sqlalchemy.sql import annotation
from sqlalchemy.sql import base
from sqlalchemy.sql import column
from sqlalchemy.sql import elements
from sqlalchemy.sql import LABEL_STYLE_DISAMBIGUATE_ONLY
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import operators
from sqlalchemy.sql import table
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import ne_
from sqlalchemy.testing.assertions import expect_raises_message


metadata = MetaData()
table1 = Table(
    "table1",
    metadata,
    Column("col1", Integer, primary_key=True),
    Column("col2", String(20)),
    Column("col3", Integer),
    Column("colx", Integer),
)

table2 = Table(
    "table2",
    metadata,
    Column("col1", Integer, primary_key=True),
    Column("col2", Integer, ForeignKey("table1.col1")),
    Column("col3", String(20)),
    Column("coly", Integer),
)

keyed = Table(
    "keyed",
    metadata,
    Column("x", Integer, key="colx"),
    Column("y", Integer, key="coly"),
    Column("z", Integer),
)


class SelectableTest(
    fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL
):
    __dialect__ = "default"

    def test_indirect_correspondence_on_labels(self):
        # this test depends upon 'distance' to
        # get the right result

        # same column three times

        s = select(
            table1.c.col1.label("c2"),
            table1.c.col1,
            table1.c.col1.label("c1"),
        ).subquery()

        # this tests the same thing as
        # test_direct_correspondence_on_labels below -
        # that the presence of label() affects the 'distance'
        assert s.corresponding_column(table1.c.col1) is s.c.col1

        assert s.corresponding_column(s.c.col1) is s.c.col1
        assert s.corresponding_column(s.c.c1) is s.c.c1

    def test_labeled_select_twice(self):
        scalar_select = select(table1.c.col1).label("foo")

        s1 = select(scalar_select)
        s2 = select(scalar_select, scalar_select)

        eq_(
            s1.selected_columns.foo.proxy_set,
            set(
                [s1.selected_columns.foo, scalar_select, scalar_select.element]
            ),
        )
        eq_(
            s2.selected_columns.foo.proxy_set,
            set(
                [s2.selected_columns.foo, scalar_select, scalar_select.element]
            ),
        )

        assert (
            s1.corresponding_column(scalar_select) is s1.selected_columns.foo
        )
        assert (
            s2.corresponding_column(scalar_select) is s2.selected_columns.foo
        )

    def test_labeled_subquery_twice(self):
        scalar_select = select(table1.c.col1).label("foo")

        s1 = select(scalar_select).subquery()
        s2 = select(scalar_select, scalar_select).subquery()

        eq_(
            s1.c.foo.proxy_set,
            set([s1.c.foo, scalar_select, scalar_select.element]),
        )
        eq_(
            s2.c.foo.proxy_set,
            set([s2.c.foo, scalar_select, scalar_select.element]),
        )

        assert s1.corresponding_column(scalar_select) is s1.c.foo
        assert s2.corresponding_column(scalar_select) is s2.c.foo

    def test_labels_name_w_separate_key(self):
        label = select(table1.c.col1).label("foo")
        label.key = "bar"

        s1 = select(label)
        assert s1.corresponding_column(label) is s1.selected_columns.bar

        # renders as foo
        self.assert_compile(
            s1, "SELECT (SELECT table1.col1 FROM table1) AS foo"
        )

    @testing.combinations(("cte",), ("subquery",), argnames="type_")
    @testing.combinations(
        ("onelevel",), ("twolevel",), ("middle",), argnames="path"
    )
    @testing.combinations((True,), (False,), argnames="require_embedded")
    def test_subquery_cte_correspondence(self, type_, require_embedded, path):
        stmt = select(table1)

        if type_ == "cte":
            cte1 = stmt.cte()
        elif type_ == "subquery":
            cte1 = stmt.subquery()

        if path == "onelevel":
            is_(
                cte1.corresponding_column(
                    table1.c.col1, require_embedded=require_embedded
                ),
                cte1.c.col1,
            )
        elif path == "twolevel":
            cte2 = cte1.alias()

            is_(
                cte2.corresponding_column(
                    table1.c.col1, require_embedded=require_embedded
                ),
                cte2.c.col1,
            )

        elif path == "middle":
            cte2 = cte1.alias()

            is_(
                cte2.corresponding_column(
                    cte1.c.col1, require_embedded=require_embedded
                ),
                cte2.c.col1,
            )

    def test_labels_anon_w_separate_key(self):
        label = select(table1.c.col1).label(None)
        label.key = "bar"

        s1 = select(label)

        # .bar is there
        assert s1.corresponding_column(label) is s1.selected_columns.bar

        # renders as anon_1
        self.assert_compile(
            s1, "SELECT (SELECT table1.col1 FROM table1) AS anon_1"
        )

    def test_labels_anon_w_separate_key_subquery(self):
        label = select(table1.c.col1).label(None)
        label.key = label._key_label = "bar"

        s1 = select(label)

        subq = s1.subquery()

        s2 = select(subq).where(subq.c.bar > 5)
        self.assert_compile(
            s2,
            "SELECT anon_2.anon_1 FROM (SELECT (SELECT table1.col1 "
            "FROM table1) AS anon_1) AS anon_2 "
            "WHERE anon_2.anon_1 > :param_1",
            checkparams={"param_1": 5},
        )

    def test_labels_anon_generate_binds_subquery(self):
        label = select(table1.c.col1).label(None)
        label.key = label._key_label = "bar"

        s1 = select(label)

        subq = s1.subquery()

        s2 = select(subq).where(subq.c[0] > 5)
        self.assert_compile(
            s2,
            "SELECT anon_2.anon_1 FROM (SELECT (SELECT table1.col1 "
            "FROM table1) AS anon_1) AS anon_2 "
            "WHERE anon_2.anon_1 > :param_1",
            checkparams={"param_1": 5},
        )

    @testing.combinations((True,), (False,))
    def test_broken_select_same_named_explicit_cols(self, use_anon):
        # this is issue #6090.  the query is "wrong" and we dont know how
        # to render this right now.
        stmt = select(
            table1.c.col1,
            table1.c.col2,
            literal_column("col2").label(None if use_anon else "col2"),
        ).select_from(table1)

        if use_anon:
            self.assert_compile(
                select(stmt.subquery()),
                "SELECT anon_1.col1, anon_1.col2, anon_1.col2_1 FROM "
                "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
                "col2 AS col2_1 FROM table1) AS anon_1",
            )
        else:
            # the keys here are not critical as they are not what was
            # requested anyway, maybe should raise here also.
            eq_(stmt.selected_columns.keys(), ["col1", "col2", "col2_1"])
            with expect_raises_message(
                exc.InvalidRequestError,
                "Label name col2 is being renamed to an anonymous "
                "label due to "
                "disambiguation which is not supported right now.  Please use "
                "unique names for explicit labels.",
            ):
                select(stmt.subquery()).compile()

    def test_select_label_grouped_still_corresponds(self):
        label = select(table1.c.col1).label("foo")
        label2 = label.self_group()

        s1 = select(label)
        s2 = select(label2)
        assert s1.corresponding_column(label) is s1.selected_columns.foo
        assert s2.corresponding_column(label) is s2.selected_columns.foo

    def test_subquery_label_grouped_still_corresponds(self):
        label = select(table1.c.col1).label("foo")
        label2 = label.self_group()

        s1 = select(label).subquery()
        s2 = select(label2).subquery()
        assert s1.corresponding_column(label) is s1.c.foo
        assert s2.corresponding_column(label) is s2.c.foo

    def test_direct_correspondence_on_labels(self):
        # this test depends on labels being part
        # of the proxy set to get the right result

        l1, l2 = table1.c.col1.label("foo"), table1.c.col1.label("bar")
        sel = select(l1, l2)

        sel2 = sel.alias()
        assert sel2.corresponding_column(l1) is sel2.c.foo
        assert sel2.corresponding_column(l2) is sel2.c.bar

        sel2 = select(table1.c.col1.label("foo"), table1.c.col2.label("bar"))

        sel3 = sel.union(sel2).alias()
        assert sel3.corresponding_column(l1) is sel3.c.foo
        assert sel3.corresponding_column(l2) is sel3.c.bar

    def test_keyed_gen(self):
        s = select(keyed)
        eq_(s.selected_columns.colx.key, "colx")

        eq_(s.selected_columns.colx.name, "x")

        assert (
            s.selected_columns.corresponding_column(keyed.c.colx)
            is s.selected_columns.colx
        )
        assert (
            s.selected_columns.corresponding_column(keyed.c.coly)
            is s.selected_columns.coly
        )
        assert (
            s.selected_columns.corresponding_column(keyed.c.z)
            is s.selected_columns.z
        )

        sel2 = s.alias()
        assert sel2.corresponding_column(keyed.c.colx) is sel2.c.colx
        assert sel2.corresponding_column(keyed.c.coly) is sel2.c.coly
        assert sel2.corresponding_column(keyed.c.z) is sel2.c.z

    def test_keyed_label_gen(self):
        s = select(keyed).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        assert (
            s.selected_columns.corresponding_column(keyed.c.colx)
            is s.selected_columns.keyed_colx
        )
        assert (
            s.selected_columns.corresponding_column(keyed.c.coly)
            is s.selected_columns.keyed_coly
        )
        assert (
            s.selected_columns.corresponding_column(keyed.c.z)
            is s.selected_columns.keyed_z
        )

        sel2 = s.alias()
        assert sel2.corresponding_column(keyed.c.colx) is sel2.c.keyed_colx
        assert sel2.corresponding_column(keyed.c.coly) is sel2.c.keyed_coly
        assert sel2.corresponding_column(keyed.c.z) is sel2.c.keyed_z

    def test_keyed_c_collection_upper(self):
        c = Column("foo", Integer, key="bar")
        t = Table("t", MetaData(), c)
        is_(t.c.bar, c)

    def test_keyed_c_collection_lower(self):
        c = column("foo")
        c.key = "bar"
        t = table("t", c)
        is_(t.c.bar, c)

    def test_clone_c_proxy_key_upper(self):
        c = Column("foo", Integer, key="bar")
        t = Table("t", MetaData(), c)
        s = select(t)._clone()
        assert c in s.selected_columns.bar.proxy_set

        s = select(t).subquery()._clone()
        assert c in s.c.bar.proxy_set

    def test_clone_c_proxy_key_lower(self):
        c = column("foo")
        c.key = "bar"
        t = table("t", c)
        s = select(t)._clone()
        assert c in s.selected_columns.bar.proxy_set

        s = select(t).subquery()._clone()
        assert c in s.c.bar.proxy_set

    def test_no_error_on_unsupported_expr_key(self):
        from sqlalchemy.sql.expression import BinaryExpression

        def myop(x, y):
            pass

        t = table("t", column("x"), column("y"))

        expr = BinaryExpression(t.c.x, t.c.y, myop)

        s = select(t, expr)

        # anon_label, e.g. a truncated_label, is used here because
        # the expr has no name, no key, and myop() can't create a
        # string, so this is the last resort
        eq_(s.selected_columns.keys(), ["x", "y", expr.anon_label])

        s = select(t, expr).subquery()
        eq_(s.c.keys(), ["x", "y", expr.anon_label])

    def test_cloned_intersection(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("x"))

        s1 = t1.select()
        s2 = t2.select()
        s3 = t1.select()

        s1c1 = s1._clone()
        s1c2 = s1._clone()
        s2c1 = s2._clone()
        s3c1 = s3._clone()

        eq_(base._cloned_intersection([s1c1, s3c1], [s2c1, s1c2]), set([s1c1]))

    def test_cloned_difference(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("x"))

        s1 = t1.select()
        s2 = t2.select()
        s3 = t1.select()

        s1c1 = s1._clone()
        s1c2 = s1._clone()
        s2c1 = s2._clone()
        s3c1 = s3._clone()

        eq_(
            base._cloned_difference([s1c1, s2c1, s3c1], [s2c1, s1c2]),
            set([s3c1]),
        )

    def test_distance_on_aliases(self):
        a1 = table1.alias("a1")
        for s in (
            select(a1, table1)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery(),
            select(table1, a1)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery(),
        ):
            assert s.corresponding_column(table1.c.col1) is s.c.table1_col1
            assert s.corresponding_column(a1.c.col1) is s.c.a1_col1

    def test_join_against_self(self):
        jj = select(table1.c.col1.label("bar_col1")).subquery()
        jjj = join(table1, jj, table1.c.col1 == jj.c.bar_col1)

        # test column directly against itself

        # joins necessarily have to prefix column names with the name
        # of the selectable, else the same-named columns will overwrite
        # one another.  In this case, we unfortunately have this
        # unfriendly "anonymous" name, whereas before when select() could
        # be a FROM the "bar_col1" label would be directly in the join()
        # object.  However this was a useless join() object because PG and
        # MySQL don't accept unnamed subqueries in joins in any case.
        name = "%s_bar_col1" % (jj.name,)

        assert jjj.corresponding_column(jjj.c.table1_col1) is jjj.c.table1_col1
        assert jjj.corresponding_column(jj.c.bar_col1) is jjj.c[name]

        # test alias of the join

        j2 = (
            jjj.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery("foo")
        )
        assert j2.corresponding_column(table1.c.col1) is j2.c.table1_col1

    def test_clone_append_column(self):
        sel = select(literal_column("1").label("a"))
        eq_(list(sel.selected_columns.keys()), ["a"])
        cloned = visitors.ReplacingCloningVisitor().traverse(sel)
        cloned.add_columns.non_generative(
            cloned, literal_column("2").label("b")
        )
        cloned.add_columns.non_generative(cloned, func.foo())
        eq_(list(cloned.selected_columns.keys()), ["a", "b", "foo()"])

    def test_clone_col_list_changes_then_proxy(self):
        t = table("t", column("q"), column("p"))
        stmt = select(t.c.q).subquery()

        def add_column(stmt):
            stmt.add_columns.non_generative(stmt, t.c.p)

        stmt2 = visitors.cloned_traverse(stmt, {}, {"select": add_column})
        eq_(list(stmt.c.keys()), ["q"])
        eq_(list(stmt2.c.keys()), ["q", "p"])

    def test_clone_col_list_changes_then_schema_proxy(self):
        t = Table("t", MetaData(), Column("q", Integer), Column("p", Integer))
        stmt = select(t.c.q).subquery()

        def add_column(stmt):
            stmt.add_columns.non_generative(stmt, t.c.p)

        stmt2 = visitors.cloned_traverse(stmt, {}, {"select": add_column})
        eq_(list(stmt.c.keys()), ["q"])
        eq_(list(stmt2.c.keys()), ["q", "p"])

    def test_append_column_after_visitor_replace(self):
        # test for a supported idiom that matches the deprecated / removed
        # replace_selectable method
        basesel = select(literal_column("1").label("a"))
        tojoin = select(
            literal_column("1").label("a"), literal_column("2").label("b")
        )
        basefrom = basesel.alias("basefrom")
        joinfrom = tojoin.alias("joinfrom")
        sel = select(basefrom.c.a)

        replace_from = basefrom.join(joinfrom, basefrom.c.a == joinfrom.c.a)

        def replace(elem):
            if elem is basefrom:
                return replace_from

        replaced = visitors.replacement_traverse(sel, {}, replace)
        self.assert_compile(
            replaced,
            "SELECT basefrom.a FROM (SELECT 1 AS a) AS basefrom "
            "JOIN (SELECT 1 AS a, 2 AS b) AS joinfrom "
            "ON basefrom.a = joinfrom.a",
        )
        replaced.add_columns.non_generative(replaced, joinfrom.c.b)
        self.assert_compile(
            replaced,
            "SELECT basefrom.a, joinfrom.b FROM (SELECT 1 AS a) AS basefrom "
            "JOIN (SELECT 1 AS a, 2 AS b) AS joinfrom "
            "ON basefrom.a = joinfrom.a",
        )

    def test_against_cloned_non_table(self):
        # test that corresponding column digs across
        # clone boundaries with anonymous labeled elements
        col = func.count().label("foo")
        sel = select(col).subquery()

        sel2 = visitors.ReplacingCloningVisitor().traverse(sel)
        assert sel2.corresponding_column(col) is sel2.c.foo

        sel3 = visitors.ReplacingCloningVisitor().traverse(sel2)
        assert sel3.corresponding_column(col) is sel3.c.foo

    def test_with_only_generative(self):
        s1 = table1.select().scalar_subquery()
        self.assert_compile(
            s1.with_only_columns(s1),
            "SELECT (SELECT table1.col1, table1.col2, "
            "table1.col3, table1.colx FROM table1) AS anon_1",
        )

    def test_type_coerce_preserve_subq(self):
        class MyType(TypeDecorator):
            impl = Integer

        stmt = select(type_coerce(column("x"), MyType).label("foo"))
        subq = stmt.subquery()
        stmt2 = subq.select()
        subq2 = stmt2.subquery()
        assert isinstance(stmt._raw_columns[0].type, MyType)
        assert isinstance(subq.c.foo.type, MyType)
        assert isinstance(stmt2.selected_columns.foo.type, MyType)
        assert isinstance(subq2.c.foo.type, MyType)

    def test_type_coerce_selfgroup(self):
        no_group = column("a") / type_coerce(column("x"), Integer)
        group = column("b") / type_coerce(column("y") * column("w"), Integer)

        self.assert_compile(no_group, "a / x")
        self.assert_compile(group, "b / (y * w)")

    def test_subquery_on_table(self):
        sel = (
            select(table1, table2)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        assert sel.corresponding_column(table1.c.col1) is sel.c.table1_col1
        assert (
            sel.corresponding_column(table1.c.col1, require_embedded=True)
            is sel.c.table1_col1
        )
        assert table1.corresponding_column(sel.c.table1_col1) is table1.c.col1
        assert (
            table1.corresponding_column(
                sel.c.table1_col1, require_embedded=True
            )
            is None
        )

    def test_join_against_join(self):

        j = outerjoin(table1, table2, table1.c.col1 == table2.c.col2)
        jj = (
            select(table1.c.col1.label("bar_col1"))
            .select_from(j)
            .alias(name="foo")
        )
        jjj = join(table1, jj, table1.c.col1 == jj.c.bar_col1)
        assert jjj.corresponding_column(jjj.c.table1_col1) is jjj.c.table1_col1
        j2 = jjj._anonymous_fromclause("foo")
        assert j2.corresponding_column(jjj.c.table1_col1) is j2.c.table1_col1
        assert jjj.corresponding_column(jj.c.bar_col1) is jj.c.bar_col1

    def test_table_alias(self):
        a = table1.alias("a")

        j = join(a, table2)

        criterion = a.c.col1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_join_doesnt_derive_from_onclause(self):
        # test issue #4621.   the hide froms from the join comes from
        # Join._from_obj(), which should not include tables in the ON clause
        t1 = table("t1", column("a"))
        t2 = table("t2", column("b"))
        t3 = table("t3", column("c"))
        t4 = table("t4", column("d"))

        j = t1.join(t2, onclause=t1.c.a == t3.c.c)

        j2 = t4.join(j, onclause=t4.c.d == t2.c.b)

        stmt = select(t1, t2, t3, t4).select_from(j2)
        self.assert_compile(
            stmt,
            "SELECT t1.a, t2.b, t3.c, t4.d FROM t3, "
            "t4 JOIN (t1 JOIN t2 ON t1.a = t3.c) ON t4.d = t2.b",
        )

        stmt = select(t1).select_from(t3).select_from(j2)
        self.assert_compile(
            stmt,
            "SELECT t1.a FROM t3, t4 JOIN (t1 JOIN t2 ON t1.a = t3.c) "
            "ON t4.d = t2.b",
        )

    @testing.fails("not supported with rework, need a new approach")
    def test_alias_handles_column_context(self):
        # not quite a use case yet but this is expected to become
        # prominent w/ PostgreSQL's tuple functions

        stmt = select(table1.c.col1, table1.c.col2)
        a = stmt.alias("a")

        # TODO: this case is crazy, sending SELECT or FROMCLAUSE has to
        # be figured out - is it a scalar row query?  what kinds of
        # statements go into functions in PG. seems likely select statement,
        # but not alias, subquery or other FROM object
        self.assert_compile(
            select(func.foo(a)),
            "SELECT foo(SELECT table1.col1, table1.col2 FROM table1) "
            "AS foo_1 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2 FROM table1) "
            "AS a",
        )

    def test_union_correspondence(self):

        # tests that we can correspond a column in a Select statement
        # with a certain Table, against a column in a Union where one of
        # its underlying Selects matches to that same Table

        u = select(
            table1.c.col1,
            table1.c.col2,
            table1.c.col3,
            table1.c.colx,
            null().label("coly"),
        ).union(
            select(
                table2.c.col1,
                table2.c.col2,
                table2.c.col3,
                null().label("colx"),
                table2.c.coly,
            )
        )
        s1 = table1.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        s2 = table2.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        assert (
            u.corresponding_column(s1.selected_columns.table1_col2)
            is u.selected_columns.col2
        )

        # right now, the "selected_columns" of a union are those of the
        # first selectable.  so without using a subquery that represents
        # all the SELECTs in the union, we can't do corresponding column
        # like this.  perhaps compoundselect shouldn't even implement
        # .corresponding_column directly
        assert (
            u.corresponding_column(s2.selected_columns.table2_col2) is None
        )  # really? u.selected_columns.col2

        usub = u.subquery()
        assert (
            usub.corresponding_column(s1.selected_columns.table1_col2)
            is usub.c.col2
        )
        assert (
            usub.corresponding_column(s2.selected_columns.table2_col2)
            is usub.c.col2
        )

        s1sub = s1.subquery()
        s2sub = s2.subquery()
        assert usub.corresponding_column(s1sub.c.table1_col2) is usub.c.col2
        assert usub.corresponding_column(s2sub.c.table2_col2) is usub.c.col2

    def test_union_precedence(self):
        # conflicting column correspondence should be resolved based on
        # the order of the select()s in the union

        s1 = select(table1.c.col1, table1.c.col2)
        s2 = select(table1.c.col2, table1.c.col1)
        s3 = select(table1.c.col3, table1.c.colx)
        s4 = select(table1.c.colx, table1.c.col3)

        u1 = union(s1, s2).subquery()
        assert u1.corresponding_column(table1.c.col1) is u1.c.col1
        assert u1.corresponding_column(table1.c.col2) is u1.c.col2

        u1 = union(s1, s2, s3, s4).subquery()
        assert u1.corresponding_column(table1.c.col1) is u1.c.col1
        assert u1.corresponding_column(table1.c.col2) is u1.c.col2
        assert u1.corresponding_column(table1.c.colx) is u1.c.col2
        assert u1.corresponding_column(table1.c.col3) is u1.c.col1

    def test_proxy_set_pollution(self):
        s1 = select(table1.c.col1, table1.c.col2)
        s2 = select(table1.c.col2, table1.c.col1)

        for c in s1.selected_columns:
            c.proxy_set
        for c in s2.selected_columns:
            c.proxy_set

        u1 = union(s1, s2).subquery()
        assert u1.corresponding_column(table1.c.col2) is u1.c.col2

    def test_singular_union(self):
        u = union(
            select(table1.c.col1, table1.c.col2, table1.c.col3),
            select(table1.c.col1, table1.c.col2, table1.c.col3),
        )
        u = union(select(table1.c.col1, table1.c.col2, table1.c.col3))
        assert u.selected_columns.col1 is not None
        assert u.selected_columns.col2 is not None
        assert u.selected_columns.col3 is not None

    def test_alias_union(self):

        # same as testunion, except its an alias of the union

        u = (
            select(
                table1.c.col1,
                table1.c.col2,
                table1.c.col3,
                table1.c.colx,
                null().label("coly"),
            )
            .union(
                select(
                    table2.c.col1,
                    table2.c.col2,
                    table2.c.col3,
                    null().label("colx"),
                    table2.c.coly,
                )
            )
            .alias("analias")
        )
        s1 = (
            table1.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        s2 = (
            table2.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_coly) is u.c.coly
        assert s2.corresponding_column(u.c.coly) is s2.c.table2_coly

    def test_union_of_alias(self):
        s1 = select(table1.c.col1, table1.c.col2)
        s2 = select(table1.c.col1, table1.c.col2).alias()

        # previously this worked
        assert_raises_message(
            exc.ArgumentError,
            "SELECT construct for inclusion in a UNION or "
            "other set construct expected",
            union,
            s1,
            s2,
        )

    def test_union_of_text(self):
        s1 = select(table1.c.col1, table1.c.col2)
        s2 = text("select col1, col2 from foo").columns(
            column("col1"), column("col2")
        )

        u1 = union(s1, s2).subquery()
        assert u1.corresponding_column(s1.selected_columns.col1) is u1.c.col1
        assert u1.corresponding_column(s2.selected_columns.col1) is u1.c.col1

        u2 = union(s2, s1).subquery()
        assert u2.corresponding_column(s1.selected_columns.col1) is u2.c.col1
        assert u2.corresponding_column(s2.selected_columns.col1) is u2.c.col1

    def test_union_alias_misc(self):
        s1 = select(table1.c.col1, table1.c.col2)
        s2 = select(table1.c.col2, table1.c.col1)

        u1 = union(s1, s2).subquery()
        assert u1.corresponding_column(table1.c.col2) is u1.c.col2

        metadata = MetaData()
        table1_new = Table(
            "table1",
            metadata,
            Column("col1", Integer, primary_key=True),
            Column("col2", String(20)),
            Column("col3", Integer),
            Column("colx", Integer),
        )
        # table1_new = table1

        s1 = select(table1_new.c.col1, table1_new.c.col2)
        s2 = select(table1_new.c.col2, table1_new.c.col1)
        u1 = union(s1, s2).subquery()

        # TODO: failing due to proxy_set not correct
        assert u1.corresponding_column(table1_new.c.col2) is u1.c.col2

    def test_union_alias_dupe_keys(self):
        s1 = select(table1.c.col1, table1.c.col2, table2.c.col1)
        s2 = select(table2.c.col1, table2.c.col2, table2.c.col3)
        u1 = union(s1, s2).subquery()

        assert (
            u1.corresponding_column(s1.selected_columns._all_columns[0])
            is u1.c._all_columns[0]
        )

        # col1 is taken by the first "col1" in the list
        assert u1.c.col1 is u1.c._all_columns[0]

        # table2.c.col1 is in two positions in this union, so...currently
        # it is the replaced one at position 2.
        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[2]

        # this is table2.c.col1, which in the first selectable is in position 2
        assert u1.corresponding_column(s2.selected_columns.col1) is u1.c[2]

        # same
        assert u1.corresponding_column(s2.subquery().c.col1) is u1.c[2]

        # col2 is working OK
        assert u1.corresponding_column(s1.selected_columns.col2) is u1.c.col2
        assert (
            u1.corresponding_column(s1.selected_columns.col2)
            is u1.c._all_columns[1]
        )
        assert u1.corresponding_column(s2.selected_columns.col2) is u1.c.col2
        assert (
            u1.corresponding_column(s2.selected_columns.col2)
            is u1.c._all_columns[1]
        )
        assert u1.corresponding_column(s2.subquery().c.col2) is u1.c.col2

        # col3 is also "correct"
        assert u1.corresponding_column(s2.selected_columns.col3) is u1.c[2]

        assert u1.corresponding_column(table1.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(table1.c.col2) is u1.c._all_columns[1]
        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[2]
        assert u1.corresponding_column(table2.c.col2) is u1.c._all_columns[1]
        assert u1.corresponding_column(table2.c.col3) is u1.c._all_columns[2]

    def test_union_alias_dupe_keys_disambiguates_in_subq_compile_one(self):
        s1 = select(table1.c.col1, table1.c.col2, table2.c.col1).limit(1)
        s2 = select(table2.c.col1, table2.c.col2, table2.c.col3).limit(1)
        u1 = union(s1, s2).subquery()

        eq_(u1.c.keys(), ["col1", "col2", "col1_1"])

        stmt = select(u1)

        eq_(stmt.selected_columns.keys(), ["col1", "col2", "col1_1"])

        # the union() sets a new labeling form in the first SELECT
        self.assert_compile(
            stmt,
            "SELECT anon_1.col1, anon_1.col2, anon_1.col1_1 FROM "
            "((SELECT table1.col1, table1.col2, table2.col1 AS col1_1 "
            "FROM table1, table2 LIMIT :param_1) UNION "
            "(SELECT table2.col1, table2.col2, table2.col3 FROM table2 "
            "LIMIT :param_2)) AS anon_1",
        )

    def test_union_alias_dupe_keys_disambiguates_in_subq_compile_two(self):
        a = table("a", column("id"))
        b = table("b", column("id"), column("aid"))
        d = table("d", column("id"), column("aid"))

        u1 = union(
            a.join(b, a.c.id == b.c.aid)
            .select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            a.join(d, a.c.id == d.c.aid)
            .select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        ).alias()

        eq_(u1.c.keys(), ["a_id", "b_id", "b_aid"])

        stmt = select(u1)

        eq_(stmt.selected_columns.keys(), ["a_id", "b_id", "b_aid"])

        # the union() detects that the first SELECT already has a labeling
        # style and uses that
        self.assert_compile(
            stmt,
            "SELECT anon_1.a_id, anon_1.b_id, anon_1.b_aid FROM "
            "(SELECT a.id AS a_id, b.id AS b_id, b.aid AS b_aid "
            "FROM a JOIN b ON a.id = b.aid "
            "UNION SELECT a.id AS a_id, d.id AS d_id, d.aid AS d_aid "
            "FROM a JOIN d ON a.id = d.aid) AS anon_1",
        )

    def test_union_alias_dupe_keys_grouped(self):
        s1 = select(table1.c.col1, table1.c.col2, table2.c.col1).limit(1)
        s2 = select(table2.c.col1, table2.c.col2, table2.c.col3).limit(1)
        u1 = union(s1, s2).subquery()

        assert (
            u1.corresponding_column(s1.selected_columns._all_columns[0])
            is u1.c._all_columns[0]
        )

        # col1 is taken by the first "col1" in the list
        assert u1.c.col1 is u1.c._all_columns[0]

        # table2.c.col1 is in two positions in this union, so...currently
        # it is the replaced one at position 2.
        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[2]

        # this is table2.c.col1, which in the first selectable is in position 2
        assert u1.corresponding_column(s2.selected_columns.col1) is u1.c[2]

        # same
        assert u1.corresponding_column(s2.subquery().c.col1) is u1.c[2]

        # col2 is working OK
        assert u1.corresponding_column(s1.selected_columns.col2) is u1.c.col2
        assert (
            u1.corresponding_column(s1.selected_columns.col2)
            is u1.c._all_columns[1]
        )
        assert u1.corresponding_column(s2.selected_columns.col2) is u1.c.col2
        assert (
            u1.corresponding_column(s2.selected_columns.col2)
            is u1.c._all_columns[1]
        )
        assert u1.corresponding_column(s2.subquery().c.col2) is u1.c.col2

        # col3 is also "correct"
        assert u1.corresponding_column(s2.selected_columns.col3) is u1.c[2]

        assert u1.corresponding_column(table1.c.col1) is u1.c._all_columns[0]
        assert u1.corresponding_column(table1.c.col2) is u1.c._all_columns[1]
        assert u1.corresponding_column(table2.c.col1) is u1.c._all_columns[2]
        assert u1.corresponding_column(table2.c.col2) is u1.c._all_columns[1]
        assert u1.corresponding_column(table2.c.col3) is u1.c._all_columns[2]

    def test_select_union(self):

        # like testaliasunion, but off a Select off the union.

        u = (
            select(
                table1.c.col1,
                table1.c.col2,
                table1.c.col3,
                table1.c.colx,
                null().label("coly"),
            )
            .union(
                select(
                    table2.c.col1,
                    table2.c.col2,
                    table2.c.col3,
                    null().label("colx"),
                    table2.c.coly,
                )
            )
            .alias("analias")
        )
        s = select(u).subquery()
        s1 = (
            table1.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        s2 = (
            table2.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        assert s.corresponding_column(s1.c.table1_col2) is s.c.col2
        assert s.corresponding_column(s2.c.table2_col2) is s.c.col2

    def test_union_against_join(self):

        # same as testunion, except its an alias of the union

        u = (
            select(
                table1.c.col1,
                table1.c.col2,
                table1.c.col3,
                table1.c.colx,
                null().label("coly"),
            )
            .union(
                select(
                    table2.c.col1,
                    table2.c.col2,
                    table2.c.col3,
                    null().label("colx"),
                    table2.c.coly,
                )
            )
            .alias("analias")
        )
        j1 = table1.join(table2)
        assert u.corresponding_column(j1.c.table1_colx) is u.c.colx
        assert j1.corresponding_column(u.c.colx) is j1.c.table1_colx

    def test_join(self):
        a = join(table1, table2)
        print(str(a.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)))
        b = table2.alias("b")
        j = join(a, b)
        print(str(j))
        criterion = a.c.table1_col1 == b.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_select_subquery_join(self):
        a = table1.select().alias("a")
        j = join(a, table2)

        criterion = a.c.col1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_subquery_labels_join(self):
        a = (
            table1.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        j = join(a, table2)

        criterion = a.c.table1_col1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_scalar_cloned_comparator(self):
        sel = select(table1.c.col1).scalar_subquery()
        sel == table1.c.col1

        sel2 = visitors.ReplacingCloningVisitor().traverse(sel)

        expr2 = sel2 == table1.c.col1
        is_(expr2.left, sel2)

    def test_column_labels(self):
        a = select(
            table1.c.col1.label("acol1"),
            table1.c.col2.label("acol2"),
            table1.c.col3.label("acol3"),
        ).subquery()
        j = join(a, table2)
        criterion = a.c.acol1 == table2.c.col2
        self.assert_(criterion.compare(j.onclause))

    def test_labeled_select_corresponding(self):
        l1 = select(func.max(table1.c.col1)).label("foo")

        s = select(l1)
        eq_(s.corresponding_column(l1), s.selected_columns.foo)

        s = select(table1.c.col1, l1)
        eq_(s.corresponding_column(l1), s.selected_columns.foo)

    def test_labeled_subquery_corresponding(self):
        l1 = select(func.max(table1.c.col1)).label("foo")
        s = select(l1).subquery()

        eq_(s.corresponding_column(l1), s.c.foo)

        s = select(table1.c.col1, l1).subquery()
        eq_(s.corresponding_column(l1), s.c.foo)

    def test_select_alias_labels(self):
        a = (
            table2.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias("a")
        )
        j = join(a, table1)

        criterion = table1.c.col1 == a.c.table2_col2
        self.assert_(criterion.compare(j.onclause))

    def test_table_joined_to_select_of_table(self):
        metadata = MetaData()
        a = Table("a", metadata, Column("id", Integer, primary_key=True))

        j2 = select(a.c.id.label("aid")).alias("bar")

        j3 = a.join(j2, j2.c.aid == a.c.id)

        j4 = select(j3).alias("foo")
        assert j4.corresponding_column(j2.c.aid) is j4.c.aid
        assert j4.corresponding_column(a.c.id) is j4.c.id

    def test_two_metadata_join_raises(self):
        m = MetaData()
        m2 = MetaData()

        t1 = Table("t1", m, Column("id", Integer), Column("id2", Integer))
        t2 = Table("t2", m, Column("id", Integer, ForeignKey("t1.id")))
        t3 = Table("t3", m2, Column("id", Integer, ForeignKey("t1.id2")))

        s = (
            select(t2, t3)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        assert_raises(exc.NoReferencedTableError, s.join, t1)

    def test_multi_label_chain_naming_col(self):
        # See [ticket:2167] for this one.
        l1 = table1.c.col1.label("a")
        l2 = select(l1).label("b")
        s = select(l2).subquery()
        assert s.c.b is not None
        self.assert_compile(
            s.select(),
            "SELECT anon_1.b FROM "
            "(SELECT (SELECT table1.col1 AS a FROM table1) AS b) AS anon_1",
        )

        s2 = select(s.element.label("c")).subquery()
        self.assert_compile(
            s2.select(),
            "SELECT anon_1.c FROM (SELECT (SELECT ("
            "SELECT table1.col1 AS a FROM table1) AS b) AS c) AS anon_1",
        )

    def test_self_referential_select_raises(self):
        t = table("t", column("x"))

        # this issue is much less likely as subquery() applies a labeling
        # style to the select, eliminating the self-referential call unless
        # the select already had labeling applied

        s = select(t).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        with testing.expect_deprecated("The SelectBase.c"):
            s.where.non_generative(s, s.c.t_x > 5)

        assert_raises_message(
            exc.InvalidRequestError,
            r"select\(\) construct refers to itself as a FROM",
            s.compile,
        )

    def test_unusual_column_elements_text(self):
        """test that .c excludes text()."""

        s = select(table1.c.col1, text("foo")).subquery()
        eq_(list(s.c), [s.c.col1])

    def test_unusual_column_elements_clauselist(self):
        """Test that raw ClauseList is expanded into .c."""

        from sqlalchemy.sql.expression import ClauseList

        s = select(
            table1.c.col1, ClauseList(table1.c.col2, table1.c.col3)
        ).subquery()
        eq_(list(s.c), [s.c.col1, s.c.col2, s.c.col3])

    def test_unusual_column_elements_boolean_clauselist(self):
        """test that BooleanClauseList is placed as single element in .c."""

        c2 = and_(table1.c.col2 == 5, table1.c.col3 == 4)
        s = select(table1.c.col1, c2).subquery()
        eq_(list(s.c), [s.c.col1, s.corresponding_column(c2)])

    def test_from_list_deferred_constructor(self):
        c1 = Column("c1", Integer)
        c2 = Column("c2", Integer)

        select(c1)

        t = Table("t", MetaData(), c1, c2)

        eq_(c1._from_objects, [t])
        eq_(c2._from_objects, [t])

        self.assert_compile(select(c1), "SELECT t.c1 FROM t")
        self.assert_compile(select(c2), "SELECT t.c2 FROM t")

    def test_from_list_deferred_whereclause(self):
        c1 = Column("c1", Integer)
        c2 = Column("c2", Integer)

        select(c1).where(c1 == 5)

        t = Table("t", MetaData(), c1, c2)

        eq_(c1._from_objects, [t])
        eq_(c2._from_objects, [t])

        self.assert_compile(select(c1), "SELECT t.c1 FROM t")
        self.assert_compile(select(c2), "SELECT t.c2 FROM t")

    def test_from_list_deferred_fromlist(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer))

        c1 = Column("c1", Integer)
        select(c1).where(c1 == 5).select_from(t1)

        t2 = Table("t2", MetaData(), c1)

        eq_(c1._from_objects, [t2])

        self.assert_compile(select(c1), "SELECT t2.c1 FROM t2")

    def test_from_list_deferred_cloning(self):
        c1 = Column("c1", Integer)
        c2 = Column("c2", Integer)

        s = select(c1)
        s2 = select(c2)
        s3 = sql_util.ClauseAdapter(s).traverse(s2)

        Table("t", MetaData(), c1, c2)

        self.assert_compile(s3, "SELECT t.c2 FROM t")

    def test_from_list_with_columns(self):
        table1 = table("t1", column("a"))
        table2 = table("t2", column("b"))
        s1 = select(table1.c.a, table2.c.b)
        self.assert_compile(s1, "SELECT t1.a, t2.b FROM t1, t2")
        s2 = s1.with_only_columns(table2.c.b)
        self.assert_compile(s2, "SELECT t2.b FROM t2")

        s3 = sql_util.ClauseAdapter(table1).traverse(s1)
        self.assert_compile(s3, "SELECT t1.a, t2.b FROM t1, t2")
        s4 = s3.with_only_columns(table2.c.b)
        self.assert_compile(s4, "SELECT t2.b FROM t2")

    def test_from_list_against_existing_one(self):
        c1 = Column("c1", Integer)
        s = select(c1)

        # force a compile.
        self.assert_compile(s, "SELECT c1")

        Table("t", MetaData(), c1)

        self.assert_compile(s, "SELECT t.c1 FROM t")

    def test_from_list_against_existing_two(self):
        c1 = Column("c1", Integer)
        c2 = Column("c2", Integer)

        s = select(c1)

        # force a compile.
        eq_(str(s), "SELECT c1")

        t = Table("t", MetaData(), c1, c2)

        eq_(c1._from_objects, [t])
        eq_(c2._from_objects, [t])

        self.assert_compile(s, "SELECT t.c1 FROM t")
        self.assert_compile(select(c1), "SELECT t.c1 FROM t")
        self.assert_compile(select(c2), "SELECT t.c2 FROM t")

    def test_label_gen_resets_on_table(self):
        c1 = Column("c1", Integer)
        eq_(c1._label, "c1")
        Table("t1", MetaData(), c1)
        eq_(c1._label, "t1_c1")

    def test_no_alias_construct(self):
        a = table("a", column("x"))

        assert_raises_message(
            NotImplementedError,
            "The Alias class is not intended to be constructed directly.  "
            r"Please use the alias\(\) standalone function",
            Alias,
            a,
            "foo",
        )

    def test_whereclause_adapted(self):
        table1 = table("t1", column("a"))

        s1 = select(table1).subquery()

        s2 = select(s1).where(s1.c.a == 5)

        assert s2._whereclause.left.table is s1

        ta = select(table1).subquery()

        s3 = sql_util.ClauseAdapter(ta).traverse(s2)

        froms = list(s3._iterate_from_elements())

        assert s1 not in froms

        # these are new assumptions with the newer approach that
        # actively swaps out whereclause and others
        assert s3._whereclause.left.table is not s1
        assert s3._whereclause.left.table in froms


class RefreshForNewColTest(fixtures.TestBase):
    def test_join_uninit(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        j = a.join(b, a.c.x == b.c.y)

        q = column("q")
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_join_init(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        j = a.join(b, a.c.x == b.c.y)
        j.c
        q = column("q")
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_join_samename_init(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        j = a.join(b, a.c.x == b.c.y)
        j.c
        q = column("x")
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_x is q

    def test_select_samename_init(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        s = select(a, b).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        s.selected_columns
        q = column("x")
        b.append_column(q)
        s._refresh_for_new_column(q)
        assert q in s.selected_columns.b_x.proxy_set

    def test_alias_alias_samename_init(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        s1 = (
            select(a, b)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        s2 = s1.alias()

        s1.c
        s2.c

        q = column("x")
        b.append_column(q)

        assert "_columns" in s2.__dict__

        s2._refresh_for_new_column(q)

        assert "_columns" not in s2.__dict__
        is_(s1.corresponding_column(s2.c.b_x), s1.c.b_x)

    def test_aliased_select_samename_uninit(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        s = (
            select(a, b)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        q = column("x")
        b.append_column(q)
        s._refresh_for_new_column(q)
        assert q in s.c.b_x.proxy_set

    def test_aliased_select_samename_init(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        s = (
            select(a, b)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        s.c
        q = column("x")
        b.append_column(q)
        s._refresh_for_new_column(q)
        assert q in s.c.b_x.proxy_set

    def test_aliased_select_irrelevant(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        c = table("c", column("z"))
        s = (
            select(a, b)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        s.c
        q = column("x")
        c.append_column(q)
        s._refresh_for_new_column(q)
        assert "c_x" not in s.c

    def test_aliased_select_no_cols_clause(self):
        a = table("a", column("x"))
        s = (
            select(a.c.x)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        s.c
        q = column("q")
        a.append_column(q)
        s._refresh_for_new_column(q)
        assert "a_q" not in s.c

    def test_union_uninit(self):
        a = table("a", column("x"))
        s1 = select(a)
        s2 = select(a)
        s3 = s1.union(s2)
        q = column("q")
        a.append_column(q)
        s3._refresh_for_new_column(q)
        assert a.c.q in s3.selected_columns.q.proxy_set

    def test_union_init(self):
        a = table("a", column("x"))
        s1 = select(a)
        s2 = select(a)
        s3 = s1.union(s2)
        s3.selected_columns
        q = column("q")
        a.append_column(q)
        s3._refresh_for_new_column(q)
        assert a.c.q in s3.selected_columns.q.proxy_set

    def test_nested_join_uninit(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        c = table("c", column("z"))
        j = a.join(b, a.c.x == b.c.y).join(c, b.c.y == c.c.z)

        q = column("q")
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_nested_join_init(self):
        a = table("a", column("x"))
        b = table("b", column("y"))
        c = table("c", column("z"))
        j = a.join(b, a.c.x == b.c.y).join(c, b.c.y == c.c.z)

        j.c
        q = column("q")
        b.append_column(q)
        j._refresh_for_new_column(q)
        assert j.c.b_q is q

    def test_fk_table(self):
        m = MetaData()
        fk = ForeignKey("x.id")
        Table("x", m, Column("id", Integer))
        a = Table("a", m, Column("x", Integer, fk))
        a.c

        q = Column("q", Integer)
        a.append_column(q)
        a._refresh_for_new_column(q)
        eq_(a.foreign_keys, set([fk]))

        fk2 = ForeignKey("g.id")
        p = Column("p", Integer, fk2)
        a.append_column(p)
        a._refresh_for_new_column(p)
        eq_(a.foreign_keys, set([fk, fk2]))

    def test_fk_join(self):
        m = MetaData()
        fk = ForeignKey("x.id")
        Table("x", m, Column("id", Integer))
        a = Table("a", m, Column("x", Integer, fk))
        b = Table("b", m, Column("y", Integer))
        j = a.join(b, a.c.x == b.c.y)
        j.c

        q = Column("q", Integer)
        b.append_column(q)
        j._refresh_for_new_column(q)
        eq_(j.foreign_keys, set([fk]))

        fk2 = ForeignKey("g.id")
        p = Column("p", Integer, fk2)
        b.append_column(p)
        j._refresh_for_new_column(p)
        eq_(j.foreign_keys, set([fk, fk2]))


class AnonLabelTest(fixtures.TestBase):

    """Test behaviors fixed by [ticket:2168]."""

    def test_anon_labels_named_column(self):
        c1 = column("x")

        assert c1.label(None) is not c1
        eq_(str(select(c1.label(None))), "SELECT x AS x_1")

    def test_anon_labels_literal_column(self):
        c1 = literal_column("x")
        assert c1.label(None) is not c1
        eq_(str(select(c1.label(None))), "SELECT x AS x_1")

    def test_anon_labels_func(self):
        c1 = func.count("*")
        assert c1.label(None) is not c1

        eq_(str(select(c1)), "SELECT count(:count_2) AS count_1")
        select(c1).compile()

        eq_(str(select(c1.label(None))), "SELECT count(:count_2) AS count_1")

    def test_named_labels_named_column(self):
        c1 = column("x")
        eq_(str(select(c1.label("y"))), "SELECT x AS y")

    def test_named_labels_literal_column(self):
        c1 = literal_column("x")
        eq_(str(select(c1.label("y"))), "SELECT x AS y")


class JoinAnonymizingTest(fixtures.TestBase, AssertsCompiledSQL):
    """test anonymous_fromclause for aliases.

    In 1.4 this function is only for ORM internal use.   The public version
    join.alias() is deprecated.


    """

    __dialect__ = "default"

    def test_flat_ok_on_non_join(self):
        a = table("a", column("a"))
        s = a.select()
        self.assert_compile(
            s.alias(flat=True).select(),
            "SELECT anon_1.a FROM (SELECT a.a AS a FROM a) AS anon_1",
        )

    def test_join_alias(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        self.assert_compile(
            a.join(b, a.c.a == b.c.b)._anonymous_fromclause(),
            "SELECT a.a AS a_a, b.b AS b_b FROM a JOIN b ON a.a = b.b",
        )

    def test_join_standalone_alias(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        self.assert_compile(
            a.join(b, a.c.a == b.c.b)._anonymous_fromclause(),
            "SELECT a.a AS a_a, b.b AS b_b FROM a JOIN b ON a.a = b.b",
        )

    def test_join_alias_flat(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        self.assert_compile(
            a.join(b, a.c.a == b.c.b)._anonymous_fromclause(flat=True),
            "a AS a_1 JOIN b AS b_1 ON a_1.a = b_1.b",
        )

    def test_join_standalone_alias_flat(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        self.assert_compile(
            a.join(b, a.c.a == b.c.b)._anonymous_fromclause(flat=True),
            "a AS a_1 JOIN b AS b_1 ON a_1.a = b_1.b",
        )

    def test_composed_join_alias_flat(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        c = table("c", column("c"))
        d = table("d", column("d"))

        j1 = a.join(b, a.c.a == b.c.b)
        j2 = c.join(d, c.c.c == d.c.d)

        # note in 1.4 the flat=True flag now descends into the whole join,
        # as it should
        self.assert_compile(
            j1.join(j2, b.c.b == c.c.c)._anonymous_fromclause(flat=True),
            "a AS a_1 JOIN b AS b_1 ON a_1.a = b_1.b JOIN "
            "(c AS c_1 JOIN d AS d_1 ON c_1.c = d_1.d) "
            "ON b_1.b = c_1.c",
        )

    def test_composed_join_alias(self):
        a = table("a", column("a"))
        b = table("b", column("b"))
        c = table("c", column("c"))
        d = table("d", column("d"))

        j1 = a.join(b, a.c.a == b.c.b)
        j2 = c.join(d, c.c.c == d.c.d)
        self.assert_compile(
            select(j1.join(j2, b.c.b == c.c.c)._anonymous_fromclause()),
            "SELECT anon_1.a_a, anon_1.b_b, anon_1.c_c, anon_1.d_d "
            "FROM (SELECT a.a AS a_a, b.b AS b_b, c.c AS c_c, d.d AS d_d "
            "FROM a JOIN b ON a.a = b.b "
            "JOIN (c JOIN d ON c.c = d.d) ON b.b = c.c) AS anon_1",
        )


class JoinConditionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_join_condition_one(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2", m, Column("id", Integer), Column("t1id", ForeignKey("t1.id"))
        )
        t3 = Table(
            "t3",
            m,
            Column("id", Integer),
            Column("t1id", ForeignKey("t1.id")),
            Column("t2id", ForeignKey("t2.id")),
        )
        t4 = Table(
            "t4", m, Column("id", Integer), Column("t2id", ForeignKey("t2.id"))
        )
        t1t2 = t1.join(t2)
        t2t3 = t2.join(t3)

        for (left, right, a_subset, expected) in [
            (t1, t2, None, t1.c.id == t2.c.t1id),
            (t1t2, t3, t2, t1t2.c.t2_id == t3.c.t2id),
            (t2t3, t1, t3, t1.c.id == t3.c.t1id),
            (t2t3, t4, None, t2t3.c.t2_id == t4.c.t2id),
            (t2t3, t4, t3, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, None, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, t1, t2t3.c.t2_id == t4.c.t2id),
            (t1t2, t2t3, t2, t1t2.c.t2_id == t2t3.c.t3_t2id),
        ]:
            assert expected.compare(
                sql_util.join_condition(left, right, a_subset=a_subset)
            )

    def test_join_condition_two(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2", m, Column("id", Integer), Column("t1id", ForeignKey("t1.id"))
        )
        t3 = Table(
            "t3",
            m,
            Column("id", Integer),
            Column("t1id", ForeignKey("t1.id")),
            Column("t2id", ForeignKey("t2.id")),
        )
        t4 = Table(
            "t4", m, Column("id", Integer), Column("t2id", ForeignKey("t2.id"))
        )
        t5 = Table(
            "t5",
            m,
            Column("t1id1", ForeignKey("t1.id")),
            Column("t1id2", ForeignKey("t1.id")),
        )

        t1t2 = t1.join(t2)
        t2t3 = t2.join(t3)

        # these are ambiguous, or have no joins
        for left, right, a_subset in [
            (t1t2, t3, None),
            (t2t3, t1, None),
            (t1, t4, None),
            (t1t2, t2t3, None),
            (t5, t1, None),
            (
                t5.select()
                .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
                .subquery(),
                t1,
                None,
            ),
        ]:
            assert_raises(
                exc.ArgumentError,
                sql_util.join_condition,
                left,
                right,
                a_subset=a_subset,
            )

    def test_join_condition_three(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2",
            m,
            Column("id", Integer),
            Column("t1id", ForeignKey("t1.id")),
        )
        t3 = Table(
            "t3",
            m,
            Column("id", Integer),
            Column("t1id", ForeignKey("t1.id")),
            Column("t2id", ForeignKey("t2.id")),
        )
        t4 = Table(
            "t4",
            m,
            Column("id", Integer),
            Column("t2id", ForeignKey("t2.id")),
        )
        t1t2 = t1.join(t2)
        t2t3 = t2.join(t3)
        als = t2t3._anonymous_fromclause()
        # test join's behavior, including natural
        for left, right, expected in [
            (t1, t2, t1.c.id == t2.c.t1id),
            (t1t2, t3, t1t2.c.t2_id == t3.c.t2id),
            (t2t3, t1, t1.c.id == t3.c.t1id),
            (t2t3, t4, t2t3.c.t2_id == t4.c.t2id),
            (t2t3, t4, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, t2t3.c.t2_id == t4.c.t2id),
            (t2t3.join(t1), t4, t2t3.c.t2_id == t4.c.t2id),
            (t1t2, als, t1t2.c.t2_id == als.c.t3_t2id),
        ]:
            assert expected.compare(left.join(right).onclause)

    def test_join_condition_four(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2", m, Column("id", Integer), Column("t1id", ForeignKey("t1.id"))
        )
        t3 = Table(
            "t3",
            m,
            Column("id", Integer),
            Column("t1id", ForeignKey("t1.id")),
            Column("t2id", ForeignKey("t2.id")),
        )
        t1t2 = t1.join(t2)
        t2t3 = t2.join(t3)

        # these are right-nested joins
        j = t1t2.join(t2t3)
        assert j.onclause.compare(t2.c.id == t3.c.t2id)
        self.assert_compile(
            j,
            "t1 JOIN t2 ON t1.id = t2.t1id JOIN "
            "(t2 JOIN t3 ON t2.id = t3.t2id) ON t2.id = t3.t2id",
        )

    def test_join_condition_five(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2", m, Column("id", Integer), Column("t1id", ForeignKey("t1.id"))
        )
        t3 = Table(
            "t3",
            m,
            Column("id", Integer),
            Column("t1id", ForeignKey("t1.id")),
            Column("t2id", ForeignKey("t2.id")),
        )
        t1t2 = t1.join(t2)
        t2t3 = t2.join(t3)

        st2t3 = (
            t2t3.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        j = t1t2.join(st2t3)
        assert j.onclause.compare(t2.c.id == st2t3.c.t3_t2id)
        self.assert_compile(
            j,
            "t1 JOIN t2 ON t1.id = t2.t1id JOIN "
            "(SELECT t2.id AS t2_id, t2.t1id AS t2_t1id, "
            "t3.id AS t3_id, t3.t1id AS t3_t1id, t3.t2id AS t3_t2id "
            "FROM t2 JOIN t3 ON t2.id = t3.t2id) AS anon_1 "
            "ON t2.id = anon_1.t3_t2id",
        )

    def test_join_multiple_equiv_fks(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            m,
            Column("t1id", Integer, ForeignKey("t1.id"), ForeignKey("t1.id")),
        )

        assert sql_util.join_condition(t1, t2).compare(t1.c.id == t2.c.t1id)

    def test_join_cond_no_such_unrelated_table(self):
        m = MetaData()
        # bounding the "good" column with two "bad" ones is so to
        # try to get coverage to get the "continue" statements
        # in the loop...
        t1 = Table(
            "t1",
            m,
            Column("y", Integer, ForeignKey("t22.id")),
            Column("x", Integer, ForeignKey("t2.id")),
            Column("q", Integer, ForeignKey("t22.id")),
        )
        t2 = Table("t2", m, Column("id", Integer))
        assert sql_util.join_condition(t1, t2).compare(t1.c.x == t2.c.id)
        assert sql_util.join_condition(t2, t1).compare(t1.c.x == t2.c.id)

    def test_join_cond_no_such_unrelated_column(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer, ForeignKey("t2.id")),
            Column("y", Integer, ForeignKey("t3.q")),
        )
        t2 = Table("t2", m, Column("id", Integer))
        Table("t3", m, Column("id", Integer))
        assert sql_util.join_condition(t1, t2).compare(t1.c.x == t2.c.id)
        assert sql_util.join_condition(t2, t1).compare(t1.c.x == t2.c.id)

    def test_join_cond_no_such_unrelated_table_dont_compare_names(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("y", Integer, ForeignKey("t22.id")),
            Column("x", Integer, ForeignKey("t2.id")),
            Column("q", Integer, ForeignKey("t22.id")),
        )
        t2 = Table(
            "t2",
            m,
            Column("id", Integer),
            Column("t3id", ForeignKey("t3.id")),
            Column("z", ForeignKey("t33.id")),
        )
        t3 = Table(
            "t3", m, Column("id", Integer), Column("q", ForeignKey("t4.id"))
        )

        j1 = t1.join(t2)

        assert sql_util.join_condition(j1, t3).compare(t2.c.t3id == t3.c.id)

    def test_join_cond_no_such_unrelated_column_dont_compare_names(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer, ForeignKey("t2.id")),
        )
        t2 = Table(
            "t2",
            m,
            Column("id", Integer),
            Column("t3id", ForeignKey("t3.id")),
            Column("q", ForeignKey("t5.q")),
        )
        t3 = Table(
            "t3", m, Column("id", Integer), Column("t4id", ForeignKey("t4.id"))
        )
        t4 = Table("t4", m, Column("id", Integer))
        Table("t5", m, Column("id", Integer))
        j1 = t1.join(t2)

        j2 = t3.join(t4)

        assert sql_util.join_condition(j1, j2).compare(t2.c.t3id == t3.c.id)

    def test_join_cond_no_such_related_table(self):
        m1 = MetaData()
        m2 = MetaData()
        t1 = Table("t1", m1, Column("x", Integer, ForeignKey("t2.id")))
        t2 = Table("t2", m2, Column("id", Integer))
        assert_raises_message(
            exc.NoReferencedTableError,
            "Foreign key associated with column 't1.x' could not find "
            "table 't2' with which to generate a foreign key to "
            "target column 'id'",
            sql_util.join_condition,
            t1,
            t2,
        )

        assert_raises_message(
            exc.NoReferencedTableError,
            "Foreign key associated with column 't1.x' could not find "
            "table 't2' with which to generate a foreign key to "
            "target column 'id'",
            sql_util.join_condition,
            t2,
            t1,
        )

    def test_join_cond_no_such_related_column(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer, ForeignKey("t2.q")))
        t2 = Table("t2", m, Column("id", Integer))
        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for "
            "ForeignKey 't2.q' on table 't1': "
            "table 't2' has no column named 'q'",
            sql_util.join_condition,
            t1,
            t2,
        )

        assert_raises_message(
            exc.NoReferencedColumnError,
            "Could not initialize target column for "
            "ForeignKey 't2.q' on table 't1': "
            "table 't2' has no column named 'q'",
            sql_util.join_condition,
            t2,
            t1,
        )


class PrimaryKeyTest(fixtures.TestBase, AssertsExecutionResults):
    def test_join_pk_collapse_implicit(self):
        """test that redundant columns in a join get 'collapsed' into a
        minimal primary key, which is the root column along a chain of
        foreign key relationships."""

        meta = MetaData()
        a = Table("a", meta, Column("id", Integer, primary_key=True))
        b = Table(
            "b",
            meta,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
        )
        c = Table(
            "c",
            meta,
            Column("id", Integer, ForeignKey("b.id"), primary_key=True),
        )
        d = Table(
            "d",
            meta,
            Column("id", Integer, ForeignKey("c.id"), primary_key=True),
        )
        assert c.c.id.references(b.c.id)
        assert not d.c.id.references(a.c.id)
        assert list(a.join(b).primary_key) == [a.c.id]
        assert list(b.join(c).primary_key) == [b.c.id]
        assert list(a.join(b).join(c).primary_key) == [a.c.id]
        assert list(b.join(c).join(d).primary_key) == [b.c.id]
        assert list(d.join(c).join(b).primary_key) == [b.c.id]
        assert list(a.join(b).join(c).join(d).primary_key) == [a.c.id]

    def test_join_pk_collapse_explicit(self):
        """test that redundant columns in a join get 'collapsed' into a
        minimal primary key, which is the root column along a chain of
        explicit join conditions."""

        meta = MetaData()
        a = Table(
            "a",
            meta,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        b = Table(
            "b",
            meta,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("x", Integer),
        )
        c = Table(
            "c",
            meta,
            Column("id", Integer, ForeignKey("b.id"), primary_key=True),
            Column("x", Integer),
        )
        d = Table(
            "d",
            meta,
            Column("id", Integer, ForeignKey("c.id"), primary_key=True),
            Column("x", Integer),
        )
        print(list(a.join(b, a.c.x == b.c.id).primary_key))
        assert list(a.join(b, a.c.x == b.c.id).primary_key) == [a.c.id]
        assert list(b.join(c, b.c.x == c.c.id).primary_key) == [b.c.id]
        assert list(a.join(b).join(c, c.c.id == b.c.x).primary_key) == [a.c.id]
        assert list(b.join(c, c.c.x == b.c.id).join(d).primary_key) == [b.c.id]
        assert list(b.join(c, c.c.id == b.c.x).join(d).primary_key) == [b.c.id]
        assert list(
            d.join(b, d.c.id == b.c.id).join(c, b.c.id == c.c.x).primary_key
        ) == [b.c.id]
        assert list(
            a.join(b).join(c, c.c.id == b.c.x).join(d).primary_key
        ) == [a.c.id]
        assert list(
            a.join(b, and_(a.c.id == b.c.id, a.c.x == b.c.id)).primary_key
        ) == [a.c.id]

    def test_init_doesnt_blowitaway(self):
        meta = MetaData()
        a = Table(
            "a",
            meta,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        b = Table(
            "b",
            meta,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("x", Integer),
        )

        j = a.join(b)
        assert list(j.primary_key) == [a.c.id]

        j.foreign_keys
        assert list(j.primary_key) == [a.c.id]

    def test_non_column_clause(self):
        meta = MetaData()
        a = Table(
            "a",
            meta,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        b = Table(
            "b",
            meta,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("x", Integer, primary_key=True),
        )

        j = a.join(b, and_(a.c.id == b.c.id, b.c.x == 5))
        assert str(j) == "a JOIN b ON a.id = b.id AND b.x = :x_1", str(j)
        assert list(j.primary_key) == [a.c.id, b.c.x]

    def test_onclause_direction(self):
        metadata = MetaData()

        employee = Table(
            "Employee",
            metadata,
            Column("name", String(100)),
            Column("id", Integer, primary_key=True),
        )

        engineer = Table(
            "Engineer",
            metadata,
            Column("id", Integer, ForeignKey("Employee.id"), primary_key=True),
        )

        eq_(
            util.column_set(
                employee.join(
                    engineer, employee.c.id == engineer.c.id
                ).primary_key
            ),
            util.column_set([employee.c.id]),
        )
        eq_(
            util.column_set(
                employee.join(
                    engineer, engineer.c.id == employee.c.id
                ).primary_key
            ),
            util.column_set([employee.c.id]),
        )


class ReduceTest(fixtures.TestBase, AssertsExecutionResults):
    def test_reduce(self):
        meta = MetaData()
        t1 = Table(
            "t1",
            meta,
            Column("t1id", Integer, primary_key=True),
            Column("t1data", String(30)),
        )
        t2 = Table(
            "t2",
            meta,
            Column("t2id", Integer, ForeignKey("t1.t1id"), primary_key=True),
            Column("t2data", String(30)),
        )
        t3 = Table(
            "t3",
            meta,
            Column("t3id", Integer, ForeignKey("t2.t2id"), primary_key=True),
            Column("t3data", String(30)),
        )

        eq_(
            util.column_set(
                sql_util.reduce_columns(
                    [
                        t1.c.t1id,
                        t1.c.t1data,
                        t2.c.t2id,
                        t2.c.t2data,
                        t3.c.t3id,
                        t3.c.t3data,
                    ]
                )
            ),
            util.column_set(
                [t1.c.t1id, t1.c.t1data, t2.c.t2data, t3.c.t3data]
            ),
        )

    def test_reduce_selectable(self):
        metadata = MetaData()
        engineers = Table(
            "engineers",
            metadata,
            Column("engineer_id", Integer, primary_key=True),
            Column("engineer_name", String(50)),
        )
        managers = Table(
            "managers",
            metadata,
            Column("manager_id", Integer, primary_key=True),
            Column("manager_name", String(50)),
        )
        s = (
            select(engineers, managers)
            .where(engineers.c.engineer_name == managers.c.manager_name)
            .subquery()
        )
        eq_(
            util.column_set(sql_util.reduce_columns(list(s.c), s)),
            util.column_set(
                [s.c.engineer_id, s.c.engineer_name, s.c.manager_id]
            ),
        )

    def test_reduce_generation(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer, primary_key=True),
            Column("y", Integer),
        )
        t2 = Table(
            "t2",
            m,
            Column("z", Integer, ForeignKey("t1.x")),
            Column("q", Integer),
        )
        s1 = select(t1, t2)
        s2 = s1.reduce_columns(only_synonyms=False)
        eq_(set(s2.selected_columns), set([t1.c.x, t1.c.y, t2.c.q]))

        s2 = s1.reduce_columns()
        eq_(set(s2.selected_columns), set([t1.c.x, t1.c.y, t2.c.z, t2.c.q]))

    def test_reduce_only_synonym_fk(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer, primary_key=True),
            Column("y", Integer),
        )
        t2 = Table(
            "t2",
            m,
            Column("x", Integer, ForeignKey("t1.x")),
            Column("q", Integer, ForeignKey("t1.y")),
        )
        s1 = select(t1, t2)
        s1 = s1.reduce_columns(only_synonyms=True)
        eq_(
            set(s1.selected_columns),
            set(
                [
                    s1.selected_columns.x,
                    s1.selected_columns.y,
                    s1.selected_columns.q,
                ]
            ),
        )

    def test_reduce_only_synonym_lineage(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer, primary_key=True),
            Column("y", Integer),
            Column("z", Integer),
        )
        # test that the first appearance in the columns clause
        # wins - t1 is first, t1.c.x wins
        s1 = select(t1).subquery()
        s2 = select(t1, s1).where(t1.c.x == s1.c.x).where(s1.c.y == t1.c.z)
        eq_(
            set(s2.reduce_columns().selected_columns),
            set([t1.c.x, t1.c.y, t1.c.z, s1.c.y, s1.c.z]),
        )

        # reverse order, s1.c.x wins
        s1 = select(t1).subquery()
        s2 = select(s1, t1).where(t1.c.x == s1.c.x).where(s1.c.y == t1.c.z)
        eq_(
            set(s2.reduce_columns().selected_columns),
            set([s1.c.x, t1.c.y, t1.c.z, s1.c.y, s1.c.z]),
        )

    def test_reduce_aliased_join(self):
        metadata = MetaData()
        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                Sequence("person_id_seq", optional=True),
                primary_key=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )
        engineers = Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("engineer_name", String(50)),
            Column("primary_language", String(50)),
        )
        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("manager_name", String(50)),
        )
        pjoin = (
            people.outerjoin(engineers)
            .outerjoin(managers)
            .select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias("pjoin")
        )
        eq_(
            util.column_set(
                sql_util.reduce_columns(
                    [
                        pjoin.c.people_person_id,
                        pjoin.c.engineers_person_id,
                        pjoin.c.managers_person_id,
                    ]
                )
            ),
            util.column_set([pjoin.c.people_person_id]),
        )

    def test_reduce_aliased_union(self):
        metadata = MetaData()

        item_table = Table(
            "item",
            metadata,
            Column(
                "id", Integer, ForeignKey("base_item.id"), primary_key=True
            ),
            Column("dummy", Integer, default=0),
        )
        base_item_table = Table(
            "base_item",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("child_name", String(255), default=None),
        )
        from sqlalchemy.orm.util import polymorphic_union

        item_join = polymorphic_union(
            {
                "BaseItem": base_item_table.select(
                    base_item_table.c.child_name == "BaseItem"
                ).subquery(),
                "Item": base_item_table.join(item_table),
            },
            None,
            "item_join",
        )
        eq_(
            util.column_set(
                sql_util.reduce_columns(
                    [item_join.c.id, item_join.c.dummy, item_join.c.child_name]
                )
            ),
            util.column_set(
                [item_join.c.id, item_join.c.dummy, item_join.c.child_name]
            ),
        )

    def test_reduce_aliased_union_2(self):
        metadata = MetaData()
        page_table = Table(
            "page", metadata, Column("id", Integer, primary_key=True)
        )
        magazine_page_table = Table(
            "magazine_page",
            metadata,
            Column(
                "page_id", Integer, ForeignKey("page.id"), primary_key=True
            ),
        )
        classified_page_table = Table(
            "classified_page",
            metadata,
            Column(
                "magazine_page_id",
                Integer,
                ForeignKey("magazine_page.page_id"),
                primary_key=True,
            ),
        )

        # this is essentially the union formed by the ORM's
        # polymorphic_union function. we define two versions with
        # different ordering of selects.
        #
        # the first selectable has the "real" column
        # classified_page.magazine_page_id

        pjoin = union(
            select(
                page_table.c.id,
                magazine_page_table.c.page_id,
                classified_page_table.c.magazine_page_id,
            ).select_from(
                page_table.join(magazine_page_table).join(
                    classified_page_table
                )
            ),
            select(
                page_table.c.id,
                magazine_page_table.c.page_id,
                cast(null(), Integer).label("magazine_page_id"),
            ).select_from(page_table.join(magazine_page_table)),
        ).alias("pjoin")
        eq_(
            util.column_set(
                sql_util.reduce_columns(
                    [pjoin.c.id, pjoin.c.page_id, pjoin.c.magazine_page_id]
                )
            ),
            util.column_set([pjoin.c.id]),
        )

        # the first selectable has a CAST, which is a placeholder for
        # classified_page.magazine_page_id in the second selectable.
        # reduce_columns needs to take into account all foreign keys
        # derived from pjoin.c.magazine_page_id. the UNION construct
        # currently makes the external column look like that of the
        # first selectable only.

        pjoin = union(
            select(
                page_table.c.id,
                magazine_page_table.c.page_id,
                cast(null(), Integer).label("magazine_page_id"),
            ).select_from(page_table.join(magazine_page_table)),
            select(
                page_table.c.id,
                magazine_page_table.c.page_id,
                classified_page_table.c.magazine_page_id,
            ).select_from(
                page_table.join(magazine_page_table).join(
                    classified_page_table
                )
            ),
        ).alias("pjoin")
        eq_(
            util.column_set(
                sql_util.reduce_columns(
                    [pjoin.c.id, pjoin.c.page_id, pjoin.c.magazine_page_id]
                )
            ),
            util.column_set([pjoin.c.id]),
        )


class DerivedTest(fixtures.TestBase, AssertsExecutionResults):
    def test_table(self):
        meta = MetaData()

        t1 = Table(
            "t1",
            meta,
            Column("c1", Integer, primary_key=True),
            Column("c2", String(30)),
        )
        t2 = Table(
            "t2",
            meta,
            Column("c1", Integer, primary_key=True),
            Column("c2", String(30)),
        )

        assert t1.is_derived_from(t1)
        assert not t2.is_derived_from(t1)

    def test_alias(self):
        meta = MetaData()
        t1 = Table(
            "t1",
            meta,
            Column("c1", Integer, primary_key=True),
            Column("c2", String(30)),
        )
        t2 = Table(
            "t2",
            meta,
            Column("c1", Integer, primary_key=True),
            Column("c2", String(30)),
        )

        assert t1.alias().is_derived_from(t1)
        assert not t2.alias().is_derived_from(t1)
        assert not t1.is_derived_from(t1.alias())
        assert not t1.is_derived_from(t2.alias())

    def test_select(self):
        meta = MetaData()

        t1 = Table(
            "t1",
            meta,
            Column("c1", Integer, primary_key=True),
            Column("c2", String(30)),
        )
        t2 = Table(
            "t2",
            meta,
            Column("c1", Integer, primary_key=True),
            Column("c2", String(30)),
        )

        assert t1.select().is_derived_from(t1)
        assert not t2.select().is_derived_from(t1)

        assert select(t1, t2).is_derived_from(t1)

        assert t1.select().alias("foo").is_derived_from(t1)
        assert select(t1, t2).alias("foo").is_derived_from(t1)
        assert not t2.select().alias("foo").is_derived_from(t1)


class AnnotationsTest(fixtures.TestBase):
    def test_hashing(self):
        t = table("t", column("x"))

        a = t.alias()

        for obj in [t, t.c.x, a, t.c.x > 1, (t.c.x > 1).label(None)]:
            annot = obj._annotate({})
            eq_(set([obj]), set([annot]))

    def test_clone_annotations_dont_hash(self):
        t = table("t", column("x"))

        s = t.select()
        a = t.alias()
        s2 = a.select()

        for obj in [s, s2]:
            annot = obj._annotate({})
            ne_(set([obj]), set([annot]))

    def test_replacement_traverse_preserve(self):
        """test that replacement traverse that hits an unannotated column
        does not use it when replacing an annotated column.

        this requires that replacement traverse store elements in the
        "seen" hash based on id(), not hash.

        """
        t = table("t", column("x"))

        stmt = select(t.c.x)

        whereclause = annotation._deep_annotate(t.c.x == 5, {"foo": "bar"})

        eq_(whereclause._annotations, {"foo": "bar"})
        eq_(whereclause.left._annotations, {"foo": "bar"})
        eq_(whereclause.right._annotations, {"foo": "bar"})

        stmt = stmt.where(whereclause)

        s2 = visitors.replacement_traverse(stmt, {}, lambda elem: None)

        whereclause = s2._where_criteria[0]
        eq_(whereclause._annotations, {"foo": "bar"})
        eq_(whereclause.left._annotations, {"foo": "bar"})
        eq_(whereclause.right._annotations, {"foo": "bar"})

    def test_proxy_set_iteration_includes_annotated(self):
        from sqlalchemy.schema import Column

        c1 = Column("foo", Integer)

        stmt = select(c1).alias()
        proxy = stmt.c.foo

        proxy.proxy_set

        # create an annotated of the column
        p2 = proxy._annotate({"weight": 10})

        # now see if our annotated version is in that column's
        # proxy_set, as corresponding_column iterates through proxy_set
        # in this way
        d = {}
        for col in p2._uncached_proxy_set():
            d.update(col._annotations)
        eq_(d, {"weight": 10})

    def test_proxy_set_iteration_includes_annotated_two(self):
        from sqlalchemy.schema import Column

        c1 = Column("foo", Integer)

        stmt = select(c1).alias()
        proxy = stmt.c.foo
        c1.proxy_set

        proxy._proxies = [c1._annotate({"weight": 10})]

        d = {}
        for col in proxy._uncached_proxy_set():
            d.update(col._annotations)
        eq_(d, {"weight": 10})

    def test_late_name_add(self):
        from sqlalchemy.schema import Column

        c1 = Column(Integer)
        c1_a = c1._annotate({"foo": "bar"})
        c1.name = "somename"
        eq_(c1_a.name, "somename")

    def test_late_table_add(self):
        c1 = Column("foo", Integer)
        c1_a = c1._annotate({"foo": "bar"})
        t = Table("t", MetaData(), c1)
        is_(c1_a.table, t)

    def test_basic_attrs(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, info={"q": "p"}),
            Column("y", Integer, key="q"),
        )
        x_a = t.c.x._annotate({})
        y_a = t.c.q._annotate({})
        t.c.x.info["z"] = "h"

        eq_(y_a.key, "q")
        is_(x_a.table, t)
        eq_(x_a.info, {"q": "p", "z": "h"})
        eq_(t.c.x.anon_label, x_a.anon_label)

    def test_custom_constructions(self):
        from sqlalchemy.schema import Column

        class MyColumn(Column):
            def __init__(self):
                Column.__init__(self, "foo", Integer)

            _constructor = Column

        t1 = Table("t1", MetaData(), MyColumn())
        s1 = t1.select().subquery()
        assert isinstance(t1.c.foo, MyColumn)
        assert isinstance(s1.c.foo, Column)

        annot_1 = t1.c.foo._annotate({})
        s2 = select(annot_1).subquery()
        assert isinstance(s2.c.foo, Column)
        annot_2 = s1._annotate({})
        assert isinstance(annot_2.c.foo, Column)

    def test_custom_construction_correct_anno_subclass(self):
        # [ticket:2918]
        from sqlalchemy.schema import Column
        from sqlalchemy.sql.elements import AnnotatedColumnElement

        class MyColumn(Column):
            pass

        assert isinstance(
            MyColumn("x", Integer)._annotate({"foo": "bar"}),
            AnnotatedColumnElement,
        )

    def test_custom_construction_correct_anno_expr(self):
        # [ticket:2918]
        from sqlalchemy.schema import Column

        class MyColumn(Column):
            pass

        col = MyColumn("x", Integer)
        col == 5
        col_anno = MyColumn("x", Integer)._annotate({"foo": "bar"})
        binary_2 = col_anno == 5
        eq_(binary_2.left._annotations, {"foo": "bar"})

    def test_annotated_corresponding_column(self):
        table1 = table("table1", column("col1"))

        s1 = select(table1.c.col1).subquery()
        t1 = s1._annotate({})
        t2 = s1

        # t1 needs to share the same _make_proxy() columns as t2, even
        # though it's annotated.  otherwise paths will diverge once they
        # are corresponded against "inner" below.

        assert t1.c is t2.c
        assert t1.c.col1 is t2.c.col1

        inner = select(s1).subquery()

        assert (
            inner.corresponding_column(t2.c.col1, require_embedded=False)
            is inner.corresponding_column(t2.c.col1, require_embedded=True)
            is inner.c.col1
        )
        assert (
            inner.corresponding_column(t1.c.col1, require_embedded=False)
            is inner.corresponding_column(t1.c.col1, require_embedded=True)
            is inner.c.col1
        )

    def test_annotated_visit(self):
        table1 = table("table1", column("col1"), column("col2"))

        bin_ = table1.c.col1 == bindparam("foo", value=None)
        assert str(bin_) == "table1.col1 = :foo"

        def visit_binary(b):
            b.right = table1.c.col2

        b2 = visitors.cloned_traverse(bin_, {}, {"binary": visit_binary})
        assert str(b2) == "table1.col1 = table1.col2"

        b3 = visitors.cloned_traverse(
            bin_._annotate({}), {}, {"binary": visit_binary}
        )
        assert str(b3) == "table1.col1 = table1.col2"

        def visit_binary(b):
            b.left = bindparam("bar")

        b4 = visitors.cloned_traverse(b2, {}, {"binary": visit_binary})
        assert str(b4) == ":bar = table1.col2"

        b5 = visitors.cloned_traverse(b3, {}, {"binary": visit_binary})
        assert str(b5) == ":bar = table1.col2"

    def test_label_accessors(self):
        t1 = table("t1", column("c1"))
        l1 = t1.c.c1.label(None)
        is_(l1._order_by_label_element, l1)
        l1a = l1._annotate({"foo": "bar"})
        is_(l1a._order_by_label_element, l1a)

    def test_annotate_aliased(self):
        t1 = table("t1", column("c1"))
        s = select((t1.c.c1 + 3).label("bat"))
        a = s.alias()
        a = sql_util._deep_annotate(a, {"foo": "bar"})
        eq_(a._annotations["foo"], "bar")
        eq_(a.element._annotations["foo"], "bar")

    def test_annotate_expressions(self):
        table1 = table("table1", column("col1"), column("col2"))
        for expr, expected in [
            (table1.c.col1, "table1.col1"),
            (table1.c.col1 == 5, "table1.col1 = :col1_1"),
            (
                table1.c.col1.in_([2, 3, 4]),
                "table1.col1 IN ([POSTCOMPILE_col1_1])",
            ),
        ]:
            eq_(str(expr), expected)
            eq_(str(expr._annotate({})), expected)
            eq_(str(sql_util._deep_annotate(expr, {})), expected)
            eq_(
                str(
                    sql_util._deep_annotate(expr, {}, exclude=[table1.c.col1])
                ),
                expected,
            )

    def test_deannotate_wrapping(self):
        table1 = table("table1", column("col1"), column("col2"))

        bin_ = table1.c.col1 == bindparam("foo", value=None)

        b2 = sql_util._deep_annotate(bin_, {"_orm_adapt": True})
        b3 = sql_util._deep_deannotate(b2)
        b4 = sql_util._deep_deannotate(bin_)

        for elem in (b2._annotations, b2.left._annotations):
            in_("_orm_adapt", elem)

        for elem in (
            b3._annotations,
            b3.left._annotations,
            b4._annotations,
            b4.left._annotations,
        ):
            eq_(elem, {})

        is_not(b2.left, bin_.left)
        is_not(b3.left, b2.left)
        is_not(b2.left, bin_.left)
        is_(b4.left, bin_.left)  # since column is immutable
        # deannotate copies the element
        is_not(bin_.right, b2.right)
        is_not(b2.right, b3.right)
        is_not(b3.right, b4.right)

    def test_deannotate_clone(self):
        table1 = table("table1", column("col1"), column("col2"))

        subq = (
            select(table1).where(table1.c.col1 == bindparam("foo")).subquery()
        )
        stmt = select(subq)

        s2 = sql_util._deep_annotate(stmt, {"_orm_adapt": True})
        s3 = sql_util._deep_deannotate(s2)
        s4 = sql_util._deep_deannotate(s3)

        eq_(stmt._annotations, {})
        eq_(subq._annotations, {})

        eq_(s2._annotations, {"_orm_adapt": True})
        eq_(s3._annotations, {})
        eq_(s4._annotations, {})

        # select._raw_columns[0] is the subq object
        eq_(s2._raw_columns[0]._annotations, {"_orm_adapt": True})
        eq_(s3._raw_columns[0]._annotations, {})
        eq_(s4._raw_columns[0]._annotations, {})

        is_not(s3, s2)
        is_not(s4, s3)  # deep deannotate makes a clone unconditionally

        is_(s3._deannotate(), s3)  # regular deannotate returns same object

    def test_annotate_unique_traversal(self):
        """test that items are copied only once during
        annotate, deannotate traversal

        #2453 - however note this was modified by
        #1401, and it's likely that re49563072578
        is helping us with the str() comparison
        case now, as deannotate is making
        clones again in some cases.
        """
        table1 = table("table1", column("x"))
        table2 = table("table2", column("y"))
        a1 = table1.alias()
        s = select(a1.c.x).select_from(a1.join(table2, a1.c.x == table2.c.y))
        for sel in (
            sql_util._deep_deannotate(s),
            visitors.cloned_traverse(s, {}, {}),
            visitors.replacement_traverse(s, {}, lambda x: None),
        ):
            # the columns clause isn't changed at all
            assert sel._raw_columns[0].table is a1
            froms = list(sel._iterate_from_elements())
            assert froms[0].element is froms[1].left.element

            eq_(str(s), str(sel))

        # when we are modifying annotations sets only
        # partially, elements are copied uniquely based on id().
        # this is new as of 1.4, previously they'd be copied every time
        for sel in (
            sql_util._deep_deannotate(s, {"foo": "bar"}),
            sql_util._deep_annotate(s, {"foo": "bar"}),
        ):
            froms = list(sel._iterate_from_elements())
            assert froms[0] is not froms[1].left

            # but things still work out due to
            # re49563072578
            eq_(str(s), str(sel))

    def test_annotate_varied_annot_same_col(self):
        """test two instances of the same column with different annotations
        preserving them when deep_annotate is run on them.

        """
        t1 = table("table1", column("col1"), column("col2"))
        s = select(t1.c.col1._annotate({"foo": "bar"}))
        s2 = select(t1.c.col1._annotate({"bat": "hoho"}))
        s3 = s.union(s2)
        sel = sql_util._deep_annotate(s3, {"new": "thing"})

        eq_(
            sel.selects[0]._raw_columns[0]._annotations,
            {"foo": "bar", "new": "thing"},
        )

        eq_(
            sel.selects[1]._raw_columns[0]._annotations,
            {"bat": "hoho", "new": "thing"},
        )

    def test_deannotate_2(self):
        table1 = table("table1", column("col1"), column("col2"))
        j = table1.c.col1._annotate(
            {"remote": True}
        ) == table1.c.col2._annotate({"local": True})
        j2 = sql_util._deep_deannotate(j)
        eq_(j.left._annotations, {"remote": True})
        eq_(j2.left._annotations, {})

    def test_deannotate_3(self):
        table1 = table(
            "table1",
            column("col1"),
            column("col2"),
            column("col3"),
            column("col4"),
        )
        j = and_(
            table1.c.col1._annotate({"remote": True})
            == table1.c.col2._annotate({"local": True}),
            table1.c.col3._annotate({"remote": True})
            == table1.c.col4._annotate({"local": True}),
        )
        j2 = sql_util._deep_deannotate(j)
        eq_(j.clauses[0].left._annotations, {"remote": True})
        eq_(j2.clauses[0].left._annotations, {})

    def test_annotate_fromlist_preservation(self):
        """test the FROM list in select still works
        even when multiple annotate runs have created
        copies of the same selectable

        #2453, continued

        """
        table1 = table("table1", column("x"))
        table2 = table("table2", column("y"))
        a1 = table1.alias()
        s = select(a1.c.x).select_from(a1.join(table2, a1.c.x == table2.c.y))

        assert_s = select(select(s.subquery()).subquery())
        for fn in (
            sql_util._deep_deannotate,
            lambda s: sql_util._deep_annotate(s, {"foo": "bar"}),
            lambda s: visitors.cloned_traverse(s, {}, {}),
            lambda s: visitors.replacement_traverse(s, {}, lambda x: None),
        ):

            sel = fn(select(fn(select(fn(s.subquery())).subquery())))
            eq_(str(assert_s), str(sel))

    def test_bind_unique_test(self):
        table("t", column("a"), column("b"))

        b = bindparam("bind", value="x", unique=True)

        # the annotation of "b" should render the
        # same.  The "unique" test in compiler should
        # also pass, [ticket:2425]
        eq_(str(or_(b, b._annotate({"foo": "bar"}))), ":bind_1 OR :bind_1")

    def test_comparators_cleaned_out_construction(self):
        c = column("a")

        comp1 = c.comparator

        c1 = c._annotate({"foo": "bar"})
        comp2 = c1.comparator
        assert comp1 is not comp2

    def test_comparators_cleaned_out_reannotate(self):
        c = column("a")

        c1 = c._annotate({"foo": "bar"})
        comp1 = c1.comparator

        c2 = c1._annotate({"bat": "hoho"})
        comp2 = c2.comparator

        assert comp1 is not comp2

    def test_comparator_cleanout_integration(self):
        c = column("a")

        c1 = c._annotate({"foo": "bar"})
        c1.comparator

        c2 = c1._annotate({"bat": "hoho"})
        c2.comparator

        assert (c2 == 5).left._annotations == {"foo": "bar", "bat": "hoho"}


class ReprTest(fixtures.TestBase):
    def test_ensure_repr_elements(self):
        for obj in [
            elements.Cast(1, 2),
            elements.TypeClause(String()),
            elements.ColumnClause("x"),
            elements.BindParameter("q"),
            elements.Null(),
            elements.True_(),
            elements.False_(),
            elements.ClauseList(),
            elements.BooleanClauseList._construct_raw(operators.and_),
            elements.BooleanClauseList._construct_raw(operators.or_),
            elements.Tuple(),
            elements.Case([]),
            elements.Extract("foo", column("x")),
            elements.UnaryExpression(column("x")),
            elements.Grouping(column("x")),
            elements.Over(func.foo()),
            elements.Label("q", column("x")),
        ]:
            repr(obj)


class WithLabelsTest(fixtures.TestBase):
    def _assert_result_keys(self, s, keys):
        compiled = s.compile()

        eq_(set(compiled._create_result_map()), set(keys))

    def _assert_subq_result_keys(self, s, keys):
        compiled = s.subquery().select().compile()
        eq_(set(compiled._create_result_map()), set(keys))

    def _names_overlap(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer))
        t2 = Table("t2", m, Column("x", Integer))
        return select(t1, t2).set_label_style(LABEL_STYLE_NONE)

    def test_names_overlap_nolabel(self):
        sel = self._names_overlap()
        self._assert_result_keys(sel, ["x"])

        self._assert_subq_result_keys(sel, ["x", "x_1"])

        eq_(sel.selected_columns.keys(), ["x", "x"])

    def test_names_overlap_label(self):
        sel = self._names_overlap().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(sel.selected_columns.keys(), ["t1_x", "t2_x"])
        eq_(list(sel.selected_columns.keys()), ["t1_x", "t2_x"])
        eq_(list(sel.subquery().c.keys()), ["t1_x", "t2_x"])
        self._assert_result_keys(sel, ["t1_x", "t2_x"])

    def _names_overlap_keys_dont(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer, key="a"))
        t2 = Table("t2", m, Column("x", Integer, key="b"))
        return select(t1, t2).set_label_style(LABEL_STYLE_NONE)

    def test_names_overlap_keys_dont_nolabel(self):
        sel = self._names_overlap_keys_dont()

        eq_(sel.selected_columns.keys(), ["a", "b"])
        eq_(list(sel.selected_columns.keys()), ["a", "b"])
        eq_(list(sel.subquery().c.keys()), ["a", "b"])
        self._assert_result_keys(sel, ["x"])

    def test_names_overlap_keys_dont_label(self):
        sel = self._names_overlap_keys_dont().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(sel.selected_columns.keys(), ["t1_a", "t2_b"])
        eq_(list(sel.selected_columns.keys()), ["t1_a", "t2_b"])
        eq_(list(sel.subquery().c.keys()), ["t1_a", "t2_b"])
        self._assert_result_keys(sel, ["t1_x", "t2_x"])

    def _columns_repeated(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer), Column("y", Integer))
        return select(t1.c.x, t1.c.y, t1.c.x).set_label_style(LABEL_STYLE_NONE)

    def test_element_repeated_nolabels(self):
        sel = self._columns_repeated().set_label_style(LABEL_STYLE_NONE)
        eq_(sel.selected_columns.keys(), ["x", "y", "x"])
        eq_(list(sel.selected_columns.keys()), ["x", "y", "x"])
        eq_(list(sel.subquery().c.keys()), ["x", "y", "x_1"])
        self._assert_result_keys(sel, ["x", "y"])

    def test_element_repeated_disambiguate(self):
        sel = self._columns_repeated().set_label_style(
            LABEL_STYLE_DISAMBIGUATE_ONLY
        )
        eq_(sel.selected_columns.keys(), ["x", "y", "x_1"])
        eq_(list(sel.selected_columns.keys()), ["x", "y", "x_1"])
        eq_(list(sel.subquery().c.keys()), ["x", "y", "x_1"])
        self._assert_result_keys(sel, ["x", "y", "x__1"])

    def test_element_repeated_labels(self):
        sel = self._columns_repeated().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(sel.selected_columns.keys(), ["t1_x", "t1_y", "t1_x_1"])
        eq_(list(sel.selected_columns.keys()), ["t1_x", "t1_y", "t1_x_1"])
        eq_(list(sel.subquery().c.keys()), ["t1_x", "t1_y", "t1_x_1"])
        self._assert_result_keys(sel, ["t1_x__1", "t1_x", "t1_y"])

    def _labels_overlap(self):
        m = MetaData()
        t1 = Table("t", m, Column("x_id", Integer))
        t2 = Table("t_x", m, Column("id", Integer))
        return select(t1, t2)

    def test_labels_overlap_nolabel(self):
        sel = self._labels_overlap()
        eq_(sel.selected_columns.keys(), ["x_id", "id"])
        eq_(list(sel.selected_columns.keys()), ["x_id", "id"])
        eq_(list(sel.subquery().c.keys()), ["x_id", "id"])
        self._assert_result_keys(sel, ["x_id", "id"])

    def test_labels_overlap_label(self):
        sel = self._labels_overlap().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(
            list(sel.selected_columns.keys()),
            ["t_x_id", "t_x_id_1"],
        )
        eq_(
            list(sel.subquery().c.keys()),
            ["t_x_id", "t_x_id_1"],
            # ["t_x_id", "t_x_id"]  # if we turn off deduping entirely,
        )
        self._assert_result_keys(sel, ["t_x_id", "t_x_id_1"])
        self._assert_subq_result_keys(sel, ["t_x_id", "t_x_id_1"])

    def _labels_overlap_keylabels_dont(self):
        m = MetaData()
        t1 = Table("t", m, Column("x_id", Integer, key="a"))
        t2 = Table("t_x", m, Column("id", Integer, key="b"))
        return select(t1, t2)

    def test_labels_overlap_keylabels_dont_nolabel(self):
        sel = self._labels_overlap_keylabels_dont()
        eq_(list(sel.selected_columns.keys()), ["a", "b"])
        eq_(list(sel.subquery().c.keys()), ["a", "b"])
        self._assert_result_keys(sel, ["x_id", "id"])

    def test_labels_overlap_keylabels_dont_label(self):
        sel = self._labels_overlap_keylabels_dont().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(list(sel.selected_columns.keys()), ["t_a", "t_x_b"])
        eq_(list(sel.subquery().c.keys()), ["t_a", "t_x_b"])
        self._assert_result_keys(sel, ["t_x_id", "t_x_id_1"])

    def _keylabels_overlap_labels_dont(self):
        m = MetaData()
        t1 = Table("t", m, Column("a", Integer, key="x_id"))
        t2 = Table("t_x", m, Column("b", Integer, key="id"))
        return select(t1, t2)

    def test_keylabels_overlap_labels_dont_nolabel(self):
        sel = self._keylabels_overlap_labels_dont()
        eq_(list(sel.selected_columns.keys()), ["x_id", "id"])
        eq_(list(sel.subquery().c.keys()), ["x_id", "id"])
        self._assert_result_keys(sel, ["a", "b"])

    def test_keylabels_overlap_labels_dont_label(self):
        sel = self._keylabels_overlap_labels_dont().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(
            list(sel.selected_columns.keys()),
            ["t_x_id", "t_x_id_1"],
        )
        eq_(
            list(sel.subquery().c.keys()),
            ["t_x_id", "t_x_id_1"],
        )
        self._assert_result_keys(sel, ["t_a", "t_x_b"])
        self._assert_subq_result_keys(sel, ["t_a", "t_x_b"])

    def _keylabels_overlap_labels_overlap(self):
        m = MetaData()
        t1 = Table("t", m, Column("x_id", Integer, key="x_a"))
        t2 = Table("t_x", m, Column("id", Integer, key="a"))
        return select(t1, t2)

    def test_keylabels_overlap_labels_overlap_nolabel(self):
        sel = self._keylabels_overlap_labels_overlap()
        eq_(list(sel.selected_columns.keys()), ["x_a", "a"])
        eq_(list(sel.subquery().c.keys()), ["x_a", "a"])
        self._assert_result_keys(sel, ["x_id", "id"])
        self._assert_subq_result_keys(sel, ["x_id", "id"])

    def test_keylabels_overlap_labels_overlap_label(self):
        sel = self._keylabels_overlap_labels_overlap().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(
            list(sel.selected_columns.keys()),
            ["t_x_a", "t_x_a_1"],
        )

        # deduping for different cols but same label
        eq_(list(sel.subquery().c.keys()), ["t_x_a", "t_x_a_1"])

        # if we turn off deduping entirely
        # eq_(list(sel.subquery().c.keys()), ["t_x_a", "t_x_a"])

        self._assert_result_keys(sel, ["t_x_id", "t_x_id_1"])
        self._assert_subq_result_keys(sel, ["t_x_id", "t_x_id_1"])

    def _keys_overlap_names_dont(self):
        m = MetaData()
        t1 = Table("t1", m, Column("a", Integer, key="x"))
        t2 = Table("t2", m, Column("b", Integer, key="x"))
        return select(t1, t2)

    def test_keys_overlap_names_dont_nolabel(self):
        sel = self._keys_overlap_names_dont()
        eq_(sel.selected_columns.keys(), ["x", "x_1"])
        self._assert_result_keys(sel, ["a", "b"])

    def test_keys_overlap_names_dont_label(self):
        sel = self._keys_overlap_names_dont().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        eq_(list(sel.selected_columns.keys()), ["t1_x", "t2_x"])
        eq_(list(sel.subquery().c.keys()), ["t1_x", "t2_x"])
        self._assert_result_keys(sel, ["t1_a", "t2_b"])


class ResultMapTest(fixtures.TestBase):
    def _fixture(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer), Column("y", Integer))
        return t

    def _mapping(self, stmt):
        compiled = stmt.compile()
        return dict(
            (elem, key)
            for key, elements in compiled._create_result_map().items()
            for elem in elements[1]
        )

    def test_select_label_alt_name(self):
        t = self._fixture()
        l1, l2 = t.c.x.label("a"), t.c.y.label("b")
        s = select(l1, l2)
        mapping = self._mapping(s)
        assert l1 in mapping

        assert t.c.x not in mapping

    def test_select_alias_label_alt_name(self):
        t = self._fixture()
        l1, l2 = t.c.x.label("a"), t.c.y.label("b")
        s = select(l1, l2).alias()
        mapping = self._mapping(s)
        assert l1 in mapping

        assert t.c.x not in mapping

    def test_select_alias_column(self):
        t = self._fixture()
        x, y = t.c.x, t.c.y
        s = select(x, y).alias()
        mapping = self._mapping(s)

        assert t.c.x in mapping

    def test_select_alias_column_apply_labels(self):
        t = self._fixture()
        x, y = t.c.x, t.c.y
        s = (
            select(x, y)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        mapping = self._mapping(s)
        assert t.c.x in mapping

    def test_select_table_alias_column(self):
        t = self._fixture()
        x = t.c.x

        ta = t.alias()
        s = select(ta.c.x, ta.c.y)
        mapping = self._mapping(s)
        assert x not in mapping

    def test_select_label_alt_name_table_alias_column(self):
        t = self._fixture()
        x = t.c.x

        ta = t.alias()
        l1, l2 = ta.c.x.label("a"), ta.c.y.label("b")

        s = select(l1, l2)
        mapping = self._mapping(s)
        assert x not in mapping
        assert l1 in mapping
        assert ta.c.x not in mapping

    def test_column_subquery_exists(self):
        t = self._fixture()
        s = exists().where(t.c.x == 5).select()
        mapping = self._mapping(s)
        assert t.c.x not in mapping
        eq_(
            [type(entry[-1]) for entry in s.compile()._result_columns],
            [Boolean],
        )

    def test_plain_exists(self):
        expr = exists([1])
        eq_(type(expr.type), Boolean)
        eq_(
            [
                type(entry[-1])
                for entry in select(expr).compile()._result_columns
            ],
            [Boolean],
        )

    def test_plain_exists_negate(self):
        expr = ~exists([1])
        eq_(type(expr.type), Boolean)
        eq_(
            [
                type(entry[-1])
                for entry in select(expr).compile()._result_columns
            ],
            [Boolean],
        )

    def test_plain_exists_double_negate(self):
        expr = ~(~exists([1]))
        eq_(type(expr.type), Boolean)
        eq_(
            [
                type(entry[-1])
                for entry in select(expr).compile()._result_columns
            ],
            [Boolean],
        )

    def test_column_subquery_plain(self):
        t = self._fixture()
        s1 = select(t.c.x).where(t.c.x > 5).scalar_subquery()
        s2 = select(s1)
        mapping = self._mapping(s2)
        assert t.c.x not in mapping
        assert s1 in mapping
        eq_(
            [type(entry[-1]) for entry in s2.compile()._result_columns],
            [Integer],
        )

    def test_unary_boolean(self):

        s1 = select(not_(True)).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        eq_(
            [type(entry[-1]) for entry in s1.compile()._result_columns],
            [Boolean],
        )


class ForUpdateTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_basic_clone(self):
        t = table("t", column("c"))
        s = select(t).with_for_update(read=True, of=t.c.c)
        s2 = visitors.ReplacingCloningVisitor().traverse(s)
        assert s2._for_update_arg is not s._for_update_arg
        eq_(s2._for_update_arg.read, True)
        eq_(s2._for_update_arg.of, [t.c.c])
        self.assert_compile(
            s2, "SELECT t.c FROM t FOR SHARE OF t", dialect="postgresql"
        )

    def test_adapt(self):
        t = table("t", column("c"))
        s = select(t).with_for_update(read=True, of=t.c.c)
        a = t.alias()
        s2 = sql_util.ClauseAdapter(a).traverse(s)
        eq_(s2._for_update_arg.of, [a.c.c])
        self.assert_compile(
            s2,
            "SELECT t_1.c FROM t AS t_1 FOR SHARE OF t_1",
            dialect="postgresql",
        )


class AliasTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_direct_element_hierarchy(self):
        t = table("t", column("c"))
        a1 = t.alias()
        a2 = a1.alias()
        a3 = a2.alias()

        is_(a1.element, t)
        is_(a2.element, a1)
        is_(a3.element, a2)

    def test_get_children_preserves_multiple_nesting(self):
        t = table("t", column("c"))
        stmt = select(t)
        a1 = stmt.alias()
        a2 = a1.alias()
        eq_(set(a2.get_children(column_collections=False)), {a1})

    def test_correspondence_multiple_nesting(self):
        t = table("t", column("c"))
        stmt = select(t)
        a1 = stmt.alias()
        a2 = a1.alias()

        is_(a1.corresponding_column(a2.c.c), a1.c.c)

    def test_copy_internals_multiple_nesting(self):
        t = table("t", column("c"))
        stmt = select(t)
        a1 = stmt.alias()
        a2 = a1.alias()

        a3 = a2._clone()
        a3._copy_internals()
        is_(a1.corresponding_column(a3.c.c), a1.c.c)
