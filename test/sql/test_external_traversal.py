import re

from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import case
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import tuple_
from sqlalchemy import union
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import column
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import operators
from sqlalchemy.sql import table
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import _clone
from sqlalchemy.sql.expression import _from_objects
from sqlalchemy.sql.visitors import ClauseVisitor
from sqlalchemy.sql.visitors import cloned_traverse
from sqlalchemy.sql.visitors import CloningVisitor
from sqlalchemy.sql.visitors import ReplacingCloningVisitor
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.util import pickle

A = B = t1 = t2 = t3 = table1 = table2 = table3 = table4 = None


class TraversalTest(
    fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL
):

    """test ClauseVisitor's traversal, particularly its
    ability to copy and modify a ClauseElement in place."""

    @classmethod
    def setup_test_class(cls):
        global A, B

        # establish two fictitious ClauseElements.
        # define deep equality semantics as well as deep
        # identity semantics.
        class A(ClauseElement):
            __visit_name__ = "a"
            _traverse_internals = []

            def __init__(self, expr):
                self.expr = expr

            def is_other(self, other):
                return other is self

            __hash__ = ClauseElement.__hash__

            def __eq__(self, other):
                return other.expr == self.expr

            def __ne__(self, other):
                return other.expr != self.expr

            def __str__(self):
                return "A(%s)" % repr(self.expr)

        class B(ClauseElement):
            __visit_name__ = "b"

            def __init__(self, *items):
                self.items = items

            def is_other(self, other):
                if other is not self:
                    return False
                for i1, i2 in zip(self.items, other.items):
                    if i1 is not i2:
                        return False
                return True

            __hash__ = ClauseElement.__hash__

            def __eq__(self, other):
                for i1, i2 in zip(self.items, other.items):
                    if i1 != i2:
                        return False
                return True

            def __ne__(self, other):
                for i1, i2 in zip(self.items, other.items):
                    if i1 != i2:
                        return True
                return False

            def _copy_internals(self, clone=_clone, **kw):
                self.items = [clone(i, **kw) for i in self.items]

            def get_children(self, **kwargs):
                return self.items

            def __str__(self):
                return "B(%s)" % repr([str(i) for i in self.items])

    def test_test_classes(self):
        a1 = A("expr1")
        struct = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct2 = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct3 = B(
            a1, A("expr2"), B(A("expr1b"), A("expr2bmodified")), A("expr3")
        )

        assert a1.is_other(a1)
        assert struct.is_other(struct)
        assert struct == struct2
        assert struct != struct3
        assert not struct.is_other(struct2)
        assert not struct.is_other(struct3)

    def test_clone(self):
        struct = B(
            A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3")
        )

        class Vis(CloningVisitor):
            def visit_a(self, a):
                pass

            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct)
        assert struct == s2
        assert not struct.is_other(s2)

    def test_no_clone(self):
        struct = B(
            A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3")
        )

        class Vis(ClauseVisitor):
            def visit_a(self, a):
                pass

            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct)
        assert struct == s2
        assert struct.is_other(s2)

    def test_clone_anon_label(self):
        from sqlalchemy.sql.elements import Grouping

        c1 = Grouping(literal_column("q"))
        s1 = select(c1)

        class Vis(CloningVisitor):
            def visit_grouping(self, elem):
                pass

        vis = Vis()
        s2 = vis.traverse(s1)
        eq_(list(s2.selected_columns)[0]._anon_name_label, c1._anon_name_label)

    @testing.combinations(
        ("clone",), ("pickle",), ("conv_to_unique"), ("none"), argnames="meth"
    )
    @testing.combinations(
        ("name with space",), ("name with [brackets]",), argnames="name"
    )
    def test_bindparam_key_proc_for_copies(self, meth, name):
        r"""test :ticket:`6249`.

        The key of the bindparam needs spaces and other characters
        escaped out for the POSTCOMPILE regex to work correctly.


        Currently, the bind key reg is::

            re.sub(r"[%\(\) \$]+", "_", body).strip("_")

        and the compiler postcompile reg is::

            re.sub(r"\[POSTCOMPILE_(\S+)\]", process_expanding, self.string)

        Interestingly, brackets in the name seems to work out.

        """
        expr = column(name).in_([1, 2, 3])

        if meth == "clone":
            expr = visitors.cloned_traverse(expr, {}, {})
        elif meth == "pickle":
            expr = pickle.loads(pickle.dumps(expr))
        elif meth == "conv_to_unique":
            expr.right.unique = False
            expr.right._convert_to_unique()

        token = re.sub(r"[%\(\) \$]+", "_", name).strip("_")
        self.assert_compile(
            expr,
            '"%(name)s" IN (:%(token)s_1_1, '
            ":%(token)s_1_2, :%(token)s_1_3)" % {"name": name, "token": token},
            render_postcompile=True,
            dialect="default",
        )

    def test_traversal_size(self):
        """Test :ticket:`6304`.

        Testing that _iterate_from_elements returns only unique FROM
        clauses; overall traversal should be short and all items unique.

        """

        t = table("t", *[column(x) for x in "pqrxyz"])

        s1 = select(t.c.p, t.c.q, t.c.r, t.c.x, t.c.y, t.c.z).subquery()

        s2 = (
            select(s1.c.p, s1.c.q, s1.c.r, s1.c.x, s1.c.y, s1.c.z)
            .select_from(s1)
            .subquery()
        )

        s3 = (
            select(s2.c.p, s2.c.q, s2.c.r, s2.c.x, s2.c.y, s2.c.z)
            .select_from(s2)
            .subquery()
        )

        tt = list(s3.element._iterate_from_elements())
        eq_(tt, [s2])

        total = list(visitors.iterate(s3))
        # before the bug was fixed, this was 750
        eq_(len(total), 25)

        seen = set()
        for elem in visitors.iterate(s3):
            assert elem not in seen
            seen.add(elem)

        eq_(len(seen), 25)

    def test_change_in_place(self):
        struct = B(
            A("expr1"), A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3")
        )
        struct2 = B(
            A("expr1"),
            A("expr2modified"),
            B(A("expr1b"), A("expr2b")),
            A("expr3"),
        )
        struct3 = B(
            A("expr1"),
            A("expr2"),
            B(A("expr1b"), A("expr2bmodified")),
            A("expr3"),
        )

        class Vis(CloningVisitor):
            def visit_a(self, a):
                if a.expr == "expr2":
                    a.expr = "expr2modified"

            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct)
        assert struct != s2
        assert not struct.is_other(s2)
        assert struct2 == s2

        class Vis2(CloningVisitor):
            def visit_a(self, a):
                if a.expr == "expr2b":
                    a.expr = "expr2bmodified"

            def visit_b(self, b):
                pass

        vis2 = Vis2()
        s3 = vis2.traverse(struct)
        assert struct != s3
        assert struct3 == s3

    def test_visit_name(self):
        # override fns in testlib/schema.py
        from sqlalchemy import Column

        class CustomObj(Column):
            pass

        assert CustomObj.__visit_name__ == Column.__visit_name__ == "column"

        foo, bar = CustomObj("foo", String), CustomObj("bar", String)
        bin_ = foo == bar
        set(ClauseVisitor().iterate(bin_))
        assert set(ClauseVisitor().iterate(bin_)) == set([foo, bar, bin_])


class BinaryEndpointTraversalTest(fixtures.TestBase):

    """test the special binary product visit"""

    def _assert_traversal(self, expr, expected):
        canary = []

        def visit(binary, l, r):
            canary.append((binary.operator, l, r))
            print(binary.operator, l, r)

        sql_util.visit_binary_product(visit, expr)
        eq_(canary, expected)

    def test_basic(self):
        a, b = column("a"), column("b")
        self._assert_traversal(a == b, [(operators.eq, a, b)])

    def test_with_tuples(self):
        a, b, c, d, b1, b1a, b1b, e, f = (
            column("a"),
            column("b"),
            column("c"),
            column("d"),
            column("b1"),
            column("b1a"),
            column("b1b"),
            column("e"),
            column("f"),
        )
        expr = tuple_(a, b, b1 == tuple_(b1a, b1b == d), c) > tuple_(
            func.go(e + f)
        )
        self._assert_traversal(
            expr,
            [
                (operators.gt, a, e),
                (operators.gt, a, f),
                (operators.gt, b, e),
                (operators.gt, b, f),
                (operators.eq, b1, b1a),
                (operators.eq, b1b, d),
                (operators.gt, c, e),
                (operators.gt, c, f),
            ],
        )

    def test_composed(self):
        a, b, e, f, q, j, r = (
            column("a"),
            column("b"),
            column("e"),
            column("f"),
            column("q"),
            column("j"),
            column("r"),
        )
        expr = and_((a + b) == q + func.sum(e + f), and_(j == r, f == q))
        self._assert_traversal(
            expr,
            [
                (operators.eq, a, q),
                (operators.eq, a, e),
                (operators.eq, a, f),
                (operators.eq, b, q),
                (operators.eq, b, e),
                (operators.eq, b, f),
                (operators.eq, j, r),
                (operators.eq, f, q),
            ],
        )

    def test_subquery(self):
        a, b, c = column("a"), column("b"), column("c")
        subq = select(c).where(c == a).scalar_subquery()
        expr = and_(a == b, b == subq)
        self._assert_traversal(
            expr, [(operators.eq, a, b), (operators.eq, b, subq)]
        )


class ClauseTest(fixtures.TestBase, AssertsCompiledSQL):

    """test copy-in-place behavior of various ClauseElements."""

    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        global t1, t2, t3
        t1 = table("table1", column("col1"), column("col2"), column("col3"))
        t2 = table("table2", column("col1"), column("col2"), column("col3"))
        t3 = Table(
            "table3",
            MetaData(),
            Column("col1", Integer),
            Column("col2", Integer),
        )

    def test_binary(self):
        clause = t1.c.col2 == t2.c.col2
        eq_(str(clause), str(CloningVisitor().traverse(clause)))

    def test_binary_anon_label_quirk(self):
        t = table("t1", column("col1"))

        f = t.c.col1 * 5
        self.assert_compile(
            select(f), "SELECT t1.col1 * :col1_1 AS anon_1 FROM t1"
        )

        f._anon_name_label

        a = t.alias()
        f = sql_util.ClauseAdapter(a).traverse(f)

        self.assert_compile(
            select(f), "SELECT t1_1.col1 * :col1_1 AS anon_1 FROM t1 AS t1_1"
        )

    @testing.combinations((null(),), (true(),))
    def test_dont_adapt_singleton_elements(self, elem):
        """test :ticket:`6259`"""
        t1 = table("t1", column("c1"))

        stmt = select(t1.c.c1, elem)

        wherecond = t1.c.c1.is_(elem)

        subq = stmt.subquery()

        adapted_wherecond = sql_util.ClauseAdapter(subq).traverse(wherecond)
        stmt = select(subq).where(adapted_wherecond)

        self.assert_compile(
            stmt,
            "SELECT anon_1.c1, anon_1.anon_2 FROM (SELECT t1.c1 AS c1, "
            "%s AS anon_2 FROM t1) AS anon_1 WHERE anon_1.c1 IS %s"
            % (str(elem), str(elem)),
            dialect="default_enhanced",
        )

    def test_adapt_funcs_etc_on_identity_one(self):
        """Adapting to a function etc. will adapt if its on identity"""
        t1 = table("t1", column("c1"))

        elem = func.foobar()

        stmt = select(t1.c.c1, elem)

        wherecond = t1.c.c1 == elem

        subq = stmt.subquery()

        adapted_wherecond = sql_util.ClauseAdapter(subq).traverse(wherecond)
        stmt = select(subq).where(adapted_wherecond)

        self.assert_compile(
            stmt,
            "SELECT anon_1.c1, anon_1.foobar_1 FROM (SELECT t1.c1 AS c1, "
            "foobar() AS foobar_1 FROM t1) AS anon_1 "
            "WHERE anon_1.c1 = anon_1.foobar_1",
            dialect="default_enhanced",
        )

    def test_adapt_funcs_etc_on_identity_two(self):
        """Adapting to a function etc. will not adapt if they are different"""
        t1 = table("t1", column("c1"))

        elem = func.foobar()
        elem2 = func.foobar()

        stmt = select(t1.c.c1, elem)

        wherecond = t1.c.c1 == elem2

        subq = stmt.subquery()

        adapted_wherecond = sql_util.ClauseAdapter(subq).traverse(wherecond)
        stmt = select(subq).where(adapted_wherecond)

        self.assert_compile(
            stmt,
            "SELECT anon_1.c1, anon_1.foobar_1 FROM (SELECT t1.c1 AS c1, "
            "foobar() AS foobar_1 FROM t1) AS anon_1 "
            "WHERE anon_1.c1 = foobar()",
            dialect="default_enhanced",
        )

    def test_join(self):
        clause = t1.join(t2, t1.c.col2 == t2.c.col2)
        c1 = str(clause)
        assert str(clause) == str(CloningVisitor().traverse(clause))

        class Vis(CloningVisitor):
            def visit_binary(self, binary):
                binary.right = t2.c.col3

        clause2 = Vis().traverse(clause)
        assert c1 == str(clause)
        assert str(clause2) == str(t1.join(t2, t1.c.col2 == t2.c.col3))

    def test_aliased_column_adapt(self):
        t1.select()

        aliased = t1.select().alias()
        aliased2 = t1.alias()

        adapter = sql_util.ColumnAdapter(aliased)

        f = select(*[adapter.columns[c] for c in aliased2.c]).select_from(
            aliased
        )

        s = select(aliased2).select_from(aliased)
        eq_(str(s), str(f))

        f = select(adapter.columns[func.count(aliased2.c.col1)]).select_from(
            aliased
        )
        eq_(
            str(select(func.count(aliased2.c.col1)).select_from(aliased)),
            str(f),
        )

    def test_aliased_cloned_column_adapt_inner(self):
        clause = select(t1.c.col1, func.foo(t1.c.col2).label("foo"))
        c_sub = clause.subquery()
        aliased1 = select(c_sub.c.col1, c_sub.c.foo).subquery()
        aliased2 = clause
        aliased2.selected_columns.col1, aliased2.selected_columns.foo
        aliased3 = cloned_traverse(aliased2, {}, {})

        # fixed by [ticket:2419].   the inside columns
        # on aliased3 have _is_clone_of pointers to those of
        # aliased2.  corresponding_column checks these
        # now.
        adapter = sql_util.ColumnAdapter(aliased1)
        f1 = select(*[adapter.columns[c] for c in aliased2._raw_columns])
        f2 = select(*[adapter.columns[c] for c in aliased3._raw_columns])
        eq_(str(f1), str(f2))

    def test_aliased_cloned_column_adapt_exported(self):
        clause = select(t1.c.col1, func.foo(t1.c.col2).label("foo")).subquery()

        aliased1 = select(clause.c.col1, clause.c.foo).subquery()
        aliased2 = clause
        aliased2.c.col1, aliased2.c.foo
        aliased3 = cloned_traverse(aliased2, {}, {})

        # also fixed by [ticket:2419].  When we look at the
        # *outside* columns of aliased3, they previously did not
        # have an _is_clone_of pointer.   But we now modified _make_proxy
        # to assign this.
        adapter = sql_util.ColumnAdapter(aliased1)
        f1 = select(*[adapter.columns[c] for c in aliased2.c])
        f2 = select(*[adapter.columns[c] for c in aliased3.c])
        eq_(str(f1), str(f2))

    def test_aliased_cloned_schema_column_adapt_exported(self):
        clause = select(t3.c.col1, func.foo(t3.c.col2).label("foo")).subquery()

        aliased1 = select(clause.c.col1, clause.c.foo).subquery()
        aliased2 = clause
        aliased2.c.col1, aliased2.c.foo
        aliased3 = cloned_traverse(aliased2, {}, {})

        # also fixed by [ticket:2419].  When we look at the
        # *outside* columns of aliased3, they previously did not
        # have an _is_clone_of pointer.   But we now modified _make_proxy
        # to assign this.
        adapter = sql_util.ColumnAdapter(aliased1)
        f1 = select(*[adapter.columns[c] for c in aliased2.c])
        f2 = select(*[adapter.columns[c] for c in aliased3.c])
        eq_(str(f1), str(f2))

    def test_labeled_expression_adapt(self):
        lbl_x = (t3.c.col1 == 1).label("x")
        t3_alias = t3.alias()

        adapter = sql_util.ColumnAdapter(t3_alias)

        lblx_adapted = adapter.traverse(lbl_x)
        is_not(lblx_adapted._element, lbl_x._element)

        lblx_adapted = adapter.traverse(lbl_x)
        self.assert_compile(
            select(lblx_adapted.self_group()),
            "SELECT (table3_1.col1 = :col1_1) AS x FROM table3 AS table3_1",
        )

        self.assert_compile(
            select(lblx_adapted.is_(True)),
            "SELECT (table3_1.col1 = :col1_1) IS 1 AS anon_1 "
            "FROM table3 AS table3_1",
        )

    def test_cte_w_union(self):
        t = select(func.values(1).label("n")).cte("t", recursive=True)
        t = t.union_all(select(t.c.n + 1).where(t.c.n < 100))
        s = select(func.sum(t.c.n))

        from sqlalchemy.sql.visitors import cloned_traverse

        cloned = cloned_traverse(s, {}, {})

        self.assert_compile(
            cloned,
            "WITH RECURSIVE t(n) AS "
            "(SELECT values(:values_1) AS n "
            "UNION ALL SELECT t.n + :n_1 AS anon_1 "
            "FROM t "
            "WHERE t.n < :n_2) "
            "SELECT sum(t.n) AS sum_1 FROM t",
        )

    def test_aliased_cte_w_union(self):
        t = (
            select(func.values(1).label("n"))
            .cte("t", recursive=True)
            .alias("foo")
        )
        t = t.union_all(select(t.c.n + 1).where(t.c.n < 100))
        s = select(func.sum(t.c.n))

        from sqlalchemy.sql.visitors import cloned_traverse

        cloned = cloned_traverse(s, {}, {})

        self.assert_compile(
            cloned,
            "WITH RECURSIVE foo(n) AS (SELECT values(:values_1) AS n "
            "UNION ALL SELECT foo.n + :n_1 AS anon_1 FROM foo "
            "WHERE foo.n < :n_2) SELECT sum(foo.n) AS sum_1 FROM foo",
        )

    def test_text(self):
        clause = text("select * from table where foo=:bar").bindparams(
            bindparam("bar")
        )
        c1 = str(clause)

        class Vis(CloningVisitor):
            def visit_textclause(self, text):
                text.text = text.text + " SOME MODIFIER=:lala"
                text._bindparams["lala"] = bindparam("lala")

        clause2 = Vis().traverse(clause)
        assert c1 == str(clause)
        assert str(clause2) == c1 + " SOME MODIFIER=:lala"
        assert list(clause._bindparams.keys()) == ["bar"]
        assert set(clause2._bindparams.keys()) == set(["bar", "lala"])

    def test_select(self):
        s2 = select(t1)
        s2_assert = str(s2)
        s3_assert = str(select(t1).where(t1.c.col2 == 7))

        class Vis(CloningVisitor):
            def visit_select(self, select):
                select.where.non_generative(select, t1.c.col2 == 7)

        s3 = Vis().traverse(s2)
        assert str(s3) == s3_assert
        assert str(s2) == s2_assert
        print(str(s2))
        print(str(s3))

        class Vis(ClauseVisitor):
            def visit_select(self, select):
                select.where.non_generative(select, t1.c.col2 == 7)

        Vis().traverse(s2)
        assert str(s2) == s3_assert

        s4_assert = str(select(t1).where(and_(t1.c.col2 == 7, t1.c.col3 == 9)))

        class Vis(CloningVisitor):
            def visit_select(self, select):
                select.where.non_generative(select, t1.c.col3 == 9)

        s4 = Vis().traverse(s3)
        print(str(s3))
        print(str(s4))
        assert str(s4) == s4_assert
        assert str(s3) == s3_assert

        s5_assert = str(select(t1).where(and_(t1.c.col2 == 7, t1.c.col1 == 9)))

        class Vis(CloningVisitor):
            def visit_binary(self, binary):
                if binary.left is t1.c.col3:
                    binary.left = t1.c.col1
                    binary.right = bindparam("col1", unique=True)

        s5 = Vis().traverse(s4)
        print(str(s4))
        print(str(s5))
        assert str(s5) == s5_assert
        assert str(s4) == s4_assert

    def test_union(self):
        u = union(t1.select(), t2.select())
        u2 = CloningVisitor().traverse(u)
        eq_(str(u), str(u2))

        eq_(
            [str(c) for c in u2.selected_columns],
            [str(c) for c in u.selected_columns],
        )

        u = union(t1.select(), t2.select())
        cols = [str(c) for c in u.selected_columns]
        u2 = CloningVisitor().traverse(u)
        eq_(str(u), str(u2))
        eq_([str(c) for c in u2.selected_columns], cols)

        s1 = select(t1).where(t1.c.col1 == bindparam("id_param"))
        s2 = select(t2)
        u = union(s1, s2)

        u2 = u.params(id_param=7)
        u3 = u.params(id_param=10)

        eq_(str(u), str(u2))
        eq_(str(u2), str(u3))
        eq_(u2.compile().params, {"id_param": 7})
        eq_(u3.compile().params, {"id_param": 10})

    def test_in(self):
        expr = t1.c.col1.in_(["foo", "bar"])
        expr2 = CloningVisitor().traverse(expr)
        assert str(expr) == str(expr2)

    def test_over(self):
        expr = func.row_number().over(order_by=t1.c.col1)
        expr2 = CloningVisitor().traverse(expr)
        assert str(expr) == str(expr2)

        assert expr in visitors.iterate(expr, {})

    def test_within_group(self):
        expr = func.row_number().within_group(t1.c.col1)
        expr2 = CloningVisitor().traverse(expr)
        assert str(expr) == str(expr2)

        assert expr in visitors.iterate(expr, {})

    def test_funcfilter(self):
        expr = func.count(1).filter(t1.c.col1 > 1)
        expr2 = CloningVisitor().traverse(expr)
        assert str(expr) == str(expr2)

    def test_adapt_union(self):
        u = union(
            t1.select().where(t1.c.col1 == 4),
            t1.select().where(t1.c.col1 == 5),
        ).alias()

        assert sql_util.ClauseAdapter(u).traverse(t1) is u

    def test_bindparams(self):
        """test that unique bindparams change their name upon clone()
        to prevent conflicts"""

        s = select(t1).where(t1.c.col1 == bindparam(None, unique=True)).alias()
        s2 = CloningVisitor().traverse(s).alias()
        s3 = select(s).where(s.c.col2 == s2.c.col2)

        self.assert_compile(
            s3,
            "SELECT anon_1.col1, anon_1.col2, anon_1.col3 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 WHERE table1.col1 = :param_1) "
            "AS anon_1, "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 "
            "AS col3 FROM table1 WHERE table1.col1 = :param_2) AS anon_2 "
            "WHERE anon_1.col2 = anon_2.col2",
        )

        s = select(t1).where(t1.c.col1 == 4).alias()
        s2 = CloningVisitor().traverse(s).alias()
        s3 = select(s).where(s.c.col2 == s2.c.col2)
        self.assert_compile(
            s3,
            "SELECT anon_1.col1, anon_1.col2, anon_1.col3 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 WHERE table1.col1 = :col1_1) "
            "AS anon_1, "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 "
            "AS col3 FROM table1 WHERE table1.col1 = :col1_2) AS anon_2 "
            "WHERE anon_1.col2 = anon_2.col2",
        )

    def test_extract(self):
        s = select(extract("foo", t1.c.col1).label("col1"))
        self.assert_compile(
            s, "SELECT EXTRACT(foo FROM table1.col1) AS col1 FROM table1"
        )

        s2 = CloningVisitor().traverse(s).alias()
        s3 = select(s2.c.col1)
        self.assert_compile(
            s, "SELECT EXTRACT(foo FROM table1.col1) AS col1 FROM table1"
        )
        self.assert_compile(
            s3,
            "SELECT anon_1.col1 FROM (SELECT EXTRACT(foo FROM "
            "table1.col1) AS col1 FROM table1) AS anon_1",
        )

    @testing.emits_warning(".*replaced by another column with the same key")
    def test_alias(self):
        subq = t2.select().alias("subq")
        s = select(t1.c.col1, subq.c.col1).select_from(
            t1, subq, t1.join(subq, t1.c.col1 == subq.c.col2)
        )
        orig = str(s)
        s2 = CloningVisitor().traverse(s)
        eq_(orig, str(s))
        eq_(str(s), str(s2))

        s4 = CloningVisitor().traverse(s2)
        eq_(orig, str(s))
        eq_(str(s), str(s2))
        eq_(str(s), str(s4))

        s3 = sql_util.ClauseAdapter(table("foo")).traverse(s)
        eq_(orig, str(s))
        eq_(str(s), str(s3))

        s4 = sql_util.ClauseAdapter(table("foo")).traverse(s3)
        eq_(orig, str(s))
        eq_(str(s), str(s3))
        eq_(str(s), str(s4))

        subq = subq.alias("subq")
        s = select(t1.c.col1, subq.c.col1).select_from(
            t1,
            subq,
            t1.join(subq, t1.c.col1 == subq.c.col2),
        )
        s5 = CloningVisitor().traverse(s)
        eq_(str(s), str(s5))

    def test_correlated_select(self):
        s = (
            select(literal_column("*"))
            .where(t1.c.col1 == t2.c.col1)
            .select_from(t1, t2)
            .correlate(t2)
        )

        class Vis(CloningVisitor):
            def visit_select(self, select):
                select.where.non_generative(select, t1.c.col2 == 7)

        self.assert_compile(
            select(t2).where(t2.c.col1 == Vis().traverse(s).scalar_subquery()),
            "SELECT table2.col1, table2.col2, table2.col3 "
            "FROM table2 WHERE table2.col1 = "
            "(SELECT * FROM table1 WHERE table1.col1 = table2.col1 "
            "AND table1.col2 = :col2_1)",
        )

    def test_this_thing(self):
        s = select(t1).where(t1.c.col1 == "foo").alias()
        s2 = select(s.c.col1)

        self.assert_compile(
            s2,
            "SELECT anon_1.col1 FROM (SELECT "
            "table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 WHERE "
            "table1.col1 = :col1_1) AS anon_1",
        )
        t1a = t1.alias()
        s2 = sql_util.ClauseAdapter(t1a).traverse(s2)
        self.assert_compile(
            s2,
            "SELECT anon_1.col1 FROM (SELECT "
            "table1_1.col1 AS col1, table1_1.col2 AS "
            "col2, table1_1.col3 AS col3 FROM table1 "
            "AS table1_1 WHERE table1_1.col1 = "
            ":col1_1) AS anon_1",
        )

    def test_this_thing_using_setup_joins_one(self):
        s = select(t1).join_from(t1, t2, t1.c.col1 == t2.c.col2).subquery()
        s2 = select(s.c.col1).join_from(t3, s, t3.c.col2 == s.c.col1)

        self.assert_compile(
            s2,
            "SELECT anon_1.col1 FROM table3 JOIN (SELECT table1.col1 AS "
            "col1, table1.col2 AS col2, table1.col3 AS col3 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2) AS anon_1 "
            "ON table3.col2 = anon_1.col1",
        )
        t1a = t1.alias()
        s2 = sql_util.ClauseAdapter(t1a).traverse(s2)
        self.assert_compile(
            s2,
            "SELECT anon_1.col1 FROM table3 JOIN (SELECT table1_1.col1 AS "
            "col1, table1_1.col2 AS col2, table1_1.col3 AS col3 "
            "FROM table1 AS table1_1 JOIN table2 ON table1_1.col1 = "
            "table2.col2) AS anon_1 ON table3.col2 = anon_1.col1",
        )

    def test_this_thing_using_setup_joins_two(self):
        s = select(t1.c.col1).join(t2, t1.c.col1 == t2.c.col2).subquery()
        s2 = select(s.c.col1)

        self.assert_compile(
            s2,
            "SELECT anon_1.col1 FROM (SELECT table1.col1 AS col1 "
            "FROM table1 JOIN table2 ON table1.col1 = table2.col2) AS anon_1",
        )

        t1alias = t1.alias("t1alias")
        j = t1.join(t1alias, t1.c.col1 == t1alias.c.col2)

        vis = sql_util.ClauseAdapter(j)

        s2 = vis.traverse(s2)
        self.assert_compile(
            s2,
            "SELECT anon_1.col1 FROM (SELECT table1.col1 AS col1 "
            "FROM table1 JOIN table1 AS t1alias "
            "ON table1.col1 = t1alias.col2 "
            "JOIN table2 ON table1.col1 = table2.col2) AS anon_1",
        )

    def test_this_thing_using_setup_joins_three(self):

        j = t1.join(t2, t1.c.col1 == t2.c.col2)

        s1 = select(j)

        s2 = s1.join(t3, t1.c.col1 == t3.c.col1)

        self.assert_compile(
            s2,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 JOIN table3 "
            "ON table3.col1 = table1.col1",
        )

        vis = sql_util.ClauseAdapter(j)

        s3 = vis.traverse(s1)

        s4 = s3.join(t3, t1.c.col1 == t3.c.col1)

        self.assert_compile(
            s4,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 JOIN table3 "
            "ON table3.col1 = table1.col1",
        )

        s5 = vis.traverse(s3)

        s6 = s5.join(t3, t1.c.col1 == t3.c.col1)

        self.assert_compile(
            s6,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 JOIN table3 "
            "ON table3.col1 = table1.col1",
        )

    def test_this_thing_using_setup_joins_four(self):

        j = t1.join(t2, t1.c.col1 == t2.c.col2)

        s1 = select(j)

        assert not s1._from_obj

        s2 = s1.join(t3, t1.c.col1 == t3.c.col1)

        self.assert_compile(
            s2,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 JOIN table3 "
            "ON table3.col1 = table1.col1",
        )

        s3 = visitors.replacement_traverse(s1, {}, lambda elem: None)

        s4 = s3.join(t3, t1.c.col1 == t3.c.col1)

        self.assert_compile(
            s4,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 JOIN table3 "
            "ON table3.col1 = table1.col1",
        )

        s5 = visitors.replacement_traverse(s3, {}, lambda elem: None)

        s6 = s5.join(t3, t1.c.col1 == t3.c.col1)

        self.assert_compile(
            s6,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 JOIN table3 "
            "ON table3.col1 = table1.col1",
        )

    def test_select_fromtwice_one(self):
        t1a = t1.alias()

        s = (
            select(1)
            .where(t1.c.col1 == t1a.c.col1)
            .select_from(t1a)
            .correlate(t1a)
        )
        s = select(t1).where(t1.c.col1 == s.scalar_subquery())
        self.assert_compile(
            s,
            "SELECT table1.col1, table1.col2, table1.col3 FROM table1 "
            "WHERE table1.col1 = "
            "(SELECT 1 FROM table1, table1 AS table1_1 "
            "WHERE table1.col1 = table1_1.col1)",
        )
        s = CloningVisitor().traverse(s)
        self.assert_compile(
            s,
            "SELECT table1.col1, table1.col2, table1.col3 FROM table1 "
            "WHERE table1.col1 = "
            "(SELECT 1 FROM table1, table1 AS table1_1 "
            "WHERE table1.col1 = table1_1.col1)",
        )

    def test_select_fromtwice_two(self):
        s = select(t1).where(t1.c.col1 == "foo").alias()

        s2 = (
            select(1).where(t1.c.col1 == s.c.col1).select_from(s).correlate(t1)
        )
        s3 = select(t1).where(t1.c.col1 == s2.scalar_subquery())
        self.assert_compile(
            s3,
            "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 WHERE table1.col1 = "
            "(SELECT 1 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 "
            "WHERE table1.col1 = :col1_1) "
            "AS anon_1 WHERE table1.col1 = anon_1.col1)",
        )

        s4 = ReplacingCloningVisitor().traverse(s3)
        self.assert_compile(
            s4,
            "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 WHERE table1.col1 = "
            "(SELECT 1 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 "
            "WHERE table1.col1 = :col1_1) "
            "AS anon_1 WHERE table1.col1 = anon_1.col1)",
        )

    def test_select_setup_joins_adapt_element_one(self):
        s = select(t1).join(t2, t1.c.col1 == t2.c.col2)

        t1a = t1.alias()

        s2 = sql_util.ClauseAdapter(t1a).traverse(s)

        self.assert_compile(
            s,
            "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 JOIN table2 ON table1.col1 = table2.col2",
        )
        self.assert_compile(
            s2,
            "SELECT table1_1.col1, table1_1.col2, table1_1.col3 "
            "FROM table1 AS table1_1 JOIN table2 "
            "ON table1_1.col1 = table2.col2",
        )

    def test_select_setup_joins_adapt_element_two(self):
        s = select(literal_column("1")).join_from(
            t1, t2, t1.c.col1 == t2.c.col2
        )

        t1a = t1.alias()

        s2 = sql_util.ClauseAdapter(t1a).traverse(s)

        self.assert_compile(
            s, "SELECT 1 FROM table1 JOIN table2 ON table1.col1 = table2.col2"
        )
        self.assert_compile(
            s2,
            "SELECT 1 FROM table1 AS table1_1 "
            "JOIN table2 ON table1_1.col1 = table2.col2",
        )

    def test_select_setup_joins_adapt_element_three(self):
        s = select(literal_column("1")).join_from(
            t1, t2, t1.c.col1 == t2.c.col2
        )

        t2a = t2.alias()

        s2 = sql_util.ClauseAdapter(t2a).traverse(s)

        self.assert_compile(
            s, "SELECT 1 FROM table1 JOIN table2 ON table1.col1 = table2.col2"
        )
        self.assert_compile(
            s2,
            "SELECT 1 FROM table1 "
            "JOIN table2 AS table2_1 ON table1.col1 = table2_1.col2",
        )

    def test_select_setup_joins_straight_clone(self):
        s = select(t1).join(t2, t1.c.col1 == t2.c.col2)

        s2 = CloningVisitor().traverse(s)

        self.assert_compile(
            s,
            "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 JOIN table2 ON table1.col1 = table2.col2",
        )
        self.assert_compile(
            s2,
            "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 JOIN table2 ON table1.col1 = table2.col2",
        )


class ColumnAdapterTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        global t1, t2
        t1 = table(
            "table1",
            column("col1"),
            column("col2"),
            column("col3"),
            column("col4"),
        )
        t2 = table("table2", column("col1"), column("col2"), column("col3"))

    def test_traverse_memoizes_w_columns(self):
        t1a = t1.alias()
        adapter = sql_util.ColumnAdapter(t1a, anonymize_labels=True)

        expr = select(t1a.c.col1).label("x")
        expr_adapted = adapter.traverse(expr)
        is_not(expr, expr_adapted)
        is_(adapter.columns[expr], expr_adapted)

    def test_traverse_memoizes_w_itself(self):
        t1a = t1.alias()
        adapter = sql_util.ColumnAdapter(t1a, anonymize_labels=True)

        expr = select(t1a.c.col1).label("x")
        expr_adapted = adapter.traverse(expr)
        is_not(expr, expr_adapted)
        is_(adapter.traverse(expr), expr_adapted)

    def test_columns_memoizes_w_itself(self):
        t1a = t1.alias()
        adapter = sql_util.ColumnAdapter(t1a, anonymize_labels=True)

        expr = select(t1a.c.col1).label("x")
        expr_adapted = adapter.columns[expr]
        is_not(expr, expr_adapted)
        is_(adapter.columns[expr], expr_adapted)

    def test_wrapping_fallthrough(self):
        t1a = t1.alias(name="t1a")
        t2a = t2.alias(name="t2a")
        a1 = sql_util.ColumnAdapter(t1a)

        s1 = (
            select(t1a.c.col1, t2a.c.col1)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        a2 = sql_util.ColumnAdapter(s1)
        a3 = a2.wrap(a1)
        a4 = a1.wrap(a2)
        a5 = a1.chain(a2)

        # t1.c.col1 -> s1.c.t1a_col1

        # adapted by a2
        is_(a3.columns[t1.c.col1], s1.c.t1a_col1)
        is_(a4.columns[t1.c.col1], s1.c.t1a_col1)

        # chaining can't fall through because a1 grabs it
        # first
        is_(a5.columns[t1.c.col1], t1a.c.col1)

        # t2.c.col1 -> s1.c.t2a_col1

        # adapted by a2
        is_(a3.columns[t2.c.col1], s1.c.t2a_col1)
        is_(a4.columns[t2.c.col1], s1.c.t2a_col1)
        # chaining, t2 hits s1
        is_(a5.columns[t2.c.col1], s1.c.t2a_col1)

        # t1.c.col2 -> t1a.c.col2

        # fallthrough to a1
        is_(a3.columns[t1.c.col2], t1a.c.col2)
        is_(a4.columns[t1.c.col2], t1a.c.col2)

        # chaining hits a1
        is_(a5.columns[t1.c.col2], t1a.c.col2)

        # t2.c.col2 -> t2.c.col2

        # fallthrough to no adaption
        is_(a3.columns[t2.c.col2], t2.c.col2)
        is_(a4.columns[t2.c.col2], t2.c.col2)

    def test_wrapping_ordering(self):
        """illustrate an example where order of wrappers matters.

        This test illustrates both the ordering being significant
        as well as a scenario where multiple translations are needed
        (e.g. wrapping vs. chaining).

        """

        stmt = (
            select(t1.c.col1, t2.c.col1)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        sa = stmt.alias()
        stmt2 = select(t2, sa).subquery()

        a1 = sql_util.ColumnAdapter(stmt)
        a2 = sql_util.ColumnAdapter(stmt2)

        a2_to_a1 = a2.wrap(a1)
        a1_to_a2 = a1.wrap(a2)

        # when stmt2 and stmt represent the same column
        # in different contexts, order of wrapping matters

        # t2.c.col1 via a2 is stmt2.c.col1; then ignored by a1
        is_(a2_to_a1.columns[t2.c.col1], stmt2.c.col1)
        # t2.c.col1 via a1 is stmt.c.table2_col1; a2 then
        # sends this to stmt2.c.table2_col1
        is_(a1_to_a2.columns[t2.c.col1], stmt2.c.table2_col1)

        # check that these aren't the same column
        is_not(stmt2.c.col1, stmt2.c.table2_col1)

        # for mutually exclusive columns, order doesn't matter
        is_(a2_to_a1.columns[t1.c.col1], stmt2.c.table1_col1)
        is_(a1_to_a2.columns[t1.c.col1], stmt2.c.table1_col1)
        is_(a2_to_a1.columns[t2.c.col2], stmt2.c.col2)

    def test_wrapping_multiple(self):
        """illustrate that wrapping runs both adapters"""

        t1a = t1.alias(name="t1a")
        t2a = t2.alias(name="t2a")
        a1 = sql_util.ColumnAdapter(t1a)
        a2 = sql_util.ColumnAdapter(t2a)
        a3 = a2.wrap(a1)

        stmt = select(t1.c.col1, t2.c.col2)

        self.assert_compile(
            a3.traverse(stmt),
            "SELECT t1a.col1, t2a.col2 FROM table1 AS t1a, table2 AS t2a",
        )

        # chaining does too because these adapters don't share any
        # columns
        a4 = a2.chain(a1)
        self.assert_compile(
            a4.traverse(stmt),
            "SELECT t1a.col1, t2a.col2 FROM table1 AS t1a, table2 AS t2a",
        )

    def test_wrapping_inclusions(self):
        """test wrapping and inclusion rules together,
        taking into account multiple objects with equivalent hash identity."""

        t1a = t1.alias(name="t1a")
        t2a = t2.alias(name="t2a")
        a1 = sql_util.ColumnAdapter(
            t1a, include_fn=lambda col: "a1" in col._annotations
        )

        s1 = (
            select(t1a, t2a)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias()
        )
        a2 = sql_util.ColumnAdapter(
            s1, include_fn=lambda col: "a2" in col._annotations
        )
        a3 = a2.wrap(a1)

        c1a1 = t1.c.col1._annotate(dict(a1=True))
        c1a2 = t1.c.col1._annotate(dict(a2=True))
        c1aa = t1.c.col1._annotate(dict(a1=True, a2=True))

        c2a1 = t2.c.col1._annotate(dict(a1=True))
        c2a2 = t2.c.col1._annotate(dict(a2=True))
        c2aa = t2.c.col1._annotate(dict(a1=True, a2=True))

        is_(a3.columns[c1a1], t1a.c.col1)
        is_(a3.columns[c1a2], s1.c.t1a_col1)
        is_(a3.columns[c1aa], s1.c.t1a_col1)

        # not covered by a1, accepted by a2
        is_(a3.columns[c2aa], s1.c.t2a_col1)

        # not covered by a1, accepted by a2
        is_(a3.columns[c2a2], s1.c.t2a_col1)
        # not covered by a1, rejected by a2
        is_(a3.columns[c2a1], c2a1)


class ClauseAdapterTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        global t1, t2
        t1 = table("table1", column("col1"), column("col2"), column("col3"))
        t2 = table("table2", column("col1"), column("col2"), column("col3"))

    def test_correlation_on_clone(self):
        t1alias = t1.alias("t1alias")
        t2alias = t2.alias("t2alias")
        vis = sql_util.ClauseAdapter(t1alias)

        s = (
            select(literal_column("*"))
            .select_from(t1alias, t2alias)
            .scalar_subquery()
        )

        froms = list(s._iterate_from_elements())
        assert t2alias in froms
        assert t1alias in froms

        self.assert_compile(
            select(literal_column("*")).where(t2alias.c.col1 == s),
            "SELECT * FROM table2 AS t2alias WHERE "
            "t2alias.col1 = (SELECT * FROM table1 AS "
            "t1alias)",
        )
        s = vis.traverse(s)

        froms = list(s._iterate_from_elements())
        assert t2alias in froms  # present because it was not cloned
        assert t1alias in froms  # present because the adapter placed
        # it there and was also not cloned

        # correlate list on "s" needs to take into account the full
        # _cloned_set for each element in _froms when correlating

        self.assert_compile(
            select(literal_column("*")).where(t2alias.c.col1 == s),
            "SELECT * FROM table2 AS t2alias WHERE "
            "t2alias.col1 = (SELECT * FROM table1 AS "
            "t1alias)",
        )
        s = (
            select(literal_column("*"))
            .select_from(t1alias, t2alias)
            .correlate(t2alias)
            .scalar_subquery()
        )
        self.assert_compile(
            select(literal_column("*")).where(t2alias.c.col1 == s),
            "SELECT * FROM table2 AS t2alias WHERE "
            "t2alias.col1 = (SELECT * FROM table1 AS "
            "t1alias)",
        )
        s = vis.traverse(s)
        self.assert_compile(
            select(literal_column("*")).where(t2alias.c.col1 == s),
            "SELECT * FROM table2 AS t2alias WHERE "
            "t2alias.col1 = (SELECT * FROM table1 AS "
            "t1alias)",
        )
        s = CloningVisitor().traverse(s)
        self.assert_compile(
            select(literal_column("*")).where(t2alias.c.col1 == s),
            "SELECT * FROM table2 AS t2alias WHERE "
            "t2alias.col1 = (SELECT * FROM table1 AS "
            "t1alias)",
        )

        s = (
            select(literal_column("*"))
            .where(t1.c.col1 == t2.c.col1)
            .scalar_subquery()
        )
        self.assert_compile(
            select(t1.c.col1, s),
            "SELECT table1.col1, (SELECT * FROM table2 "
            "WHERE table1.col1 = table2.col1) AS "
            "anon_1 FROM table1",
        )
        vis = sql_util.ClauseAdapter(t1alias)
        s = vis.traverse(s)
        self.assert_compile(
            select(t1alias.c.col1, s),
            "SELECT t1alias.col1, (SELECT * FROM "
            "table2 WHERE t1alias.col1 = table2.col1) "
            "AS anon_1 FROM table1 AS t1alias",
        )
        s = CloningVisitor().traverse(s)
        self.assert_compile(
            select(t1alias.c.col1, s),
            "SELECT t1alias.col1, (SELECT * FROM "
            "table2 WHERE t1alias.col1 = table2.col1) "
            "AS anon_1 FROM table1 AS t1alias",
        )
        s = (
            select(literal_column("*"))
            .where(t1.c.col1 == t2.c.col1)
            .correlate(t1)
            .scalar_subquery()
        )
        self.assert_compile(
            select(t1.c.col1, s),
            "SELECT table1.col1, (SELECT * FROM table2 "
            "WHERE table1.col1 = table2.col1) AS "
            "anon_1 FROM table1",
        )
        vis = sql_util.ClauseAdapter(t1alias)
        s = vis.traverse(s)
        self.assert_compile(
            select(t1alias.c.col1, s),
            "SELECT t1alias.col1, (SELECT * FROM "
            "table2 WHERE t1alias.col1 = table2.col1) "
            "AS anon_1 FROM table1 AS t1alias",
        )
        s = CloningVisitor().traverse(s)
        self.assert_compile(
            select(t1alias.c.col1, s),
            "SELECT t1alias.col1, (SELECT * FROM "
            "table2 WHERE t1alias.col1 = table2.col1) "
            "AS anon_1 FROM table1 AS t1alias",
        )

    def test_adapt_select_w_unlabeled_fn(self):

        expr = func.count(t1.c.col1)
        stmt = select(t1, expr)

        self.assert_compile(
            stmt,
            "SELECT table1.col1, table1.col2, table1.col3, "
            "count(table1.col1) AS count_1 FROM table1",
        )

        stmt2 = select(stmt.subquery())

        self.assert_compile(
            stmt2,
            "SELECT anon_1.col1, anon_1.col2, anon_1.col3, anon_1.count_1 "
            "FROM (SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3, count(table1.col1) AS count_1 "
            "FROM table1) AS anon_1",
        )

        is_(
            stmt2.selected_columns[3],
            stmt2.selected_columns.corresponding_column(expr),
        )

        is_(
            sql_util.ClauseAdapter(stmt2).replace(expr),
            stmt2.selected_columns[3],
        )

        column_adapter = sql_util.ColumnAdapter(stmt2)
        is_(column_adapter.columns[expr], stmt2.selected_columns[3])

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_correlate_except_on_clone(self, use_adapt_from):
        # test [ticket:4537]'s issue

        t1alias = t1.alias("t1alias")
        j = t1.join(t1alias, t1.c.col1 == t1alias.c.col2)

        if use_adapt_from:
            vis = sql_util.ClauseAdapter(j, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(j)

        # "control" subquery - uses correlate which has worked w/ adaption
        # for a long time
        control_s = (
            select(t2.c.col1)
            .where(t2.c.col1 == t1.c.col1)
            .correlate(t2)
            .scalar_subquery()
        )

        # test subquery - given only t1 and t2 in the enclosing selectable,
        # will do the same thing as the "control" query since the correlation
        # works out the same
        s = (
            select(t2.c.col1)
            .where(t2.c.col1 == t1.c.col1)
            .correlate_except(t1)
            .scalar_subquery()
        )

        # use both subqueries in statements
        control_stmt = select(control_s, t1.c.col1, t2.c.col1).select_from(
            t1.join(t2, t1.c.col1 == t2.c.col1)
        )

        stmt = select(s, t1.c.col1, t2.c.col1).select_from(
            t1.join(t2, t1.c.col1 == t2.c.col1)
        )
        # they are the same
        self.assert_compile(
            control_stmt,
            "SELECT "
            "(SELECT table2.col1 FROM table1 "
            "WHERE table2.col1 = table1.col1) AS anon_1, "
            "table1.col1, table2.col1 AS col1_1 "
            "FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col1",
        )
        self.assert_compile(
            stmt,
            "SELECT "
            "(SELECT table2.col1 FROM table1 "
            "WHERE table2.col1 = table1.col1) AS anon_1, "
            "table1.col1, table2.col1 AS col1_1 "
            "FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col1",
        )

        # now test against the adaption of "t1" into "t1 JOIN t1alias".
        # note in the control case, we aren't actually testing that
        # Select is processing the "correlate" list during the adaption
        # since we aren't adapting the "correlate"
        self.assert_compile(
            vis.traverse(control_stmt),
            "SELECT "
            "(SELECT table2.col1 FROM "
            "table1 JOIN table1 AS t1alias ON table1.col1 = t1alias.col2 "
            "WHERE table2.col1 = table1.col1) AS anon_1, "
            "table1.col1, table2.col1 AS col1_1 "
            "FROM table1 JOIN table1 AS t1alias ON table1.col1 = t1alias.col2 "
            "JOIN table2 ON table1.col1 = table2.col1",
        )

        # but here, correlate_except() does have the thing we're adapting
        # so whatever is in there has to be expanded out to include
        # the adaptation target, in this case "t1 JOIN t1alias".
        self.assert_compile(
            vis.traverse(stmt),
            "SELECT "
            "(SELECT table2.col1 FROM "
            "table1 JOIN table1 AS t1alias ON table1.col1 = t1alias.col2 "
            "WHERE table2.col1 = table1.col1) AS anon_1, "
            "table1.col1, table2.col1 AS col1_1 "
            "FROM table1 JOIN table1 AS t1alias ON table1.col1 = t1alias.col2 "
            "JOIN table2 ON table1.col1 = table2.col1",
        )

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_correlate_except_with_mixed_tables(self, use_adapt_from):
        # test [ticket:6060]'s issue

        stmt = select(
            t1.c.col1,
            select(func.count(t2.c.col1))
            .where(t2.c.col1 == t1.c.col1)
            .correlate_except(t2)
            .scalar_subquery(),
        )
        self.assert_compile(
            stmt,
            "SELECT table1.col1, "
            "(SELECT count(table2.col1) AS count_1 FROM table2 "
            "WHERE table2.col1 = table1.col1) AS anon_1 "
            "FROM table1",
        )

        subq = (
            select(t1)
            .join(t2, t1.c.col1 == t2.c.col1)
            .where(t2.c.col2 == "x")
            .subquery()
        )

        if use_adapt_from:
            vis = sql_util.ClauseAdapter(subq, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(subq)

        if use_adapt_from:
            self.assert_compile(
                vis.traverse(stmt),
                "SELECT anon_1.col1, "
                "(SELECT count(table2.col1) AS count_1 FROM table2 WHERE "
                "table2.col1 = anon_1.col1) AS anon_2 "
                "FROM (SELECT table1.col1 AS col1, table1.col2 AS col2, "
                "table1.col3 AS col3 FROM table1 JOIN table2 ON table1.col1 = "
                "table2.col1 WHERE table2.col2 = :col2_1) AS anon_1",
            )
        else:
            # here's the buggy version.  table2 gets yanked out of the
            # correlated subquery also.  AliasedClass now uses
            # adapt_from_selectables in all cases
            self.assert_compile(
                vis.traverse(stmt),
                "SELECT anon_1.col1, "
                "(SELECT count(table2.col1) AS count_1 FROM table2, "
                "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
                "table1.col3 AS col3 FROM table1 JOIN table2 ON "
                "table1.col1 = table2.col1 WHERE table2.col2 = :col2_1) AS "
                "anon_1 WHERE table2.col1 = anon_1.col1) AS anon_2 "
                "FROM (SELECT table1.col1 AS col1, table1.col2 AS col2, "
                "table1.col3 AS col3 FROM table1 JOIN table2 "
                "ON table1.col1 = table2.col1 "
                "WHERE table2.col2 = :col2_1) AS anon_1",
            )

    @testing.fails_on_everything_except()
    def test_joins_dont_adapt(self):
        # adapting to a join, i.e. ClauseAdapter(t1.join(t2)), doesn't
        # make much sense. ClauseAdapter doesn't make any changes if
        # it's against a straight join.

        users = table("users", column("id"))
        addresses = table("addresses", column("id"), column("user_id"))

        ualias = users.alias()

        s = (
            select(func.count(addresses.c.id))
            .where(users.c.id == addresses.c.user_id)
            .correlate(users)
        )
        s = sql_util.ClauseAdapter(ualias).traverse(s)

        j1 = addresses.join(ualias, addresses.c.user_id == ualias.c.id)

        self.assert_compile(
            sql_util.ClauseAdapter(j1).traverse(s),
            "SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE users_1.id = "
            "addresses.user_id",
        )

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_table_to_alias_1(self, use_adapt_from):
        t1alias = t1.alias("t1alias")

        if use_adapt_from:
            vis = sql_util.ClauseAdapter(t1alias, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(t1alias)
        ff = vis.traverse(func.count(t1.c.col1).label("foo"))
        assert list(_from_objects(ff)) == [t1alias]

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_table_to_alias_2(self, use_adapt_from):
        t1alias = t1.alias("t1alias")
        if use_adapt_from:
            vis = sql_util.ClauseAdapter(t1alias, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(select(literal_column("*")).select_from(t1)),
            "SELECT * FROM table1 AS t1alias",
        )

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_table_to_alias_3(self, use_adapt_from):
        t1alias = t1.alias("t1alias")
        if use_adapt_from:
            vis = sql_util.ClauseAdapter(t1alias, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(
                select(literal_column("*")).where(t1.c.col1 == t2.c.col2)
            ),
            "SELECT * FROM table1 AS t1alias, table2 "
            "WHERE t1alias.col1 = table2.col2",
        )

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_table_to_alias_4(self, use_adapt_from):
        t1alias = t1.alias("t1alias")
        if use_adapt_from:
            vis = sql_util.ClauseAdapter(t1alias, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(
                select(literal_column("*"))
                .where(t1.c.col1 == t2.c.col2)
                .select_from(t1, t2)
            ),
            "SELECT * FROM table1 AS t1alias, table2 "
            "WHERE t1alias.col1 = table2.col2",
        )

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_table_to_alias_5(self, use_adapt_from):
        t1alias = t1.alias("t1alias")
        if use_adapt_from:
            vis = sql_util.ClauseAdapter(t1alias, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            select(t1alias, t2).where(
                t1alias.c.col1
                == vis.traverse(
                    select(literal_column("*"))
                    .where(t1.c.col1 == t2.c.col2)
                    .select_from(t1, t2)
                    .correlate(t1)
                    .scalar_subquery()
                )
            ),
            "SELECT t1alias.col1, t1alias.col2, t1alias.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 "
            "FROM table1 AS t1alias, table2 WHERE t1alias.col1 = "
            "(SELECT * FROM table2 WHERE t1alias.col1 = table2.col2)",
        )

    @testing.combinations((True,), (False,), argnames="use_adapt_from")
    def test_table_to_alias_6(self, use_adapt_from):
        t1alias = t1.alias("t1alias")
        if use_adapt_from:
            vis = sql_util.ClauseAdapter(t1alias, adapt_from_selectables=[t1])
        else:
            vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            select(t1alias, t2).where(
                t1alias.c.col1
                == vis.traverse(
                    select(literal_column("*"))
                    .where(t1.c.col1 == t2.c.col2)
                    .select_from(t1, t2)
                    .correlate(t2)
                    .scalar_subquery()
                )
            ),
            "SELECT t1alias.col1, t1alias.col2, t1alias.col3, "
            "table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 "
            "FROM table1 AS t1alias, table2 "
            "WHERE t1alias.col1 = "
            "(SELECT * FROM table1 AS t1alias "
            "WHERE t1alias.col1 = table2.col2)",
        )

    def test_table_to_alias_7(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(case((t1.c.col1 == 5, t1.c.col2), else_=t1.c.col1)),
            "CASE WHEN (t1alias.col1 = :col1_1) THEN "
            "t1alias.col2 ELSE t1alias.col1 END",
        )

    def test_table_to_alias_8(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(
                case((5, t1.c.col2), value=t1.c.col1, else_=t1.c.col1)
            ),
            "CASE t1alias.col1 WHEN :param_1 THEN "
            "t1alias.col2 ELSE t1alias.col1 END",
        )

    def test_table_to_alias_9(self):
        s = select(literal_column("*")).select_from(t1).alias("foo")
        self.assert_compile(
            s.select(), "SELECT foo.* FROM (SELECT * FROM table1) " "AS foo"
        )

    def test_table_to_alias_10(self):
        s = select(literal_column("*")).select_from(t1).alias("foo")
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(s.select()),
            "SELECT foo.* FROM (SELECT * FROM table1 " "AS t1alias) AS foo",
        )

    def test_table_to_alias_11(self):
        s = select(literal_column("*")).select_from(t1).alias("foo")
        self.assert_compile(
            s.select(), "SELECT foo.* FROM (SELECT * FROM table1) " "AS foo"
        )

    def test_table_to_alias_12(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        ff = vis.traverse(func.count(t1.c.col1).label("foo"))
        self.assert_compile(
            select(ff),
            "SELECT count(t1alias.col1) AS foo FROM " "table1 AS t1alias",
        )
        assert list(_from_objects(ff)) == [t1alias]

    # def test_table_to_alias_2(self):
    # TODO: self.assert_compile(vis.traverse(select(func.count(t1.c
    # .col1).l abel('foo')), clone=True), "SELECT
    # count(t1alias.col1) AS foo FROM table1 AS t1alias")

    def test_table_to_alias_13(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias("t2alias")
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            vis.traverse(
                select(literal_column("*")).where(t1.c.col1 == t2.c.col2)
            ),
            "SELECT * FROM table1 AS t1alias, table2 "
            "AS t2alias WHERE t1alias.col1 = "
            "t2alias.col2",
        )

    def test_table_to_alias_14(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias("t2alias")
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            vis.traverse(
                select("*").where(t1.c.col1 == t2.c.col2).select_from(t1, t2)
            ),
            "SELECT * FROM table1 AS t1alias, table2 "
            "AS t2alias WHERE t1alias.col1 = "
            "t2alias.col2",
        )

    def test_table_to_alias_15(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias("t2alias")
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            select(t1alias, t2alias).where(
                t1alias.c.col1
                == vis.traverse(
                    select("*")
                    .where(t1.c.col1 == t2.c.col2)
                    .select_from(t1, t2)
                    .correlate(t1)
                    .scalar_subquery()
                )
            ),
            "SELECT t1alias.col1, t1alias.col2, t1alias.col3, "
            "t2alias.col1 AS col1_1, t2alias.col2 AS col2_1, "
            "t2alias.col3 AS col3_1 "
            "FROM table1 AS t1alias, table2 AS t2alias "
            "WHERE t1alias.col1 = "
            "(SELECT * FROM table2 AS t2alias "
            "WHERE t1alias.col1 = t2alias.col2)",
        )

    def test_table_to_alias_16(self):
        t1alias = t1.alias("t1alias")
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias("t2alias")
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            t2alias.select().where(
                t2alias.c.col2
                == vis.traverse(
                    select("*")
                    .where(t1.c.col1 == t2.c.col2)
                    .select_from(t1, t2)
                    .correlate(t2)
                    .scalar_subquery()
                )
            ),
            "SELECT t2alias.col1, t2alias.col2, t2alias.col3 "
            "FROM table2 AS t2alias WHERE t2alias.col2 = "
            "(SELECT * FROM table1 AS t1alias WHERE "
            "t1alias.col1 = t2alias.col2)",
        )

    def test_include_exclude(self):
        m = MetaData()
        a = Table(
            "a",
            m,
            Column("id", Integer, primary_key=True),
            Column(
                "xxx_id",
                Integer,
                ForeignKey("a.id", name="adf", use_alter=True),
            ),
        )

        e = a.c.id == a.c.xxx_id
        assert str(e) == "a.id = a.xxx_id"
        b = a.alias()

        e = sql_util.ClauseAdapter(
            b,
            include_fn=lambda x: x in set([a.c.id]),
            equivalents={a.c.id: set([a.c.id])},
        ).traverse(e)

        assert str(e) == "a_1.id = a.xxx_id"

    def test_recursive_equivalents(self):
        m = MetaData()
        a = Table("a", m, Column("x", Integer), Column("y", Integer))
        b = Table("b", m, Column("x", Integer), Column("y", Integer))
        c = Table("c", m, Column("x", Integer), Column("y", Integer))

        # force a recursion overflow, by linking a.c.x<->c.c.x, and
        # asking for a nonexistent col.  corresponding_column should prevent
        # endless depth.
        adapt = sql_util.ClauseAdapter(
            b, equivalents={a.c.x: set([c.c.x]), c.c.x: set([a.c.x])}
        )
        assert adapt._corresponding_column(a.c.x, False) is None

    def test_multilevel_equivalents(self):
        m = MetaData()
        a = Table("a", m, Column("x", Integer), Column("y", Integer))
        b = Table("b", m, Column("x", Integer), Column("y", Integer))
        c = Table("c", m, Column("x", Integer), Column("y", Integer))

        alias = select(a).select_from(a.join(b, a.c.x == b.c.x)).alias()

        # two levels of indirection from c.x->b.x->a.x, requires recursive
        # corresponding_column call
        adapt = sql_util.ClauseAdapter(
            alias, equivalents={b.c.x: set([a.c.x]), c.c.x: set([b.c.x])}
        )
        assert adapt._corresponding_column(a.c.x, False) is alias.c.x
        assert adapt._corresponding_column(c.c.x, False) is alias.c.x

    def test_join_to_alias(self):
        metadata = MetaData()
        a = Table("a", metadata, Column("id", Integer, primary_key=True))
        b = Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", Integer, ForeignKey("a.id")),
        )
        c = Table(
            "c",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("bid", Integer, ForeignKey("b.id")),
        )

        d = Table(
            "d",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", Integer, ForeignKey("a.id")),
        )

        j1 = a.outerjoin(b)
        j2 = (
            select(j1)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        j3 = c.join(j2, j2.c.b_id == c.c.bid)

        j4 = j3.outerjoin(d)
        self.assert_compile(
            j4,
            "c JOIN (SELECT a.id AS a_id, b.id AS "
            "b_id, b.aid AS b_aid FROM a LEFT OUTER "
            "JOIN b ON a.id = b.aid) AS anon_1 ON anon_1.b_id = c.bid "
            "LEFT OUTER JOIN d ON anon_1.a_id = d.aid",
        )
        j5 = (
            j3.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery("foo")
        )
        j6 = sql_util.ClauseAdapter(j5).copy_and_process([j4])[0]

        # this statement takes c join(a join b), wraps it inside an
        # aliased "select * from c join(a join b) AS foo". the outermost
        # right side "left outer join d" stays the same, except "d"
        # joins against foo.a_id instead of plain "a_id"

        self.assert_compile(
            j6,
            "(SELECT c.id AS c_id, c.bid AS c_bid, "
            "anon_1.a_id AS anon_1_a_id, anon_1.b_id AS anon_1_b_id, "
            "anon_1.b_aid AS "
            "anon_1_b_aid FROM c JOIN (SELECT a.id AS a_id, "
            "b.id AS b_id, b.aid AS b_aid FROM a LEFT "
            "OUTER JOIN b ON a.id = b.aid) AS anon_1 ON anon_1.b_id = "
            "c.bid) AS foo LEFT OUTER JOIN d ON "
            "foo.anon_1_a_id = d.aid",
        )

    def test_derived_from(self):
        assert select(t1).is_derived_from(t1)
        assert not select(t2).is_derived_from(t1)
        assert not t1.is_derived_from(select(t1))
        assert t1.alias().is_derived_from(t1)

        s1 = select(t1, t2).alias("foo")
        s2 = select(s1).limit(5).offset(10).alias()
        assert s2.is_derived_from(s1)
        s2 = s2._clone()
        assert s2.is_derived_from(s1)

    def test_aliasedselect_to_aliasedselect_straight(self):

        # original issue from ticket #904

        s1 = select(t1).alias("foo")
        s2 = select(s1).limit(5).offset(10).alias()
        self.assert_compile(
            sql_util.ClauseAdapter(s2).traverse(s1),
            "SELECT foo.col1, foo.col2, foo.col3 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 "
            "AS col2, table1.col3 AS col3 FROM table1) "
            "AS foo LIMIT :param_1 OFFSET :param_2",
            {"param_1": 5, "param_2": 10},
        )

    def test_aliasedselect_to_aliasedselect_join(self):
        s1 = select(t1).alias("foo")
        s2 = select(s1).limit(5).offset(10).alias()
        j = s1.outerjoin(t2, s1.c.col1 == t2.c.col1)
        self.assert_compile(
            sql_util.ClauseAdapter(s2).traverse(j).select(),
            "SELECT anon_1.col1, anon_1.col2, "
            "anon_1.col3, table2.col1 AS col1_1, table2.col2 AS col2_1, "
            "table2.col3 AS col3_1 FROM (SELECT foo.col1 AS "
            "col1, foo.col2 AS col2, foo.col3 AS col3 "
            "FROM (SELECT table1.col1 AS col1, "
            "table1.col2 AS col2, table1.col3 AS col3 "
            "FROM table1) AS foo LIMIT :param_1 OFFSET "
            ":param_2) AS anon_1 LEFT OUTER JOIN "
            "table2 ON anon_1.col1 = table2.col1",
            {"param_1": 5, "param_2": 10},
        )

    def test_aliasedselect_to_aliasedselect_join_nested_table(self):
        s1 = select(t1).alias("foo")
        s2 = select(s1).limit(5).offset(10).alias()
        talias = t1.alias("bar")

        # here is the problem.   s2 is derived from s1 which is derived
        # from t1
        assert s2.is_derived_from(t1)

        # however, s2 is not derived from talias, which *is* derived from t1
        assert not s2.is_derived_from(talias)

        # therefore, talias gets its table replaced, except for a rule
        # we added to ClauseAdapter to stop traversal if the selectable is
        # not derived from an alias of a table.  This rule was previously
        # in Alias._copy_internals().

        j = s1.outerjoin(talias, s1.c.col1 == talias.c.col1)

        self.assert_compile(
            sql_util.ClauseAdapter(s2).traverse(j).select(),
            "SELECT anon_1.col1, anon_1.col2, "
            "anon_1.col3, bar.col1 AS col1_1, bar.col2 AS col2_1, "
            "bar.col3 AS col3_1 "
            "FROM (SELECT foo.col1 AS col1, foo.col2 "
            "AS col2, foo.col3 AS col3 FROM (SELECT "
            "table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1) AS foo "
            "LIMIT :param_1 OFFSET :param_2) AS anon_1 "
            "LEFT OUTER JOIN table1 AS bar ON "
            "anon_1.col1 = bar.col1",
            {"param_1": 5, "param_2": 10},
        )

    def test_functions(self):
        self.assert_compile(
            sql_util.ClauseAdapter(t1.alias()).traverse(func.count(t1.c.col1)),
            "count(table1_1.col1)",
        )
        s = select(func.count(t1.c.col1))
        self.assert_compile(
            sql_util.ClauseAdapter(t1.alias()).traverse(s),
            "SELECT count(table1_1.col1) AS count_1 "
            "FROM table1 AS table1_1",
        )

    def test_recursive(self):
        metadata = MetaData()
        a = Table("a", metadata, Column("id", Integer, primary_key=True))
        b = Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", Integer, ForeignKey("a.id")),
        )
        c = Table(
            "c",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("bid", Integer, ForeignKey("b.id")),
        )

        d = Table(
            "d",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", Integer, ForeignKey("a.id")),
        )

        u = union(
            a.join(b).select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            a.join(d).select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        ).alias()

        self.assert_compile(
            sql_util.ClauseAdapter(u).traverse(
                select(c.c.bid).where(c.c.bid == u.c.b_aid)
            ),
            "SELECT c.bid "
            "FROM c, (SELECT a.id AS a_id, b.id AS b_id, b.aid AS b_aid "
            "FROM a JOIN b ON a.id = b.aid UNION SELECT a.id AS a_id, d.id "
            "AS d_id, d.aid AS d_aid "
            "FROM a JOIN d ON a.id = d.aid) AS anon_1 "
            "WHERE c.bid = anon_1.b_aid",
        )

    def test_label_anonymize_one(self):
        t1a = t1.alias()
        adapter = sql_util.ClauseAdapter(t1a, anonymize_labels=True)

        expr = select(t1.c.col2).where(t1.c.col3 == 5).label("expr")
        expr_adapted = adapter.traverse(expr)

        stmt = select(expr, expr_adapted).order_by(expr, expr_adapted)
        self.assert_compile(
            stmt,
            "SELECT "
            "(SELECT table1.col2 FROM table1 WHERE table1.col3 = :col3_1) "
            "AS expr, "
            "(SELECT table1_1.col2 FROM table1 AS table1_1 "
            "WHERE table1_1.col3 = :col3_2) AS anon_1 "
            "ORDER BY expr, anon_1",
        )

    def test_label_anonymize_two(self):
        t1a = t1.alias()
        adapter = sql_util.ClauseAdapter(t1a, anonymize_labels=True)

        expr = select(t1.c.col2).where(t1.c.col3 == 5).label(None)
        expr_adapted = adapter.traverse(expr)

        stmt = select(expr, expr_adapted).order_by(expr, expr_adapted)
        self.assert_compile(
            stmt,
            "SELECT "
            "(SELECT table1.col2 FROM table1 WHERE table1.col3 = :col3_1) "
            "AS anon_1, "
            "(SELECT table1_1.col2 FROM table1 AS table1_1 "
            "WHERE table1_1.col3 = :col3_2) AS anon_2 "
            "ORDER BY anon_1, anon_2",
        )

    def test_label_anonymize_three(self):
        t1a = t1.alias()
        adapter = sql_util.ColumnAdapter(
            t1a, anonymize_labels=True, allow_label_resolve=False
        )

        expr = select(t1.c.col2).where(t1.c.col3 == 5).label(None)
        l1 = expr
        is_(l1._order_by_label_element, l1)
        eq_(l1._allow_label_resolve, True)

        expr_adapted = adapter.traverse(expr)
        l2 = expr_adapted
        is_(l2._order_by_label_element, l2)
        eq_(l2._allow_label_resolve, False)

        l3 = adapter.traverse(expr)
        is_(l3._order_by_label_element, l3)
        eq_(l3._allow_label_resolve, False)


class SpliceJoinsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        global table1, table2, table3, table4

        def _table(name):
            return table(name, column("col1"), column("col2"), column("col3"))

        table1, table2, table3, table4 = [
            _table(name) for name in ("table1", "table2", "table3", "table4")
        ]

    def test_splice(self):
        t1, t2, t3, t4 = table1, table2, table1.alias(), table2.alias()
        j = (
            t1.join(t2, t1.c.col1 == t2.c.col1)
            .join(t3, t2.c.col1 == t3.c.col1)
            .join(t4, t4.c.col1 == t1.c.col1)
        )
        s = select(t1).where(t1.c.col2 < 5).alias()
        self.assert_compile(
            sql_util.splice_joins(s, j),
            "(SELECT table1.col1 AS col1, table1.col2 "
            "AS col2, table1.col3 AS col3 FROM table1 "
            "WHERE table1.col2 < :col2_1) AS anon_1 "
            "JOIN table2 ON anon_1.col1 = table2.col1 "
            "JOIN table1 AS table1_1 ON table2.col1 = "
            "table1_1.col1 JOIN table2 AS table2_1 ON "
            "table2_1.col1 = anon_1.col1",
        )

    def test_stop_on(self):
        t1, t2, t3 = table1, table2, table3
        j1 = t1.join(t2, t1.c.col1 == t2.c.col1)
        j2 = j1.join(t3, t2.c.col1 == t3.c.col1)
        s = select(t1).select_from(j1).alias()
        self.assert_compile(
            sql_util.splice_joins(s, j2),
            "(SELECT table1.col1 AS col1, table1.col2 "
            "AS col2, table1.col3 AS col3 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col1) "
            "AS anon_1 JOIN table2 ON anon_1.col1 = "
            "table2.col1 JOIN table3 ON table2.col1 = "
            "table3.col1",
        )
        self.assert_compile(
            sql_util.splice_joins(s, j2, j1),
            "(SELECT table1.col1 AS col1, table1.col2 "
            "AS col2, table1.col3 AS col3 FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col1) "
            "AS anon_1 JOIN table3 ON table2.col1 = "
            "table3.col1",
        )

    def test_splice_2(self):
        t2a = table2.alias()
        t3a = table3.alias()
        j1 = table1.join(t2a, table1.c.col1 == t2a.c.col1).join(
            t3a, t2a.c.col2 == t3a.c.col2
        )
        t2b = table4.alias()
        j2 = table1.join(t2b, table1.c.col3 == t2b.c.col3)
        self.assert_compile(
            sql_util.splice_joins(table1, j1),
            "table1 JOIN table2 AS table2_1 ON "
            "table1.col1 = table2_1.col1 JOIN table3 "
            "AS table3_1 ON table2_1.col2 = "
            "table3_1.col2",
        )
        self.assert_compile(
            sql_util.splice_joins(table1, j2),
            "table1 JOIN table4 AS table4_1 ON " "table1.col3 = table4_1.col3",
        )
        self.assert_compile(
            sql_util.splice_joins(sql_util.splice_joins(table1, j1), j2),
            "table1 JOIN table2 AS table2_1 ON "
            "table1.col1 = table2_1.col1 JOIN table3 "
            "AS table3_1 ON table2_1.col2 = "
            "table3_1.col2 JOIN table4 AS table4_1 ON "
            "table1.col3 = table4_1.col3",
        )


class SelectTest(fixtures.TestBase, AssertsCompiledSQL):

    """tests the generative capability of Select"""

    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        global t1, t2
        t1 = table("table1", column("col1"), column("col2"), column("col3"))
        t2 = table("table2", column("col1"), column("col2"), column("col3"))

    def test_columns(self):
        s = t1.select()
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, " "table1.col3 FROM table1"
        )
        select_copy = s.add_columns(column("yyy"))
        self.assert_compile(
            select_copy,
            "SELECT table1.col1, table1.col2, " "table1.col3, yyy FROM table1",
        )
        is_not(s.selected_columns, select_copy.selected_columns)
        is_not(s._raw_columns, select_copy._raw_columns)
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, " "table1.col3 FROM table1"
        )

    def test_froms(self):
        s = t1.select()
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, " "table1.col3 FROM table1"
        )
        select_copy = s.select_from(t2)
        self.assert_compile(
            select_copy,
            "SELECT table1.col1, table1.col2, "
            "table1.col3 FROM table1, table2",
        )

        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, " "table1.col3 FROM table1"
        )

    def test_prefixes(self):
        s = t1.select()
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, " "table1.col3 FROM table1"
        )
        select_copy = s.prefix_with("FOOBER")
        self.assert_compile(
            select_copy,
            "SELECT FOOBER table1.col1, table1.col2, "
            "table1.col3 FROM table1",
        )
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, " "table1.col3 FROM table1"
        )

    def test_execution_options(self):
        s = select().execution_options(foo="bar")
        s2 = s.execution_options(bar="baz")
        s3 = s.execution_options(foo="not bar")
        # The original select should not be modified.
        eq_(s.get_execution_options(), dict(foo="bar"))
        # s2 should have its execution_options based on s, though.
        eq_(s2.get_execution_options(), dict(foo="bar", bar="baz"))
        eq_(s3.get_execution_options(), dict(foo="not bar"))

    def test_invalid_options(self):
        assert_raises(
            exc.ArgumentError, select().execution_options, compiled_cache={}
        )

        assert_raises(
            exc.ArgumentError,
            select().execution_options,
            isolation_level="READ_COMMITTED",
        )

    # this feature not available yet
    def _NOTYET_test_execution_options_in_kwargs(self):
        s = select(execution_options=dict(foo="bar"))
        s2 = s.execution_options(bar="baz")
        # The original select should not be modified.
        assert s._execution_options == dict(foo="bar")
        # s2 should have its execution_options based on s, though.
        assert s2._execution_options == dict(foo="bar", bar="baz")

    # this feature not available yet
    def _NOTYET_test_execution_options_in_text(self):
        s = text("select 42", execution_options=dict(foo="bar"))
        assert s._execution_options == dict(foo="bar")


class ValuesBaseTest(fixtures.TestBase, AssertsCompiledSQL):

    """Tests the generative capability of Insert, Update"""

    __dialect__ = "default"

    # fixme: consolidate converage from elsewhere here and expand

    @classmethod
    def setup_test_class(cls):
        global t1, t2
        t1 = table("table1", column("col1"), column("col2"), column("col3"))
        t2 = table("table2", column("col1"), column("col2"), column("col3"))

    def test_prefixes(self):
        i = t1.insert()
        self.assert_compile(
            i,
            "INSERT INTO table1 (col1, col2, col3) "
            "VALUES (:col1, :col2, :col3)",
        )

        gen = i.prefix_with("foober")
        self.assert_compile(
            gen,
            "INSERT foober INTO table1 (col1, col2, col3) "
            "VALUES (:col1, :col2, :col3)",
        )

        self.assert_compile(
            i,
            "INSERT INTO table1 (col1, col2, col3) "
            "VALUES (:col1, :col2, :col3)",
        )

        i2 = t1.insert().prefix_with("squiznart")
        self.assert_compile(
            i2,
            "INSERT squiznart INTO table1 (col1, col2, col3) "
            "VALUES (:col1, :col2, :col3)",
        )

        gen2 = i2.prefix_with("quux")
        self.assert_compile(
            gen2,
            "INSERT squiznart quux INTO "
            "table1 (col1, col2, col3) "
            "VALUES (:col1, :col2, :col3)",
        )

    def test_add_kwarg(self):
        i = t1.insert()
        compile_state = i._compile_state_factory(i, None)
        eq_(compile_state._dict_parameters, None)
        i = i.values(col1=5)
        compile_state = i._compile_state_factory(i, None)
        self._compare_param_dict(compile_state._dict_parameters, {"col1": 5})
        i = i.values(col2=7)
        compile_state = i._compile_state_factory(i, None)
        self._compare_param_dict(
            compile_state._dict_parameters, {"col1": 5, "col2": 7}
        )

    def test_via_tuple_single(self):
        i = t1.insert()

        compile_state = i._compile_state_factory(i, None)
        eq_(compile_state._dict_parameters, None)

        i = i.values((5, 6, 7))
        compile_state = i._compile_state_factory(i, None)

        self._compare_param_dict(
            compile_state._dict_parameters,
            {"col1": 5, "col2": 6, "col3": 7},
        )

    def test_kw_and_dict_simultaneously_single(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            r"Can't pass positional and kwargs to values\(\) simultaneously",
            i.values,
            {"col1": 5},
            col2=7,
        )

    def test_via_tuple_multi(self):
        i = t1.insert()
        compile_state = i._compile_state_factory(i, None)
        eq_(compile_state._dict_parameters, None)

        i = i.values([(5, 6, 7), (8, 9, 10)])
        compile_state = i._compile_state_factory(i, None)
        eq_(
            compile_state._dict_parameters,
            {"col1": 5, "col2": 6, "col3": 7},
        )
        eq_(compile_state._has_multi_parameters, True)
        eq_(
            compile_state._multi_parameters,
            [
                {"col1": 5, "col2": 6, "col3": 7},
                {"col1": 8, "col2": 9, "col3": 10},
            ],
        )

    def test_inline_values_single(self):
        i = t1.insert().values({"col1": 5})

        compile_state = i._compile_state_factory(i, None)

        self._compare_param_dict(compile_state._dict_parameters, {"col1": 5})
        is_(compile_state._has_multi_parameters, False)

    def test_inline_values_multi(self):
        i = t1.insert().values([{"col1": 5}, {"col1": 6}])

        compile_state = i._compile_state_factory(i, None)

        # multiparams are not converted to bound parameters
        eq_(compile_state._dict_parameters, {"col1": 5})

        # multiparams are not converted to bound parameters
        eq_(compile_state._multi_parameters, [{"col1": 5}, {"col1": 6}])
        is_(compile_state._has_multi_parameters, True)

    def _compare_param_dict(self, a, b):
        if list(a) != list(b):
            return False

        from sqlalchemy.types import NullType

        for a_k, a_i in a.items():
            b_i = b[a_k]

            # compare BindParameter on the left to
            # literal value on the right
            assert a_i.compare(literal(b_i, type_=NullType()))

    def test_add_dictionary(self):
        i = t1.insert()

        compile_state = i._compile_state_factory(i, None)

        eq_(compile_state._dict_parameters, None)
        i = i.values({"col1": 5})

        compile_state = i._compile_state_factory(i, None)

        self._compare_param_dict(compile_state._dict_parameters, {"col1": 5})
        is_(compile_state._has_multi_parameters, False)

        i = i.values({"col1": 6})
        # note replaces
        compile_state = i._compile_state_factory(i, None)

        self._compare_param_dict(compile_state._dict_parameters, {"col1": 6})
        is_(compile_state._has_multi_parameters, False)

        i = i.values({"col2": 7})
        compile_state = i._compile_state_factory(i, None)
        self._compare_param_dict(
            compile_state._dict_parameters, {"col1": 6, "col2": 7}
        )
        is_(compile_state._has_multi_parameters, False)

    def test_add_kwarg_disallowed_multi(self):
        i = t1.insert()
        i = i.values([{"col1": 5}, {"col1": 7}])
        i = i.values(col2=7)
        assert_raises_message(
            exc.InvalidRequestError,
            "Can't mix single and multiple VALUES formats",
            i.compile,
        )

    def test_cant_mix_single_multi_formats_dict_to_list(self):
        i = t1.insert().values(col1=5)
        i = i.values([{"col1": 6}])
        assert_raises_message(
            exc.InvalidRequestError,
            "Can't mix single and multiple VALUES "
            "formats in one INSERT statement",
            i.compile,
        )

    def test_cant_mix_single_multi_formats_list_to_dict(self):
        i = t1.insert().values([{"col1": 6}])
        i = i.values({"col1": 5})
        assert_raises_message(
            exc.InvalidRequestError,
            "Can't mix single and multiple VALUES "
            "formats in one INSERT statement",
            i.compile,
        )

    def test_erroneous_multi_args_dicts(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            "Only a single dictionary/tuple or list of "
            "dictionaries/tuples is accepted positionally.",
            i.values,
            {"col1": 5},
            {"col1": 7},
        )

    def test_erroneous_multi_args_tuples(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            "Only a single dictionary/tuple or list of "
            "dictionaries/tuples is accepted positionally.",
            i.values,
            (5, 6, 7),
            (8, 9, 10),
        )

    def test_erroneous_multi_args_plus_kw(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            r"Can't pass positional and kwargs to values\(\) simultaneously",
            i.values,
            [{"col1": 5}],
            col2=7,
        )

    def test_update_no_support_multi_values(self):
        u = t1.update()
        u = u.values([{"col1": 5}, {"col1": 7}])
        assert_raises_message(
            exc.InvalidRequestError,
            "UPDATE construct does not support multiple parameter sets.",
            u.compile,
        )

    def test_update_no_support_multi_constructor(self):
        stmt = t1.update().values([{"col1": 5}, {"col1": 7}])

        assert_raises_message(
            exc.InvalidRequestError,
            "UPDATE construct does not support multiple parameter sets.",
            stmt.compile,
        )
