from sqlalchemy.sql import table, column, ClauseElement, operators
from sqlalchemy.sql.expression import _clone, _from_objects
from sqlalchemy import func, select, Integer, Table, \
    Column, MetaData, extract, String, bindparam, tuple_, and_, union, text,\
    case, ForeignKey
from sqlalchemy.testing import fixtures, AssertsExecutionResults, \
    AssertsCompiledSQL
from sqlalchemy import testing
from sqlalchemy.sql.visitors import ClauseVisitor, CloningVisitor, \
    cloned_traverse, ReplacingCloningVisitor
from sqlalchemy import exc
from sqlalchemy.sql import util as sql_util
from sqlalchemy.testing import eq_, is_, assert_raises, assert_raises_message

A = B = t1 = t2 = t3 = table1 = table2 = table3 = table4 = None


class TraversalTest(fixtures.TestBase, AssertsExecutionResults):

    """test ClauseVisitor's traversal, particularly its
    ability to copy and modify a ClauseElement in place."""

    @classmethod
    def setup_class(cls):
        global A, B

        # establish two fictitious ClauseElements.
        # define deep equality semantics as well as deep
        # identity semantics.
        class A(ClauseElement):
            __visit_name__ = 'a'

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
            __visit_name__ = 'b'

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

            def _copy_internals(self, clone=_clone):
                self.items = [clone(i) for i in self.items]

            def get_children(self, **kwargs):
                return self.items

            def __str__(self):
                return "B(%s)" % repr([str(i) for i in self.items])

    def test_test_classes(self):
        a1 = A("expr1")
        struct = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct2 = B(a1, A("expr2"), B(A("expr1b"), A("expr2b")), A("expr3"))
        struct3 = B(a1, A("expr2"), B(A("expr1b"),
                                      A("expr2bmodified")), A("expr3"))

        assert a1.is_other(a1)
        assert struct.is_other(struct)
        assert struct == struct2
        assert struct != struct3
        assert not struct.is_other(struct2)
        assert not struct.is_other(struct3)

    def test_clone(self):
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"),
                                             A("expr2b")), A("expr3"))

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
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"),
                                             A("expr2b")), A("expr3"))

        class Vis(ClauseVisitor):

            def visit_a(self, a):
                pass

            def visit_b(self, b):
                pass

        vis = Vis()
        s2 = vis.traverse(struct)
        assert struct == s2
        assert struct.is_other(s2)

    def test_change_in_place(self):
        struct = B(A("expr1"), A("expr2"), B(A("expr1b"),
                                             A("expr2b")), A("expr3"))
        struct2 = B(A("expr1"), A("expr2modified"), B(A("expr1b"),
                                                      A("expr2b")), A("expr3"))
        struct3 = B(A("expr1"), A("expr2"), B(A("expr1b"),
                                              A("expr2bmodified")), A("expr3"))

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

        assert CustomObj.__visit_name__ == Column.__visit_name__ == 'column'

        foo, bar = CustomObj('foo', String), CustomObj('bar', String)
        bin = foo == bar
        set(ClauseVisitor().iterate(bin))
        assert set(ClauseVisitor().iterate(bin)) == set([foo, bar, bin])


class BinaryEndpointTraversalTest(fixtures.TestBase):

    """test the special binary product visit"""

    def _assert_traversal(self, expr, expected):
        canary = []

        def visit(binary, l, r):
            canary.append((binary.operator, l, r))
            print(binary.operator, l, r)
        sql_util.visit_binary_product(visit, expr)
        eq_(
            canary, expected
        )

    def test_basic(self):
        a, b = column("a"), column("b")
        self._assert_traversal(
            a == b,
            [
                (operators.eq, a, b)
            ]
        )

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
            column("f")
        )
        expr = tuple_(
            a, b, b1 == tuple_(b1a, b1b == d), c
        ) > tuple_(
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
                (operators.gt, c, f)
            ]
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
        expr = and_(
            (a + b) == q + func.sum(e + f),
            and_(
                j == r,
                f == q
            )
        )
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
            ]
        )

    def test_subquery(self):
        a, b, c = column("a"), column("b"), column("c")
        subq = select([c]).where(c == a).as_scalar()
        expr = and_(a == b, b == subq)
        self._assert_traversal(
            expr,
            [
                (operators.eq, a, b),
                (operators.eq, b, subq),
            ]
        )


class ClauseTest(fixtures.TestBase, AssertsCompiledSQL):

    """test copy-in-place behavior of various ClauseElements."""

    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        global t1, t2, t3
        t1 = table("table1",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )
        t2 = table("table2",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )
        t3 = Table('table3', MetaData(),
                   Column('col1', Integer),
                   Column('col2', Integer)
                   )

    def test_binary(self):
        clause = t1.c.col2 == t2.c.col2
        eq_(str(clause), str(CloningVisitor().traverse(clause)))

    def test_binary_anon_label_quirk(self):
        t = table('t1', column('col1'))

        f = t.c.col1 * 5
        self.assert_compile(select([f]),
                            "SELECT t1.col1 * :col1_1 AS anon_1 FROM t1")

        f.anon_label

        a = t.alias()
        f = sql_util.ClauseAdapter(a).traverse(f)

        self.assert_compile(
            select(
                [f]),
            "SELECT t1_1.col1 * :col1_1 AS anon_1 FROM t1 AS t1_1")

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
        clause = t1.select()

        aliased = t1.select().alias()
        aliased2 = t1.alias()

        adapter = sql_util.ColumnAdapter(aliased)

        f = select([
            adapter.columns[c]
            for c in aliased2.c
        ]).select_from(aliased)

        s = select([aliased2]).select_from(aliased)
        eq_(str(s), str(f))

        f = select([
            adapter.columns[func.count(aliased2.c.col1)]
        ]).select_from(aliased)
        eq_(
            str(select([func.count(aliased2.c.col1)]).select_from(aliased)),
            str(f)
        )

    def test_aliased_cloned_column_adapt_inner(self):
        clause = select([t1.c.col1, func.foo(t1.c.col2).label('foo')])

        aliased1 = select([clause.c.col1, clause.c.foo])
        aliased2 = clause
        aliased2.c.col1, aliased2.c.foo
        aliased3 = cloned_traverse(aliased2, {}, {})

        # fixed by [ticket:2419].   the inside columns
        # on aliased3 have _is_clone_of pointers to those of
        # aliased2.  corresponding_column checks these
        # now.
        adapter = sql_util.ColumnAdapter(aliased1)
        f1 = select([
            adapter.columns[c]
            for c in aliased2._raw_columns
        ])
        f2 = select([
            adapter.columns[c]
            for c in aliased3._raw_columns
        ])
        eq_(
            str(f1), str(f2)
        )

    def test_aliased_cloned_column_adapt_exported(self):
        clause = select([t1.c.col1, func.foo(t1.c.col2).label('foo')])

        aliased1 = select([clause.c.col1, clause.c.foo])
        aliased2 = clause
        aliased2.c.col1, aliased2.c.foo
        aliased3 = cloned_traverse(aliased2, {}, {})

        # also fixed by [ticket:2419].  When we look at the
        # *outside* columns of aliased3, they previously did not
        # have an _is_clone_of pointer.   But we now modified _make_proxy
        # to assign this.
        adapter = sql_util.ColumnAdapter(aliased1)
        f1 = select([
            adapter.columns[c]
            for c in aliased2.c
        ])
        f2 = select([
            adapter.columns[c]
            for c in aliased3.c
        ])
        eq_(
            str(f1), str(f2)
        )

    def test_aliased_cloned_schema_column_adapt_exported(self):
        clause = select([t3.c.col1, func.foo(t3.c.col2).label('foo')])

        aliased1 = select([clause.c.col1, clause.c.foo])
        aliased2 = clause
        aliased2.c.col1, aliased2.c.foo
        aliased3 = cloned_traverse(aliased2, {}, {})

        # also fixed by [ticket:2419].  When we look at the
        # *outside* columns of aliased3, they previously did not
        # have an _is_clone_of pointer.   But we now modified _make_proxy
        # to assign this.
        adapter = sql_util.ColumnAdapter(aliased1)
        f1 = select([
            adapter.columns[c]
            for c in aliased2.c
        ])
        f2 = select([
            adapter.columns[c]
            for c in aliased3.c
        ])
        eq_(
            str(f1), str(f2)
        )

    def test_text(self):
        clause = text(
            "select * from table where foo=:bar",
            bindparams=[bindparam('bar')])
        c1 = str(clause)

        class Vis(CloningVisitor):

            def visit_textclause(self, text):
                text.text = text.text + " SOME MODIFIER=:lala"
                text._bindparams['lala'] = bindparam('lala')

        clause2 = Vis().traverse(clause)
        assert c1 == str(clause)
        assert str(clause2) == c1 + " SOME MODIFIER=:lala"
        assert list(clause._bindparams.keys()) == ['bar']
        assert set(clause2._bindparams.keys()) == set(['bar', 'lala'])

    def test_select(self):
        s2 = select([t1])
        s2_assert = str(s2)
        s3_assert = str(select([t1], t1.c.col2 == 7))

        class Vis(CloningVisitor):

            def visit_select(self, select):
                select.append_whereclause(t1.c.col2 == 7)
        s3 = Vis().traverse(s2)
        assert str(s3) == s3_assert
        assert str(s2) == s2_assert
        print(str(s2))
        print(str(s3))

        class Vis(ClauseVisitor):

            def visit_select(self, select):
                select.append_whereclause(t1.c.col2 == 7)
        Vis().traverse(s2)
        assert str(s2) == s3_assert

        s4_assert = str(select([t1], and_(t1.c.col2 == 7, t1.c.col3 == 9)))

        class Vis(CloningVisitor):

            def visit_select(self, select):
                select.append_whereclause(t1.c.col3 == 9)
        s4 = Vis().traverse(s3)
        print(str(s3))
        print(str(s4))
        assert str(s4) == s4_assert
        assert str(s3) == s3_assert

        s5_assert = str(select([t1], and_(t1.c.col2 == 7, t1.c.col1 == 9)))

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
        assert str(u) == str(u2)
        assert [str(c) for c in u2.c] == [str(c) for c in u.c]

        u = union(t1.select(), t2.select())
        cols = [str(c) for c in u.c]
        u2 = CloningVisitor().traverse(u)
        assert str(u) == str(u2)
        assert [str(c) for c in u2.c] == cols

        s1 = select([t1], t1.c.col1 == bindparam('id_param'))
        s2 = select([t2])
        u = union(s1, s2)

        u2 = u.params(id_param=7)
        u3 = u.params(id_param=10)
        assert str(u) == str(u2) == str(u3)
        assert u2.compile().params == {'id_param': 7}
        assert u3.compile().params == {'id_param': 10}

    def test_in(self):
        expr = t1.c.col1.in_(['foo', 'bar'])
        expr2 = CloningVisitor().traverse(expr)
        assert str(expr) == str(expr2)

    def test_over(self):
        expr = func.row_number().over(order_by=t1.c.col1)
        expr2 = CloningVisitor().traverse(expr)
        assert str(expr) == str(expr2)

    def test_adapt_union(self):
        u = union(
            t1.select().where(t1.c.col1 == 4),
            t1.select().where(t1.c.col1 == 5)
        ).alias()

        assert sql_util.ClauseAdapter(u).traverse(t1) is u

    def test_binds(self):
        """test that unique bindparams change their name upon clone()
        to prevent conflicts"""

        s = select([t1], t1.c.col1 == bindparam(None, unique=True)).alias()
        s2 = CloningVisitor().traverse(s).alias()
        s3 = select([s], s.c.col2 == s2.c.col2)

        self.assert_compile(
            s3, "SELECT anon_1.col1, anon_1.col2, anon_1.col3 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 WHERE table1.col1 = :param_1) "
            "AS anon_1, "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 "
            "AS col3 FROM table1 WHERE table1.col1 = :param_2) AS anon_2 "
            "WHERE anon_1.col2 = anon_2.col2")

        s = select([t1], t1.c.col1 == 4).alias()
        s2 = CloningVisitor().traverse(s).alias()
        s3 = select([s], s.c.col2 == s2.c.col2)
        self.assert_compile(
            s3, "SELECT anon_1.col1, anon_1.col2, anon_1.col3 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 WHERE table1.col1 = :col1_1) "
            "AS anon_1, "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, table1.col3 "
            "AS col3 FROM table1 WHERE table1.col1 = :col1_2) AS anon_2 "
            "WHERE anon_1.col2 = anon_2.col2")

    def test_extract(self):
        s = select([extract('foo', t1.c.col1).label('col1')])
        self.assert_compile(
            s,
            "SELECT EXTRACT(foo FROM table1.col1) AS col1 FROM table1")

        s2 = CloningVisitor().traverse(s).alias()
        s3 = select([s2.c.col1])
        self.assert_compile(
            s,
            "SELECT EXTRACT(foo FROM table1.col1) AS col1 FROM table1")
        self.assert_compile(s3,
                            "SELECT anon_1.col1 FROM (SELECT EXTRACT(foo FROM "
                            "table1.col1) AS col1 FROM table1) AS anon_1")

    @testing.emits_warning('.*replaced by another column with the same key')
    def test_alias(self):
        subq = t2.select().alias('subq')
        s = select([t1.c.col1, subq.c.col1],
                   from_obj=[t1, subq,
                             t1.join(subq, t1.c.col1 == subq.c.col2)]
                   )
        orig = str(s)
        s2 = CloningVisitor().traverse(s)
        assert orig == str(s) == str(s2)

        s4 = CloningVisitor().traverse(s2)
        assert orig == str(s) == str(s2) == str(s4)

        s3 = sql_util.ClauseAdapter(table('foo')).traverse(s)
        assert orig == str(s) == str(s3)

        s4 = sql_util.ClauseAdapter(table('foo')).traverse(s3)
        assert orig == str(s) == str(s3) == str(s4)

        subq = subq.alias('subq')
        s = select([t1.c.col1, subq.c.col1],
                   from_obj=[t1, subq,
                             t1.join(subq, t1.c.col1 == subq.c.col2)]
                   )
        s5 = CloningVisitor().traverse(s)
        assert orig == str(s) == str(s5)

    def test_correlated_select(self):
        s = select(['*'], t1.c.col1 == t2.c.col1,
                   from_obj=[t1, t2]).correlate(t2)

        class Vis(CloningVisitor):

            def visit_select(self, select):
                select.append_whereclause(t1.c.col2 == 7)

        self.assert_compile(
            select([t2]).where(t2.c.col1 == Vis().traverse(s)),
            "SELECT table2.col1, table2.col2, table2.col3 "
            "FROM table2 WHERE table2.col1 = "
            "(SELECT * FROM table1 WHERE table1.col1 = table2.col1 "
            "AND table1.col2 = :col2_1)"
        )

    def test_this_thing(self):
        s = select([t1]).where(t1.c.col1 == 'foo').alias()
        s2 = select([s.c.col1])

        self.assert_compile(s2,
                            'SELECT anon_1.col1 FROM (SELECT '
                            'table1.col1 AS col1, table1.col2 AS col2, '
                            'table1.col3 AS col3 FROM table1 WHERE '
                            'table1.col1 = :col1_1) AS anon_1')
        t1a = t1.alias()
        s2 = sql_util.ClauseAdapter(t1a).traverse(s2)
        self.assert_compile(s2,
                            'SELECT anon_1.col1 FROM (SELECT '
                            'table1_1.col1 AS col1, table1_1.col2 AS '
                            'col2, table1_1.col3 AS col3 FROM table1 '
                            'AS table1_1 WHERE table1_1.col1 = '
                            ':col1_1) AS anon_1')

    def test_select_fromtwice_one(self):
        t1a = t1.alias()

        s = select([1], t1.c.col1 == t1a.c.col1, from_obj=t1a).correlate(t1a)
        s = select([t1]).where(t1.c.col1 == s)
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1 "
            "WHERE table1.col1 = "
            "(SELECT 1 FROM table1, table1 AS table1_1 "
            "WHERE table1.col1 = table1_1.col1)")
        s = CloningVisitor().traverse(s)
        self.assert_compile(
            s, "SELECT table1.col1, table1.col2, table1.col3 FROM table1 "
            "WHERE table1.col1 = "
            "(SELECT 1 FROM table1, table1 AS table1_1 "
            "WHERE table1.col1 = table1_1.col1)")

    def test_select_fromtwice_two(self):
        s = select([t1]).where(t1.c.col1 == 'foo').alias()

        s2 = select([1], t1.c.col1 == s.c.col1, from_obj=s).correlate(t1)
        s3 = select([t1]).where(t1.c.col1 == s2)
        self.assert_compile(
            s3, "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 WHERE table1.col1 = "
            "(SELECT 1 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 "
            "WHERE table1.col1 = :col1_1) "
            "AS anon_1 WHERE table1.col1 = anon_1.col1)")

        s4 = ReplacingCloningVisitor().traverse(s3)
        self.assert_compile(
            s4, "SELECT table1.col1, table1.col2, table1.col3 "
            "FROM table1 WHERE table1.col1 = "
            "(SELECT 1 FROM "
            "(SELECT table1.col1 AS col1, table1.col2 AS col2, "
            "table1.col3 AS col3 FROM table1 "
            "WHERE table1.col1 = :col1_1) "
            "AS anon_1 WHERE table1.col1 = anon_1.col1)")


class ClauseAdapterTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        global t1, t2
        t1 = table("table1",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )
        t2 = table("table2",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )

    def test_correlation_on_clone(self):
        t1alias = t1.alias('t1alias')
        t2alias = t2.alias('t2alias')
        vis = sql_util.ClauseAdapter(t1alias)

        s = select(['*'], from_obj=[t1alias, t2alias]).as_scalar()
        assert t2alias in s._froms
        assert t1alias in s._froms

        self.assert_compile(select(['*'], t2alias.c.col1 == s),
                            'SELECT * FROM table2 AS t2alias WHERE '
                            't2alias.col1 = (SELECT * FROM table1 AS '
                            't1alias)')
        s = vis.traverse(s)

        assert t2alias not in s._froms  # not present because it's been
        # cloned
        assert t1alias in s._froms  # present because the adapter placed
        # it there

        # correlate list on "s" needs to take into account the full
        # _cloned_set for each element in _froms when correlating

        self.assert_compile(select(['*'], t2alias.c.col1 == s),
                            'SELECT * FROM table2 AS t2alias WHERE '
                            't2alias.col1 = (SELECT * FROM table1 AS '
                            't1alias)')
        s = select(['*'], from_obj=[t1alias,
                                    t2alias]).correlate(t2alias).as_scalar()
        self.assert_compile(select(['*'], t2alias.c.col1 == s),
                            'SELECT * FROM table2 AS t2alias WHERE '
                            't2alias.col1 = (SELECT * FROM table1 AS '
                            't1alias)')
        s = vis.traverse(s)
        self.assert_compile(select(['*'], t2alias.c.col1 == s),
                            'SELECT * FROM table2 AS t2alias WHERE '
                            't2alias.col1 = (SELECT * FROM table1 AS '
                            't1alias)')
        s = CloningVisitor().traverse(s)
        self.assert_compile(select(['*'], t2alias.c.col1 == s),
                            'SELECT * FROM table2 AS t2alias WHERE '
                            't2alias.col1 = (SELECT * FROM table1 AS '
                            't1alias)')

        s = select(['*']).where(t1.c.col1 == t2.c.col1).as_scalar()
        self.assert_compile(select([t1.c.col1, s]),
                            'SELECT table1.col1, (SELECT * FROM table2 '
                            'WHERE table1.col1 = table2.col1) AS '
                            'anon_1 FROM table1')
        vis = sql_util.ClauseAdapter(t1alias)
        s = vis.traverse(s)
        self.assert_compile(select([t1alias.c.col1, s]),
                            'SELECT t1alias.col1, (SELECT * FROM '
                            'table2 WHERE t1alias.col1 = table2.col1) '
                            'AS anon_1 FROM table1 AS t1alias')
        s = CloningVisitor().traverse(s)
        self.assert_compile(select([t1alias.c.col1, s]),
                            'SELECT t1alias.col1, (SELECT * FROM '
                            'table2 WHERE t1alias.col1 = table2.col1) '
                            'AS anon_1 FROM table1 AS t1alias')
        s = select(['*']).where(t1.c.col1
                                == t2.c.col1).correlate(t1).as_scalar()
        self.assert_compile(select([t1.c.col1, s]),
                            'SELECT table1.col1, (SELECT * FROM table2 '
                            'WHERE table1.col1 = table2.col1) AS '
                            'anon_1 FROM table1')
        vis = sql_util.ClauseAdapter(t1alias)
        s = vis.traverse(s)
        self.assert_compile(select([t1alias.c.col1, s]),
                            'SELECT t1alias.col1, (SELECT * FROM '
                            'table2 WHERE t1alias.col1 = table2.col1) '
                            'AS anon_1 FROM table1 AS t1alias')
        s = CloningVisitor().traverse(s)
        self.assert_compile(select([t1alias.c.col1, s]),
                            'SELECT t1alias.col1, (SELECT * FROM '
                            'table2 WHERE t1alias.col1 = table2.col1) '
                            'AS anon_1 FROM table1 AS t1alias')

    @testing.fails_on_everything_except()
    def test_joins_dont_adapt(self):
        # adapting to a join, i.e. ClauseAdapter(t1.join(t2)), doesn't
        # make much sense. ClauseAdapter doesn't make any changes if
        # it's against a straight join.

        users = table('users', column('id'))
        addresses = table('addresses', column('id'), column('user_id'))

        ualias = users.alias()

        s = select([func.count(addresses.c.id)], users.c.id
                   == addresses.c.user_id).correlate(users)
        s = sql_util.ClauseAdapter(ualias).traverse(s)

        j1 = addresses.join(ualias, addresses.c.user_id == ualias.c.id)

        self.assert_compile(sql_util.ClauseAdapter(j1).traverse(s),
                            'SELECT count(addresses.id) AS count_1 '
                            'FROM addresses WHERE users_1.id = '
                            'addresses.user_id')

    def test_table_to_alias_1(self):
        t1alias = t1.alias('t1alias')

        vis = sql_util.ClauseAdapter(t1alias)
        ff = vis.traverse(func.count(t1.c.col1).label('foo'))
        assert list(_from_objects(ff)) == [t1alias]

    def test_table_to_alias_2(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(vis.traverse(select(['*'], from_obj=[t1])),
                            'SELECT * FROM table1 AS t1alias')

    def test_table_to_alias_3(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(select(['*'], t1.c.col1 == t2.c.col2),
                            'SELECT * FROM table1, table2 WHERE '
                            'table1.col1 = table2.col2')

    def test_table_to_alias_4(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1
                                                == t2.c.col2)),
                            'SELECT * FROM table1 AS t1alias, table2 '
                            'WHERE t1alias.col1 = table2.col2')

    def test_table_to_alias_5(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(
                select(
                    ['*'],
                    t1.c.col1 == t2.c.col2,
                    from_obj=[
                        t1,
                        t2])),
            'SELECT * FROM table1 AS t1alias, table2 '
            'WHERE t1alias.col1 = table2.col2')

    def test_table_to_alias_6(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            select([t1alias, t2]).where(
                t1alias.c.col1 == vis.traverse(
                    select(['*'], t1.c.col1 == t2.c.col2, from_obj=[t1, t2]).
                    correlate(t1)
                )
            ),
            "SELECT t1alias.col1, t1alias.col2, t1alias.col3, "
            "table2.col1, table2.col2, table2.col3 "
            "FROM table1 AS t1alias, table2 WHERE t1alias.col1 = "
            "(SELECT * FROM table2 WHERE t1alias.col1 = table2.col2)"
        )

    def test_table_to_alias_7(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            select([t1alias, t2]).
            where(t1alias.c.col1 == vis.traverse(
                select(['*'], t1.c.col1 == t2.c.col2, from_obj=[t1, t2]).
                correlate(t2))),
            "SELECT t1alias.col1, t1alias.col2, t1alias.col3, "
            "table2.col1, table2.col2, table2.col3 "
            "FROM table1 AS t1alias, table2 "
            "WHERE t1alias.col1 = "
            "(SELECT * FROM table1 AS t1alias "
            "WHERE t1alias.col1 = table2.col2)")

    def test_table_to_alias_8(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(case([(t1.c.col1 == 5, t1.c.col2)], else_=t1.c.col1)),
            'CASE WHEN (t1alias.col1 = :col1_1) THEN '
            't1alias.col2 ELSE t1alias.col1 END')

    def test_table_to_alias_9(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(
            vis.traverse(
                case(
                    [
                        (5,
                         t1.c.col2)],
                    value=t1.c.col1,
                    else_=t1.c.col1)),
            'CASE t1alias.col1 WHEN :param_1 THEN '
            't1alias.col2 ELSE t1alias.col1 END')

    def test_table_to_alias_10(self):
        s = select(['*'], from_obj=[t1]).alias('foo')
        self.assert_compile(s.select(),
                            'SELECT foo.* FROM (SELECT * FROM table1) '
                            'AS foo')

    def test_table_to_alias_11(self):
        s = select(['*'], from_obj=[t1]).alias('foo')
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        self.assert_compile(vis.traverse(s.select()),
                            'SELECT foo.* FROM (SELECT * FROM table1 '
                            'AS t1alias) AS foo')

    def test_table_to_alias_12(self):
        s = select(['*'], from_obj=[t1]).alias('foo')
        self.assert_compile(s.select(),
                            'SELECT foo.* FROM (SELECT * FROM table1) '
                            'AS foo')

    def test_table_to_alias_13(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        ff = vis.traverse(func.count(t1.c.col1).label('foo'))
        self.assert_compile(select([ff]),
                            'SELECT count(t1alias.col1) AS foo FROM '
                            'table1 AS t1alias')
        assert list(_from_objects(ff)) == [t1alias]

    # def test_table_to_alias_2(self):
        # TODO: self.assert_compile(vis.traverse(select([func.count(t1.c
        # .col1).l abel('foo')]), clone=True), "SELECT
        # count(t1alias.col1) AS foo FROM table1 AS t1alias")

    def test_table_to_alias_14(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias('t2alias')
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(vis.traverse(select(['*'], t1.c.col1
                                                == t2.c.col2)),
                            'SELECT * FROM table1 AS t1alias, table2 '
                            'AS t2alias WHERE t1alias.col1 = '
                            't2alias.col2')

    def test_table_to_alias_15(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias('t2alias')
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            vis.traverse(
                select(
                    ['*'],
                    t1.c.col1 == t2.c.col2,
                    from_obj=[
                        t1,
                        t2])),
            'SELECT * FROM table1 AS t1alias, table2 '
            'AS t2alias WHERE t1alias.col1 = '
            't2alias.col2')

    def test_table_to_alias_16(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias('t2alias')
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            select([t1alias, t2alias]).where(
                t1alias.c.col1 ==
                vis.traverse(select(['*'],
                                    t1.c.col1 == t2.c.col2,
                                    from_obj=[t1, t2]).correlate(t1))
            ),
            "SELECT t1alias.col1, t1alias.col2, t1alias.col3, "
            "t2alias.col1, t2alias.col2, t2alias.col3 "
            "FROM table1 AS t1alias, table2 AS t2alias "
            "WHERE t1alias.col1 = "
            "(SELECT * FROM table2 AS t2alias "
            "WHERE t1alias.col1 = t2alias.col2)"
        )

    def test_table_to_alias_17(self):
        t1alias = t1.alias('t1alias')
        vis = sql_util.ClauseAdapter(t1alias)
        t2alias = t2.alias('t2alias')
        vis.chain(sql_util.ClauseAdapter(t2alias))
        self.assert_compile(
            t2alias.select().where(
                t2alias.c.col2 == vis.traverse(
                    select(
                        ['*'],
                        t1.c.col1 == t2.c.col2,
                        from_obj=[
                            t1,
                            t2]).correlate(t2))),
            'SELECT t2alias.col1, t2alias.col2, t2alias.col3 '
            'FROM table2 AS t2alias WHERE t2alias.col2 = '
            '(SELECT * FROM table1 AS t1alias WHERE '
            't1alias.col1 = t2alias.col2)')

    def test_include_exclude(self):
        m = MetaData()
        a = Table('a', m,
                  Column('id', Integer, primary_key=True),
                  Column('xxx_id', Integer,
                         ForeignKey('a.id', name='adf', use_alter=True)
                         )
                  )

        e = (a.c.id == a.c.xxx_id)
        assert str(e) == "a.id = a.xxx_id"
        b = a.alias()

        e = sql_util.ClauseAdapter(b, include=set([a.c.id]),
                                   equivalents={a.c.id: set([a.c.id])}
                                   ).traverse(e)

        assert str(e) == "a_1.id = a.xxx_id"

    def test_recursive_equivalents(self):
        m = MetaData()
        a = Table('a', m, Column('x', Integer), Column('y', Integer))
        b = Table('b', m, Column('x', Integer), Column('y', Integer))
        c = Table('c', m, Column('x', Integer), Column('y', Integer))

        # force a recursion overflow, by linking a.c.x<->c.c.x, and
        # asking for a nonexistent col.  corresponding_column should prevent
        # endless depth.
        adapt = sql_util.ClauseAdapter(
            b, equivalents={a.c.x: set([c.c.x]), c.c.x: set([a.c.x])})
        assert adapt._corresponding_column(a.c.x, False) is None

    def test_multilevel_equivalents(self):
        m = MetaData()
        a = Table('a', m, Column('x', Integer), Column('y', Integer))
        b = Table('b', m, Column('x', Integer), Column('y', Integer))
        c = Table('c', m, Column('x', Integer), Column('y', Integer))

        alias = select([a]).select_from(a.join(b, a.c.x == b.c.x)).alias()

        # two levels of indirection from c.x->b.x->a.x, requires recursive
        # corresponding_column call
        adapt = sql_util.ClauseAdapter(
            alias, equivalents={b.c.x: set([a.c.x]), c.c.x: set([b.c.x])})
        assert adapt._corresponding_column(a.c.x, False) is alias.c.x
        assert adapt._corresponding_column(c.c.x, False) is alias.c.x

    def test_join_to_alias(self):
        metadata = MetaData()
        a = Table('a', metadata,
                  Column('id', Integer, primary_key=True))
        b = Table('b', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('aid', Integer, ForeignKey('a.id')),
                  )
        c = Table('c', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('bid', Integer, ForeignKey('b.id')),
                  )

        d = Table('d', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('aid', Integer, ForeignKey('a.id')),
                  )

        j1 = a.outerjoin(b)
        j2 = select([j1], use_labels=True)

        j3 = c.join(j2, j2.c.b_id == c.c.bid)

        j4 = j3.outerjoin(d)
        self.assert_compile(j4,
                            'c JOIN (SELECT a.id AS a_id, b.id AS '
                            'b_id, b.aid AS b_aid FROM a LEFT OUTER '
                            'JOIN b ON a.id = b.aid) ON b_id = c.bid '
                            'LEFT OUTER JOIN d ON a_id = d.aid')
        j5 = j3.alias('foo')
        j6 = sql_util.ClauseAdapter(j5).copy_and_process([j4])[0]

        # this statement takes c join(a join b), wraps it inside an
        # aliased "select * from c join(a join b) AS foo". the outermost
        # right side "left outer join d" stays the same, except "d"
        # joins against foo.a_id instead of plain "a_id"

        self.assert_compile(j6,
                            '(SELECT c.id AS c_id, c.bid AS c_bid, '
                            'a_id AS a_id, b_id AS b_id, b_aid AS '
                            'b_aid FROM c JOIN (SELECT a.id AS a_id, '
                            'b.id AS b_id, b.aid AS b_aid FROM a LEFT '
                            'OUTER JOIN b ON a.id = b.aid) ON b_id = '
                            'c.bid) AS foo LEFT OUTER JOIN d ON '
                            'foo.a_id = d.aid')

    def test_derived_from(self):
        assert select([t1]).is_derived_from(t1)
        assert not select([t2]).is_derived_from(t1)
        assert not t1.is_derived_from(select([t1]))
        assert t1.alias().is_derived_from(t1)

        s1 = select([t1, t2]).alias('foo')
        s2 = select([s1]).limit(5).offset(10).alias()
        assert s2.is_derived_from(s1)
        s2 = s2._clone()
        assert s2.is_derived_from(s1)

    def test_aliasedselect_to_aliasedselect_straight(self):

        # original issue from ticket #904

        s1 = select([t1]).alias('foo')
        s2 = select([s1]).limit(5).offset(10).alias()
        self.assert_compile(sql_util.ClauseAdapter(s2).traverse(s1),
                            'SELECT foo.col1, foo.col2, foo.col3 FROM '
                            '(SELECT table1.col1 AS col1, table1.col2 '
                            'AS col2, table1.col3 AS col3 FROM table1) '
                            'AS foo LIMIT :param_1 OFFSET :param_2',
                            {'param_1': 5, 'param_2': 10})

    def test_aliasedselect_to_aliasedselect_join(self):
        s1 = select([t1]).alias('foo')
        s2 = select([s1]).limit(5).offset(10).alias()
        j = s1.outerjoin(t2, s1.c.col1 == t2.c.col1)
        self.assert_compile(sql_util.ClauseAdapter(s2).traverse(j).select(),
                            'SELECT anon_1.col1, anon_1.col2, '
                            'anon_1.col3, table2.col1, table2.col2, '
                            'table2.col3 FROM (SELECT foo.col1 AS '
                            'col1, foo.col2 AS col2, foo.col3 AS col3 '
                            'FROM (SELECT table1.col1 AS col1, '
                            'table1.col2 AS col2, table1.col3 AS col3 '
                            'FROM table1) AS foo LIMIT :param_1 OFFSET '
                            ':param_2) AS anon_1 LEFT OUTER JOIN '
                            'table2 ON anon_1.col1 = table2.col1',
                            {'param_1': 5, 'param_2': 10})

    def test_aliasedselect_to_aliasedselect_join_nested_table(self):
        s1 = select([t1]).alias('foo')
        s2 = select([s1]).limit(5).offset(10).alias()
        talias = t1.alias('bar')

        assert not s2.is_derived_from(talias)
        j = s1.outerjoin(talias, s1.c.col1 == talias.c.col1)

        self.assert_compile(sql_util.ClauseAdapter(s2).traverse(j).select(),
                            'SELECT anon_1.col1, anon_1.col2, '
                            'anon_1.col3, bar.col1, bar.col2, bar.col3 '
                            'FROM (SELECT foo.col1 AS col1, foo.col2 '
                            'AS col2, foo.col3 AS col3 FROM (SELECT '
                            'table1.col1 AS col1, table1.col2 AS col2, '
                            'table1.col3 AS col3 FROM table1) AS foo '
                            'LIMIT :param_1 OFFSET :param_2) AS anon_1 '
                            'LEFT OUTER JOIN table1 AS bar ON '
                            'anon_1.col1 = bar.col1', {'param_1': 5,
                                                       'param_2': 10})

    def test_functions(self):
        self.assert_compile(
            sql_util.ClauseAdapter(t1.alias()).
            traverse(func.count(t1.c.col1)),
            'count(table1_1.col1)')
        s = select([func.count(t1.c.col1)])
        self.assert_compile(sql_util.ClauseAdapter(t1.alias()).traverse(s),
                            'SELECT count(table1_1.col1) AS count_1 '
                            'FROM table1 AS table1_1')

    def test_recursive(self):
        metadata = MetaData()
        a = Table('a', metadata,
                  Column('id', Integer, primary_key=True))
        b = Table('b', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('aid', Integer, ForeignKey('a.id')),
                  )
        c = Table('c', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('bid', Integer, ForeignKey('b.id')),
                  )

        d = Table('d', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('aid', Integer, ForeignKey('a.id')),
                  )

        u = union(
            a.join(b).select().apply_labels(),
            a.join(d).select().apply_labels()
        ).alias()

        self.assert_compile(
            sql_util.ClauseAdapter(u).
            traverse(select([c.c.bid]).where(c.c.bid == u.c.b_aid)),
            "SELECT c.bid "
            "FROM c, (SELECT a.id AS a_id, b.id AS b_id, b.aid AS b_aid "
            "FROM a JOIN b ON a.id = b.aid UNION SELECT a.id AS a_id, d.id "
            "AS d_id, d.aid AS d_aid "
            "FROM a JOIN d ON a.id = d.aid) AS anon_1 "
            "WHERE c.bid = anon_1.b_aid"
        )


class SpliceJoinsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        global table1, table2, table3, table4

        def _table(name):
            return table(name, column('col1'), column('col2'),
                         column('col3'))

        table1, table2, table3, table4 = [
            _table(name) for name in (
                'table1', 'table2', 'table3', 'table4')]

    def test_splice(self):
        t1, t2, t3, t4 = table1, table2, table1.alias(), table2.alias()
        j = t1.join(
            t2,
            t1.c.col1 == t2.c.col1).join(
            t3,
            t2.c.col1 == t3.c.col1).join(
            t4,
            t4.c.col1 == t1.c.col1)
        s = select([t1]).where(t1.c.col2 < 5).alias()
        self.assert_compile(sql_util.splice_joins(s, j),
                            '(SELECT table1.col1 AS col1, table1.col2 '
                            'AS col2, table1.col3 AS col3 FROM table1 '
                            'WHERE table1.col2 < :col2_1) AS anon_1 '
                            'JOIN table2 ON anon_1.col1 = table2.col1 '
                            'JOIN table1 AS table1_1 ON table2.col1 = '
                            'table1_1.col1 JOIN table2 AS table2_1 ON '
                            'table2_1.col1 = anon_1.col1')

    def test_stop_on(self):
        t1, t2, t3 = table1, table2, table3
        j1 = t1.join(t2, t1.c.col1 == t2.c.col1)
        j2 = j1.join(t3, t2.c.col1 == t3.c.col1)
        s = select([t1]).select_from(j1).alias()
        self.assert_compile(sql_util.splice_joins(s, j2),
                            '(SELECT table1.col1 AS col1, table1.col2 '
                            'AS col2, table1.col3 AS col3 FROM table1 '
                            'JOIN table2 ON table1.col1 = table2.col1) '
                            'AS anon_1 JOIN table2 ON anon_1.col1 = '
                            'table2.col1 JOIN table3 ON table2.col1 = '
                            'table3.col1')
        self.assert_compile(sql_util.splice_joins(s, j2, j1),
                            '(SELECT table1.col1 AS col1, table1.col2 '
                            'AS col2, table1.col3 AS col3 FROM table1 '
                            'JOIN table2 ON table1.col1 = table2.col1) '
                            'AS anon_1 JOIN table3 ON table2.col1 = '
                            'table3.col1')

    def test_splice_2(self):
        t2a = table2.alias()
        t3a = table3.alias()
        j1 = table1.join(
            t2a,
            table1.c.col1 == t2a.c.col1).join(
            t3a,
            t2a.c.col2 == t3a.c.col2)
        t2b = table4.alias()
        j2 = table1.join(t2b, table1.c.col3 == t2b.c.col3)
        self.assert_compile(sql_util.splice_joins(table1, j1),
                            'table1 JOIN table2 AS table2_1 ON '
                            'table1.col1 = table2_1.col1 JOIN table3 '
                            'AS table3_1 ON table2_1.col2 = '
                            'table3_1.col2')
        self.assert_compile(sql_util.splice_joins(table1, j2),
                            'table1 JOIN table4 AS table4_1 ON '
                            'table1.col3 = table4_1.col3')
        self.assert_compile(
            sql_util.splice_joins(
                sql_util.splice_joins(
                    table1,
                    j1),
                j2),
            'table1 JOIN table2 AS table2_1 ON '
            'table1.col1 = table2_1.col1 JOIN table3 '
            'AS table3_1 ON table2_1.col2 = '
            'table3_1.col2 JOIN table4 AS table4_1 ON '
            'table1.col3 = table4_1.col3')


class SelectTest(fixtures.TestBase, AssertsCompiledSQL):

    """tests the generative capability of Select"""

    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        global t1, t2
        t1 = table("table1",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )
        t2 = table("table2",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )

    def test_columns(self):
        s = t1.select()
        self.assert_compile(s,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1')
        select_copy = s.column('yyy')
        self.assert_compile(select_copy,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3, yyy FROM table1')
        assert s.columns is not select_copy.columns
        assert s._columns is not select_copy._columns
        assert s._raw_columns is not select_copy._raw_columns
        self.assert_compile(s,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1')

    def test_froms(self):
        s = t1.select()
        self.assert_compile(s,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1')
        select_copy = s.select_from(t2)
        self.assert_compile(select_copy,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1, table2')
        assert s._froms is not select_copy._froms
        self.assert_compile(s,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1')

    def test_prefixes(self):
        s = t1.select()
        self.assert_compile(s,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1')
        select_copy = s.prefix_with('FOOBER')
        self.assert_compile(select_copy,
                            'SELECT FOOBER table1.col1, table1.col2, '
                            'table1.col3 FROM table1')
        self.assert_compile(s,
                            'SELECT table1.col1, table1.col2, '
                            'table1.col3 FROM table1')

    def test_execution_options(self):
        s = select().execution_options(foo='bar')
        s2 = s.execution_options(bar='baz')
        s3 = s.execution_options(foo='not bar')
        # The original select should not be modified.
        assert s._execution_options == dict(foo='bar')
        # s2 should have its execution_options based on s, though.
        assert s2._execution_options == dict(foo='bar', bar='baz')
        assert s3._execution_options == dict(foo='not bar')

    def test_invalid_options(self):
        assert_raises(
            exc.ArgumentError,
            select().execution_options, compiled_cache={}
        )

        assert_raises(
            exc.ArgumentError,
            select().execution_options,
            isolation_level='READ_COMMITTED'
        )

    # this feature not available yet
    def _NOTYET_test_execution_options_in_kwargs(self):
        s = select(execution_options=dict(foo='bar'))
        s2 = s.execution_options(bar='baz')
        # The original select should not be modified.
        assert s._execution_options == dict(foo='bar')
        # s2 should have its execution_options based on s, though.
        assert s2._execution_options == dict(foo='bar', bar='baz')

    # this feature not available yet
    def _NOTYET_test_execution_options_in_text(self):
        s = text('select 42', execution_options=dict(foo='bar'))
        assert s._execution_options == dict(foo='bar')


class ValuesBaseTest(fixtures.TestBase, AssertsCompiledSQL):

    """Tests the generative capability of Insert, Update"""

    __dialect__ = 'default'

    # fixme: consolidate converage from elsewhere here and expand

    @classmethod
    def setup_class(cls):
        global t1, t2
        t1 = table("table1",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )
        t2 = table("table2",
                   column("col1"),
                   column("col2"),
                   column("col3"),
                   )

    def test_prefixes(self):
        i = t1.insert()
        self.assert_compile(i,
                            "INSERT INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        gen = i.prefix_with("foober")
        self.assert_compile(gen,
                            "INSERT foober INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        self.assert_compile(i,
                            "INSERT INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        i2 = t1.insert(prefixes=['squiznart'])
        self.assert_compile(i2,
                            "INSERT squiznart INTO table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

        gen2 = i2.prefix_with("quux")
        self.assert_compile(gen2,
                            "INSERT squiznart quux INTO "
                            "table1 (col1, col2, col3) "
                            "VALUES (:col1, :col2, :col3)")

    def test_add_kwarg(self):
        i = t1.insert()
        eq_(i.parameters, None)
        i = i.values(col1=5)
        eq_(i.parameters, {"col1": 5})
        i = i.values(col2=7)
        eq_(i.parameters, {"col1": 5, "col2": 7})

    def test_via_tuple_single(self):
        i = t1.insert()
        eq_(i.parameters, None)
        i = i.values((5, 6, 7))
        eq_(i.parameters, {"col1": 5, "col2": 6, "col3": 7})

    def test_kw_and_dict_simulatenously_single(self):
        i = t1.insert()
        i = i.values({"col1": 5}, col2=7)
        eq_(i.parameters, {"col1": 5, "col2": 7})

    def test_via_tuple_multi(self):
        i = t1.insert()
        eq_(i.parameters, None)
        i = i.values([(5, 6, 7), (8, 9, 10)])
        eq_(i.parameters, [
            {"col1": 5, "col2": 6, "col3": 7},
            {"col1": 8, "col2": 9, "col3": 10},
            ]
            )

    def test_inline_values_single(self):
        i = t1.insert(values={"col1": 5})
        eq_(i.parameters, {"col1": 5})
        is_(i._has_multi_parameters, False)

    def test_inline_values_multi(self):
        i = t1.insert(values=[{"col1": 5}, {"col1": 6}])
        eq_(i.parameters, [{"col1": 5}, {"col1": 6}])
        is_(i._has_multi_parameters, True)

    def test_add_dictionary(self):
        i = t1.insert()
        eq_(i.parameters, None)
        i = i.values({"col1": 5})
        eq_(i.parameters, {"col1": 5})
        is_(i._has_multi_parameters, False)

        i = i.values({"col1": 6})
        # note replaces
        eq_(i.parameters, {"col1": 6})
        is_(i._has_multi_parameters, False)

        i = i.values({"col2": 7})
        eq_(i.parameters, {"col1": 6, "col2": 7})
        is_(i._has_multi_parameters, False)

    def test_add_kwarg_disallowed_multi(self):
        i = t1.insert()
        i = i.values([{"col1": 5}, {"col1": 7}])
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct already has multiple parameter sets.",
            i.values, col2=7
        )

    def test_cant_mix_single_multi_formats_dict_to_list(self):
        i = t1.insert().values(col1=5)
        assert_raises_message(
            exc.ArgumentError,
            "Can't mix single-values and multiple values "
            "formats in one statement",
            i.values, [{"col1": 6}]
        )

    def test_cant_mix_single_multi_formats_list_to_dict(self):
        i = t1.insert().values([{"col1": 6}])
        assert_raises_message(
            exc.ArgumentError,
            "Can't mix single-values and multiple values "
            "formats in one statement",
            i.values, {"col1": 5}
        )

    def test_erroneous_multi_args_dicts(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            "Only a single dictionary/tuple or list of "
            "dictionaries/tuples is accepted positionally.",
            i.values, {"col1": 5}, {"col1": 7}
        )

    def test_erroneous_multi_args_tuples(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            "Only a single dictionary/tuple or list of "
            "dictionaries/tuples is accepted positionally.",
            i.values, (5, 6, 7), (8, 9, 10)
        )

    def test_erroneous_multi_args_plus_kw(self):
        i = t1.insert()
        assert_raises_message(
            exc.ArgumentError,
            "Can't pass kwargs and multiple parameter sets simultaenously",
            i.values, [{"col1": 5}], col2=7
        )

    def test_update_no_support_multi_values(self):
        u = t1.update()
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct does not support multiple parameter sets.",
            u.values, [{"col1": 5}, {"col1": 7}]
        )

    def test_update_no_support_multi_constructor(self):
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct does not support multiple parameter sets.",
            t1.update, values=[{"col1": 5}, {"col1": 7}]
        )
