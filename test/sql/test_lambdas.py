from sqlalchemy import exc
from sqlalchemy import testing
from sqlalchemy.future import select as future_select
from sqlalchemy.schema import Column
from sqlalchemy.schema import ForeignKey
from sqlalchemy.schema import Table
from sqlalchemy.sql import and_
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import coercions
from sqlalchemy.sql import column
from sqlalchemy.sql import join
from sqlalchemy.sql import lambda_stmt
from sqlalchemy.sql import lambdas
from sqlalchemy.sql import roles
from sqlalchemy.sql import select
from sqlalchemy.sql import table
from sqlalchemy.sql import util as sql_util
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import ne_
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.types import Integer
from sqlalchemy.types import String


class DeferredLambdaTest(
    fixtures.TestBase, testing.AssertsExecutionResults, AssertsCompiledSQL
):
    __dialect__ = "default"

    def test_select_whereclause(self):
        t1 = table("t1", column("q"), column("p"))

        x = 10
        y = 5

        def go():
            return select(t1).where(lambda: and_(t1.c.q == x, t1.c.p == y))

        self.assert_compile(
            go(), "SELECT t1.q, t1.p FROM t1 WHERE t1.q = :x_1 AND t1.p = :y_1"
        )

        self.assert_compile(
            go(), "SELECT t1.q, t1.p FROM t1 WHERE t1.q = :x_1 AND t1.p = :y_1"
        )

    def test_global_tracking(self):
        t1 = table("t1", column("q"), column("p"))

        global global_x, global_y

        global_x = 10
        global_y = 17

        def go():
            return select(t1).where(
                lambda: and_(t1.c.q == global_x, t1.c.p == global_y)
            )

        self.assert_compile(
            go(),
            "SELECT t1.q, t1.p FROM t1 WHERE t1.q = :global_x_1 "
            "AND t1.p = :global_y_1",
            checkparams={"global_x_1": 10, "global_y_1": 17},
        )

        global_y = 9

        self.assert_compile(
            go(),
            "SELECT t1.q, t1.p FROM t1 WHERE t1.q = :global_x_1 "
            "AND t1.p = :global_y_1",
            checkparams={"global_x_1": 10, "global_y_1": 9},
        )

    def test_stale_checker_embedded(self):
        def go(x):

            stmt = select(lambda: x)
            return stmt

        c1 = column("x")
        s1 = go(c1)
        s2 = go(c1)

        self.assert_compile(s1, "SELECT x")
        self.assert_compile(s2, "SELECT x")

        c1 = column("q")

        s3 = go(c1)
        self.assert_compile(s3, "SELECT q")

    def test_stale_checker_statement(self):
        def go(x):

            stmt = lambdas.lambda_stmt(lambda: select(x))
            return stmt

        c1 = column("x")
        s1 = go(c1)
        s2 = go(c1)

        self.assert_compile(s1, "SELECT x")
        self.assert_compile(s2, "SELECT x")

        c1 = column("q")

        s3 = go(c1)
        self.assert_compile(s3, "SELECT q")

    def test_stale_checker_linked(self):
        def go(x, y):

            stmt = lambdas.lambda_stmt(lambda: select(x)) + (
                lambda s: s.where(y > 5)
            )
            return stmt

        c1 = oldc1 = column("x")
        c2 = oldc2 = column("y")
        s1 = go(c1, c2)
        s2 = go(c1, c2)

        self.assert_compile(s1, "SELECT x WHERE y > :y_1")
        self.assert_compile(s2, "SELECT x WHERE y > :y_1")

        c1 = column("q")
        c2 = column("p")

        s3 = go(c1, c2)
        self.assert_compile(s3, "SELECT q WHERE p > :p_1")

        s4 = go(c1, c2)
        self.assert_compile(s4, "SELECT q WHERE p > :p_1")

        s5 = go(oldc1, oldc2)
        self.assert_compile(s5, "SELECT x WHERE y > :y_1")

    def test_stmt_lambda_w_additional_hascachekey_variants(self):
        def go(col_expr, q):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt += lambda stmt: stmt.where(col_expr == q)

            return stmt

        c1 = column("x")
        c2 = column("y")

        s1 = go(c1, 5)
        s2 = go(c2, 10)
        s3 = go(c1, 8)
        s4 = go(c2, 12)

        self.assert_compile(
            s1, "SELECT x WHERE x = :q_1", checkparams={"q_1": 5}
        )
        self.assert_compile(
            s2, "SELECT y WHERE y = :q_1", checkparams={"q_1": 10}
        )
        self.assert_compile(
            s3, "SELECT x WHERE x = :q_1", checkparams={"q_1": 8}
        )
        self.assert_compile(
            s4, "SELECT y WHERE y = :q_1", checkparams={"q_1": 12}
        )

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()
        s3key = s3._generate_cache_key()
        s4key = s4._generate_cache_key()

        eq_(s1key[0], s3key[0])
        eq_(s2key[0], s4key[0])
        ne_(s1key[0], s2key[0])

    def test_stmt_lambda_w_atonce_whereclause_values_notrack(self):
        def go(col_expr, whereclause):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt = stmt.add_criteria(
                lambda stmt: stmt.where(whereclause), enable_tracking=False
            )

            return stmt

        c1 = column("x")

        s1 = go(c1, c1 == 5)
        s2 = go(c1, c1 == 10)

        self.assert_compile(
            s1, "SELECT x WHERE x = :x_1", checkparams={"x_1": 5}
        )

        # and as we see, this is wrong.   Because whereclause
        # is fixed for the lambda and we do not re-evaluate the closure
        # for this value changing.   this can't be passed unless
        # enable_tracking=False.
        self.assert_compile(
            s2, "SELECT x WHERE x = :x_1", checkparams={"x_1": 5}
        )

    def test_stmt_lambda_w_atonce_whereclause_values(self):
        c2 = column("y")

        def go(col_expr, whereclause, x):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt = stmt.add_criteria(
                lambda stmt: stmt.where(whereclause).order_by(c2 > x),
            )

            return stmt

        c1 = column("x")

        s1 = go(c1, c1 == 5, 9)
        s2 = go(c1, c1 == 10, 15)

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()

        eq_([b.value for b in s1key.bindparams], [5, 9])
        eq_([b.value for b in s2key.bindparams], [10, 15])

        self.assert_compile(
            s1,
            "SELECT x WHERE x = :x_1 ORDER BY y > :x_2",
            checkparams={"x_1": 5, "x_2": 9},
        )

        self.assert_compile(
            s2,
            "SELECT x WHERE x = :x_1 ORDER BY y > :x_2",
            checkparams={"x_1": 10, "x_2": 15},
        )

    def test_stmt_lambda_plain_customtrack(self):
        c2 = column("y")

        def go(col_expr, whereclause, p):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt = stmt.add_criteria(lambda stmt: stmt.where(whereclause))
            stmt = stmt.add_criteria(
                lambda stmt: stmt.order_by(col_expr), track_on=(col_expr,)
            )
            stmt = stmt.add_criteria(lambda stmt: stmt.where(col_expr == p))
            return stmt

        c1 = column("x")
        c2 = column("y")

        s1 = go(c1, c1 == 5, 9)
        s2 = go(c1, c1 == 10, 15)
        s3 = go(c2, c2 == 18, 12)

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()
        s3key = s3._generate_cache_key()

        eq_([b.value for b in s1key.bindparams], [5, 9])
        eq_([b.value for b in s2key.bindparams], [10, 15])
        eq_([b.value for b in s3key.bindparams], [18, 12])

        self.assert_compile(
            s1,
            "SELECT x WHERE x = :x_1 AND x = :p_1 ORDER BY x",
            checkparams={"x_1": 5, "p_1": 9},
        )

        self.assert_compile(
            s2,
            "SELECT x WHERE x = :x_1 AND x = :p_1 ORDER BY x",
            checkparams={"x_1": 10, "p_1": 15},
        )

        self.assert_compile(
            s3,
            "SELECT y WHERE y = :y_1 AND y = :p_1 ORDER BY y",
            checkparams={"y_1": 18, "p_1": 12},
        )

    def test_stmt_lambda_w_atonce_whereclause_customtrack_binds(self):
        c2 = column("y")

        # this pattern is *completely unnecessary*, and I would prefer
        # if we can detect this and just raise, because when it is not done
        # correctly, it is *extremely* difficult to catch it failing.
        # however I also can't come up with a reliable way to catch it.
        # so we will keep the use of "track_on" to be internal.

        def go(col_expr, whereclause, p):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt = stmt.add_criteria(
                lambda stmt: stmt.where(whereclause).order_by(col_expr > p),
                track_on=(whereclause, whereclause.right.value),
            )

            return stmt

        c1 = column("x")
        c2 = column("y")

        s1 = go(c1, c1 == 5, 9)
        s2 = go(c1, c1 == 10, 15)
        s3 = go(c2, c2 == 18, 12)

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()
        s3key = s3._generate_cache_key()

        eq_([b.value for b in s1key.bindparams], [5, 9])
        eq_([b.value for b in s2key.bindparams], [10, 15])
        eq_([b.value for b in s3key.bindparams], [18, 12])

        self.assert_compile(
            s1,
            "SELECT x WHERE x = :x_1 ORDER BY x > :p_1",
            checkparams={"x_1": 5, "p_1": 9},
        )

        self.assert_compile(
            s2,
            "SELECT x WHERE x = :x_1 ORDER BY x > :p_1",
            checkparams={"x_1": 10, "p_1": 15},
        )

        self.assert_compile(
            s3,
            "SELECT y WHERE y = :y_1 ORDER BY y > :p_1",
            checkparams={"y_1": 18, "p_1": 12},
        )

    def test_stmt_lambda_track_closure_binds_one(self):
        def go(col_expr, whereclause):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt += lambda stmt: stmt.where(whereclause)

            return stmt

        c1 = column("x")

        s1 = go(c1, c1 == 5)
        s2 = go(c1, c1 == 10)

        self.assert_compile(
            s1, "SELECT x WHERE x = :x_1", checkparams={"x_1": 5}
        )
        self.assert_compile(
            s2, "SELECT x WHERE x = :x_1", checkparams={"x_1": 10}
        )

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()

        eq_(s1key.key, s2key.key)

        eq_([b.value for b in s1key.bindparams], [5])
        eq_([b.value for b in s2key.bindparams], [10])

    def test_stmt_lambda_track_closure_binds_two(self):
        def go(col_expr, whereclause, x, y):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt += lambda stmt: stmt.where(whereclause).where(
                and_(c1 == x, c1 < y)
            )

            return stmt

        c1 = column("x")

        s1 = go(c1, c1 == 5, 8, 9)
        s2 = go(c1, c1 == 10, 12, 14)

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()

        self.assert_compile(
            s1,
            "SELECT x WHERE x = :x_1 AND x = :x_2 AND x < :y_1",
            checkparams={"x_1": 5, "x_2": 8, "y_1": 9},
        )
        self.assert_compile(
            s2,
            "SELECT x WHERE x = :x_1 AND x = :x_2 AND x < :y_1",
            checkparams={"x_1": 10, "x_2": 12, "y_1": 14},
        )

        eq_([b.value for b in s1key.bindparams], [5, 8, 9])
        eq_([b.value for b in s2key.bindparams], [10, 12, 14])

        s1_compiled_cached = s1.compile(cache_key=s1key)

        params = s1_compiled_cached.construct_params(
            extracted_parameters=s2key[1]
        )

        eq_(params, {"x_1": 10, "x_2": 12, "y_1": 14})

    def test_stmt_lambda_track_closure_binds_three(self):
        def go(col_expr, whereclause, x, y):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt += lambda stmt: stmt.where(whereclause)
            stmt += lambda stmt: stmt.where(and_(c1 == x, c1 < y))

            return stmt

        c1 = column("x")

        s1 = go(c1, c1 == 5, 8, 9)
        s2 = go(c1, c1 == 10, 12, 14)

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()

        self.assert_compile(
            s1,
            "SELECT x WHERE x = :x_1 AND x = :x_2 AND x < :y_1",
            checkparams={"x_1": 5, "x_2": 8, "y_1": 9},
        )
        self.assert_compile(
            s2,
            "SELECT x WHERE x = :x_1 AND x = :x_2 AND x < :y_1",
            checkparams={"x_1": 10, "x_2": 12, "y_1": 14},
        )

        eq_([b.value for b in s1key.bindparams], [5, 8, 9])
        eq_([b.value for b in s2key.bindparams], [10, 12, 14])

        s1_compiled_cached = s1.compile(cache_key=s1key)

        params = s1_compiled_cached.construct_params(
            extracted_parameters=s2key[1]
        )

        eq_(params, {"x_1": 10, "x_2": 12, "y_1": 14})

    def test_stmt_lambda_w_atonce_whereclause_novalue(self):
        def go(col_expr, whereclause):
            stmt = lambdas.lambda_stmt(lambda: select(col_expr))
            stmt += lambda stmt: stmt.where(whereclause)

            return stmt

        c1 = column("x")

        s1 = go(c1, bindparam("x"))

        self.assert_compile(s1, "SELECT x WHERE :x")

    def test_stmt_lambda_w_additional_hashable_variants(self):
        # note a Python 2 old style class would fail here because it
        # isn't hashable.   right now we do a hard check for __hash__ which
        # will raise if the attr isn't present
        class Thing(object):
            def __init__(self, col_expr):
                self.col_expr = col_expr

        def go(thing, q):
            stmt = lambdas.lambda_stmt(lambda: select(thing.col_expr))
            stmt += lambda stmt: stmt.where(thing.col_expr == q)

            return stmt

        c1 = Thing(column("x"))
        c2 = Thing(column("y"))

        s1 = go(c1, 5)
        s2 = go(c2, 10)
        s3 = go(c1, 8)
        s4 = go(c2, 12)

        self.assert_compile(
            s1, "SELECT x WHERE x = :q_1", checkparams={"q_1": 5}
        )
        self.assert_compile(
            s2, "SELECT y WHERE y = :q_1", checkparams={"q_1": 10}
        )
        self.assert_compile(
            s3, "SELECT x WHERE x = :q_1", checkparams={"q_1": 8}
        )
        self.assert_compile(
            s4, "SELECT y WHERE y = :q_1", checkparams={"q_1": 12}
        )

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()
        s3key = s3._generate_cache_key()
        s4key = s4._generate_cache_key()

        eq_(s1key[0], s3key[0])
        eq_(s2key[0], s4key[0])
        ne_(s1key[0], s2key[0])

    def test_stmt_lambda_w_set_of_opts(self):

        stmt = lambdas.lambda_stmt(lambda: select(column("x")))

        opts = {column("x"), column("y")}

        assert_raises_message(
            exc.ArgumentError,
            'Can\'t create a cache key for lambda closure variable "opts" '
            "because it's a set.  try using a list",
            stmt.__add__,
            lambda stmt: stmt.options(*opts),
        )

    def test_stmt_lambda_w_list_of_opts(self):
        def go(opts):
            stmt = lambdas.lambda_stmt(lambda: select(column("x")))
            stmt += lambda stmt: stmt.options(*opts)

            return stmt

        s1 = go([column("a"), column("b")])

        s2 = go([column("a"), column("b")])

        s3 = go([column("q"), column("b")])

        s1key = s1._generate_cache_key()
        s2key = s2._generate_cache_key()
        s3key = s3._generate_cache_key()

        eq_(s1key.key, s2key.key)
        ne_(s1key.key, s3key.key)

    def test_stmt_lambda_hey_theres_multiple_paths(self):
        def go(x, y):
            stmt = lambdas.lambda_stmt(lambda: select(column("x")))

            if x > 5:
                stmt += lambda stmt: stmt.where(column("x") == x)
            else:
                stmt += lambda stmt: stmt.where(column("y") == y)

            stmt += lambda stmt: stmt.order_by(column("q"))

            # TODO: need more path variety here to exercise
            # using a full path key

            return stmt

        s1 = go(2, 5)
        s2 = go(8, 7)
        s3 = go(4, 9)
        s4 = go(10, 1)

        self.assert_compile(s1, "SELECT x WHERE y = :y_1 ORDER BY q")
        self.assert_compile(s2, "SELECT x WHERE x = :x_1 ORDER BY q")
        self.assert_compile(s3, "SELECT x WHERE y = :y_1 ORDER BY q")
        self.assert_compile(s4, "SELECT x WHERE x = :x_1 ORDER BY q")

    def test_coercion_cols_clause(self):
        assert_raises_message(
            exc.ArgumentError,
            "Textual column expression 'f' should be explicitly declared",
            select,
            [lambda: "foo"],
        )

    def test_coercion_where_clause(self):
        assert_raises_message(
            exc.ArgumentError,
            "SQL expression for WHERE/HAVING role expected, got 5",
            select(column("q")).where,
            5,
        )

    def test_propagate_attrs_full_stmt(self):
        col = column("q")
        col._propagate_attrs = col._propagate_attrs.union(
            {"compile_state_plugin": "x", "plugin_subject": "y"}
        )

        stmt = lambdas.lambda_stmt(lambda: select(col))

        eq_(
            stmt._propagate_attrs,
            {"compile_state_plugin": "x", "plugin_subject": "y"},
        )

    def test_propagate_attrs_cols_clause(self):
        col = column("q")
        col._propagate_attrs = col._propagate_attrs.union(
            {"compile_state_plugin": "x", "plugin_subject": "y"}
        )

        stmt = select(lambda: col)

        eq_(
            stmt._propagate_attrs,
            {"compile_state_plugin": "x", "plugin_subject": "y"},
        )

    def test_propagate_attrs_from_clause(self):
        col = column("q")

        t = table("t", column("y"))

        t._propagate_attrs = t._propagate_attrs.union(
            {"compile_state_plugin": "x", "plugin_subject": "y"}
        )

        stmt = future_select(lambda: col).join(t)

        eq_(
            stmt._propagate_attrs,
            {"compile_state_plugin": "x", "plugin_subject": "y"},
        )

    def test_select_legacy_expanding_columns(self):
        q, p, r = column("q"), column("p"), column("r")

        stmt = select(lambda: (q, p, r))

        self.assert_compile(stmt, "SELECT q, p, r")

    def test_select_future_expanding_columns(self):
        q, p, r = column("q"), column("p"), column("r")

        stmt = future_select(lambda: (q, p, r))

        self.assert_compile(stmt, "SELECT q, p, r")

    def test_select_fromclause(self):
        t1 = table("t1", column("q"), column("p"))
        t2 = table("t2", column("y"))

        def go():
            return select(t1).select_from(
                lambda: join(t1, t2, lambda: t1.c.q == t2.c.y)
            )

        self.assert_compile(
            go(), "SELECT t1.q, t1.p FROM t1 JOIN t2 ON t1.q = t2.y"
        )

        self.assert_compile(
            go(), "SELECT t1.q, t1.p FROM t1 JOIN t2 ON t1.q = t2.y"
        )

    def test_in_parameters_one(self):

        expr1 = select(1).where(column("q").in_(["a", "b", "c"]))
        self.assert_compile(expr1, "SELECT 1 WHERE q IN ([POSTCOMPILE_q_1])")

        self.assert_compile(
            expr1,
            "SELECT 1 WHERE q IN (:q_1_1, :q_1_2, :q_1_3)",
            render_postcompile=True,
            checkparams={"q_1_1": "a", "q_1_2": "b", "q_1_3": "c"},
        )

    def test_in_parameters_two(self):
        expr2 = select(1).where(lambda: column("q").in_(["a", "b", "c"]))
        self.assert_compile(expr2, "SELECT 1 WHERE q IN ([POSTCOMPILE_q_1])")
        self.assert_compile(
            expr2,
            "SELECT 1 WHERE q IN (:q_1_1, :q_1_2, :q_1_3)",
            render_postcompile=True,
            checkparams={"q_1_1": "a", "q_1_2": "b", "q_1_3": "c"},
        )

    def test_in_parameters_three(self):
        expr3 = lambdas.lambda_stmt(
            lambda: select(1).where(column("q").in_(["a", "b", "c"]))
        )
        self.assert_compile(expr3, "SELECT 1 WHERE q IN ([POSTCOMPILE_q_1])")
        self.assert_compile(
            expr3,
            "SELECT 1 WHERE q IN (:q_1_1, :q_1_2, :q_1_3)",
            render_postcompile=True,
            checkparams={"q_1_1": "a", "q_1_2": "b", "q_1_3": "c"},
        )

    def test_in_parameters_four(self):
        def go(names):
            return lambdas.lambda_stmt(
                lambda: select(1).where(column("q").in_(names))
            )

        expr4 = go(["a", "b", "c"])
        self.assert_compile(
            expr4, "SELECT 1 WHERE q IN ([POSTCOMPILE_names_1])"
        )
        self.assert_compile(
            expr4,
            "SELECT 1 WHERE q IN (:names_1_1, :names_1_2, :names_1_3)",
            render_postcompile=True,
            checkparams={"names_1_1": "a", "names_1_2": "b", "names_1_3": "c"},
        )

    def test_in_parameters_five(self):
        def go(n1, n2):
            stmt = lambdas.lambda_stmt(
                lambda: select(1).where(column("q").in_(n1))
            )
            stmt += lambda s: s.where(column("y").in_(n2))
            return stmt

        expr = go(["a", "b", "c"], ["d", "e", "f"])
        self.assert_compile(
            expr,
            "SELECT 1 WHERE q IN (:n1_1_1, :n1_1_2, :n1_1_3) "
            "AND y IN (:n2_1_1, :n2_1_2, :n2_1_3)",
            render_postcompile=True,
            checkparams={
                "n1_1_1": "a",
                "n1_1_2": "b",
                "n1_1_3": "c",
                "n2_1_1": "d",
                "n2_1_2": "e",
                "n2_1_3": "f",
            },
        )

    def test_select_columns_clause(self):
        t1 = table("t1", column("q"), column("p"))

        g = 5

        def go():
            return select(lambda: t1.c.q, lambda: t1.c.p + g)

        stmt = go()
        self.assert_compile(
            stmt,
            "SELECT t1.q, t1.p + :g_1 AS anon_1 FROM t1",
            checkparams={"g_1": 5},
        )
        eq_(stmt._generate_cache_key()._generate_param_dict(), {"g_1": 5})

        g = 10
        stmt = go()
        self.assert_compile(
            stmt,
            "SELECT t1.q, t1.p + :g_1 AS anon_1 FROM t1",
            checkparams={"g_1": 10},
        )
        eq_(stmt._generate_cache_key()._generate_param_dict(), {"g_1": 10})

    @testing.metadata_fixture()
    def user_address_fixture(self, metadata):
        users = Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )
        addresses = Table(
            "addresses",
            metadata,
            Column("id", Integer),
            Column("user_id", ForeignKey("users.id")),
            Column("email", String(50)),
        )
        return users, addresses

    def test_adapt_select(self, user_address_fixture):
        users, addresses = user_address_fixture

        stmt = (
            select(users)
            .select_from(
                users.join(
                    addresses, lambda: users.c.id == addresses.c.user_id
                )
            )
            .where(lambda: users.c.name == "ed")
        )

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id "
            "WHERE users.name = :name_1",
        )

        u1 = users.alias()
        adapter = sql_util.ClauseAdapter(u1)

        s2 = adapter.traverse(stmt)

        self.assert_compile(
            s2,
            "SELECT users_1.id, users_1.name FROM users AS users_1 "
            "JOIN addresses ON users_1.id = addresses.user_id "
            "WHERE users_1.name = :name_1",
        )

    def test_no_var_dict_keys(self, user_address_fixture):
        users, addresses = user_address_fixture

        names = {"x": "some name"}
        foo = "x"
        expr = lambda: users.c.name == names[foo]  # noqa

        assert_raises_message(
            exc.InvalidRequestError,
            "Dictionary keys / list indexes inside of a cached "
            "lambda must be Python literals only",
            coercions.expect,
            roles.WhereHavingRole,
            expr,
        )

    def test_dict_literal_keys(self, user_address_fixture):
        users, addresses = user_address_fixture

        names = {"x": "some name"}
        lmb = lambda: users.c.name == names["x"]  # noqa

        expr = coercions.expect(roles.WhereHavingRole, lmb)

        self.assert_compile(
            expr,
            "users.name = :x_1",
            params=expr._param_dict(),
            checkparams={"x_1": "some name"},
        )

    def test_assignment_one(self, user_address_fixture):
        users, addresses = user_address_fixture

        x = 5

        def my_lambda():

            y = 10
            z = y + 18

            expr1 = users.c.name > x
            expr2 = users.c.name < z
            return and_(expr1, expr2)

        expr = coercions.expect(roles.WhereHavingRole, my_lambda)
        self.assert_compile(
            expr,
            "users.name > :x_1 AND users.name < :name_1",
            params=expr._param_dict(),
            checkparams={"name_1": 28, "x_1": 5},
        )

        expr = coercions.expect(roles.WhereHavingRole, my_lambda)
        self.assert_compile(
            expr,
            "users.name > :x_1 AND users.name < :name_1",
            params=expr._param_dict(),
            checkparams={"name_1": 28, "x_1": 5},
        )

    def test_assignment_two(self, user_address_fixture):
        users, addresses = user_address_fixture

        x = 5
        z = 10

        def my_lambda():

            y = x + z

            expr1 = users.c.name > x
            expr2 = users.c.name < y
            return and_(expr1, expr2)

        expr = coercions.expect(roles.WhereHavingRole, my_lambda)
        self.assert_compile(
            expr,
            "users.name > :x_1 AND users.name < :x_1 + :z_1",
            params=expr._param_dict(),
            checkparams={"x_1": 5, "z_1": 10},
        )

        x = 15
        z = 18

        expr = coercions.expect(roles.WhereHavingRole, my_lambda)
        self.assert_compile(
            expr,
            "users.name > :x_1 AND users.name < :x_1 + :z_1",
            params=expr._param_dict(),
            checkparams={"x_1": 15, "z_1": 18},
        )

    def test_assignment_three(self, user_address_fixture):
        users, addresses = user_address_fixture

        x = 5
        z = 10

        def my_lambda():

            y = 10 + z

            expr1 = users.c.name > x
            expr2 = users.c.name < y
            return and_(expr1, expr2)

        expr = coercions.expect(roles.WhereHavingRole, my_lambda)
        self.assert_compile(
            expr,
            "users.name > :x_1 AND users.name < :param_1 + :z_1",
            params=expr._param_dict(),
            checkparams={"x_1": 5, "z_1": 10, "param_1": 10},
        )

        x = 15
        z = 18

        expr = coercions.expect(roles.WhereHavingRole, my_lambda)
        self.assert_compile(
            expr,
            "users.name > :x_1 AND users.name < :param_1 + :z_1",
            params=expr._param_dict(),
            checkparams={"x_1": 15, "z_1": 18, "param_1": 10},
        )

    def test_op_reverse(self, user_address_fixture):
        user, addresses = user_address_fixture

        x = "foo"

        def mylambda():
            return x + user.c.name

        expr = coercions.expect(roles.WhereHavingRole, mylambda)
        self.assert_compile(
            expr, ":x_1 || users.name", checkparams={"x_1": "foo"}
        )

        x = "bar"
        expr = coercions.expect(roles.WhereHavingRole, mylambda)
        self.assert_compile(
            expr, ":x_1 || users.name", checkparams={"x_1": "bar"}
        )

    def test_op_forwards(self, user_address_fixture):
        user, addresses = user_address_fixture

        x = "foo"

        def mylambda():
            return user.c.name + x

        expr = coercions.expect(roles.WhereHavingRole, mylambda)
        self.assert_compile(
            expr, "users.name || :x_1", checkparams={"x_1": "foo"}
        )

        x = "bar"
        expr = coercions.expect(roles.WhereHavingRole, mylambda)
        self.assert_compile(
            expr, "users.name || :x_1", checkparams={"x_1": "bar"}
        )

    def test_execute_constructed_uncached(self, user_address_fixture):
        users, addresses = user_address_fixture

        def go(name):
            stmt = select(lambda: users.c.id).where(
                lambda: users.c.name == name
            )
            with testing.db.connect().execution_options(
                compiled_cache=None
            ) as conn:
                conn.execute(stmt)

        with self.sql_execution_asserter(testing.db) as asserter:
            go("name1")
            go("name2")
            go("name1")
            go("name3")

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name2"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name3"}],
            ),
        )

    def test_execute_full_uncached(self, user_address_fixture):
        users, addresses = user_address_fixture

        def go(name):
            stmt = lambda_stmt(
                lambda: select(users.c.id).where(users.c.name == name)  # noqa
            )

            with testing.db.connect().execution_options(
                compiled_cache=None
            ) as conn:
                conn.execute(stmt)

        with self.sql_execution_asserter(testing.db) as asserter:
            go("name1")
            go("name2")
            go("name1")
            go("name3")

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name2"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name3"}],
            ),
        )

    def test_execute_constructed_cached(self, user_address_fixture):
        users, addresses = user_address_fixture

        cache = {}

        def go(name):
            stmt = select(lambda: users.c.id).where(
                lambda: users.c.name == name
            )

            with testing.db.connect().execution_options(
                compiled_cache=cache
            ) as conn:
                conn.execute(stmt)

        with self.sql_execution_asserter(testing.db) as asserter:
            go("name1")
            go("name2")
            go("name1")
            go("name3")

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name2"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name3"}],
            ),
        )

    def test_execute_full_cached(self, user_address_fixture):
        users, addresses = user_address_fixture

        cache = {}

        def go(name):
            stmt = lambda_stmt(
                lambda: select(users.c.id).where(users.c.name == name)  # noqa
            )

            with testing.db.connect().execution_options(
                compiled_cache=cache
            ) as conn:
                conn.execute(stmt)

        with self.sql_execution_asserter(testing.db) as asserter:
            go("name1")
            go("name2")
            go("name1")
            go("name3")

        asserter.assert_(
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name2"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name1"}],
            ),
            CompiledSQL(
                "SELECT users.id FROM users WHERE users.name = :name_1",
                lambda ctx: [{"name_1": "name3"}],
            ),
        )

    def test_cache_key_thing(self):
        t1 = table("t1", column("q"), column("p"))

        def go(x):
            return coercions.expect(roles.WhereHavingRole, lambda: t1.c.q == x)

        expr1 = go(5)
        expr2 = go(10)

        is_(expr1._generate_cache_key().bindparams[0], expr1._resolved.right)
        is_(expr2._generate_cache_key().bindparams[0], expr2._resolved.right)
