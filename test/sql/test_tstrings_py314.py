"""Test the TString construct for Python 3.14+ template strings."""

from itertools import zip_longest

from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import tstring
from sqlalchemy.engine.interfaces import CacheStats
from sqlalchemy.sql import table
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.sqltypes import TypeEngine
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message

table1 = table(
    "mytable",
    column("myid", Integer),
    column("name", String),
    column("description", String),
)

table2 = table(
    "myothertable", column("otherid", Integer), column("othername", String)
)


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_basic_literal_interpolation(self):
        a = 5
        b = 10
        stmt = tstring(t"select {a}, {b}")
        self.assert_compile(
            stmt,
            "select :param_1, :param_2",
            checkparams={"param_1": 5, "param_2": 10},
        )

    def test_no_strings(self):
        with expect_raises_message(
            exc.ArgumentError, r"pep-750 Tstring \(e.g. t'...'\) expected"
        ):
            tstring("select * from table")  # type: ignore

    def test_tstring_literal_passthrough(self):
        stmt = tstring(t"select * from foo where lala = bar")
        self.assert_compile(stmt, "select * from foo where lala = bar")

    def test_sqlalchemy_expression_interpolation(self):
        subq = select(literal(1)).scalar_subquery()
        stmt = tstring(t"SELECT {subq}")
        self.assert_compile(
            stmt,
            "SELECT (SELECT :param_1 AS anon_1)",
            checkparams={"param_1": 1},
        )

    def test_column_interpolation(self):
        stmt = tstring(t"SELECT {table1.c.myid}, {table1.c.name} FROM mytable")
        self.assert_compile(
            stmt, "SELECT mytable.myid, mytable.name FROM mytable"
        )

    def test_column_interpolation_labeled(self):
        # Labels are not supported inside tstring as they're ambiguous
        # (should they render with AS in all contexts?)
        label1 = table1.c.myid.label("label1")
        label2 = table1.c.name.label("label2")

        with expect_raises_message(
            exc.CompileError,
            "Using label\\(\\) directly inside tstring is not supported",
        ):
            tstring(t"SELECT {label1}, {label2} FROM mytable").compile()

    def test_arithmetic_expression(self):
        # Python arithmetic is evaluated before being passed to tstring
        a = 1
        stmt = tstring(t"SELECT {a + 7}")
        self.assert_compile(
            stmt, "SELECT :param_1", checkparams={"param_1": 8}
        )

    def test_embed_tstring_as_select_criteria(self):
        user_id = 123
        stmt = select(table1).where(tstring(t"{table1.c.myid} = {user_id}"))
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description FROM "
            "mytable WHERE mytable.myid = :param_1",
            checkparams={"param_1": 123},
        )

    def test_embed_tstring_as_fromclause(self):
        status = "some status"
        stmt = select(column("x")).select_from(
            tstring(
                t"foobar left outer join lala on foobar.foo = lala.foo "
                t"AND foobar.status = {status}"
            )
        )
        self.assert_compile(
            stmt,
            "SELECT x FROM foobar left outer join "
            "lala on foobar.foo = lala.foo AND foobar.status = :param_1",
            checkparams={"param_1": "some status"},
        )

    def test_and_operator(self):
        stmt = tstring(t"1 = 1") & tstring(t"2 = 2")
        self.assert_compile(stmt, "1 = 1 AND 2 = 2")

    def test_multiple_literals(self):
        a, b, c, d = 1, 2, 3, 4
        stmt = tstring(t"SELECT {a}, {b}, {c}, {d}")
        self.assert_compile(
            stmt,
            "SELECT :param_1, :param_2, :param_3, :param_4",
            checkparams={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
            },
        )

    def test_nested_tstring_execution(self):
        inner = tstring(t"(SELECT {'some value'} AS anon_1)")
        self.assert_compile(
            tstring(t"select {inner}"),
            "select (SELECT :param_1 AS anon_1)",
            checkparams={"param_1": "some value"},
        )

    def test_nested_scalar_subquery_execution(self):
        inner = select(literal("some value")).scalar_subquery()
        self.assert_compile(
            tstring(t"select {inner}"),
            "select (SELECT :param_1 AS anon_1)",
            checkparams={"param_1": "some value"},
        )

    def test_nested_subquery_execution(self):
        inner = select(literal("some value")).subquery()
        self.assert_compile(
            tstring(t"select * from {inner}"),
            "select * from (SELECT :param_1 AS anon_2) AS anon_1",
            checkparams={"param_1": "some value"},
        )


class ColumnsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _assert_columns(self, stmt, columns):
        """Assert that stmt.selected_columns matches the given columns.

        Also verifies that the result map structure matches what we'd get
        from a regular select() statement with the same columns.
        """
        # Check that selected_columns matches
        eq_(
            [c.name for c in stmt.selected_columns],
            [c.name for c in columns],
        )
        for stmt_col, expected_col in zip(stmt.selected_columns, columns):
            eq_(stmt_col.type._type_affinity, expected_col.type._type_affinity)

        # Verify result map structure matches what select() would produce
        stmt_compiled = stmt.compile()
        select_compiled = select(*columns).compile()
        stmt_map = stmt_compiled._create_result_map()
        select_map = select_compiled._create_result_map()

        # Compare result map structure using recursive comparison
        eq_(list(stmt_map.keys()), list(select_map.keys()))
        for key in stmt_map:
            stmt_entry = stmt_map[key]
            select_entry = select_map[key]
            # Use recursive comparison for the entire entry tuple
            assert self._compare_recursive(
                stmt_entry, select_entry
            ), f"Result map entries differ:\n  {stmt_entry}\n  {select_entry}"

    def _compare_recursive(self, left, right):
        if isinstance(left, ColumnClause) and isinstance(right, ColumnClause):
            return (
                left.name == right.name
                and left.type._type_affinity == right.type._type_affinity
            )
        elif isinstance(left, TypeEngine) and isinstance(right, TypeEngine):
            return left._type_affinity == right._type_affinity
        elif isinstance(left, (tuple, list)) and isinstance(
            right, (tuple, list)
        ):
            return all(
                self._compare_recursive(l, r)
                for l, r in zip_longest(left, right)
            )
        else:
            return left == right

    def test_columns_positional(self):
        cols = [column("id", Integer), column("name", String)]
        stmt = tstring(t"SELECT id, name FROM users").columns(*cols)
        self.assert_compile(stmt, "SELECT id, name FROM users")
        self._assert_columns(stmt, cols)

    def test_columns_keyword(self):
        stmt = tstring(t"SELECT id, name FROM users").columns(
            id=Integer, name=String
        )
        self.assert_compile(stmt, "SELECT id, name FROM users")
        cols = [column("id", Integer), column("name", String)]
        self._assert_columns(stmt, cols)

    def test_columns_mixed(self):
        cols = [
            column("id", Integer),
            column("name", String),
            column("age", Integer),
        ]
        stmt = tstring(t"SELECT id, name, age FROM users").columns(
            cols[0], name=String, age=Integer
        )
        self.assert_compile(stmt, "SELECT id, name, age FROM users")
        self._assert_columns(stmt, cols)

    def test_columns_subquery(self):
        stmt = (
            tstring(t"SELECT id, name FROM users")
            .columns(column("id", Integer), column("name", String))
            .subquery("st")
        )
        outer = select(table1).select_from(
            table1.join(stmt, table1.c.name == stmt.c.name)
        )
        self.assert_compile(
            outer,
            "SELECT mytable.myid, mytable.name, mytable.description FROM "
            "mytable JOIN (SELECT id, name FROM users) AS st ON "
            "mytable.name = st.name",
        )


class ExecutionTest(fixtures.TestBase):
    __backend__ = True

    def test_basic_execution(self, connection):
        a = 1
        b = 2
        result = connection.execute(tstring(t"select {a + 7}, {b}"))
        eq_(result.all(), [(8, 2)])

    @testing.requires.json_type
    def test_json_literal_execution(self, connection):
        some_json = {"foo": "bar"}
        stmt = tstring(t"select {literal(some_json, JSON)}").columns(
            column("jj", JSON)
        )
        result = connection.execute(stmt)
        row = result.scalar()
        eq_(row, {"foo": "bar"})

    @testing.requires.json_type
    def test_statement_caching(self, connection):
        """Test that tstring statements are properly cached."""
        some_json = {"foo": "bar"}
        stmt1 = tstring(t"select {literal(some_json, JSON)}").columns(
            column("jj", JSON)
        )
        result1 = connection.execute(stmt1)
        eq_(result1.scalar(), {"foo": "bar"})

        # Execute same structure with different value
        some_json = {"foo": "newbar", "bat": "hoho"}
        stmt2 = tstring(t"select {literal(some_json, JSON)}").columns(
            column("jj", JSON)
        )
        result2 = connection.execute(stmt2)

        # Should hit cache
        if hasattr(result2.context, "cache_hit"):
            eq_(result2.context.cache_hit, CacheStats.CACHE_HIT)

        eq_(result2.scalar(), {"foo": "newbar", "bat": "hoho"})

    def test_nested_scalar_subquery_execution(self, connection):
        inner = select(literal("some value")).scalar_subquery()
        result = connection.execute(tstring(t"select {inner}"))
        eq_(result.all(), [("some value",)])

    def test_nested_subquery_execution(self, connection):
        inner = select(literal("some value")).subquery()
        result = connection.execute(tstring(t"select * from {inner}"))
        eq_(result.all(), [("some value",)])

    def test_multiple_values(self, connection):
        values = [1, 2, 3, 4, 5]
        result = connection.execute(
            tstring(
                t"select {values[0]}, {values[1]}, {values[2]}, "
                t"{values[3]}, {values[4]}"
            )
        )
        eq_(result.all(), [(1, 2, 3, 4, 5)])


class IntegrationTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy import Column
        from sqlalchemy import Table

        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.users.insert(),
            [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
                {"id": 3, "name": "charlie"},
            ],
        )

    def test_select_from_real_table(self, connection):
        user_id = 2
        stmt = tstring(t"SELECT * FROM users WHERE id = {user_id}").columns(
            column("id", Integer), column("name", String)
        )
        result = connection.execute(stmt)
        row = result.one()
        eq_(row.id, 2)
        eq_(row.name, "bob")

    def test_where_clause_with_real_table(self, connection):
        users = self.tables.users
        name_filter = "alice"
        stmt = select(users).where(
            tstring(t"{users.c.name} = {literal(name_filter)}")
        )
        result = connection.execute(stmt)
        row = result.one()
        eq_(row.id, 1)
        eq_(row.name, "alice")

    def test_complex_query(self, connection):
        min_id = 1
        max_id = 2
        stmt = tstring(
            t"SELECT id, name FROM users WHERE id >= {min_id} "
            t"AND id <= {max_id}"
        ).columns(column("id", Integer), column("name", String))
        result = connection.execute(stmt)
        rows = result.all()
        eq_(len(rows), 2)
        eq_(rows[0].name, "alice")
        eq_(rows[1].name, "bob")


class CacheKeyTest(fixtures.CacheKeyFixture, fixtures.TestBase):
    """Test cache key generation for tstring constructs."""

    @fixtures.CacheKeySuite.run_suite_tests
    def test_tstring_cache_key(self):

        def stmt1():
            # Basic tstring with literal
            a = 5
            return tstring(t"SELECT {a}")

        def stmt2():
            # Different structure - two literals
            a = 5
            b = 10
            return tstring(t"SELECT {a}, {b}")

        def stmt3():
            # With column reference
            return tstring(t"SELECT {table1.c.myid}")

        def stmt4():
            # Different column - different cache key
            return tstring(t"SELECT {table1.c.name}")

        def stmt5():
            # With .columns()
            a = 5
            return tstring(t"SELECT {a}").columns(column("val", Integer))

        def stmt6():
            # String literal passthrough
            return tstring(t"SELECT * FROM users")

        def stmt7():
            # Different string literal
            return tstring(t"SELECT id FROM users")

        def stmt8():
            # With SQLAlchemy scalar subquery
            return tstring(t"SELECT {select(literal(1)).scalar_subquery()}")

        def stmt9():
            # Mixed: text and literal
            user_id = 42
            return tstring(t"SELECT * FROM users WHERE id = {user_id}")

        def stmt10():
            # Mixed: text and column
            return tstring(t"SELECT * FROM users WHERE id = {table1.c.myid}")

        return lambda: [
            stmt1(),
            stmt2(),
            stmt3(),
            stmt4(),
            stmt5(),
            stmt6(),
            stmt7(),
            stmt8(),
            stmt9(),
            stmt10(),
        ]
