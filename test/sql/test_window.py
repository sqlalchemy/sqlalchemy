from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import window
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures


class NamedWindowTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_object_reference_adds_definition(self):
        w = window("w", partition_by=column("x"), order_by=column("y"))

        stmt = select(func.sum(column("z")).over(w))

        self.assert_compile(
            stmt,
            "SELECT sum(z) OVER w AS anon_1 "
            "WINDOW w AS (PARTITION BY x ORDER BY y)",
        )

    def test_order_by_reference_adds_definition(self):
        w = window("w", partition_by=column("x"))

        stmt = select(column("z")).order_by(func.sum(column("z")).over(w))

        self.assert_compile(
            stmt,
            "SELECT z WINDOW w AS (PARTITION BY x) ORDER BY sum(z) OVER w",
        )

    def test_string_reference_with_explicit_definition(self):
        w = window("w", partition_by=column("x"))

        stmt = select(func.sum(column("z")).over("w")).add_window(w)

        self.assert_compile(
            stmt,
            "SELECT sum(z) OVER w AS anon_1 " "WINDOW w AS (PARTITION BY x)",
        )

    def test_derived_window_dependency_order(self):
        w1 = window("w1", partition_by=column("x"))
        w2 = w1.window("w2", order_by=column("y"))

        stmt = select(func.sum(column("z")).over(w2))

        self.assert_compile(
            stmt,
            "SELECT sum(z) OVER w2 AS anon_1 "
            "WINDOW w1 AS (PARTITION BY x), w2 AS (w1 ORDER BY y)",
        )

    def test_named_window_with_local_frame(self):
        w = window("w", order_by=column("x"))

        stmt = select(func.sum(column("z")).over(w, rows=(None, 0)))

        self.assert_compile(
            stmt,
            "SELECT sum(z) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING "
            "AND CURRENT ROW) AS anon_1 WINDOW w AS (ORDER BY x)",
        )

    def test_window_name_quoted(self):
        w = window("select", partition_by=column("x"))

        stmt = select(func.sum(column("z")).over(w))

        self.assert_compile(
            stmt,
            'SELECT sum(z) OVER "select" AS anon_1 '
            'WINDOW "select" AS (PARTITION BY x)',
        )

    def test_conflicting_window_names(self):
        w1 = window("w", partition_by=column("x"))
        w2 = window("w", partition_by=column("y"))
        stmt = select(column("z")).add_window(w1, w2)

        assert_raises_message(
            exc.CompileError,
            "named window 'w' is defined more than once",
            stmt.compile,
        )

    def test_window_definition_contributes_from_clause(self):
        t = table("t", column("x"))
        w = window("w", partition_by=t.c.x)

        stmt = select(func.count().over("w")).add_window(w)

        self.assert_compile(
            stmt,
            "SELECT count(*) OVER w AS anon_1 FROM t "
            "WINDOW w AS (PARTITION BY t.x)",
        )

    def test_window_cache_key_tracks_definition(self):
        w1 = window("w", partition_by=column("x"))
        w2 = window("w", partition_by=column("x"))
        w3 = window("w", partition_by=column("y"))

        stmt1 = select(func.sum(column("z")).over(w1))
        stmt2 = select(func.sum(column("z")).over(w2))
        stmt3 = select(func.sum(column("z")).over(w3))

        assert stmt1._generate_cache_key() == stmt2._generate_cache_key()
        assert stmt1._generate_cache_key() != stmt3._generate_cache_key()

    def test_nested_select_keeps_window_scope_local(self):
        w = window("inner_w", partition_by=column("x"))
        inner = select(func.sum(column("z")).over(w).label("total")).subquery()

        self.assert_compile(
            select(inner.c.total),
            "SELECT anon_1.total FROM "
            "(SELECT sum(z) OVER inner_w AS total "
            "WINDOW inner_w AS (PARTITION BY x)) AS anon_1",
        )

    def test_string_derived_window(self):
        w1 = window("w1", partition_by=column("x"))
        w2 = window("w2", existing_window="w1", order_by=column("y"))

        stmt = select(func.sum(column("z")).over("w2")).add_window(w1, w2)

        self.assert_compile(
            stmt,
            "SELECT sum(z) OVER w2 AS anon_1 "
            "WINDOW w1 AS (PARTITION BY x), w2 AS (w1 ORDER BY y)",
        )

    def test_invalid_window_reference(self):
        assert_raises_message(
            exc.ArgumentError,
            "window must be a Window object or a non-empty name",
            func.sum(column("z")).over,
            "",
        )

        assert_raises_message(
            exc.ArgumentError,
            "existing_window must be a Window object or a non-empty name",
            window,
            "w",
            existing_window="",
        )

    def test_cyclic_window_definitions(self):
        w1 = window("w1")
        w2 = window("w2", existing_window=w1)
        w1.existing_window = w2

        stmt = select(column("z")).add_window(w1)

        assert_raises_message(
            exc.CompileError,
            "named window definitions are cyclic",
            stmt.compile,
        )
