import re

from sqlalchemy import bindparam
from sqlalchemy import literal
from sqlalchemy import testing
from sqlalchemy.engine import default as default_engine
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql import column
from sqlalchemy.sql import select
from sqlalchemy.sql import table
from sqlalchemy.sql.ddl import CreateTableAs
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import expect_raises_message


class CreateTableAsDefaultDialectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def src_table(self):
        return table("src", column("id"), column("name"))

    @testing.fixture
    def src_two_tables(self):
        a = table("a", column("id"), column("name"))
        b = table("b", column("id"), column("status"))
        return a, b

    def assert_inner_params(self, stmt, expected, dialect=None):
        d = default_engine.DefaultDialect() if dialect is None else dialect
        inner = stmt.selectable.compile(dialect=d)
        assert (
            inner.params == expected
        ), f"Got {inner.params}, expected {expected}"

    def test_basic_element(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id, src.c.name).select_from(src),
            "dst",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS SELECT src.id, src.name FROM src",
        )

    def test_schema_element_qualified(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst",
            schema="analytics",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE analytics.dst AS SELECT src.id FROM src",
        )

    def test_blank_schema_treated_as_none(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src), "dst", schema=""
        )
        self.assert_compile(stmt, "CREATE TABLE dst AS SELECT src.id FROM src")

    def test_binds_preserved(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(bindparam("tag", value="x").label("tag")).select_from(src),
            "dst",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS SELECT :tag AS tag FROM src",
        )
        self.assert_inner_params(stmt, {"tag": "x"})

    def test_flags_not_rendered_in_default(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst",
            schema="sch",
            temporary=True,
            if_not_exists=True,
        )
        # Default baseline omits TEMPORARY / IF NOT EXISTS; dialects add them.
        self.assert_compile(
            stmt,
            "CREATE TABLE sch.dst AS SELECT src.id FROM src",
        )

    def test_join_with_binds_preserved(self, src_two_tables):
        a, b = src_two_tables

        s = (
            select(a.c.id, a.c.name)
            .select_from(a.join(b, a.c.id == b.c.id))
            .where(b.c.status == bindparam("p_status", value="active"))
        ).into("dst")

        # Ensure WHERE survives into CTAS and params are preserved
        self.assert_compile(
            s,
            "CREATE TABLE dst AS "
            "SELECT a.id, a.name FROM a JOIN b ON a.id = b.id "
            "WHERE b.status = :p_status",
        )
        self.assert_inner_params(s, {"p_status": "active"})

    def test_into_equivalent_to_element(self, src_table):
        src = src_table
        s = (
            select(src.c.id)
            .select_from(src)
            .where(src.c.id == bindparam("p", value=2))
        )
        via_into = s.into("dst")
        via_element = CreateTableAs(s, "dst")

        self.assert_compile(
            via_into,
            "CREATE TABLE dst AS SELECT src.id FROM src WHERE src.id = :p",
        )
        self.assert_compile(
            via_element,
            "CREATE TABLE dst AS SELECT src.id FROM src WHERE src.id = :p",
        )
        # Param parity (inner SELECT of both)
        self.assert_inner_params(via_into, {"p": 2})
        self.assert_inner_params(via_element, {"p": 2})

    def test_into_does_not_mutate_original_select(self, src_table):
        src = src_table
        s = select(src.c.id).select_from(src).where(src.c.id == 5)

        # compile original SELECT
        self.assert_compile(
            s,
            "SELECT src.id FROM src WHERE src.id = :id_1",
        )

        # build CTAS
        _ = s.into("dst")

        # original is still a SELECT
        self.assert_compile(
            s,
            "SELECT src.id FROM src WHERE src.id = :id_1",
        )

    def test_into_with_schema_argument(self, src_table):
        src = src_table
        s = select(src.c.id).select_from(src).into("t", schema="analytics")
        self.assert_compile(
            s,
            "CREATE TABLE analytics.t AS SELECT src.id FROM src",
        )

    def test_target_table_without_schema_accepts_schema_kw(self):
        tgt = table("dst")

        s = select(bindparam("v", value=1).label("anon_1")).select_from(
            table("x")
        )

        stmt = CreateTableAs(
            s,
            tgt,
            schema="sch",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE sch.dst AS SELECT :v AS anon_1 FROM x",
        )
        self.assert_inner_params(stmt, {"v": 1})

    def test_target_as_table_with_schema_and_conflict(self):
        # Target object with schema set
        tgt = table("dst", schema="sch")

        # Conflicting schema in ctor should raise ArgumentError
        with expect_raises_message(
            ArgumentError,
            r"Conflicting schema",
        ):
            CreateTableAs(
                select(literal(1)).select_from(table("x")),
                tgt,
                schema="other",
            )

    def test_target_string_must_be_unqualified(self, src_table):
        src = src_table
        with expect_raises_message(
            ArgumentError,
            re.escape("Target string must be unqualified (use schema=)."),
        ):
            CreateTableAs(select(src.c.id).select_from(src), "sch.dst")

    def test_empty_name(self):
        with expect_raises_message(
            ArgumentError, "Table name must be non-empty"
        ):
            CreateTableAs(select(literal(1)), "")

    def test_generated_table_property(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src), "dst", schema="sch"
        )
        gt = stmt.generated_table
        assert gt.name == "dst"
        assert gt.schema == "sch"

    def test_labels_in_select_list_preserved(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(
                src.c.id.label("user_id"), src.c.name.label("user_name")
            ).select_from(src),
            "dst",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT src.id AS user_id, src.name AS user_name FROM src",
        )

    def test_distinct_and_group_by_survive(self, src_table):
        src = src_table
        sel = (
            select(src.c.name).select_from(src).distinct().group_by(src.c.name)
        )
        stmt = CreateTableAs(sel, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT DISTINCT src.name FROM src GROUP BY src.name",
        )

    def test_union_all_with_binds_preserved(self, src_two_tables):
        a, b = src_two_tables

        # Named binds so params are deterministic
        s1 = (
            select(a.c.id)
            .select_from(a)
            .where(a.c.id == bindparam("p_a", value=1))
        )
        s2 = (
            select(b.c.id)
            .select_from(b)
            .where(b.c.id == bindparam("p_b", value=2))
        )

        u_all = s1.union_all(s2)
        stmt = CreateTableAs(u_all, "dst")

        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT a.id FROM a WHERE a.id = :p_a "
            "UNION ALL SELECT b.id FROM b WHERE b.id = :p_b",
        )

        self.assert_inner_params(stmt, {"p_a": 1, "p_b": 2})

    def test_union_labels_follow_first_select(self, src_two_tables):
        # Many engines take column names
        # of a UNION from the first SELECT’s labels.
        a = table("a", column("val"))
        b = table("b", column("val"))

        s1 = select(a.c.val.label("first_name")).select_from(a)
        s2 = select(b.c.val).select_from(b)  # unlabeled second branch

        u = s1.union(s2)
        stmt = CreateTableAs(u, "dst")

        # We only assert what’s stable across dialects:
        #  - first SELECT has the label
        #  - a UNION occurs
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT a.val AS first_name FROM a "
            "UNION "
            "SELECT b.val FROM b",
        )
