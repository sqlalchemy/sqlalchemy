import re

from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.exc import ArgumentError
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import column
from sqlalchemy.sql import select
from sqlalchemy.sql import table
from sqlalchemy.sql.ddl import CreateTableAs
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertions import expect_warnings


class CreateTableAsDefaultDialectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def src_table(self):
        return Table(
            "src",
            MetaData(),
            Column("id", Integer),
            Column("name", String(50)),
        )

    @testing.fixture
    def src_two_tables(self):
        a = table("a", column("id"), column("name"))
        b = table("b", column("id"), column("status"))
        return a, b

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

    def test_binds_rendered_inline(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(literal("x").label("tag")).select_from(src),
            "dst",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS SELECT 'x' AS tag FROM src",
        )

    def test_temporary_no_schema(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id, src.c.name).select_from(src),
            "dst",
            temporary=True,
        )
        self.assert_compile(
            stmt,
            "CREATE TEMPORARY TABLE dst AS "
            "SELECT src.id, src.name FROM src",
        )

    def test_temporary_exists_flags(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst",
            schema="sch",
            temporary=True,
            if_not_exists=True,
        )
        self.assert_compile(
            stmt,
            "CREATE TEMPORARY TABLE "
            "IF NOT EXISTS sch.dst AS SELECT src.id FROM src",
        )

    def test_if_not_exists(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id, src.c.name).select_from(src),
            "dst",
            if_not_exists=True,
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE IF NOT EXISTS dst AS "
            "SELECT src.id, src.name FROM src",
        )

    def test_join_with_binds_rendered_inline(self, src_two_tables):
        a, b = src_two_tables

        s = (
            select(a.c.id, a.c.name)
            .select_from(a.join(b, a.c.id == b.c.id))
            .where(b.c.status == "active")
        ).into("dst")

        # Ensure WHERE survives into CTAS and binds are rendered inline
        self.assert_compile(
            s,
            "CREATE TABLE dst AS "
            "SELECT a.id, a.name FROM a JOIN b ON a.id = b.id "
            "WHERE b.status = 'active'",
        )

    def test_into_equivalent_to_element(self, src_table):
        src = src_table
        s = select(src.c.id).select_from(src).where(src.c.id == 2)
        via_into = s.into("dst")
        via_element = CreateTableAs(s, "dst")

        self.assert_compile(
            via_into,
            "CREATE TABLE dst AS SELECT src.id FROM src WHERE src.id = 2",
        )
        self.assert_compile(
            via_element,
            "CREATE TABLE dst AS SELECT src.id FROM src WHERE src.id = 2",
        )

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

    @testing.variation("provide_metadata", [True, False])
    def test_generated_metadata_table_property(
        self, src_table, provide_metadata
    ):
        src = src_table

        if provide_metadata:
            metadata = MetaData()
        else:
            metadata = None

        stmt = CreateTableAs(
            select(src.c.name.label("thename"), src.c.id).select_from(src),
            "dst",
            schema="sch",
            metadata=metadata,
        )

        if metadata is not None:
            is_(stmt.metadata, metadata)

        assert isinstance(stmt.table, Table)
        is_(stmt.table.metadata, stmt.metadata)

        self.assert_compile(
            CreateTable(stmt.table),
            "CREATE TABLE sch.dst (thename VARCHAR(50), id INTEGER)",
        )

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

    def test_bindparam_no_value_raises(self, src_table):
        src = src_table
        sel = select(src.c.name).where(src.c.name == bindparam("x"))
        stmt = CreateTableAs(sel, "dst")

        with expect_warnings(
            "Bound parameter 'x' rendering literal NULL in a SQL expression;"
        ):
            self.assert_compile(
                stmt,
                "CREATE TABLE dst AS SELECT src.name FROM src "
                "WHERE src.name = NULL",
            )

    def test_union_all_with_binds_rendered_inline(self, src_two_tables):
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
            "SELECT a.id FROM a WHERE a.id = 1 "
            "UNION ALL SELECT b.id FROM b WHERE b.id = 2",
        )

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

    def test_union_all_with_inlined_literals_smoke(self, src_two_tables):
        # Proves literal_binds=True behavior applies across branches.
        a, b = src_two_tables
        u = (
            select(literal(1).label("x"))
            .select_from(a)
            .union_all(select(literal("b").label("x")).select_from(b))
        )
        stmt = CreateTableAs(u, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT 1 AS x FROM a UNION ALL SELECT 'b' AS x FROM b",
        )

    def test_select_shape_where_order_limit(self, src_table):
        src = src_table
        sel = (
            select(src.c.id, src.c.name)
            .select_from(src)
            .where(src.c.id > literal(10))
            .order_by(src.c.name)
            .limit(5)
            .offset(0)
        )
        stmt = CreateTableAs(sel, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT src.id, src.name FROM src "
            "WHERE src.id > 10 ORDER BY src.name LIMIT 5 OFFSET 0",
        )

    def test_cte_smoke(self, src_two_tables):
        # Proves CTAS works with a WITH-CTE wrapper and labeled column.
        a, _ = src_two_tables
        cte = select(a.c.id.label("aid")).select_from(a).cte("u")
        stmt = CreateTableAs(select(cte.c.aid), "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "WITH u AS (SELECT a.id AS aid FROM a) "
            "SELECT u.aid FROM u",
        )
