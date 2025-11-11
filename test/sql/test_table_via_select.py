from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import CreateTableAs
from sqlalchemy import CreateView
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.schema import CreateTable
from sqlalchemy.schema import DropView
from sqlalchemy.sql import column
from sqlalchemy.sql import select
from sqlalchemy.sql import table
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import expect_warnings


class TableViaSelectTest(fixtures.TestBase, AssertsCompiledSQL):
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

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_basic_element(self, src_table, type_: testing.Variation):
        src = src_table
        if type_.create_table_as:
            stmt = CreateTableAs(
                select(src.c.id, src.c.name),
                "dst",
            )
        elif type_.create_view:
            stmt = CreateView(
                select(src.c.id, src.c.name),
                "dst",
            )
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f"dst AS SELECT src.id, src.name FROM src",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_schema_element_qualified(
        self, src_table, type_: testing.Variation
    ):
        src = src_table
        if type_.create_table_as:
            stmt = CreateTableAs(
                select(src.c.id),
                "dst",
                schema="analytics",
            )
        elif type_.create_view:
            stmt = CreateView(
                select(src.c.id),
                "dst",
                schema="analytics",
            )
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f"analytics.dst AS SELECT src.id FROM src",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_quoting(self, type_: testing.Variation):

        src = Table(
            "SourceTable",
            MetaData(),
            Column("Some Name", Integer),
            Column("Other Col", String),
        )
        if type_.create_table_as:
            stmt = CreateTableAs(
                select(src),
                "My Analytic Table",
                schema="Analysis",
            )
        elif type_.create_view:
            stmt = CreateView(
                select(src),
                "My Analytic View",
                schema="Analysis",
            )
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f'"Analysis"."My Analytic '
            f'{"Table" if type_.create_table_as else "View"}" AS SELECT '
            f'"SourceTable"."Some Name", "SourceTable"."Other Col" '
            f'FROM "SourceTable"',
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_blank_schema_treated_as_none(
        self, src_table, type_: testing.Variation
    ):
        src = src_table
        if type_.create_table_as:
            stmt = CreateTableAs(select(src.c.id), "dst", schema="")
        elif type_.create_view:
            stmt = CreateView(select(src.c.id), "dst", schema="")
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f"dst AS SELECT src.id FROM src",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_binds_rendered_inline(self, src_table, type_: testing.Variation):
        src = src_table
        if type_.create_table_as:
            stmt = CreateTableAs(
                select(literal("x").label("tag")).select_from(src),
                "dst",
            )
        elif type_.create_view:
            stmt = CreateView(
                select(literal("x").label("tag")).select_from(src),
                "dst",
            )
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f"dst AS SELECT 'x' AS tag FROM src",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_temporary_no_schema(self, src_table, type_: testing.Variation):
        src = src_table
        if type_.create_table_as:
            stmt = CreateTableAs(
                select(src.c.id, src.c.name),
                "dst",
                temporary=True,
            )
        elif type_.create_view:
            stmt = CreateView(
                select(src.c.id, src.c.name),
                "dst",
                temporary=True,
            )
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE TEMPORARY {"TABLE" if type_.create_table_as else "VIEW"} '
            f"dst AS SELECT src.id, src.name FROM src",
        )

    @testing.variation("temporary", [True, False])
    @testing.variation("if_not_exists", [True, False])
    def test_create_table_as_flags(
        self,
        src_table,
        temporary: testing.Variation,
        if_not_exists: testing.Variation,
    ):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id),
            "dst",
            schema="sch",
            temporary=bool(temporary),
            if_not_exists=bool(if_not_exists),
        )

        self.assert_compile(
            stmt,
            f"""CREATE {
                'TEMPORARY ' if temporary else ''
            }TABLE {
                'IF NOT EXISTS ' if if_not_exists else ''
            }sch.dst AS SELECT src.id FROM src""",
        )

    def test_temporary_or_replace_create_view(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id),
            "dst",
            schema="sch",
            temporary=True,
            or_replace=True,
        )

        self.assert_compile(
            stmt,
            "CREATE OR REPLACE TEMPORARY VIEW sch.dst AS "
            "SELECT src.id FROM src",
        )

    def test_or_replace(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id, src.c.name),
            "dst",
            or_replace=True,
        )

        self.assert_compile(
            stmt,
            "CREATE OR REPLACE VIEW dst AS SELECT src.id, src.name FROM src",
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
        s = select(src.c.id).where(src.c.id == 2)
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
        s = select(src.c.id).where(src.c.id == 5)

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
        s = select(src.c.id).into("t", schema="analytics")
        self.assert_compile(
            s,
            "CREATE TABLE analytics.t AS SELECT src.id FROM src",
        )

    @testing.variation("provide_metadata", [True, False])
    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_generated_metadata_table_property(
        self, src_table, provide_metadata, type_: testing.Variation
    ):
        src = src_table

        if provide_metadata:
            metadata = MetaData()
        else:
            metadata = None

        if type_.create_table_as:
            stmt = CreateTableAs(
                select(src.c.name.label("thename"), src.c.id),
                "dst",
                schema="sch",
                metadata=metadata,
            )
        elif type_.create_view:
            stmt = CreateView(
                select(src.c.name.label("thename"), src.c.id),
                "dst",
                schema="sch",
                metadata=metadata,
            )
        else:
            type_.fail()

        if metadata is not None:
            is_(stmt.metadata, metadata)

        assert isinstance(stmt.table, Table)
        is_(stmt.table.metadata, stmt.metadata)

        # this is validating the structure of the table but is not
        # looking at CreateTable being the appropriate construct
        self.assert_compile(
            CreateTable(stmt.table),
            "CREATE TABLE sch.dst (thename VARCHAR(50), id INTEGER)",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_labels_in_select_list_preserved(
        self, src_table, type_: testing.Variation
    ):
        src = src_table
        if type_.create_table_as:
            stmt = CreateTableAs(
                select(
                    src.c.id.label("user_id"),
                    src.c.name.label("user_name"),
                ),
                "dst",
            )
        elif type_.create_view:
            stmt = CreateView(
                select(
                    src.c.id.label("user_id"),
                    src.c.name.label("user_name"),
                ),
                "dst",
            )
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f"dst AS SELECT src.id AS user_id, src.name AS user_name FROM src",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_distinct_and_group_by_survive(
        self, src_table, type_: testing.Variation
    ):
        src = src_table
        sel = select(src.c.name).distinct().group_by(src.c.name)
        if type_.create_table_as:
            stmt = CreateTableAs(sel, "dst")
        elif type_.create_view:
            stmt = CreateView(sel, "dst")
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f"dst AS SELECT DISTINCT src.name FROM src GROUP BY src.name",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_bindparam_no_value_raises(
        self, src_table, type_: testing.Variation
    ):
        src = src_table
        sel = select(src.c.name).where(src.c.name == bindparam("x"))
        if type_.create_table_as:
            stmt = CreateTableAs(sel, "dst")
        elif type_.create_view:
            stmt = CreateView(sel, "dst")
        else:
            type_.fail()

        with expect_warnings(
            "Bound parameter 'x' rendering literal NULL in a SQL expression;"
        ):
            self.assert_compile(
                stmt,
                f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
                f"dst AS SELECT src.name FROM src WHERE src.name = NULL",
            )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_union_all_with_binds_rendered_inline(
        self, src_two_tables, type_: testing.Variation
    ):
        a, b = src_two_tables

        # Named binds so params are deterministic
        s1 = select(a.c.id).where(a.c.id == bindparam("p_a", value=1))
        s2 = select(b.c.id).where(b.c.id == bindparam("p_b", value=2))

        u_all = s1.union_all(s2)
        if type_.create_table_as:
            stmt = CreateTableAs(u_all, "dst")
        elif type_.create_view:
            stmt = CreateView(u_all, "dst")
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} dst AS '
            f"SELECT a.id FROM a WHERE a.id = 1 "
            f"UNION ALL SELECT b.id FROM b WHERE b.id = 2",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_union_labels_follow_first_select(
        self, src_two_tables, type_: testing.Variation
    ):
        # Many engines take column names
        # of a UNION from the first SELECT's labels.
        a = table("a", column("val"))
        b = table("b", column("val"))

        s1 = select(a.c.val.label("first_name"))
        s2 = select(b.c.val)  # unlabeled second branch

        u = s1.union(s2)
        if type_.create_table_as:
            stmt = CreateTableAs(u, "dst")
        elif type_.create_view:
            stmt = CreateView(u, "dst")
        else:
            type_.fail()

        # We only assert what's stable across dialects:
        #  - first SELECT has the label
        #  - a UNION occurs
        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} dst AS '
            f"SELECT a.val AS first_name FROM a UNION SELECT b.val FROM b",
        )

        self.assert_compile(
            select(stmt.table), "SELECT dst.first_name FROM dst"
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_union_all_with_inlined_literals_smoke(
        self, src_two_tables, type_: testing.Variation
    ):
        # Proves literal_binds=True behavior applies across branches.
        a, b = src_two_tables
        u = (
            select(literal(1).label("x"))
            .select_from(a)
            .union_all(select(literal("b").label("x")).select_from(b))
        )
        if type_.create_table_as:
            stmt = CreateTableAs(u, "dst")
        elif type_.create_view:
            stmt = CreateView(u, "dst")
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} dst AS '
            f"SELECT 1 AS x FROM a UNION ALL SELECT 'b' AS x FROM b",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_select_shape_where_order_limit(
        self, src_table, type_: testing.Variation
    ):
        src = src_table
        sel = (
            select(src.c.id, src.c.name)
            .where(src.c.id > literal(10))
            .order_by(src.c.name)
            .limit(5)
            .offset(0)
        )
        if type_.create_table_as:
            stmt = CreateTableAs(sel, "dst")
        elif type_.create_view:
            stmt = CreateView(sel, "dst")
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} dst AS '
            f"SELECT src.id, src.name FROM src "
            f"WHERE src.id > 10 ORDER BY src.name LIMIT 5 OFFSET 0",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    def test_cte_smoke(self, src_two_tables, type_: testing.Variation):
        # Proves CTAS works with a WITH-CTE wrapper and labeled column.
        a, _ = src_two_tables
        cte = select(a.c.id.label("aid")).cte("u")
        if type_.create_table_as:
            stmt = CreateTableAs(select(cte.c.aid), "dst")
        elif type_.create_view:
            stmt = CreateView(select(cte.c.aid), "dst")
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} dst AS '
            f"WITH u AS (SELECT a.id AS aid FROM a) SELECT u.aid FROM u",
        )

        # Verify the created table uses the label name for its column
        from sqlalchemy.testing import eq_

        eq_(list(stmt.table.c.keys()), ["aid"])

    def test_materialized_view_basic(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id, src.c.name),
            "dst",
            materialized=True,
        )
        self.assert_compile(
            stmt,
            "CREATE MATERIALIZED VIEW dst AS SELECT src.id, src.name FROM src",
        )

    def test_materialized_view_with_schema(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id),
            "dst",
            schema="analytics",
            materialized=True,
        )
        self.assert_compile(
            stmt,
            "CREATE MATERIALIZED VIEW analytics.dst AS SELECT src.id FROM src",
        )

    def test_materialized_view_or_replace(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id, src.c.name),
            "dst",
            materialized=True,
            or_replace=True,
        )
        self.assert_compile(
            stmt,
            "CREATE OR REPLACE MATERIALIZED VIEW dst AS "
            "SELECT src.id, src.name FROM src",
        )

    def test_materialized_view_temporary(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id, src.c.name),
            "dst",
            materialized=True,
            temporary=True,
        )
        self.assert_compile(
            stmt,
            "CREATE TEMPORARY MATERIALIZED VIEW dst AS "
            "SELECT src.id, src.name FROM src",
        )

    def test_materialized_view_all_flags(self, src_table):
        src = src_table
        stmt = CreateView(
            select(src.c.id),
            "dst",
            schema="sch",
            materialized=True,
            temporary=True,
            or_replace=True,
        )
        self.assert_compile(
            stmt,
            "CREATE OR REPLACE TEMPORARY MATERIALIZED VIEW "
            "sch.dst AS SELECT src.id FROM src",
        )

    @testing.variation("type_", ["create_table_as", "create_view"])
    @testing.variation("use_schema", [True, False])
    def test_textual_select(
        self, type_: testing.Variation, use_schema: testing.Variation
    ):
        """Test using text().columns() with CreateView and CreateTableAs.

        This is likely how we will get alembic to autogenerate a CreateView()
        construct since we dont want to rewrite a whole select() construct
        in a migration file.

        """
        textual = text(
            "SELECT a, b, c FROM source_table WHERE x > 10"
        ).columns(
            column("a", Integer),
            column("b", String),
            column("c", Integer),
        )

        schema = "analytics" if use_schema else None

        if type_.create_table_as:
            stmt = CreateTableAs(textual, "dst", schema=schema)
        elif type_.create_view:
            stmt = CreateView(textual, "dst", schema=schema)
        else:
            type_.fail()

        self.assert_compile(
            stmt,
            f'CREATE {"TABLE" if type_.create_table_as else "VIEW"} '
            f'{"analytics." if use_schema else ""}dst AS '
            f"SELECT a, b, c FROM source_table WHERE x > 10",
        )

        # Verify the generated table has the correct columns
        assert "a" in stmt.table.c
        assert "b" in stmt.table.c
        assert "c" in stmt.table.c
        assert isinstance(stmt.table.c.a.type, Integer)
        assert isinstance(stmt.table.c.b.type, String)
        assert isinstance(stmt.table.c.c.type, Integer)

    @testing.variation("temporary", [True, False])
    @testing.variation("if_not_exists", [True, False])
    def test_textual_select_with_flags_create_table_as(
        self, temporary: testing.Variation, if_not_exists: testing.Variation
    ):
        """Test TextualSelect with flags for CREATE TABLE AS."""
        textual = text("SELECT * FROM temp_data").columns(
            column("x", Integer),
            column("y", String),
        )

        stmt = CreateTableAs(
            textual,
            "snapshot",
            temporary=bool(temporary),
            if_not_exists=bool(if_not_exists),
        )

        self.assert_compile(
            stmt,
            f"""CREATE {
                'TEMPORARY ' if temporary else ''
            }TABLE {
                'IF NOT EXISTS ' if if_not_exists else ''
            }snapshot AS SELECT * FROM temp_data""",
        )

    @testing.variation("temporary", [True, False])
    @testing.variation("or_replace", [True, False])
    def test_textual_select_with_flags_create_view(
        self, temporary: testing.Variation, or_replace: testing.Variation
    ):
        """Test TextualSelect with flags for CREATE VIEW."""
        textual = text("SELECT * FROM temp_data").columns(
            column("x", Integer),
            column("y", String),
        )

        stmt = CreateView(
            textual,
            "snapshot_view",
            temporary=bool(temporary),
            or_replace=bool(or_replace),
        )

        self.assert_compile(
            stmt,
            f"""CREATE {
                'OR REPLACE ' if or_replace else ''
            }{
                'TEMPORARY ' if temporary else ''
            }VIEW snapshot_view AS SELECT * FROM temp_data""",
        )

    def test_drop_view_basic(self):
        """Test basic DROP VIEW compilation."""
        src = table("src", column("id"), column("name"))
        create_view = CreateView(select(src), "my_view")
        view_table = create_view.table

        drop_stmt = DropView(view_table)

        self.assert_compile(drop_stmt, "DROP VIEW my_view")

    def test_drop_view_materialized(self):
        """Test DROP MATERIALIZED VIEW compilation."""
        src = table("src", column("id"), column("name"))
        create_view = CreateView(select(src), "my_mat_view", materialized=True)
        view_table = create_view.table

        drop_stmt = DropView(view_table, materialized=True)

        self.assert_compile(drop_stmt, "DROP MATERIALIZED VIEW my_mat_view")

    def test_drop_view_from_create_view(self):
        """Test that CreateView automatically creates proper DropView."""
        src = table("src", column("id"), column("name"))

        # Regular view
        create_view = CreateView(select(src), "regular_view")
        drop_stmt = create_view.table._dropper_ddl

        self.assert_compile(drop_stmt, "DROP VIEW regular_view")

    def test_drop_materialized_view_from_create_view(self):
        """Test CreateView with materialized=True creates proper DropView."""
        src = table("src", column("id"), column("name"))

        # Materialized view
        create_mat_view = CreateView(
            select(src), "materialized_view", materialized=True
        )
        drop_stmt = create_mat_view.table._dropper_ddl

        self.assert_compile(
            drop_stmt, "DROP MATERIALIZED VIEW materialized_view"
        )
