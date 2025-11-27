# testing/suite/test_table_via_select.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

from .. import fixtures
from ..assertions import eq_
from ..provision import get_temp_table_name
from ... import bindparam
from ... import Column
from ... import func
from ... import inspect
from ... import Integer
from ... import literal
from ... import MetaData
from ... import select
from ... import String
from ... import testing
from ...schema import CreateTableAs
from ...schema import CreateView
from ...schema import DropTable
from ...schema import DropView
from ...schema import Table
from ...testing import config


class TableViaSelectTest(fixtures.TablesTest):
    __sparse_driver_backend__ = True

    @classmethod
    def temp_table_name(cls):
        return get_temp_table_name(
            config, config.db, f"user_tmp_{config.ident}"
        )

    @classmethod
    def temp_view_name(cls):
        return get_temp_table_name(
            config, config.db, f"user_tmp_view_{config.ident}"
        )

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "source_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("name", String(50)),
            Column("value", Integer),
        )
        Table("a", metadata, Column("id", Integer))
        Table("b", metadata, Column("id", Integer))

        if testing.requires.schemas.enabled:
            Table(
                "source_table_s",
                metadata,
                Column("id", Integer, primary_key=True, autoincrement=False),
                Column("name", String(50)),
                Column("value", Integer),
                schema=config.test_schema,
            )

    @classmethod
    def insert_data(cls, connection):
        table = cls.tables.source_table
        connection.execute(
            table.insert(),
            [
                {"id": 1, "name": "alice", "value": 100},
                {"id": 2, "name": "bob", "value": 200},
                {"id": 3, "name": "charlie", "value": 300},
            ],
        )

        a = cls.tables.a
        b = cls.tables.b

        connection.execute(a.insert(), [{"id": v} for v in [1, 3]])
        connection.execute(b.insert(), [{"id": v} for v in [2, 4]])

        if testing.requires.schemas.enabled:
            table = cls.tables[f"{config.test_schema}.source_table_s"]
            connection.execute(
                table.insert(),
                [
                    {"id": 1, "name": "alice", "value": 100},
                    {"id": 2, "name": "bob", "value": 200},
                    {"id": 3, "name": "charlie", "value": 300},
                ],
            )

    @testing.fixture(scope="function", autouse=True)
    def drop_dest_table(self, connection):
        for schema in None, config.test_schema:
            for name in ("dest_table", self.temp_table_name()):
                if inspect(connection).has_table(name, schema=schema):
                    connection.execute(
                        DropTable(Table(name, MetaData(), schema=schema))
                    )
            for name in ("dest_view", self.temp_view_name()):
                if inspect(connection).has_table(name, schema=schema):
                    connection.execute(
                        DropView(Table(name, MetaData(), schema=schema))
                    )
        connection.commit()

    @testing.combinations(
        ("plain", False, False, False),
        (
            "use_temp",
            False,
            True,
            False,
            testing.requires.create_temp_table_as,
        ),
        ("use_schema", True, False, False, testing.requires.schemas),
        ("plain", False, False, False),
        ("use_temp", False, True, True, testing.requires.temporary_views),
        ("use_schema", True, False, True, testing.requires.schemas),
        argnames="use_schemas,use_temp,use_view",
        id_="iaaa",
    )
    def test_without_metadata(
        self, connection, use_temp, use_schemas, use_view
    ):
        source_table = self.tables.source_table

        if not use_view:
            tablename = self.temp_table_name() if use_temp else "dest_table"
            stmt = CreateTableAs(
                select(source_table.c.id, source_table.c.name).where(
                    source_table.c.value > 100
                ),
                tablename,
                temporary=bool(use_temp),
                schema=config.test_schema if use_schemas else None,
            )
        else:
            if use_schemas:
                source_table = self.tables[
                    f"{config.test_schema}.source_table_s"
                ]

            tablename = self.temp_view_name() if use_temp else "dest_view"
            stmt = CreateView(
                select(source_table.c.id, source_table.c.name).where(
                    source_table.c.value > 100
                ),
                tablename,
                temporary=bool(use_temp),
                schema=config.test_schema if use_schemas else None,
            )

        connection.execute(stmt)

        # Verify we can SELECT from the generated table
        dest = stmt.table
        result = connection.execute(
            select(dest.c.id, dest.c.name).order_by(dest.c.id)
        ).fetchall()

        eq_(result, [(2, "bob"), (3, "charlie")])

        # Verify reflection works
        insp = inspect(connection)
        cols = insp.get_columns(
            tablename,
            schema=config.test_schema if use_schemas else None,
        )

        eq_(len(cols), 2)
        eq_(cols[0]["name"], "id")
        eq_(cols[1]["name"], "name")

        # Verify type affinity
        eq_(cols[0]["type"]._type_affinity, Integer)
        eq_(cols[1]["type"]._type_affinity, String)

    @testing.variation(
        "table_type",
        [
            ("create_table_as", testing.requires.create_table_as),
            ("select_into", testing.requires.create_table_as),
            ("create_view", testing.requires.views),
        ],
    )
    @testing.variation(
        "use_temp",
        [
            False,
            (
                True,
                testing.requires.create_temp_table_as
                + testing.requires.temporary_views,
            ),
        ],
    )
    @testing.variation("use_drop_all", [True, False])
    def test_with_metadata(
        self,
        connection,
        metadata,
        use_temp,
        table_type,
        use_drop_all,
    ):
        source_table = self.tables.source_table

        select_stmt = select(
            source_table.c.id,
            source_table.c.name,
            source_table.c.value,
        ).where(source_table.c.value > 100)

        match table_type:
            case "create_table_as":
                tablename = (
                    self.temp_table_name() if use_temp else "dest_table"
                )
                stmt = CreateTableAs(
                    select_stmt,
                    tablename,
                    metadata=metadata,
                    temporary=bool(use_temp),
                )
            case "select_into":
                tablename = (
                    self.temp_table_name() if use_temp else "dest_table"
                )
                stmt = select_stmt.into(
                    tablename,
                    temporary=use_temp,
                    metadata=metadata,
                )
            case "create_view":
                tablename = self.temp_view_name() if use_temp else "dest_view"
                stmt = CreateView(
                    select_stmt,
                    tablename,
                    metadata=metadata,
                    temporary=bool(use_temp),
                )
            case _:
                table_type.fail()

        # these are metadata attached, create all
        metadata.create_all(connection)

        # Verify the generated table is a proper Table object
        dest = stmt.table
        assert isinstance(dest, Table)
        assert dest.metadata is metadata
        eq_(dest.name, tablename)

        # SELECT from the generated table - should only have rows with
        # value > 100 (bob and charlie)
        result = connection.execute(
            select(dest.c.id, dest.c.name).order_by(dest.c.id)
        ).fetchall()

        eq_(result, [(2, "bob"), (3, "charlie")])

        # Drop the table using either metadata.drop_all() or dest.drop()
        if use_drop_all:
            metadata.drop_all(connection)
        else:
            dest.drop(connection)

        # Verify it's gone
        if use_temp:
            if testing.requires.temp_table_names.enabled:
                insp = inspect(connection)
                assert tablename not in insp.get_temp_table_names()
        else:
            insp = inspect(connection)
            if table_type.create_view:
                assert tablename not in insp.get_view_names()
            else:
                assert tablename not in insp.get_table_names()

    @testing.variation(
        "table_type",
        [
            ("create_table_as", testing.requires.create_table_as),
            ("create_view", testing.requires.views),
        ],
    )
    def test_with_labels(self, connection, table_type):
        source_table = self.tables.source_table

        match table_type:
            case "create_table_as":
                tablename = "dest_table"
                stmt = CreateTableAs(
                    select(
                        source_table.c.id.label("user_id"),
                        source_table.c.name.label("user_name"),
                    ),
                    tablename,
                )
            case "create_view":
                tablename = "dest_view"
                stmt = CreateView(
                    select(
                        source_table.c.id.label("user_id"),
                        source_table.c.name.label("user_name"),
                    ),
                    tablename,
                )
            case _:
                table_type.fail()

        connection.execute(stmt)

        # Verify column names from labels
        insp = inspect(connection)
        cols = insp.get_columns(tablename)
        eq_(len(cols), 2)
        eq_(cols[0]["name"], "user_id")
        eq_(cols[1]["name"], "user_name")

        # Verify we can query using the labels
        dest = stmt.table
        result = connection.execute(
            select(dest.c.user_id, dest.c.user_name).where(dest.c.user_id == 1)
        ).fetchall()

        eq_(result, [(1, "alice")])

    @testing.requires.table_ddl_if_exists
    @testing.requires.create_table_as
    def test_create_table_as_if_not_exists(self, connection):
        source_table = self.tables.source_table
        tablename = "dest_table"

        stmt = CreateTableAs(
            select(source_table.c.id).select_from(source_table),
            tablename,
            if_not_exists=True,
        )

        insp = inspect(connection)
        assert tablename not in insp.get_table_names()

        connection.execute(stmt)

        insp = inspect(connection)
        assert tablename in insp.get_table_names()

        # succeeds even though table exists
        connection.execute(stmt)

    @testing.requires.create_or_replace_view
    def test_create_or_replace_view(self, connection):
        source_table = self.tables.source_table
        viewname = "dest_view"

        # Create initial view that selects all rows
        stmt = CreateView(
            select(source_table.c.id).select_from(source_table),
            viewname,
            or_replace=True,
        )

        insp = inspect(connection)
        assert viewname not in insp.get_view_names()

        connection.execute(stmt)

        insp = inspect(connection)
        assert viewname in insp.get_view_names()

        # Verify initial view returns all 3 rows
        result = connection.execute(select(stmt.table)).fetchall()
        eq_(len(result), 3)

        # Replace view with filtered query (only id > 1)
        stmt = CreateView(
            select(source_table.c.id)
            .select_from(source_table)
            .where(source_table.c.id > 1),
            viewname,
            or_replace=True,
        )
        connection.execute(stmt)

        # Verify view was replaced - should now return only 2 rows
        insp = inspect(connection)
        assert viewname in insp.get_view_names()

        result = connection.execute(select(stmt.table)).fetchall()
        eq_(len(result), 2)

    @testing.requires.materialized_views
    @testing.variation("use_metadata", [True, False])
    def test_create_drop_materialized_view(self, connection, use_metadata):
        source_table = self.tables.source_table
        viewname = "dest_mat_view"

        if use_metadata:
            # Create with metadata
            metadata = MetaData()
            stmt = CreateView(
                select(source_table.c.id, source_table.c.name).select_from(
                    source_table
                ),
                viewname,
                materialized=True,
                metadata=metadata,
            )
        else:
            # Create without metadata
            stmt = CreateView(
                select(source_table.c.id, source_table.c.name).select_from(
                    source_table
                ),
                viewname,
                materialized=True,
            )

        insp = inspect(connection)
        assert viewname not in insp.get_materialized_view_names()

        if use_metadata:
            metadata.create_all(connection)
        else:
            connection.execute(stmt)

        insp = inspect(connection)
        assert viewname in insp.get_materialized_view_names()

        # Verify materialized view returns data
        dst_view = stmt.table
        result = connection.execute(select(dst_view)).fetchall()
        eq_(len(result), 3)
        eq_(set(r[0] for r in result), {1, 2, 3})

        # Drop materialized view
        if use_metadata:
            metadata.drop_all(connection)
        else:
            drop_stmt = DropView(dst_view, materialized=True)
            connection.execute(drop_stmt)

        insp = inspect(connection)
        assert viewname not in insp.get_materialized_view_names()

    @testing.variation(
        "table_type",
        [
            ("create_table_as", testing.requires.create_table_as),
            ("create_view", testing.requires.views),
        ],
    )
    def test_literal_inlining_inside_select(self, connection, table_type):
        src = self.tables.source_table
        sel = select(
            (src.c.id + 1).label("id2"),
            literal("x").label("tag"),
        ).select_from(src)

        match table_type:
            case "create_table_as":
                tablename = "dest_table"
                stmt = CreateTableAs(sel, tablename)
            case "create_view":
                tablename = "dest_view"
                stmt = CreateView(sel, tablename)
            case _:
                table_type.fail()

        connection.execute(stmt)

        tbl = stmt.table
        row = connection.execute(
            select(func.count(), func.min(tbl.c.tag), func.max(tbl.c.tag))
        ).first()
        eq_(row, (3, "x", "x"))

    @testing.variation(
        "table_type",
        [
            ("create_table_as", testing.requires.create_table_as),
            ("create_view", testing.requires.views),
        ],
    )
    def test_with_bind_param_executes(self, connection, table_type):
        src = self.tables.source_table

        sel = (
            select(src.c.id, src.c.name)
            .select_from(src)
            .where(src.c.name == bindparam("p", value="alice"))
        )

        match table_type:
            case "create_table_as":
                tablename = "dest_table"
                stmt = CreateTableAs(sel, tablename)
            case "create_view":
                tablename = "dest_view"
                stmt = CreateView(sel, tablename)
            case _:
                table_type.fail()

        connection.execute(stmt)

        tbl = stmt.table

        row = connection.execute(
            select(func.count(), func.min(tbl.c.name), func.max(tbl.c.name))
        ).first()
        eq_(row, (1, "alice", "alice"))

    @testing.variation(
        "table_type",
        [
            ("create_table_as", testing.requires.create_table_as),
            ("create_view", testing.requires.views),
        ],
    )
    def test_compound_select_smoke(self, connection, table_type):
        a, b = self.tables("a", "b")

        sel = select(a.c.id).union_all(select(b.c.id))

        match table_type:
            case "create_table_as":
                tablename = "dest_table"
                stmt = CreateTableAs(sel, tablename)
            case "create_view":
                tablename = "dest_view"
                stmt = CreateView(sel, tablename)
            case _:
                table_type.fail()

        connection.execute(stmt)

        vals = (
            connection.execute(
                select(stmt.table.c.id).order_by(stmt.table.c.id)
            )
            .scalars()
            .all()
        )
        eq_(vals, [1, 2, 3, 4])

    @testing.requires.views
    def test_view_dependencies_with_metadata(self, connection, metadata):
        """Test that views with dependencies are created/dropped in correct
        order.

        This validates that when views are attached to metadata: - create_all()
        creates base tables first, then dependent views in order - drop_all()
        drops dependent views first, then base tables in reverse order
        """
        # Create three base tables
        table1 = Table(
            "base_table1",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("value", Integer),
        )
        table2 = Table(
            "base_table2",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("amount", Integer),
        )
        table3 = Table(
            "base_table3",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("total", Integer),
        )

        # First view depends on table1 and table2
        view1_stmt = CreateView(
            select(
                table1.c.id,
                table1.c.value,
                table2.c.amount,
            )
            .select_from(table1.join(table2, table1.c.id == table2.c.id))
            .where(table1.c.value > 0),
            "view1",
            metadata=metadata,
        )

        # Second view depends on table3 and view1
        view2_stmt = CreateView(
            select(
                view1_stmt.table.c.id,
                view1_stmt.table.c.value,
                table3.c.total,
            )
            .select_from(
                view1_stmt.table.join(
                    table3, view1_stmt.table.c.id == table3.c.id
                )
            )
            .where(table3.c.total > 100),
            "view2",
            metadata=metadata,
        )

        # Verify metadata knows about all objects
        eq_(
            {"base_table1", "base_table2", "base_table3", "view1", "view2"},
            set(metadata.tables),
        )

        # Create all in correct dependency order
        metadata.create_all(connection)

        # Verify all tables and views were created
        insp = inspect(connection)
        assert {"base_table1", "base_table2", "base_table3"}.issubset(
            insp.get_table_names()
        )
        assert {"view1", "view2"}.issubset(insp.get_view_names())

        # Insert test data
        connection.execute(
            table1.insert(),
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ],
        )
        connection.execute(
            table2.insert(),
            [
                {"id": 1, "amount": 100},
                {"id": 2, "amount": 200},
                {"id": 3, "amount": 300},
            ],
        )
        connection.execute(
            table3.insert(),
            [
                {"id": 1, "total": 50},
                {"id": 2, "total": 150},
                {"id": 3, "total": 250},
            ],
        )

        # Query view1 to verify it works
        view1_results = connection.execute(
            select(view1_stmt.table).order_by(view1_stmt.table.c.id)
        ).fetchall()
        eq_(
            view1_results,
            [
                (1, 10, 100),
                (2, 20, 200),
                (3, 30, 300),
            ],
        )

        # Query view2 to verify it works (should filter total > 100)
        view2_results = connection.execute(
            select(view2_stmt.table).order_by(view2_stmt.table.c.id)
        ).fetchall()
        eq_(
            view2_results,
            [
                (2, 20, 150),
                (3, 30, 250),
            ],
        )

        # Drop all in correct reverse dependency order
        metadata.drop_all(connection)

        # Verify all tables and views were dropped
        insp = inspect(connection)
        assert {"base_table1", "base_table2", "base_table3"}.isdisjoint(
            insp.get_table_names()
        )
        assert {"view1", "view2"}.isdisjoint(insp.get_view_names())
