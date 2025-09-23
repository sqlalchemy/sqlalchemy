# testing/suite/test_create_table_as.py
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
from ...schema import DropTable
from ...schema import Table
from ...sql.ddl import CreateTableAs
from ...testing import config


class CreateTableAsTest(fixtures.TablesTest):
    __backend__ = True
    __requires__ = ("create_table_as",)

    @classmethod
    def temp_table_name(cls):
        return get_temp_table_name(
            config, config.db, f"user_tmp_{config.ident}"
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

    @testing.fixture(scope="function", autouse=True)
    def drop_dest_table(self, connection):
        for schema in None, config.test_schema:
            for name in ("dest_table", self.temp_table_name()):
                if inspect(connection).has_table(name, schema=schema):
                    connection.execute(
                        DropTable(Table(name, MetaData(), schema=schema))
                    )
        connection.commit()

    @testing.combinations(
        ("plain", False, False),
        ("use_temp", False, True, testing.requires.create_temp_table_as),
        ("use_schema", True, False, testing.requires.schemas),
        argnames="use_schemas,use_temp",
        id_="iaa",
    )
    def test_create_table_as_tableclause(
        self, connection, use_temp, use_schemas
    ):
        source_table = self.tables.source_table
        stmt = CreateTableAs(
            select(source_table.c.id, source_table.c.name).where(
                source_table.c.value > 100
            ),
            self.temp_table_name() if use_temp else "dest_table",
            temporary=bool(use_temp),
            schema=config.test_schema if use_schemas else None,
        )

        # Execute the CTAS
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
            self.temp_table_name() if use_temp else "dest_table",
            schema=config.test_schema if use_schemas else None,
        )
        eq_(len(cols), 2)
        eq_(cols[0]["name"], "id")
        eq_(cols[1]["name"], "name")

        # Verify type affinity
        eq_(cols[0]["type"]._type_affinity, Integer)
        eq_(cols[1]["type"]._type_affinity, String)

    @testing.variation(
        "use_temp", [False, (True, testing.requires.create_temp_table_as)]
    )
    def test_create_table_as_with_metadata(
        self, connection, metadata, use_temp
    ):
        source_table = self.tables.source_table
        stmt = CreateTableAs(
            select(
                source_table.c.id, source_table.c.name, source_table.c.value
            ),
            self.temp_table_name() if use_temp else "dest_table",
            metadata=metadata,
            temporary=bool(use_temp),
        )

        # Execute the CTAS
        connection.execute(stmt)

        # Verify the generated table is a proper Table object
        dest = stmt.table
        assert isinstance(dest, Table)
        assert dest.metadata is metadata

        # SELECT from the generated table
        result = connection.execute(
            select(dest.c.id, dest.c.name, dest.c.value).where(dest.c.id == 2)
        ).fetchall()

        eq_(result, [(2, "bob", 200)])

        # Drop the table using the Table object
        dest.drop(connection)

        # Verify it's gone
        if not use_temp:
            insp = inspect(connection)
            assert "dest_table" not in insp.get_table_names()
        elif testing.requires.temp_table_names.enabled:
            insp = inspect(connection)
            assert self.temp_table_name() not in insp.get_temp_table_names()

    def test_create_table_as_with_labels(self, connection):
        source_table = self.tables.source_table

        stmt = CreateTableAs(
            select(
                source_table.c.id.label("user_id"),
                source_table.c.name.label("user_name"),
            ),
            "dest_table",
        )

        connection.execute(stmt)

        # Verify column names from labels
        insp = inspect(connection)
        cols = insp.get_columns("dest_table")
        eq_(len(cols), 2)
        eq_(cols[0]["name"], "user_id")
        eq_(cols[1]["name"], "user_name")

        # Verify we can query using the labels
        dest = stmt.table
        result = connection.execute(
            select(dest.c.user_id, dest.c.user_name).where(dest.c.user_id == 1)
        ).fetchall()

        eq_(result, [(1, "alice")])

    def test_create_table_as_into_method(self, connection):
        source_table = self.tables.source_table
        stmt = select(source_table.c.id, source_table.c.value).into(
            "dest_table"
        )

        connection.execute(stmt)

        # Verify the table was created and can be queried
        dest = stmt.table
        result = connection.execute(
            select(dest.c.id, dest.c.value).order_by(dest.c.id)
        ).fetchall()

        eq_(result, [(1, 100), (2, 200), (3, 300)])

    @testing.variation(
        "use_temp", [False, (True, testing.requires.create_temp_table_as)]
    )
    @testing.variation("use_into", [True, False])
    def test_metadata_use_cases(
        self, use_temp, use_into, metadata, connection
    ):
        table_name = self.temp_table_name() if use_temp else "dest_table"
        source_table = self.tables.source_table
        select_stmt = select(
            source_table.c.id, source_table.c.name, source_table.c.value
        ).where(source_table.c.value > 100)

        if use_into:
            cas = select_stmt.into(
                table_name, temporary=use_temp, metadata=metadata
            )
        else:
            cas = CreateTableAs(
                select_stmt,
                table_name,
                temporary=use_temp,
                metadata=metadata,
            )

        connection.execute(cas)
        dest = cas.table
        eq_(dest.name, table_name)
        result = connection.execute(
            select(dest.c.id, dest.c.name).order_by(dest.c.id)
        ).fetchall()

        eq_(result, [(2, "bob"), (3, "charlie")])

        if use_temp:
            if testing.requires.temp_table_names.enabled:
                insp = inspect(connection)
                assert table_name in insp.get_temp_table_names()

                metadata.drop_all(connection)
                insp = inspect(connection)
                assert table_name not in insp.get_temp_table_names()
        else:
            insp = inspect(connection)
            assert table_name in insp.get_table_names()

            metadata.drop_all(connection)
            insp = inspect(connection)
            assert table_name not in insp.get_table_names()

    @testing.requires.table_ddl_if_exists
    def test_if_not_exists(self, connection):
        source_table = self.tables.source_table
        cas = CreateTableAs(
            select(source_table.c.id).select_from(source_table),
            "dest_table",
            if_not_exists=True,
        )

        insp = inspect(connection)
        assert "dest_table" not in insp.get_table_names()

        connection.execute(cas)

        insp = inspect(connection)
        assert "dest_table" in insp.get_table_names()

        # succeeds even though table exists
        connection.execute(cas)

    def test_literal_inlining_inside_select(self, connection):
        src = self.tables.source_table
        sel = select(
            (src.c.id + 1).label("id2"),
            literal("x").label("tag"),
        ).select_from(src)

        stmt = CreateTableAs(sel, "dest_table")
        connection.execute(stmt)

        tbl = stmt.table
        row = connection.execute(
            select(func.count(), func.min(tbl.c.tag), func.max(tbl.c.tag))
        ).first()
        eq_(row, (3, "x", "x"))

    def test_create_table_as_with_bind_param_executes(self, connection):
        src = self.tables.source_table

        sel = (
            select(src.c.id, src.c.name)
            .select_from(src)
            .where(src.c.name == bindparam("p", value="alice"))
        )

        stmt = CreateTableAs(sel, "dest_table")
        connection.execute(stmt)

        tbl = stmt.table

        row = connection.execute(
            select(func.count(), func.min(tbl.c.name), func.max(tbl.c.name))
        ).first()
        eq_(row, (1, "alice", "alice"))

    def test_compound_select_smoke(self, connection):

        a, b = self.tables("a", "b")

        sel = select(a.c.id).union_all(select(b.c.id)).order_by(a.c.id)
        stmt = CreateTableAs(sel, "dest_table")
        connection.execute(stmt)

        vals = (
            connection.execute(
                select(stmt.table.c.id).order_by(stmt.table.c.id)
            )
            .scalars()
            .all()
        )
        eq_(vals, [1, 2, 3, 4])
