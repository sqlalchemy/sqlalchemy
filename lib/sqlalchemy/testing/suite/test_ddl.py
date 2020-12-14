from .. import config
from .. import fixtures
from .. import util
from ..assertions import eq_
from ..assertions import is_false
from ..assertions import is_true
from ..config import requirements
from ... import Column
from ... import Index
from ... import inspect
from ... import Integer
from ... import schema
from ... import String
from ... import Table


class TableDDLTest(fixtures.TestBase):
    __backend__ = True

    def _simple_fixture(self, schema=None):
        return Table(
            "test_table",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
            schema=schema,
        )

    def _underscore_fixture(self):
        return Table(
            "_test_table",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("_data", String(50)),
        )

    def _table_index_fixture(self, schema=None):
        table = self._simple_fixture(schema=schema)
        idx = Index("test_index", table.c.data)
        return table, idx

    def _simple_roundtrip(self, table):
        with config.db.begin() as conn:
            conn.execute(table.insert().values((1, "some data")))
            result = conn.execute(table.select())
            eq_(result.first(), (1, "some data"))

    @requirements.create_table
    @util.provide_metadata
    def test_create_table(self):
        table = self._simple_fixture()
        table.create(config.db, checkfirst=False)
        self._simple_roundtrip(table)

    @requirements.create_table
    @requirements.schemas
    @util.provide_metadata
    def test_create_table_schema(self):
        table = self._simple_fixture(schema=config.test_schema)
        table.create(config.db, checkfirst=False)
        self._simple_roundtrip(table)

    @requirements.drop_table
    @util.provide_metadata
    def test_drop_table(self):
        table = self._simple_fixture()
        table.create(config.db, checkfirst=False)
        table.drop(config.db, checkfirst=False)

    @requirements.create_table
    @util.provide_metadata
    def test_underscore_names(self):
        table = self._underscore_fixture()
        table.create(config.db, checkfirst=False)
        self._simple_roundtrip(table)

    @requirements.comment_reflection
    @util.provide_metadata
    def test_add_table_comment(self, connection):
        table = self._simple_fixture()
        table.create(connection, checkfirst=False)
        table.comment = "a comment"
        connection.execute(schema.SetTableComment(table))
        eq_(
            inspect(connection).get_table_comment("test_table"),
            {"text": "a comment"},
        )

    @requirements.comment_reflection
    @util.provide_metadata
    def test_drop_table_comment(self, connection):
        table = self._simple_fixture()
        table.create(connection, checkfirst=False)
        table.comment = "a comment"
        connection.execute(schema.SetTableComment(table))
        connection.execute(schema.DropTableComment(table))
        eq_(
            inspect(connection).get_table_comment("test_table"), {"text": None}
        )

    @requirements.table_ddl_if_exists
    @util.provide_metadata
    def test_create_table_if_not_exists(self, connection):
        table = self._simple_fixture()

        connection.execute(schema.CreateTable(table, if_not_exists=True))

        is_true(inspect(connection).has_table("test_table"))
        connection.execute(schema.CreateTable(table, if_not_exists=True))

    @requirements.index_ddl_if_exists
    @util.provide_metadata
    def test_create_index_if_not_exists(self, connection):
        table, idx = self._table_index_fixture()

        connection.execute(schema.CreateTable(table, if_not_exists=True))
        is_true(inspect(connection).has_table("test_table"))
        is_false(
            "test_index"
            in [
                ix["name"]
                for ix in inspect(connection).get_indexes("test_table")
            ]
        )

        connection.execute(schema.CreateIndex(idx, if_not_exists=True))

        is_true(
            "test_index"
            in [
                ix["name"]
                for ix in inspect(connection).get_indexes("test_table")
            ]
        )

        connection.execute(schema.CreateIndex(idx, if_not_exists=True))

    @requirements.table_ddl_if_exists
    @util.provide_metadata
    def test_drop_table_if_exists(self, connection):
        table = self._simple_fixture()

        table.create(connection)

        is_true(inspect(connection).has_table("test_table"))

        connection.execute(schema.DropTable(table, if_exists=True))

        is_false(inspect(connection).has_table("test_table"))

        connection.execute(schema.DropTable(table, if_exists=True))

    @requirements.index_ddl_if_exists
    @util.provide_metadata
    def test_drop_index_if_exists(self, connection):
        table, idx = self._table_index_fixture()

        table.create(connection)

        is_true(
            "test_index"
            in [
                ix["name"]
                for ix in inspect(connection).get_indexes("test_table")
            ]
        )

        connection.execute(schema.DropIndex(idx, if_exists=True))

        is_false(
            "test_index"
            in [
                ix["name"]
                for ix in inspect(connection).get_indexes("test_table")
            ]
        )

        connection.execute(schema.DropIndex(idx, if_exists=True))


class FutureTableDDLTest(fixtures.FutureEngineMixin, TableDDLTest):
    pass


__all__ = ("TableDDLTest", "FutureTableDDLTest")
