from .. import config
from .. import fixtures
from .. import util
from ..assertions import eq_
from ..config import requirements
from ... import Column
from ... import inspect
from ... import Integer
from ... import schema
from ... import String
from ... import Table


class TableDDLTest(fixtures.TestBase):
    __backend__ = True

    def _simple_fixture(self):
        return Table(
            "test_table",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )

    def _underscore_fixture(self):
        return Table(
            "_test_table",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("_data", String(50)),
        )

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
    def test_add_table_comment(self):
        table = self._simple_fixture()
        table.create(config.db, checkfirst=False)
        table.comment = "a comment"
        config.db.execute(schema.SetTableComment(table))
        eq_(
            inspect(config.db).get_table_comment("test_table"),
            {"text": "a comment"},
        )

    @requirements.comment_reflection
    @util.provide_metadata
    def test_drop_table_comment(self):
        table = self._simple_fixture()
        table.create(config.db, checkfirst=False)
        table.comment = "a comment"
        config.db.execute(schema.SetTableComment(table))
        config.db.execute(schema.DropTableComment(table))
        eq_(inspect(config.db).get_table_comment("test_table"), {"text": None})


__all__ = ("TableDDLTest",)
