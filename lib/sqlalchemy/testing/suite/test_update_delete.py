from .. import fixtures
from ..assertions import eq_
from ..schema import Column
from ..schema import Table
from ... import Integer
from ... import String


class SimpleUpdateDeleteTest(fixtures.TablesTest):
    run_deletes = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "plain_pk",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.plain_pk.insert(),
            [
                {"id": 1, "data": "d1"},
                {"id": 2, "data": "d2"},
                {"id": 3, "data": "d3"},
            ],
        )

    def test_update(self, connection):
        t = self.tables.plain_pk
        r = connection.execute(
            t.update().where(t.c.id == 2), dict(data="d2_new")
        )
        assert not r.is_insert
        assert not r.returns_rows
        assert r.rowcount == 1

        eq_(
            connection.execute(t.select().order_by(t.c.id)).fetchall(),
            [(1, "d1"), (2, "d2_new"), (3, "d3")],
        )

    def test_delete(self, connection):
        t = self.tables.plain_pk
        r = connection.execute(t.delete().where(t.c.id == 2))
        assert not r.is_insert
        assert not r.returns_rows
        assert r.rowcount == 1
        eq_(
            connection.execute(t.select().order_by(t.c.id)).fetchall(),
            [(1, "d1"), (3, "d3")],
        )


__all__ = ("SimpleUpdateDeleteTest",)
