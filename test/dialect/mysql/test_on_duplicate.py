import random

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import eq_


class OnDuplicateTest(fixtures.TablesTest):
    __only_on__ = ("mysql", "mariadb")
    __backend__ = True
    run_define_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foos",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("bar", String(10)),
            Column("baz", String(10)),
            Column("updated_once", Boolean, default=False),
        )

    def test_bad_args(self):
        assert_raises(
            ValueError,
            insert(self.tables.foos).values({}).on_duplicate_key_update,
        )
        assert_raises(
            exc.ArgumentError,
            insert(self.tables.foos).values({}).on_duplicate_key_update,
            {"id": 1, "bar": "b"},
            id=1,
            bar="b",
        )
        assert_raises(
            exc.ArgumentError,
            insert(self.tables.foos).values({}).on_duplicate_key_update,
            {"id": 1, "bar": "b"},
            {"id": 2, "bar": "baz"},
        )

    def test_on_duplicate_key_update_multirow(self, connection):
        foos = self.tables.foos
        conn = connection
        conn.execute(insert(foos).values(dict(id=1, bar="b", baz="bz")))
        stmt = insert(foos).values([dict(id=1, bar="ab"), dict(id=2, bar="b")])
        stmt = stmt.on_duplicate_key_update(bar=stmt.inserted.bar)

        result = conn.execute(stmt)

        # multirow, so its ambiguous.  this is a behavioral change
        # in 1.4
        eq_(result.inserted_primary_key, (None,))
        eq_(
            conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
            [(1, "ab", "bz", False)],
        )

    def test_on_duplicate_key_from_select(self, connection):
        foos = self.tables.foos
        conn = connection
        conn.execute(insert(foos).values(dict(id=1, bar="b", baz="bz")))
        stmt = insert(foos).from_select(
            ["id", "bar", "baz"],
            select(foos.c.id, literal("bar2"), literal("baz2")),
        )
        stmt = stmt.on_duplicate_key_update(bar=stmt.inserted.bar)

        conn.execute(stmt)
        eq_(
            conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
            [(1, "bar2", "bz", False)],
        )

    def test_on_duplicate_key_update_singlerow(self, connection):
        foos = self.tables.foos
        conn = connection
        conn.execute(insert(foos).values(dict(id=1, bar="b", baz="bz")))
        stmt = insert(foos).values(dict(id=2, bar="b"))
        stmt = stmt.on_duplicate_key_update(bar=stmt.inserted.bar)

        result = conn.execute(stmt)

        # only one row in the INSERT so we do inserted_primary_key
        eq_(result.inserted_primary_key, (2,))
        eq_(
            conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
            [(1, "b", "bz", False)],
        )

    def test_on_duplicate_key_update_null_multirow(self, connection):
        foos = self.tables.foos
        conn = connection
        conn.execute(insert(foos).values(dict(id=1, bar="b", baz="bz")))
        stmt = insert(foos).values([dict(id=1, bar="ab"), dict(id=2, bar="b")])
        stmt = stmt.on_duplicate_key_update(updated_once=None)
        result = conn.execute(stmt)

        # ambiguous
        eq_(result.inserted_primary_key, (None,))
        eq_(
            conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
            [(1, "b", "bz", None)],
        )

    def test_on_duplicate_key_update_expression_multirow(self, connection):
        foos = self.tables.foos
        conn = connection
        conn.execute(insert(foos).values(dict(id=1, bar="b", baz="bz")))
        stmt = insert(foos).values([dict(id=1, bar="ab"), dict(id=2, bar="b")])
        stmt = stmt.on_duplicate_key_update(
            bar=func.concat(stmt.inserted.bar, "_foo"),
            baz=func.concat(stmt.inserted.bar, "_", foos.c.baz),
        )
        result = conn.execute(stmt)
        eq_(result.inserted_primary_key, (None,))
        eq_(
            conn.execute(foos.select()).fetchall(),
            [
                # first entry triggers ON DUPLICATE
                (1, "ab_foo", "ab_bz", False),
                # second entry must be an insert
                (2, "b", None, False),
            ],
        )

    def test_on_duplicate_key_update_preserve_order(self, connection):
        foos = self.tables.foos
        conn = connection
        conn.execute(
            insert(foos).values(
                [
                    dict(id=1, bar="b", baz="bz"),
                    dict(id=2, bar="b", baz="bz2"),
                ],
            )
        )

        stmt = insert(foos)
        update_condition = foos.c.updated_once == False

        # The following statements show importance of the columns update
        # ordering as old values being referenced in UPDATE clause are
        # getting replaced one by one from left to right with their new
        # values.
        stmt1 = stmt.on_duplicate_key_update(
            [
                (
                    "bar",
                    func.if_(
                        update_condition,
                        func.values(foos.c.bar),
                        foos.c.bar,
                    ),
                ),
                (
                    "updated_once",
                    func.if_(update_condition, True, foos.c.updated_once),
                ),
            ]
        )
        stmt2 = stmt.on_duplicate_key_update(
            [
                (
                    "updated_once",
                    func.if_(update_condition, True, foos.c.updated_once),
                ),
                (
                    "bar",
                    func.if_(
                        update_condition,
                        func.values(foos.c.bar),
                        foos.c.bar,
                    ),
                ),
            ]
        )
        # First statement should succeed updating column bar
        conn.execute(stmt1, dict(id=1, bar="ab"))
        eq_(
            conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
            [(1, "ab", "bz", True)],
        )
        # Second statement will do noop update of column bar
        conn.execute(stmt2, dict(id=2, bar="ab"))
        eq_(
            conn.execute(foos.select().where(foos.c.id == 2)).fetchall(),
            [(2, "b", "bz2", True)],
        )

    def test_last_inserted_id(self, connection):
        foos = self.tables.foos
        conn = connection
        stmt = insert(foos).values({"bar": "b", "baz": "bz"})
        result = conn.execute(
            stmt.on_duplicate_key_update(bar=stmt.inserted.bar, baz="newbz")
        )
        eq_(result.inserted_primary_key, (1,))

        stmt = insert(foos).values({"id": 1, "bar": "b", "baz": "bz"})
        result = conn.execute(
            stmt.on_duplicate_key_update(bar=stmt.inserted.bar, baz="newbz")
        )
        eq_(result.inserted_primary_key, (1,))

    def test_bound_caching(self, connection):
        foos = self.tables.foos
        connection.execute(insert(foos).values(dict(id=1, bar="b", baz="bz")))

        for scenario in [
            (random.choice(["c", "d", "e"]), random.choice(["f", "g", "h"]))
            for i in range(10)
        ]:
            stmt = insert(foos).values(dict(id=1, bar="q"))
            stmt = stmt.on_duplicate_key_update(
                bar=scenario[0], baz=scenario[1]
            )

            connection.execute(stmt)

            eq_(
                connection.execute(
                    foos.select().where(foos.c.id == 1)
                ).fetchall(),
                [(1, scenario[0], scenario[1], False)],
            )
