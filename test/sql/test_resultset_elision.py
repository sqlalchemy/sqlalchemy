"""Cursor-level integration tests for the ScalarResult /
MappingResult row-shape fast paths (perf audit item T1.4).

Complements the unit tests in test/base/test_result.py by exercising
real CursorResult metadata: result-type processors, engine
echo="debug" row logging (the _row_logging_fn fallback gate), empty
results, uniquing and partitions against a live backend.
"""

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.engine.row import RowMapping
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.types import TypeDecorator


class UpperDecorated(TypeDecorator):
    """String type with a result processor, proving the scalar fast
    path applies the hoisted column processor."""

    impl = String(30)
    cache_ok = True

    def process_result_value(self, value, dialect):
        return value.upper() if value is not None else None


class RowShapeElisionCursorTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "elision_data",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("val", UpperDecorated()),
            Column("num", Integer),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.elision_data.insert(),
            [{"id": i, "val": "val-%d" % i, "num": i % 2} for i in range(5)],
        )

    def test_scalars_plain(self, connection):
        t = self.tables.elision_data
        eq_(
            connection.execute(select(t.c.num).order_by(t.c.id))
            .scalars()
            .all(),
            [0, 1, 0, 1, 0],
        )

    def test_scalars_processor(self, connection):
        t = self.tables.elision_data
        eq_(
            connection.execute(select(t.c.val).order_by(t.c.id))
            .scalars()
            .all(),
            ["VAL-0", "VAL-1", "VAL-2", "VAL-3", "VAL-4"],
        )

    def test_scalars_keyed_index_processor(self, connection):
        t = self.tables.elision_data
        result = connection.execute(select(t).order_by(t.c.id))
        eq_(result.scalars("val").first(), "VAL-0")

    def test_scalars_unique(self, connection):
        t = self.tables.elision_data
        eq_(
            connection.execute(select(t.c.num).order_by(t.c.id))
            .scalars()
            .unique()
            .all(),
            [0, 1],
        )

    def test_scalars_empty(self, connection):
        t = self.tables.elision_data
        eq_(
            connection.execute(select(t.c.val).where(t.c.id < 0))
            .scalars()
            .all(),
            [],
        )

    def test_scalars_partitions(self, connection):
        t = self.tables.elision_data
        result = connection.execute(select(t.c.id).order_by(t.c.id))
        eq_(
            [list(p) for p in result.scalars().partitions(2)],
            [[0, 1], [2, 3], [4]],
        )

    def test_mappings_content(self, connection):
        t = self.tables.elision_data
        rows = (
            connection.execute(select(t).order_by(t.c.id).limit(2))
            .mappings()
            .all()
        )
        eq_(
            [dict(row) for row in rows],
            [
                {"id": 0, "val": "VAL-0", "num": 0},
                {"id": 1, "val": "VAL-1", "num": 1},
            ],
        )
        assert all(isinstance(row, RowMapping) for row in rows)
        eq_(list(rows[0].keys()), ["id", "val", "num"])
        eq_(list(rows[0]), ["id", "val", "num"])

    def test_mappings_empty(self, connection):
        t = self.tables.elision_data
        eq_(
            connection.execute(select(t).where(t.c.id < 0)).mappings().all(),
            [],
        )

    def test_echo_debug_scalars(self, testing_engine):
        # sqlite_share_pool lets the new engine see this test's tables
        # on memory-based SQLite; ignored on other backends
        eng = testing_engine(
            options={"echo": "debug", "sqlite_share_pool": True}
        )
        t = self.tables.elision_data
        with eng.connect() as conn:
            result = conn.execute(select(t.c.val).order_by(t.c.id))
            # echo="debug" installs _row_logging_fn; the fast path
            # must be gated OFF while values remain identical
            assert result._row_logging_fn is not None
            eq_(
                result.scalars().all(),
                ["VAL-0", "VAL-1", "VAL-2", "VAL-3", "VAL-4"],
            )

    def test_echo_debug_mappings(self, testing_engine):
        eng = testing_engine(
            options={"echo": "debug", "sqlite_share_pool": True}
        )
        t = self.tables.elision_data
        with eng.connect() as conn:
            result = conn.execute(select(t).order_by(t.c.id).limit(1))
            assert result._row_logging_fn is not None
            eq_(
                [dict(row) for row in result.mappings().all()],
                [{"id": 0, "val": "VAL-0", "num": 0}],
            )
