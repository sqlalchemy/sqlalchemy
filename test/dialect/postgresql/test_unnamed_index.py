"""Integration tests for the postgresql_unnamed Index dialect kwarg
(issue #4289 follow-up).

These require a real PostgreSQL connection (T7, T8a-e from the design
plan) and are intentionally kept out of test/dialect/postgresql/
test_compiler.py, which only covers unit-level DDL string compilation
(T1-T6, T9, T10) against a bare ``postgresql.dialect()`` with no
connection.

NOT IMPLEMENTED YET. Every test below constructs an
``Index(..., postgresql_unnamed=True)``, which currently raises
``sqlalchemy.exc.ArgumentError`` at construction time because "unnamed"
is not yet registered in ``PGDialect.construct_arguments``. That failure
is expected at this stage -- these tests pin down the contract the
future implementation must satisfy.
"""

import warnings

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.testing import assertions
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock


class UnnamedIndexCreateTest(fixtures.TestBase):
    """T7: CREATE INDEX with no name actually succeeds against a real
    PostgreSQL server, and the server assigns its own name."""

    __only_on__ = "postgresql"

    def test_create_unnamed_index_against_real_server(
        self, metadata, connection
    ):
        tbl = Table(
            "unnamed_idx_t7",
            metadata,
            Column("data", Integer),
        )
        Index(None, tbl.c.data, postgresql_unnamed=True)

        metadata.create_all(connection)

        indexes = inspect(connection).get_indexes("unnamed_idx_t7")
        assert len(indexes) == 1
        # the server picked its own name -- SQLAlchemy's naming
        # convention output ("ix_unnamed_idx_t7_data") must NOT appear,
        # since it was never sent to the server at all.
        assert indexes[0]["name"] != "ix_unnamed_idx_t7_data"
        assert indexes[0]["column_names"] == ["data"]


class UnnamedIndexCheckfirstTest(fixtures.TestBase):
    """T8: interaction between postgresql_unnamed and checkfirst."""

    __only_on__ = "postgresql"

    def test_checkfirst_true_bypassed_causes_duplicate_on_second_call(
        self, metadata, connection
    ):
        # T8a + T8b
        tbl = Table(
            "unnamed_idx_t8a",
            metadata,
            Column("data", Integer),
        )
        Index(None, tbl.c.data, postgresql_unnamed=True)

        with assertions.expect_warnings(
            r".*postgresql_unnamed.*checkfirst.*"
            r"|.*checkfirst.*postgresql_unnamed.*"
        ):
            metadata.create_all(connection, checkfirst=True)

        indexes_after_first = inspect(connection).get_indexes(
            "unnamed_idx_t8a"
        )
        assert len(indexes_after_first) == 1

        # second call: must NOT raise, and -- per the documented
        # trade-off -- is expected to create a second physical index,
        # since checkfirst cannot verify an unnamed index's existence.
        with assertions.expect_warnings(
            r".*postgresql_unnamed.*checkfirst.*"
            r"|.*checkfirst.*postgresql_unnamed.*"
        ):
            metadata.create_all(connection, checkfirst=True)

        indexes_after_second = inspect(connection).get_indexes(
            "unnamed_idx_t8a"
        )
        assert len(indexes_after_second) == 2

    def test_checkfirst_true_named_index_is_not_duplicated(
        self, metadata, connection
    ):
        # T8c (control / regression guard)
        tbl = Table(
            "unnamed_idx_t8c",
            metadata,
            Column("data", Integer),
        )
        Index("named_idx_t8c", tbl.c.data)

        metadata.create_all(connection, checkfirst=True)
        metadata.create_all(connection, checkfirst=True)

        indexes = inspect(connection).get_indexes("unnamed_idx_t8c")
        assert len(indexes) == 1

    def test_checkfirst_false_emits_no_warning(self, metadata, connection):
        # T8d
        tbl = Table(
            "unnamed_idx_t8d",
            metadata,
            Column("data", Integer),
        )
        Index(None, tbl.c.data, postgresql_unnamed=True)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            metadata.create_all(connection, checkfirst=False)

        assert not any(
            "postgresql_unnamed" in str(w.message) for w in recorded
        )

    def test_checkfirst_true_never_calls_has_index_for_unnamed(
        self, metadata, connection
    ):
        # T8e: pins down that the bypass skips calling has_index
        # entirely, rather than calling it and ignoring the result.
        tbl = Table(
            "unnamed_idx_t8e",
            metadata,
            Column("data", Integer),
        )
        Index(None, tbl.c.data, postgresql_unnamed=True)

        with mock.patch.object(
            connection.dialect,
            "has_index",
            wraps=connection.dialect.has_index,
        ) as has_index:
            metadata.create_all(connection, checkfirst=True)

        has_index.assert_not_called()
