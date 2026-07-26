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
    """T8: interaction between postgresql_unnamed and checkfirst.

    IMPORTANT (discovered while running these against a real server):
    ``metadata.create_all()`` never reaches ``_can_create_index`` for a
    table's own indexes -- ``SchemaGenerator.visit_table`` traverses them
    with ``create_ok=True`` unconditionally (sql/ddl.py:1530-1531), since
    the checkfirst gate for the whole operation already happened one
    level up, at the *table* (``_can_create_table``). A table that
    already exists is skipped entirely -- its indexes are never
    revisited, named or unnamed. So calling ``create_all()`` twice in a
    row is not the scenario that exercises checkfirst-for-indexes at
    all.

    The code path that actually reaches ``_can_create_index`` with
    ``create_ok=False`` is a *direct* ``Index.create(bind,
    checkfirst=...)`` call against an index attached to a table that
    already exists -- e.g. an idempotent migration script, or Alembic
    re-running ``create_index`` defensively. All tests below use that
    call directly instead of ``create_all()``.
    """

    __only_on__ = "postgresql"

    def test_direct_create_checkfirst_true_bypassed_causes_duplicate(
        self, metadata, connection
    ):
        # T8a + T8b
        tbl = Table(
            "unnamed_idx_t8a",
            metadata,
            Column("data", Integer),
        )
        idx = Index(None, tbl.c.data, postgresql_unnamed=True)

        # table + its index created once, normally, via create_all()
        metadata.create_all(connection)
        assert len(inspect(connection).get_indexes("unnamed_idx_t8a")) == 1

        # first *direct* checkfirst=True call: must warn, and must still
        # attempt (and succeed at) creating a second physical index,
        # since checkfirst can't verify an unnamed index's existence.
        with assertions.expect_warnings(
            r".*postgresql_unnamed.*checkfirst.*"
            r"|.*checkfirst.*postgresql_unnamed.*"
        ):
            idx.create(connection, checkfirst=True)

        assert len(inspect(connection).get_indexes("unnamed_idx_t8a")) == 2

        # second direct call: must NOT raise, and -- per the documented
        # trade-off -- creates yet another duplicate.
        with assertions.expect_warnings(
            r".*postgresql_unnamed.*checkfirst.*"
            r"|.*checkfirst.*postgresql_unnamed.*"
        ):
            idx.create(connection, checkfirst=True)

        assert len(inspect(connection).get_indexes("unnamed_idx_t8a")) == 3

    def test_direct_create_checkfirst_true_named_index_is_not_duplicated(
        self, metadata, connection
    ):
        # T8c (control / regression guard)
        tbl = Table(
            "unnamed_idx_t8c",
            metadata,
            Column("data", Integer),
        )
        idx = Index("named_idx_t8c", tbl.c.data)

        metadata.create_all(connection)
        assert len(inspect(connection).get_indexes("unnamed_idx_t8c")) == 1

        # named index CAN be looked up by has_index -- checkfirst
        # correctly prevents the duplicate, unlike the unnamed case.
        idx.create(connection, checkfirst=True)
        idx.create(connection, checkfirst=True)

        assert len(inspect(connection).get_indexes("unnamed_idx_t8c")) == 1

    def test_direct_create_checkfirst_false_emits_no_warning(
        self, metadata, connection
    ):
        # T8d
        tbl = Table(
            "unnamed_idx_t8d",
            metadata,
            Column("data", Integer),
        )
        idx = Index(None, tbl.c.data, postgresql_unnamed=True)
        metadata.create_all(connection)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            idx.create(connection, checkfirst=False)

        assert not any(
            "postgresql_unnamed" in str(w.message) for w in recorded
        )

    def test_direct_create_checkfirst_true_never_calls_has_index_for_unnamed(
        self, metadata, connection
    ):
        # T8e: pins down that the bypass skips calling has_index
        # entirely, rather than calling it and ignoring the result.
        tbl = Table(
            "unnamed_idx_t8e",
            metadata,
            Column("data", Integer),
        )
        idx = Index(None, tbl.c.data, postgresql_unnamed=True)
        metadata.create_all(connection)

        with (
            assertions.expect_warnings(
                r".*postgresql_unnamed.*checkfirst.*"
                r"|.*checkfirst.*postgresql_unnamed.*"
            ),
            mock.patch.object(
                connection.dialect,
                "has_index",
                wraps=connection.dialect.has_index,
            ) as has_index,
        ):
            idx.create(connection, checkfirst=True)

        has_index.assert_not_called()

    def test_direct_create_checkfirst_true_calls_has_index_for_named(
        self, metadata, connection
    ):
        # positive control for T8e: proves the mock actually observes a
        # has_index call when the index *does* have a name, so T8e's
        # "not called" result is meaningful and not just an artifact of
        # this code path never calling has_index at all.
        tbl = Table(
            "unnamed_idx_t8e_control",
            metadata,
            Column("data", Integer),
        )
        idx = Index("named_idx_t8e_control", tbl.c.data)
        metadata.create_all(connection)

        with mock.patch.object(
            connection.dialect,
            "has_index",
            wraps=connection.dialect.has_index,
        ) as has_index:
            idx.create(connection, checkfirst=True)

        has_index.assert_called_once()
