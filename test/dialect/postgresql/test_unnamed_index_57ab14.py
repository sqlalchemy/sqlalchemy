import warnings

from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import Table
from sqlalchemy.testing import assertions
from sqlalchemy.testing import fixtures


class UnnamedIndexCreateTest(fixtures.TestBase):
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
        assert indexes[0]["name"] != "ix_unnamed_idx_t7_data"
        assert indexes[0]["column_names"] == ["data"]


class UnnamedIndexCheckfirstTest(fixtures.TestBase):
    # extra coverage for how checkfirst interacts with an unnamed index
    # against a live server; not run by test.sh, which has no network
    # access and can't reach a postgres instance.
    __only_on__ = "postgresql"

    def test_direct_create_checkfirst_true_bypassed_causes_duplicate(
        self, metadata, connection
    ):
        tbl = Table(
            "unnamed_idx_t8a",
            metadata,
            Column("data", Integer),
        )
        idx = Index(None, tbl.c.data, postgresql_unnamed=True)

        metadata.create_all(connection)
        assert len(inspect(connection).get_indexes("unnamed_idx_t8a")) == 1

        with assertions.expect_warnings(
            r".*postgresql_unnamed.*checkfirst.*"
            r"|.*checkfirst.*postgresql_unnamed.*"
        ):
            idx.create(connection, checkfirst=True)

        assert len(inspect(connection).get_indexes("unnamed_idx_t8a")) == 2

        with assertions.expect_warnings(
            r".*postgresql_unnamed.*checkfirst.*"
            r"|.*checkfirst.*postgresql_unnamed.*"
        ):
            idx.create(connection, checkfirst=True)

        assert len(inspect(connection).get_indexes("unnamed_idx_t8a")) == 3

    def test_direct_create_checkfirst_true_named_index_is_not_duplicated(
        self, metadata, connection
    ):
        tbl = Table(
            "unnamed_idx_t8c",
            metadata,
            Column("data", Integer),
        )
        idx = Index("named_idx_t8c", tbl.c.data)

        metadata.create_all(connection)
        assert len(inspect(connection).get_indexes("unnamed_idx_t8c")) == 1

        idx.create(connection, checkfirst=True)
        idx.create(connection, checkfirst=True)

        assert len(inspect(connection).get_indexes("unnamed_idx_t8c")) == 1

    def test_direct_create_checkfirst_false_emits_no_warning(
        self, metadata, connection
    ):
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
