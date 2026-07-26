from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import Table
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
