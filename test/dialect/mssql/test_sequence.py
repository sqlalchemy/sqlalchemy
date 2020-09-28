from decimal import Decimal

from sqlalchemy import BIGINT
from sqlalchemy import Column
from sqlalchemy import DECIMAL
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class SequenceTest(fixtures.TablesTest):
    __only_on__ = "mssql"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "int_seq_t",
            metadata,
            Column(
                "id", Integer, default=Sequence("int_seq", data_type=Integer())
            ),
            Column("txt", String(50)),
        )

        Table(
            "bigint_seq_t",
            metadata,
            Column(
                "id",
                BIGINT,
                default=Sequence("bigint_seq", start=3000000000),
            ),
            Column("txt", String(50)),
        )

        Table(
            "decimal_seq_t",
            metadata,
            Column(
                "id",
                DECIMAL(10, 0),
                default=Sequence(
                    "decimal_seq",
                    data_type=DECIMAL(10, 0),
                    start=3000000000,
                ),
            ),
            Column("txt", String(50)),
        )

    def test_int_seq(self, connection):
        t = self.tables.int_seq_t
        connection.execute(t.insert({"txt": "int_seq test"}))
        result = connection.scalar(select(t.c.id))
        eq_(result, 1)

    def test_bigint_seq(self, connection):
        t = self.tables.bigint_seq_t
        connection.execute(t.insert({"txt": "bigint_seq test"}))
        result = connection.scalar(select(t.c.id))
        eq_(result, 3000000000)

    def test_decimal_seq(self, connection):
        t = self.tables.decimal_seq_t
        connection.execute(t.insert({"txt": "decimal_seq test"}))
        result = connection.scalar(select(t.c.id))
        eq_(result, Decimal("3000000000"))
