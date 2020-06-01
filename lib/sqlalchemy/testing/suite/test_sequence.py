from .. import config
from .. import fixtures
from ..assertions import eq_
from ..config import requirements
from ..schema import Column
from ..schema import Table
from ... import Integer
from ... import MetaData
from ... import schema
from ... import Sequence
from ... import String
from ... import testing


class SequenceTest(fixtures.TablesTest):
    __requires__ = ("sequences",)
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "seq_pk",
            metadata,
            Column("id", Integer, Sequence("tab_id_seq"), primary_key=True,),
            Column("data", String(50)),
        )

        Table(
            "seq_opt_pk",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("tab_id_seq", data_type=Integer, optional=True),
                primary_key=True,
            ),
            Column("data", String(50)),
        )

    def test_insert_roundtrip(self, connection):
        connection.execute(self.tables.seq_pk.insert(), data="some data")
        self._assert_round_trip(self.tables.seq_pk, connection)

    def test_insert_lastrowid(self, connection):
        r = connection.execute(self.tables.seq_pk.insert(), data="some data")
        eq_(r.inserted_primary_key, [testing.db.dialect.default_sequence_base])

    def test_nextval_direct(self, connection):
        r = connection.execute(self.tables.seq_pk.c.id.default)
        eq_(r, testing.db.dialect.default_sequence_base)

    @requirements.sequences_optional
    def test_optional_seq(self, connection):
        r = connection.execute(
            self.tables.seq_opt_pk.insert(), data="some data"
        )
        eq_(r.inserted_primary_key, [1])

    def _assert_round_trip(self, table, conn):
        row = conn.execute(table.select()).first()
        eq_(row, (testing.db.dialect.default_sequence_base, "some data"))


class SequenceCompilerTest(testing.AssertsCompiledSQL, fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    def test_literal_binds_inline_compile(self, connection):
        table = Table(
            "x",
            MetaData(),
            Column("y", Integer, Sequence("y_seq")),
            Column("q", Integer),
        )

        stmt = table.insert().values(q=5)

        seq_nextval = connection.dialect.statement_compiler(
            statement=None, dialect=connection.dialect
        ).visit_sequence(Sequence("y_seq"))
        self.assert_compile(
            stmt,
            "INSERT INTO x (y, q) VALUES (%s, 5)" % (seq_nextval,),
            literal_binds=True,
            dialect=connection.dialect,
        )


class HasSequenceTest(fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    def test_has_sequence(self, connection):
        s1 = Sequence("user_id_seq")
        connection.execute(schema.CreateSequence(s1))
        try:
            eq_(
                connection.dialect.has_sequence(connection, "user_id_seq"),
                True,
            )
        finally:
            connection.execute(schema.DropSequence(s1))

    @testing.requires.schemas
    def test_has_sequence_schema(self, connection):
        s1 = Sequence("user_id_seq", schema=config.test_schema)
        connection.execute(schema.CreateSequence(s1))
        try:
            eq_(
                connection.dialect.has_sequence(
                    connection, "user_id_seq", schema=config.test_schema
                ),
                True,
            )
        finally:
            connection.execute(schema.DropSequence(s1))

    def test_has_sequence_neg(self, connection):
        eq_(connection.dialect.has_sequence(connection, "user_id_seq"), False)

    @testing.requires.schemas
    def test_has_sequence_schemas_neg(self, connection):
        eq_(
            connection.dialect.has_sequence(
                connection, "user_id_seq", schema=config.test_schema
            ),
            False,
        )

    @testing.requires.schemas
    def test_has_sequence_default_not_in_remote(self, connection):
        s1 = Sequence("user_id_seq")
        connection.execute(schema.CreateSequence(s1))
        try:
            eq_(
                connection.dialect.has_sequence(
                    connection, "user_id_seq", schema=config.test_schema
                ),
                False,
            )
        finally:
            connection.execute(schema.DropSequence(s1))

    @testing.requires.schemas
    def test_has_sequence_remote_not_in_default(self, connection):
        s1 = Sequence("user_id_seq", schema=config.test_schema)
        connection.execute(schema.CreateSequence(s1))
        try:
            eq_(
                connection.dialect.has_sequence(connection, "user_id_seq"),
                False,
            )
        finally:
            connection.execute(schema.DropSequence(s1))
