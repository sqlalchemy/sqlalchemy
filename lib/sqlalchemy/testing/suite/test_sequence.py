from .. import config
from .. import fixtures
from ..assertions import eq_
from ..assertions import is_true
from ..config import requirements
from ..schema import Column
from ..schema import Table
from ... import inspect
from ... import Integer
from ... import MetaData
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
            Column(
                "id",
                Integer,
                Sequence("tab_id_seq"),
                primary_key=True,
            ),
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
        connection.execute(self.tables.seq_pk.insert(), dict(data="some data"))
        self._assert_round_trip(self.tables.seq_pk, connection)

    def test_insert_lastrowid(self, connection):
        r = connection.execute(
            self.tables.seq_pk.insert(), dict(data="some data")
        )
        eq_(
            r.inserted_primary_key, (testing.db.dialect.default_sequence_base,)
        )

    def test_nextval_direct(self, connection):
        r = connection.execute(self.tables.seq_pk.c.id.default)
        eq_(r, testing.db.dialect.default_sequence_base)

    @requirements.sequences_optional
    def test_optional_seq(self, connection):
        r = connection.execute(
            self.tables.seq_opt_pk.insert(), dict(data="some data")
        )
        eq_(r.inserted_primary_key, (1,))

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


class HasSequenceTest(fixtures.TablesTest):
    run_deletes = None

    __requires__ = ("sequences",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Sequence("user_id_seq", metadata=metadata)
        Sequence(
            "other_seq", metadata=metadata, nomaxvalue=True, nominvalue=True
        )
        if testing.requires.schemas.enabled:
            Sequence(
                "user_id_seq", schema=config.test_schema, metadata=metadata
            )
            Sequence(
                "schema_seq", schema=config.test_schema, metadata=metadata
            )
        Table(
            "user_id_table",
            metadata,
            Column("id", Integer, primary_key=True),
        )

    def test_has_sequence(self, connection):
        eq_(
            inspect(connection).has_sequence("user_id_seq"),
            True,
        )

    def test_has_sequence_other_object(self, connection):
        eq_(
            inspect(connection).has_sequence("user_id_table"),
            False,
        )

    @testing.requires.schemas
    def test_has_sequence_schema(self, connection):
        eq_(
            inspect(connection).has_sequence(
                "user_id_seq", schema=config.test_schema
            ),
            True,
        )

    def test_has_sequence_neg(self, connection):
        eq_(
            inspect(connection).has_sequence("some_sequence"),
            False,
        )

    @testing.requires.schemas
    def test_has_sequence_schemas_neg(self, connection):
        eq_(
            inspect(connection).has_sequence(
                "some_sequence", schema=config.test_schema
            ),
            False,
        )

    @testing.requires.schemas
    def test_has_sequence_default_not_in_remote(self, connection):
        eq_(
            inspect(connection).has_sequence(
                "other_sequence", schema=config.test_schema
            ),
            False,
        )

    @testing.requires.schemas
    def test_has_sequence_remote_not_in_default(self, connection):
        eq_(
            inspect(connection).has_sequence("schema_seq"),
            False,
        )

    def test_get_sequence_names(self, connection):
        exp = {"other_seq", "user_id_seq"}

        res = set(inspect(connection).get_sequence_names())
        is_true(res.intersection(exp) == exp)
        is_true("schema_seq" not in res)

    @testing.requires.schemas
    def test_get_sequence_names_no_sequence_schema(self, connection):
        eq_(
            inspect(connection).get_sequence_names(
                schema=config.test_schema_2
            ),
            [],
        )

    @testing.requires.schemas
    def test_get_sequence_names_sequences_schema(self, connection):
        eq_(
            sorted(
                inspect(connection).get_sequence_names(
                    schema=config.test_schema
                )
            ),
            ["schema_seq", "user_id_seq"],
        )


class HasSequenceTestEmpty(fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    def test_get_sequence_names_no_sequence(self, connection):
        eq_(
            inspect(connection).get_sequence_names(),
            [],
        )
