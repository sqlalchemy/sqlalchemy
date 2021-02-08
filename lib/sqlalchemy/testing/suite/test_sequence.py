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
            Column("id", Integer, Sequence("tab_id_seq"), primary_key=True),
            Column("data", String(50)),
        )

        Table(
            "seq_opt_pk",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("tab_id_seq", optional=True),
                primary_key=True,
            ),
            Column("data", String(50)),
        )

        Table(
            "seq_no_returning",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("noret_id_seq"),
                primary_key=True,
            ),
            Column("data", String(50)),
            implicit_returning=False,
        )

        if testing.requires.schemas.enabled:
            Table(
                "seq_no_returning_sch",
                metadata,
                Column(
                    "id",
                    Integer,
                    Sequence("noret_sch_id_seq", schema=config.test_schema),
                    primary_key=True,
                ),
                Column("data", String(50)),
                implicit_returning=False,
                schema=config.test_schema,
            )

    def test_insert_roundtrip(self):
        config.db.execute(self.tables.seq_pk.insert(), data="some data")
        self._assert_round_trip(self.tables.seq_pk, config.db)

    def test_insert_lastrowid(self):
        r = config.db.execute(self.tables.seq_pk.insert(), data="some data")
        eq_(r.inserted_primary_key, [1])

    def test_nextval_direct(self):
        r = config.db.execute(self.tables.seq_pk.c.id.default)
        eq_(r, 1)

    @requirements.sequences_optional
    def test_optional_seq(self):
        r = config.db.execute(
            self.tables.seq_opt_pk.insert(), data="some data"
        )
        eq_(r.inserted_primary_key, [1])

    def _assert_round_trip(self, table, conn):
        row = conn.execute(table.select()).first()
        eq_(row, (1, "some data"))

    def test_insert_roundtrip_no_implicit_returning(self, connection):
        connection.execute(
            self.tables.seq_no_returning.insert(), dict(data="some data")
        )
        self._assert_round_trip(self.tables.seq_no_returning, connection)

    @testing.combinations((True,), (False,), argnames="implicit_returning")
    @testing.requires.schemas
    def test_insert_roundtrip_translate(self, connection, implicit_returning):

        seq_no_returning = Table(
            "seq_no_returning_sch",
            MetaData(),
            Column(
                "id",
                Integer,
                Sequence("noret_sch_id_seq", schema="alt_schema"),
                primary_key=True,
            ),
            Column("data", String(50)),
            implicit_returning=implicit_returning,
            schema="alt_schema",
        )

        connection = connection.execution_options(
            schema_translate_map={"alt_schema": config.test_schema}
        )
        connection.execute(seq_no_returning.insert(), dict(data="some data"))
        self._assert_round_trip(seq_no_returning, connection)

    @testing.requires.schemas
    def test_nextval_direct_schema_translate(self, connection):
        seq = Sequence("noret_sch_id_seq", schema="alt_schema")
        connection = connection.execution_options(
            schema_translate_map={"alt_schema": config.test_schema}
        )

        r = connection.execute(seq)
        eq_(r, testing.db.dialect.default_sequence_base)


class SequenceCompilerTest(testing.AssertsCompiledSQL, fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    def test_literal_binds_inline_compile(self):
        table = Table(
            "x",
            MetaData(),
            Column("y", Integer, Sequence("y_seq")),
            Column("q", Integer),
        )

        stmt = table.insert().values(q=5)

        seq_nextval = testing.db.dialect.statement_compiler(
            statement=None, dialect=testing.db.dialect
        ).visit_sequence(Sequence("y_seq"))
        self.assert_compile(
            stmt,
            "INSERT INTO x (y, q) VALUES (%s, 5)" % (seq_nextval,),
            literal_binds=True,
            dialect=testing.db.dialect,
        )


class HasSequenceTest(fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    def test_has_sequence(self):
        s1 = Sequence("user_id_seq")
        testing.db.execute(schema.CreateSequence(s1))
        try:
            eq_(
                testing.db.dialect.has_sequence(testing.db, "user_id_seq"),
                True,
            )
        finally:
            testing.db.execute(schema.DropSequence(s1))

    @testing.requires.schemas
    def test_has_sequence_schema(self):
        s1 = Sequence("user_id_seq", schema=config.test_schema)
        testing.db.execute(schema.CreateSequence(s1))
        try:
            eq_(
                testing.db.dialect.has_sequence(
                    testing.db, "user_id_seq", schema=config.test_schema
                ),
                True,
            )
        finally:
            testing.db.execute(schema.DropSequence(s1))

    def test_has_sequence_neg(self):
        eq_(testing.db.dialect.has_sequence(testing.db, "user_id_seq"), False)

    @testing.requires.schemas
    def test_has_sequence_schemas_neg(self):
        eq_(
            testing.db.dialect.has_sequence(
                testing.db, "user_id_seq", schema=config.test_schema
            ),
            False,
        )

    @testing.requires.schemas
    def test_has_sequence_default_not_in_remote(self):
        s1 = Sequence("user_id_seq")
        testing.db.execute(schema.CreateSequence(s1))
        try:
            eq_(
                testing.db.dialect.has_sequence(
                    testing.db, "user_id_seq", schema=config.test_schema
                ),
                False,
            )
        finally:
            testing.db.execute(schema.DropSequence(s1))

    @testing.requires.schemas
    def test_has_sequence_remote_not_in_default(self):
        s1 = Sequence("user_id_seq", schema=config.test_schema)
        testing.db.execute(schema.CreateSequence(s1))
        try:
            eq_(
                testing.db.dialect.has_sequence(testing.db, "user_id_seq"),
                False,
            )
        finally:
            testing.db.execute(schema.DropSequence(s1))
