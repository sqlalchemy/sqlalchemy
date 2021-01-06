import itertools

from sqlalchemy import Boolean
from sqlalchemy import delete
from sqlalchemy import exc as sa_exc
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import type_coerce
from sqlalchemy import update
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.types import TypeDecorator


class ReturnCombinationTests(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "postgresql"

    @testing.fixture
    def table_fixture(self):
        return Table(
            "foo",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("q", Integer, server_default="5"),
            Column("x", Integer),
            Column("y", Integer),
        )

    @testing.combinations(
        (
            insert,
            "INSERT INTO foo (id, q, x, y) "
            "VALUES (%(id)s, %(q)s, %(x)s, %(y)s)",
        ),
        (update, "UPDATE foo SET id=%(id)s, q=%(q)s, x=%(x)s, y=%(y)s"),
        (delete, "DELETE FROM foo"),
        argnames="dml_fn, sql_frag",
        id_="na",
    )
    def test_return_combinations(self, table_fixture, dml_fn, sql_frag):
        t = table_fixture
        stmt = dml_fn(t)

        stmt = stmt.returning(t.c.x)

        stmt = stmt.returning(t.c.y)

        self.assert_compile(
            stmt,
            "%s RETURNING foo.x, foo.y" % (sql_frag),
        )

    def test_return_no_return_defaults(self, table_fixture):
        t = table_fixture

        stmt = t.insert()

        stmt = stmt.returning(t.c.x)

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "RETURNING is already configured on this statement",
            stmt.return_defaults,
        )

    def test_return_defaults_no_returning(self, table_fixture):
        t = table_fixture

        stmt = t.insert()

        stmt = stmt.return_defaults()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"return_defaults\(\) is already configured on this statement",
            stmt.returning,
            t.c.x,
        )


class ReturningTest(fixtures.TablesTest, AssertsExecutionResults):
    __requires__ = ("returning",)
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        class GoofyType(TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                if value is None:
                    return None
                return "FOO" + value

            def process_result_value(self, value, dialect):
                if value is None:
                    return None
                return value + "BAR"

        cls.GoofyType = GoofyType

        Table(
            "tables",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("persons", Integer),
            Column("full", Boolean),
            Column("goofy", GoofyType(50)),
            Column("strval", String(50)),
        )

    def test_column_targeting(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert().returning(table.c.id, table.c.full),
            {"persons": 1, "full": False},
        )

        row = result.first()._mapping
        assert row[table.c.id] == row["id"] == 1
        assert row[table.c.full] == row["full"]
        assert row["full"] is False

        result = connection.execute(
            table.insert()
            .values(persons=5, full=True, goofy="somegoofy")
            .returning(table.c.persons, table.c.full, table.c.goofy)
        )
        row = result.first()._mapping
        assert row[table.c.persons] == row["persons"] == 5
        assert row[table.c.full] == row["full"]

        eq_(row[table.c.goofy], row["goofy"])
        eq_(row["goofy"], "FOOsomegoofyBAR")

    @testing.fails_on("firebird", "fb can't handle returning x AS y")
    def test_labeling(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert()
            .values(persons=6)
            .returning(table.c.persons.label("lala"))
        )
        row = result.first()._mapping
        assert row["lala"] == 6

    @testing.fails_on(
        "firebird", "fb/kintersbasdb can't handle the bind params"
    )
    def test_anon_expressions(self, connection):
        table = self.tables.tables
        GoofyType = self.GoofyType
        result = connection.execute(
            table.insert()
            .values(goofy="someOTHERgoofy")
            .returning(func.lower(table.c.goofy, type_=GoofyType))
        )
        row = result.first()
        eq_(row[0], "foosomeothergoofyBAR")

        result = connection.execute(
            table.insert().values(persons=12).returning(table.c.persons + 18)
        )
        row = result.first()
        eq_(row[0], 30)

    def test_update_returning(self, connection):
        table = self.tables.tables
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.update(table.c.persons > 4, dict(full=True)).returning(
                table.c.id
            )
        )
        eq_(result.fetchall(), [(1,)])

        result2 = connection.execute(
            select(table.c.id, table.c.full).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, True), (2, False)])

    @testing.fails_on(
        "mssql",
        "driver has unknown issue with string concatenation "
        "in INSERT RETURNING",
    )
    def test_insert_returning_w_expression_one(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert().returning(table.c.strval + "hi"),
            {"persons": 5, "full": False, "strval": "str1"},
        )

        eq_(result.fetchall(), [("str1hi",)])

        result2 = connection.execute(
            select(table.c.id, table.c.strval).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, "str1")])

    def test_insert_returning_w_type_coerce_expression(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert().returning(type_coerce(table.c.goofy, String)),
            {"persons": 5, "goofy": "somegoofy"},
        )

        eq_(result.fetchall(), [("FOOsomegoofy",)])

        result2 = connection.execute(
            select(table.c.id, table.c.goofy).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, "FOOsomegoofyBAR")])

    def test_update_returning_w_expression_one(self, connection):
        table = self.tables.tables
        connection.execute(
            table.insert(),
            [
                {"persons": 5, "full": False, "strval": "str1"},
                {"persons": 3, "full": False, "strval": "str2"},
            ],
        )

        result = connection.execute(
            table.update()
            .where(table.c.persons > 4)
            .values(full=True)
            .returning(table.c.strval + "hi")
        )
        eq_(result.fetchall(), [("str1hi",)])

        result2 = connection.execute(
            select(table.c.id, table.c.strval).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, "str1"), (2, "str2")])

    def test_update_returning_w_type_coerce_expression(self, connection):
        table = self.tables.tables
        connection.execute(
            table.insert(),
            [
                {"persons": 5, "goofy": "somegoofy1"},
                {"persons": 3, "goofy": "somegoofy2"},
            ],
        )

        result = connection.execute(
            table.update()
            .where(table.c.persons > 4)
            .values(goofy="newgoofy")
            .returning(type_coerce(table.c.goofy, String))
        )
        eq_(result.fetchall(), [("FOOnewgoofy",)])

        result2 = connection.execute(
            select(table.c.id, table.c.goofy).order_by(table.c.id)
        )
        eq_(
            result2.fetchall(),
            [(1, "FOOnewgoofyBAR"), (2, "FOOsomegoofy2BAR")],
        )

    @testing.requires.full_returning
    def test_update_full_returning(self, connection):
        table = self.tables.tables
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.update(table.c.persons > 2)
            .values(full=True)
            .returning(table.c.id, table.c.full)
        )
        eq_(result.fetchall(), [(1, True), (2, True)])

    @testing.requires.full_returning
    def test_delete_full_returning(self, connection):
        table = self.tables.tables
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.delete().returning(table.c.id, table.c.full)
        )
        eq_(result.fetchall(), [(1, False), (2, False)])

    def test_insert_returning(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert().returning(table.c.id), {"persons": 1, "full": False}
        )

        eq_(result.fetchall(), [(1,)])

    @testing.requires.multivalues_inserts
    def test_multirow_returning(self, connection):
        table = self.tables.tables
        ins = (
            table.insert()
            .returning(table.c.id, table.c.persons)
            .values(
                [
                    {"persons": 1, "full": False},
                    {"persons": 2, "full": True},
                    {"persons": 3, "full": False},
                ]
            )
        )
        result = connection.execute(ins)
        eq_(result.fetchall(), [(1, 1), (2, 2), (3, 3)])

    def test_no_ipk_on_returning(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert().returning(table.c.id), {"persons": 1, "full": False}
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"Can't call inserted_primary_key when returning\(\) is used.",
            getattr,
            result,
            "inserted_primary_key",
        )

    @testing.fails_on_everything_except("postgresql", "firebird")
    def test_literal_returning(self, connection):
        if testing.against("postgresql"):
            literal_true = "true"
        else:
            literal_true = "1"

        result4 = connection.exec_driver_sql(
            'insert into tables (id, persons, "full") '
            "values (5, 10, %s) returning persons" % literal_true
        )
        eq_([dict(row._mapping) for row in result4], [{"persons": 10}])

    def test_delete_returning(self, connection):
        table = self.tables.tables
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.delete(table.c.persons > 4).returning(table.c.id)
        )
        eq_(result.fetchall(), [(1,)])

        result2 = connection.execute(
            select(table.c.id, table.c.full).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(2, False)])


class CompositeStatementTest(fixtures.TestBase):
    __requires__ = ("returning",)
    __backend__ = True

    @testing.provide_metadata
    def test_select_doesnt_pollute_result(self, connection):
        class MyType(TypeDecorator):
            impl = Integer

            def process_result_value(self, value, dialect):
                raise Exception("I have not been selected")

        t1 = Table("t1", self.metadata, Column("x", MyType()))

        t2 = Table("t2", self.metadata, Column("x", Integer))

        self.metadata.create_all(connection)
        connection.execute(t1.insert().values(x=5))

        stmt = (
            t2.insert()
            .values(x=select(t1.c.x).scalar_subquery())
            .returning(t2.c.x)
        )

        result = connection.execute(stmt)
        eq_(result.scalar(), 5)


class SequenceReturningTest(fixtures.TablesTest):
    __requires__ = "returning", "sequences"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        seq = Sequence("tid_seq")
        Table(
            "tables",
            metadata,
            Column(
                "id",
                Integer,
                seq,
                primary_key=True,
            ),
            Column("data", String(50)),
        )
        cls.sequences.tid_seq = seq

    def test_insert(self, connection):
        table = self.tables.tables
        r = connection.execute(
            table.insert().values(data="hi").returning(table.c.id)
        )
        eq_(r.first(), tuple([testing.db.dialect.default_sequence_base]))
        eq_(
            connection.execute(self.sequences.tid_seq),
            testing.db.dialect.default_sequence_base + 1,
        )


class KeyReturningTest(fixtures.TablesTest, AssertsExecutionResults):

    """test returning() works with columns that define 'key'."""

    __requires__ = ("returning",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tables",
            metadata,
            Column(
                "id",
                Integer,
                primary_key=True,
                key="foo_id",
                test_needs_autoincrement=True,
            ),
            Column("data", String(20)),
        )

    @testing.exclude("firebird", "<", (2, 0), "2.0+ feature")
    @testing.exclude("postgresql", "<", (8, 2), "8.2+ feature")
    def test_insert(self, connection):
        table = self.tables.tables
        result = connection.execute(
            table.insert().returning(table.c.foo_id), dict(data="somedata")
        )
        row = result.first()._mapping
        assert row[table.c.foo_id] == row["id"] == 1

        result = connection.execute(table.select()).first()._mapping
        assert row[table.c.foo_id] == row["id"] == 1


class ReturnDefaultsTest(fixtures.TablesTest):
    __requires__ = ("returning",)
    run_define_tables = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy.sql import ColumnElement
        from sqlalchemy.ext.compiler import compiles

        counter = itertools.count()

        class IncDefault(ColumnElement):
            pass

        @compiles(IncDefault)
        def compile_(element, compiler, **kw):
            return str(next(counter))

        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("insdef", Integer, default=IncDefault()),
            Column("upddef", Integer, onupdate=IncDefault()),
        )

    def test_chained_insert_pk(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().values(upddef=1).return_defaults(t1.c.insdef)
        )
        eq_(
            [
                result.returned_defaults._mapping[k]
                for k in (t1.c.id, t1.c.insdef)
            ],
            [1, 0],
        )

    def test_arg_insert_pk(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().return_defaults(t1.c.insdef).values(upddef=1)
        )
        eq_(
            [
                result.returned_defaults._mapping[k]
                for k in (t1.c.id, t1.c.insdef)
            ],
            [1, 0],
        )

    def test_chained_update_pk(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().values(data="d1").return_defaults(t1.c.upddef)
        )
        eq_(
            [result.returned_defaults._mapping[k] for k in (t1.c.upddef,)], [1]
        )

    def test_arg_update_pk(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().return_defaults(t1.c.upddef).values(data="d1")
        )
        eq_(
            [result.returned_defaults._mapping[k] for k in (t1.c.upddef,)], [1]
        )

    def test_insert_non_default(self, connection):
        """test that a column not marked at all as a
        default works with this feature."""

        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().values(upddef=1).return_defaults(t1.c.data)
        )
        eq_(
            [
                result.returned_defaults._mapping[k]
                for k in (t1.c.id, t1.c.data)
            ],
            [1, None],
        )

    def test_update_non_default(self, connection):
        """test that a column not marked at all as a
        default works with this feature."""

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().values(upddef=2).return_defaults(t1.c.data)
        )
        eq_(
            [result.returned_defaults._mapping[k] for k in (t1.c.data,)],
            [None],
        )

    def test_insert_sql_expr(self, connection):
        from sqlalchemy import literal

        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().return_defaults().values(insdef=literal(10) + 5)
        )

        eq_(
            result.returned_defaults._mapping,
            {"id": 1, "data": None, "insdef": 15, "upddef": None},
        )

    def test_update_sql_expr(self, connection):
        from sqlalchemy import literal

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().values(upddef=literal(10) + 5).return_defaults()
        )

        eq_(result.returned_defaults._mapping, {"upddef": 15})

    def test_insert_non_default_plus_default(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert()
            .values(upddef=1)
            .return_defaults(t1.c.data, t1.c.insdef)
        )
        eq_(
            dict(result.returned_defaults._mapping),
            {"id": 1, "data": None, "insdef": 0},
        )
        eq_(result.inserted_primary_key, (1,))

    def test_update_non_default_plus_default(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update()
            .values(insdef=2)
            .return_defaults(t1.c.data, t1.c.upddef)
        )
        eq_(
            dict(result.returned_defaults._mapping),
            {"data": None, "upddef": 1},
        )

    def test_insert_all(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().values(upddef=1).return_defaults()
        )
        eq_(
            dict(result.returned_defaults._mapping),
            {"id": 1, "data": None, "insdef": 0},
        )
        eq_(result.inserted_primary_key, (1,))

    def test_update_all(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().values(insdef=2).return_defaults()
        )
        eq_(dict(result.returned_defaults._mapping), {"upddef": 1})

    @testing.requires.insert_executemany_returning
    def test_insert_executemany_no_defaults_passed(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().return_defaults(),
            [
                {"data": "d1"},
                {"data": "d2"},
                {"data": "d3"},
                {"data": "d4"},
                {"data": "d5"},
                {"data": "d6"},
            ],
        )

        eq_(
            [row._mapping for row in result.returned_defaults_rows],
            [
                {"id": 1, "insdef": 0, "upddef": None},
                {"id": 2, "insdef": 0, "upddef": None},
                {"id": 3, "insdef": 0, "upddef": None},
                {"id": 4, "insdef": 0, "upddef": None},
                {"id": 5, "insdef": 0, "upddef": None},
                {"id": 6, "insdef": 0, "upddef": None},
            ],
        )

        eq_(
            result.inserted_primary_key_rows,
            [(1,), (2,), (3,), (4,), (5,), (6,)],
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This statement was an executemany call; "
            "if return defaults is supported",
            lambda: result.returned_defaults,
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This statement was an executemany call; "
            "if primary key returning is supported",
            lambda: result.inserted_primary_key,
        )

    @testing.requires.insert_executemany_returning
    def test_insert_executemany_insdefault_passed(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().return_defaults(),
            [
                {"data": "d1", "insdef": 11},
                {"data": "d2", "insdef": 12},
                {"data": "d3", "insdef": 13},
                {"data": "d4", "insdef": 14},
                {"data": "d5", "insdef": 15},
                {"data": "d6", "insdef": 16},
            ],
        )

        eq_(
            [row._mapping for row in result.returned_defaults_rows],
            [
                {"id": 1, "upddef": None},
                {"id": 2, "upddef": None},
                {"id": 3, "upddef": None},
                {"id": 4, "upddef": None},
                {"id": 5, "upddef": None},
                {"id": 6, "upddef": None},
            ],
        )

        eq_(
            result.inserted_primary_key_rows,
            [(1,), (2,), (3,), (4,), (5,), (6,)],
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This statement was an executemany call; "
            "if return defaults is supported",
            lambda: result.returned_defaults,
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This statement was an executemany call; "
            "if primary key returning is supported",
            lambda: result.inserted_primary_key,
        )

    @testing.requires.insert_executemany_returning
    def test_insert_executemany_only_pk_passed(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().return_defaults(),
            [
                {"id": 10, "data": "d1"},
                {"id": 11, "data": "d2"},
                {"id": 12, "data": "d3"},
                {"id": 13, "data": "d4"},
                {"id": 14, "data": "d5"},
                {"id": 15, "data": "d6"},
            ],
        )

        eq_(
            [row._mapping for row in result.returned_defaults_rows],
            [
                {"insdef": 0, "upddef": None},
                {"insdef": 0, "upddef": None},
                {"insdef": 0, "upddef": None},
                {"insdef": 0, "upddef": None},
                {"insdef": 0, "upddef": None},
                {"insdef": 0, "upddef": None},
            ],
        )
        eq_(
            result.inserted_primary_key_rows,
            [(10,), (11,), (12,), (13,), (14,), (15,)],
        )


class ImplicitReturningFlag(fixtures.TestBase):
    __backend__ = True

    def test_flag_turned_off(self):
        e = engines.testing_engine(options={"implicit_returning": False})
        assert e.dialect.implicit_returning is False
        c = e.connect()
        c.close()
        assert e.dialect.implicit_returning is False

    def test_flag_turned_on(self):
        e = engines.testing_engine(options={"implicit_returning": True})
        assert e.dialect.implicit_returning is True
        c = e.connect()
        c.close()
        assert e.dialect.implicit_returning is True

    def test_flag_turned_default(self):
        supports = [False]

        def go():
            supports[0] = True

        testing.requires.returning(go)()
        e = engines.testing_engine()

        # version detection on connect sets it
        c = e.connect()
        c.close()
        assert e.dialect.implicit_returning is supports[0]
