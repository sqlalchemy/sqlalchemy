import itertools

from sqlalchemy import Boolean
from sqlalchemy import column
from sqlalchemy import delete
from sqlalchemy import exc as sa_exc
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import type_coerce
from sqlalchemy import update
from sqlalchemy.sql import crud
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import provision
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

        stmt = stmt.return_defaults()

        assert_raises_message(
            sa_exc.CompileError,
            r"Can't compile statement that includes returning\(\) "
            r"and return_defaults\(\) simultaneously",
            stmt.compile,
        )

    @testing.combinations("return_defaults", "returning", argnames="methname")
    @testing.combinations(insert, update, delete, argnames="construct")
    def test_sort_by_parameter_ordering_param(
        self, methname, construct, table_fixture
    ):
        t = table_fixture

        stmt = construct(t)

        if construct is insert:
            is_false(stmt._sort_by_parameter_order)

        meth = getattr(stmt, methname)

        if construct in (update, delete):
            with expect_raises_message(
                sa_exc.ArgumentError,
                rf"The 'sort_by_parameter_order' argument to "
                rf"{methname}\(\) only applies to INSERT statements",
            ):
                meth(t.c.id, sort_by_parameter_order=True)
        else:
            new = meth(t.c.id, sort_by_parameter_order=True)
            is_true(new._sort_by_parameter_order)

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

    def test_named_expressions_selected_columns(self, table_fixture):
        table = table_fixture
        stmt = (
            table.insert()
            .values(goofy="someOTHERgoofy")
            .returning(func.lower(table.c.x).label("goof"))
        )
        self.assert_compile(
            select(stmt.exported_columns.goof),
            "SELECT lower(foo.x) AS goof FROM foo",
        )

    def test_anon_expressions_selected_columns(self, table_fixture):
        table = table_fixture
        stmt = (
            table.insert()
            .values(goofy="someOTHERgoofy")
            .returning(func.lower(table.c.x))
        )
        self.assert_compile(
            select(stmt.exported_columns[0]),
            "SELECT lower(foo.x) AS lower_1 FROM foo",
        )

    def test_returning_cte_labeled_expression(self, table_fixture):
        t = table_fixture

        stmt = delete(t).returning(
            t.c.id,
            (t.c.id * -1).label("negative_id")
        ).cte()

        eq_(list(stmt.c.keys()), ["id", "negative_id"])
        eq_(stmt.c.negative_id.name, "negative_id")

    def test_returning_cte_multiple_unlabeled_expressions(self, table_fixture):
        t = table_fixture

        stmt = delete(t).returning(
            t.c.id,
            t.c.id * -1,
            t.c.id + 10,
            t.c.id - 10,
            -1 * t.c.id
        ).cte()

        assert stmt.c.id is not None
        assert all(col is not None for col in stmt.c)


class InsertReturningTest(fixtures.TablesTest, AssertsExecutionResults):
    __requires__ = ("insert_returning",)
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        class GoofyType(TypeDecorator):
            impl = String
            cache_ok = True

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
            "returning_tbl",
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
        table = self.tables.returning_tbl
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

    def test_labeling(self, connection):
        table = self.tables.returning_tbl
        result = connection.execute(
            table.insert()
            .values(persons=6)
            .returning(table.c.persons.label("lala"))
        )
        row = result.first()._mapping
        assert row["lala"] == 6

    def test_anon_expressions(self, connection):
        table = self.tables.returning_tbl
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

    @testing.combinations(
        (lambda table: (table.c.strval + "hi",), ("str1hi",)),
        (
            lambda table: (
                table.c.persons,
                table.c.full,
                table.c.strval + "hi",
            ),
            (
                5,
                False,
                "str1hi",
            ),
        ),
        (
            lambda table: (
                table.c.persons,
                table.c.strval + "hi",
                table.c.full,
            ),
            (5, "str1hi", False),
        ),
        (
            lambda table: (
                table.c.strval + "hi",
                table.c.persons,
                table.c.full,
            ),
            ("str1hi", 5, False),
        ),
        argnames="testcase, expected_row",
    )
    def test_insert_returning_w_expression(
        self, connection, testcase, expected_row
    ):
        table = self.tables.returning_tbl

        exprs = testing.resolve_lambda(testcase, table=table)

        result = connection.execute(
            table.insert().returning(*exprs),
            {"persons": 5, "full": False, "strval": "str1"},
        )

        eq_(result.fetchall(), [expected_row])

        result2 = connection.execute(
            select(table.c.id, table.c.strval).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, "str1")])

    def test_insert_explicit_pk_col(self, connection):
        table = self.tables.returning_tbl
        result = connection.execute(
            table.insert().returning(table.c.id, table.c.strval),
            {"id": 1, "strval": "str1"},
        )

        eq_(
            result.fetchall(),
            [
                (
                    1,
                    "str1",
                )
            ],
        )

    def test_insert_returning_w_type_coerce_expression(self, connection):
        table = self.tables.returning_tbl
        result = connection.execute(
            table.insert().returning(type_coerce(table.c.goofy, String)),
            {"persons": 5, "goofy": "somegoofy"},
        )

        eq_(result.fetchall(), [("FOOsomegoofy",)])

        result2 = connection.execute(
            select(table.c.id, table.c.goofy).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, "FOOsomegoofyBAR")])

    def test_no_ipk_on_returning(self, connection, close_result_when_finished):
        table = self.tables.returning_tbl
        result = connection.execute(
            table.insert().returning(table.c.id), {"persons": 1, "full": False}
        )
        close_result_when_finished(result)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"Can't call inserted_primary_key when returning\(\) is used.",
            getattr,
            result,
            "inserted_primary_key",
        )

    def test_insert_returning(self, connection):
        table = self.tables.returning_tbl
        result = connection.execute(
            table.insert().returning(table.c.id), {"persons": 1, "full": False}
        )

        eq_(result.fetchall(), [(1,)])

    @testing.requires.multivalues_inserts
    def test_multivalues_insert_returning(self, connection):
        table = self.tables.returning_tbl
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

    @testing.fixture
    def column_expression_fixture(self, metadata, connection):
        class MyString(TypeDecorator):
            cache_ok = True
            impl = String(50)

            def column_expression(self, column):
                return func.lower(column)

        t1 = Table(
            "some_table",
            metadata,
            Column("name", String(50)),
            Column("value", MyString(50)),
        )
        metadata.create_all(connection)
        return t1

    @testing.combinations("columns", "table", argnames="use_columns")
    def test_plain_returning_column_expression(
        self, column_expression_fixture, use_columns, connection
    ):
        """test #8770"""
        table1 = column_expression_fixture

        if use_columns == "columns":
            stmt = (
                insert(table1)
                .values(name="n1", value="ValUE1")
                .returning(table1)
            )
        else:
            stmt = (
                insert(table1)
                .values(name="n1", value="ValUE1")
                .returning(table1.c.name, table1.c.value)
            )

        result = connection.execute(stmt)
        row = result.first()

        eq_(row._mapping["name"], "n1")
        eq_(row._mapping["value"], "value1")

    @testing.fails_on_everything_except(
        "postgresql", "mariadb>=10.5", "sqlite>=3.34"
    )
    def test_literal_returning(self, connection):
        if testing.against("mariadb"):
            quote = "`"
        else:
            quote = '"'
        if testing.against("postgresql"):
            literal_true = "true"
        else:
            literal_true = "1"

        result4 = connection.exec_driver_sql(
            "insert into returning_tbl (id, persons, %sfull%s) "
            "values (5, 10, %s) returning persons"
            % (quote, quote, literal_true)
        )
        eq_([dict(row._mapping) for row in result4], [{"persons": 10}])


class UpdateReturningTest(fixtures.TablesTest, AssertsExecutionResults):
    __requires__ = ("update_returning",)
    __backend__ = True

    run_create_tables = "each"

    define_tables = InsertReturningTest.define_tables

    def test_update_returning(self, connection):
        table = self.tables.returning_tbl
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.update()
            .values(dict(full=True))
            .where(table.c.persons > 4)
            .returning(table.c.id)
        )
        eq_(result.fetchall(), [(1,)])

        result2 = connection.execute(
            select(table.c.id, table.c.full).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, True), (2, False)])

    def test_update_returning_w_expression_one(self, connection):
        table = self.tables.returning_tbl
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
        table = self.tables.returning_tbl
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

    def test_update_full_returning(self, connection):
        table = self.tables.returning_tbl
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.update()
            .where(table.c.persons > 2)
            .values(full=True)
            .returning(table.c.id, table.c.full)
        )
        eq_(result.fetchall(), [(1, True), (2, True)])


class DeleteReturningTest(fixtures.TablesTest, AssertsExecutionResults):
    __requires__ = ("delete_returning",)
    __backend__ = True

    run_create_tables = "each"

    define_tables = InsertReturningTest.define_tables

    def test_delete_returning(self, connection):
        table = self.tables.returning_tbl
        connection.execute(
            table.insert(),
            [{"persons": 5, "full": False}, {"persons": 3, "full": False}],
        )

        result = connection.execute(
            table.delete().where(table.c.persons > 4).returning(table.c.id)
        )
        eq_(result.fetchall(), [(1,)])

        result2 = connection.execute(
            select(table.c.id, table.c.full).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(2, False)])


class CompositeStatementTest(fixtures.TestBase):
    __requires__ = ("insert_returning",)
    __backend__ = True

    @testing.provide_metadata
    def test_select_doesnt_pollute_result(self, connection):
        class MyType(TypeDecorator):
            impl = Integer
            cache_ok = True

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
    __requires__ = "insert_returning", "sequences"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        seq = provision.normalize_sequence(config, Sequence("tid_seq"))
        Table(
            "returning_tbl",
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
        table = self.tables.returning_tbl
        r = connection.execute(
            table.insert().values(data="hi").returning(table.c.id)
        )
        eq_(r.first(), tuple([testing.db.dialect.default_sequence_base]))
        eq_(
            connection.scalar(self.sequences.tid_seq),
            testing.db.dialect.default_sequence_base + 1,
        )


class KeyReturningTest(fixtures.TablesTest, AssertsExecutionResults):
    """test returning() works with columns that define 'key'."""

    __requires__ = ("insert_returning",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "returning_tbl",
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

    @testing.exclude("postgresql", "<", (8, 2), "8.2+ feature")
    def test_insert(self, connection):
        table = self.tables.returning_tbl
        result = connection.execute(
            table.insert().returning(table.c.foo_id), dict(data="somedata")
        )
        row = result.first()._mapping
        assert row[table.c.foo_id] == row["id"] == 1

        result = connection.execute(table.select()).first()._mapping
        assert row[table.c.foo_id] == row["id"] == 1


class InsertReturnDefaultsTest(fixtures.TablesTest):
    __requires__ = ("insert_returning",)
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

        Table(
            "table_no_addtl_defaults",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

        class MyType(TypeDecorator):
            impl = String(50)

            def process_result_value(self, value, dialect):
                return f"PROCESSED! {value}"

        Table(
            "table_datatype_has_result_proc",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", MyType()),
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

    def test_insert_w_defaults_supplemental_cols(self, connection):
        t1 = self.tables.t1
        result = connection.execute(
            t1.insert().return_defaults(supplemental_cols=[t1.c.id]),
            {"data": "d1"},
        )
        eq_(result.all(), [(1, 0, None)])

    def test_insert_w_no_defaults_supplemental_cols(self, connection):
        t1 = self.tables.table_no_addtl_defaults
        result = connection.execute(
            t1.insert().return_defaults(supplemental_cols=[t1.c.id]),
            {"data": "d1"},
        )
        eq_(result.all(), [(1,)])

    def test_insert_w_defaults_supplemental_processor_cols(self, connection):
        """test that the cursor._rewind() used by supplemental RETURNING
        clears out result-row processors as we will have already processed
        the rows.

        """

        t1 = self.tables.table_datatype_has_result_proc
        result = connection.execute(
            t1.insert().return_defaults(
                supplemental_cols=[t1.c.id, t1.c.data]
            ),
            {"data": "d1"},
        )
        eq_(result.all(), [(1, "PROCESSED! d1")])


class UpdatedReturnDefaultsTest(fixtures.TablesTest):
    __requires__ = ("update_returning",)
    run_define_tables = "each"
    __backend__ = True

    define_tables = InsertReturnDefaultsTest.define_tables

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

    def test_update_values_col_is_excluded(self, connection):
        """columns that are in values() are not returned"""
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))

        result = connection.execute(
            t1.update().values(data="x", upddef=2).return_defaults(t1.c.data)
        )
        is_(result.returned_defaults, None)

        result = connection.execute(
            t1.update()
            .values(data="x", upddef=2)
            .return_defaults(t1.c.data, t1.c.id)
        )
        eq_(result.returned_defaults, (1,))

    def test_update_supplemental_cols(self, connection):
        """with supplemental_cols, we can get back arbitrary cols."""

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update()
            .values(data="x", insdef=3)
            .return_defaults(supplemental_cols=[t1.c.data, t1.c.insdef])
        )

        row = result.returned_defaults

        # row has all the cols in it
        eq_(row, ("x", 3, 1))
        eq_(row._mapping[t1.c.upddef], 1)
        eq_(row._mapping[t1.c.insdef], 3)

        # result is rewound
        # but has both return_defaults + supplemental_cols
        eq_(result.all(), [("x", 3, 1)])

    def test_update_expl_return_defaults_plus_supplemental_cols(
        self, connection
    ):
        """with supplemental_cols, we can get back arbitrary cols."""

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update()
            .values(data="x", insdef=3)
            .return_defaults(
                t1.c.id, supplemental_cols=[t1.c.data, t1.c.insdef]
            )
        )

        row = result.returned_defaults

        # row has all the cols in it
        eq_(row, (1, "x", 3))
        eq_(row._mapping[t1.c.id], 1)
        eq_(row._mapping[t1.c.insdef], 3)
        assert t1.c.upddef not in row._mapping

        # result is rewound
        # but has both return_defaults + supplemental_cols
        eq_(result.all(), [(1, "x", 3)])

    def test_update_sql_expr(self, connection):
        from sqlalchemy import literal

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().values(upddef=literal(10) + 5).return_defaults()
        )

        eq_(result.returned_defaults._mapping, {"upddef": 15})

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

    def test_update_all(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.update().values(insdef=2).return_defaults()
        )
        eq_(dict(result.returned_defaults._mapping), {"upddef": 1})


class DeleteReturnDefaultsTest(fixtures.TablesTest):
    __requires__ = ("delete_returning",)
    run_define_tables = "each"
    __backend__ = True

    define_tables = InsertReturnDefaultsTest.define_tables

    def test_delete(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(t1.delete().return_defaults(t1.c.upddef))
        eq_(
            [result.returned_defaults._mapping[k] for k in (t1.c.upddef,)], [1]
        )

    def test_delete_empty_return_defaults(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=5))
        result = connection.execute(t1.delete().return_defaults())

        # there's no "delete" default, so we get None.  we have to
        # ask for them in all cases
        eq_(result.returned_defaults, None)

    def test_delete_non_default(self, connection):
        """test that a column not marked at all as a
        default works with this feature."""

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(t1.delete().return_defaults(t1.c.data))
        eq_(
            [result.returned_defaults._mapping[k] for k in (t1.c.data,)],
            [None],
        )

    def test_delete_non_default_plus_default(self, connection):
        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.delete().return_defaults(t1.c.data, t1.c.upddef)
        )
        eq_(
            dict(result.returned_defaults._mapping),
            {"data": None, "upddef": 1},
        )

    def test_delete_supplemental_cols(self, connection):
        """with supplemental_cols, we can get back arbitrary cols."""

        t1 = self.tables.t1
        connection.execute(t1.insert().values(upddef=1))
        result = connection.execute(
            t1.delete().return_defaults(
                t1.c.id, supplemental_cols=[t1.c.data, t1.c.insdef]
            )
        )

        row = result.returned_defaults

        # row has all the cols in it
        eq_(row, (1, None, 0))
        eq_(row._mapping[t1.c.insdef], 0)

        # result is rewound
        # but has both return_defaults + supplemental_cols
        eq_(result.all(), [(1, None, 0)])


class InsertManyReturnDefaultsTest(fixtures.TablesTest):
    __requires__ = ("insert_executemany_returning",)
    run_define_tables = "each"
    __backend__ = True

    define_tables = InsertReturnDefaultsTest.define_tables

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

        if connection.dialect.insert_null_pk_still_autoincrements:
            eq_(
                [row._mapping for row in result.returned_defaults_rows],
                [
                    {"id": 10, "insdef": 0, "upddef": None},
                    {"id": 11, "insdef": 0, "upddef": None},
                    {"id": 12, "insdef": 0, "upddef": None},
                    {"id": 13, "insdef": 0, "upddef": None},
                    {"id": 14, "insdef": 0, "upddef": None},
                    {"id": 15, "insdef": 0, "upddef": None},
                ],
            )
        else:
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


class InsertManyReturningTest(fixtures.TablesTest):
    __requires__ = ("insert_executemany_returning",)
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
            "default_cases",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("insdef", Integer, default=IncDefault()),
            Column("upddef", Integer, onupdate=IncDefault()),
        )

        class GoofyType(TypeDecorator):
            impl = String
            cache_ok = True

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
            "type_cases",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("persons", Integer),
            Column("full", Boolean),
            Column("goofy", GoofyType(50)),
            Column("strval", String(50)),
        )

        Table(
            "no_implicit_returning",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            implicit_returning=False,
        )

    @testing.combinations(
        (
            lambda table: (table.c.strval + "hi",),
            [("str1hi",), ("str2hi",), ("str3hi",)],
        ),
        (
            lambda table: (
                table.c.persons,
                table.c.full,
                table.c.strval + "hi",
            ),
            [
                (5, False, "str1hi"),
                (6, True, "str2hi"),
                (7, False, "str3hi"),
            ],
        ),
        (
            lambda table: (
                table.c.persons,
                table.c.strval + "hi",
                table.c.full,
            ),
            [
                (5, "str1hi", False),
                (6, "str2hi", True),
                (7, "str3hi", False),
            ],
        ),
        (
            lambda table: (
                table.c.strval + "hi",
                table.c.persons,
                table.c.full,
            ),
            [
                ("str1hi", 5, False),
                ("str2hi", 6, True),
                ("str3hi", 7, False),
            ],
        ),
        argnames="testcase, expected_rows",
    )
    def test_insert_returning_w_expression(
        self, connection, testcase, expected_rows
    ):
        table = self.tables.type_cases

        exprs = testing.resolve_lambda(testcase, table=table)
        result = connection.execute(
            table.insert().returning(*exprs),
            [
                {"persons": 5, "full": False, "strval": "str1"},
                {"persons": 6, "full": True, "strval": "str2"},
                {"persons": 7, "full": False, "strval": "str3"},
            ],
        )

        eq_(result.fetchall(), expected_rows)

        result2 = connection.execute(
            select(table.c.id, table.c.strval).order_by(table.c.id)
        )
        eq_(result2.fetchall(), [(1, "str1"), (2, "str2"), (3, "str3")])

    @testing.fails_if(
        # Oracle has native executemany() + returning and does not use
        # insertmanyvalues to achieve this.  so test that for
        # that particular dialect, the exception expected is not raised
        # in the case that the compiler vetoed insertmanyvalues (
        # since Oracle's compiler will always veto it)
        lambda config: not config.db.dialect.use_insertmanyvalues
    )
    def test_iie_supported_but_not_this_statement(self, connection):
        """test the case where INSERT..RETURNING w/ executemany is used,
        the dialect requires use_insertmanyreturning, but
        the compiler vetoed the use of insertmanyvalues."""

        t1 = self.tables.type_cases

        grm = crud._get_returning_modifiers

        def _grm(*arg, **kw):
            (
                need_pks,
                implicit_returning,
                implicit_return_defaults,
                postfetch_lastrowid,
                _,
                _,
            ) = grm(*arg, **kw)

            return (
                need_pks,
                implicit_returning,
                implicit_return_defaults,
                postfetch_lastrowid,
                False,
                None,
            )

        with mock.patch.object(
            crud,
            "_get_returning_modifiers",
            new=_grm,
        ):
            with expect_raises_message(
                sa_exc.StatementError,
                r'Statement does not have "insertmanyvalues" enabled, '
                r"can\'t use INSERT..RETURNING with executemany in this "
                "case.",
            ):
                connection.execute(
                    t1.insert().returning(t1.c.id, t1.c.goofy, t1.c.full),
                    [
                        {"persons": 5, "full": True},
                        {"persons": 6, "full": True},
                        {"persons": 7, "full": False},
                    ],
                )

    @testing.variation(
        "style",
        ["no_cols", "cols", "cols_plus_supplemental", "normal_returning"],
    )
    def test_no_executemany_w_no_implicit_returning(self, connection, style):
        """test a refinement made during fixes for #10453;
        return_defaults() with 'supplemental_cols' is considered to be an
        explicit returning case, bypassing the implicit_returning parameter.

        """
        t1 = self.tables.no_implicit_returning

        if style.cols_plus_supplemental:
            result = connection.execute(
                t1.insert().return_defaults(
                    t1.c.id, supplemental_cols=[t1.c.data]
                ),
                [
                    {"data": "d1"},
                    {"data": "d2"},
                    {"data": "d3"},
                ],
            )
            eq_(result.scalars().all(), ["d1", "d2", "d3"])
        elif style.normal_returning:
            result = connection.execute(
                t1.insert().returning(t1.c.data),
                [
                    {"data": "d1"},
                    {"data": "d2"},
                    {"data": "d3"},
                ],
            )
            eq_(result.scalars().all(), ["d1", "d2", "d3"])
        elif style.cols:
            result = connection.execute(
                t1.insert().return_defaults(t1.c.id),
                [
                    {"data": "d1"},
                    {"data": "d2"},
                    {"data": "d3"},
                ],
            )
            assert not result.returns_rows
        elif style.no_cols:
            result = connection.execute(
                t1.insert().return_defaults(t1.c.id),
                [
                    {"data": "d1"},
                    {"data": "d2"},
                    {"data": "d3"},
                ],
            )
            assert not result.returns_rows

    def test_insert_executemany_type_test(self, connection):
        t1 = self.tables.type_cases
        result = connection.execute(
            t1.insert().returning(t1.c.id, t1.c.goofy, t1.c.full),
            [
                {"persons": 5, "full": True, "goofy": "row1", "strval": "s1"},
                {"persons": 6, "full": True, "goofy": "row2", "strval": "s2"},
                {"persons": 7, "full": False, "goofy": "row3", "strval": "s3"},
                {"persons": 8, "full": True, "goofy": "row4", "strval": "s4"},
            ],
        )
        eq_(
            result.mappings().all(),
            [
                {"id": 1, "goofy": "FOOrow1BAR", "full": True},
                {"id": 2, "goofy": "FOOrow2BAR", "full": True},
                {"id": 3, "goofy": "FOOrow3BAR", "full": False},
                {"id": 4, "goofy": "FOOrow4BAR", "full": True},
            ],
        )

    def test_insert_executemany_default_generators(self, connection):
        t1 = self.tables.default_cases
        result = connection.execute(
            t1.insert().returning(t1.c.id, t1.c.insdef, t1.c.upddef),
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
            result.mappings().all(),
            [
                {"id": 1, "insdef": 0, "upddef": None},
                {"id": 2, "insdef": 0, "upddef": None},
                {"id": 3, "insdef": 0, "upddef": None},
                {"id": 4, "insdef": 0, "upddef": None},
                {"id": 5, "insdef": 0, "upddef": None},
                {"id": 6, "insdef": 0, "upddef": None},
            ],
        )

    @testing.combinations(True, False, argnames="update_cols")
    @testing.requires.provisioned_upsert
    def test_upsert_data_w_defaults(self, connection, update_cols):
        t1 = self.tables.default_cases

        new_rows = connection.execute(
            t1.insert().returning(t1.c.id, t1.c.insdef, t1.c.data),
            [
                {"data": "d1"},
                {"data": "d2"},
                {"data": "d3"},
                {"data": "d4"},
                {"data": "d5"},
                {"data": "d6"},
            ],
        ).all()

        eq_(
            new_rows,
            [
                (1, 0, "d1"),
                (2, 0, "d2"),
                (3, 0, "d3"),
                (4, 0, "d4"),
                (5, 0, "d5"),
                (6, 0, "d6"),
            ],
        )

        stmt = provision.upsert(
            config,
            t1,
            (t1.c.id, t1.c.insdef, t1.c.data),
            set_lambda=(
                (lambda excluded: {"data": excluded.data + " excluded"})
                if update_cols
                else None
            ),
        )

        upserted_rows = connection.execute(
            stmt,
            [
                {"id": 1, "data": "d1 upserted"},
                {"id": 4, "data": "d4 upserted"},
                {"id": 5, "data": "d5 upserted"},
                {"id": 7, "data": "d7 upserted"},
                {"id": 8, "data": "d8 upserted"},
                {"id": 9, "data": "d9 upserted"},
            ],
        ).all()

        if update_cols:
            eq_(
                upserted_rows,
                [
                    (1, 0, "d1 upserted excluded"),
                    (4, 0, "d4 upserted excluded"),
                    (5, 0, "d5 upserted excluded"),
                    (7, 1, "d7 upserted"),
                    (8, 1, "d8 upserted"),
                    (9, 1, "d9 upserted"),
                ],
            )
        else:
            if testing.against("sqlite", "postgresql"):
                eq_(
                    upserted_rows,
                    [
                        (7, 1, "d7 upserted"),
                        (8, 1, "d8 upserted"),
                        (9, 1, "d9 upserted"),
                    ],
                )
            elif testing.against("mariadb"):
                # mariadb does not seem to have an "empty" upsert,
                # so the provision.upsert() sets table.c.id to itself.
                # this means we get all the rows back
                eq_(
                    upserted_rows,
                    [
                        (1, 0, "d1"),
                        (4, 0, "d4"),
                        (5, 0, "d5"),
                        (7, 1, "d7 upserted"),
                        (8, 1, "d8 upserted"),
                        (9, 1, "d9 upserted"),
                    ],
                )

        resulting_data = connection.execute(
            t1.select().order_by(t1.c.id)
        ).all()

        if update_cols:
            eq_(
                resulting_data,
                [
                    (1, "d1 upserted excluded", 0, None),
                    (2, "d2", 0, None),
                    (3, "d3", 0, None),
                    (4, "d4 upserted excluded", 0, None),
                    (5, "d5 upserted excluded", 0, None),
                    (6, "d6", 0, None),
                    (7, "d7 upserted", 1, None),
                    (8, "d8 upserted", 1, None),
                    (9, "d9 upserted", 1, None),
                ],
            )
        else:
            eq_(
                resulting_data,
                [
                    (1, "d1", 0, None),
                    (2, "d2", 0, None),
                    (3, "d3", 0, None),
                    (4, "d4", 0, None),
                    (5, "d5", 0, None),
                    (6, "d6", 0, None),
                    (7, "d7 upserted", 1, None),
                    (8, "d8 upserted", 1, None),
                    (9, "d9 upserted", 1, None),
                ],
            )
