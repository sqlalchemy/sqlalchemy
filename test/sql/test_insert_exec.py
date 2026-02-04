import contextlib
import functools
import itertools
import uuid

from sqlalchemy import and_
from sqlalchemy import ARRAY
from sqlalchemy import bindparam
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import insert
from sqlalchemy import insert_sentinel
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import TypeDecorator
from sqlalchemy import Uuid
from sqlalchemy import VARCHAR
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.sql.compiler import InsertmanyvaluesSentinelOpts
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing import provision
from sqlalchemy.testing.fixtures import insertmanyvalues_fixture
from sqlalchemy.testing.provision import normalize_sequence
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class ExpectExpr:
    def __init__(self, element):
        self.element = element

    def __clause_element__(self):
        return self.element


class InsertExecTest(fixtures.TablesTest):
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    @testing.requires.multivalues_inserts
    @testing.combinations("string", "column", "expect", argnames="keytype")
    def test_multivalues_insert(self, connection, keytype):
        users = self.tables.users

        if keytype == "string":
            user_id, user_name = "user_id", "user_name"
        elif keytype == "column":
            user_id, user_name = users.c.user_id, users.c.user_name
        elif keytype == "expect":
            user_id, user_name = ExpectExpr(users.c.user_id), ExpectExpr(
                users.c.user_name
            )
        else:
            assert False

        connection.execute(
            users.insert().values(
                [
                    {user_id: 7, user_name: "jack"},
                    {user_id: 8, user_name: "ed"},
                ]
            )
        )
        rows = connection.execute(
            users.select().order_by(users.c.user_id)
        ).all()
        eq_(rows[0], (7, "jack"))
        eq_(rows[1], (8, "ed"))
        connection.execute(users.insert().values([(9, "jack"), (10, "ed")]))
        rows = connection.execute(
            users.select().order_by(users.c.user_id)
        ).all()
        eq_(rows[2], (9, "jack"))
        eq_(rows[3], (10, "ed"))

    def test_insert_heterogeneous_params(self, connection):
        """test that executemany parameters are asserted to match the
        parameter set of the first."""
        users = self.tables.users

        assert_raises_message(
            exc.StatementError,
            r"\(sqlalchemy.exc.InvalidRequestError\) A value is required for "
            "bind parameter 'user_name', in "
            "parameter group 2\n"
            r"\[SQL: u?INSERT INTO users",
            connection.execute,
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9},
            ],
        )

        # this succeeds however.   We aren't yet doing
        # a length check on all subsequent parameters.
        connection.execute(
            users.insert(),
            [
                {"user_id": 7},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9},
            ],
        )

    def _test_lastrow_accessor(self, connection, table_, values, assertvalues):
        """Tests the inserted_primary_key and lastrow_has_id() functions."""

        def insert_values(table_, values):
            """
            Inserts a row into a table, returns the full list of values
            INSERTed including defaults that fired off on the DB side and
            detects rows that had defaults and post-fetches.
            """

            # verify implicit_returning is working
            if (
                connection.dialect.insert_returning
                and table_.implicit_returning
                and not connection.dialect.postfetch_lastrowid
            ):
                ins = table_.insert()
                comp = ins.compile(connection, column_keys=list(values))
                if not set(values).issuperset(
                    c.key for c in table_.primary_key
                ):
                    is_(bool(comp.returning), True)

            result = connection.execute(table_.insert(), values)
            ret = values.copy()

            ipk = result.inserted_primary_key
            for col, id_ in zip(table_.primary_key, ipk):
                ret[col.key] = id_

            if result.lastrow_has_defaults():
                criterion = and_(
                    *[
                        col == id_
                        for col, id_ in zip(
                            table_.primary_key, result.inserted_primary_key
                        )
                    ]
                )
                row = connection.execute(
                    table_.select().where(criterion)
                ).first()
                for c in table_.c:
                    ret[c.key] = row._mapping[c]
            return ret, ipk

        table_.create(connection, checkfirst=True)
        i, ipk = insert_values(table_, values)
        eq_(i, assertvalues)

        # named tuple tests
        for col in table_.primary_key:
            eq_(getattr(ipk, col.key), assertvalues[col.key])
            eq_(ipk._mapping[col.key], assertvalues[col.key])

        eq_(ipk._fields, tuple([col.key for col in table_.primary_key]))

    @testing.requires.supports_autoincrement_w_composite_pk
    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_one(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t1",
                metadata,
                Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                ),
                Column("foo", String(30), primary_key=True),
                implicit_returning=implicit_returning,
            ),
            {"foo": "hi"},
            {"id": 1, "foo": "hi"},
        )

    @testing.requires.supports_autoincrement_w_composite_pk
    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_two(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t2",
                metadata,
                Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                ),
                Column("foo", String(30), primary_key=True),
                Column("bar", String(30), server_default="hi"),
                implicit_returning=implicit_returning,
            ),
            {"foo": "hi"},
            {"id": 1, "foo": "hi", "bar": "hi"},
        )

    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_three(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t3",
                metadata,
                Column("id", String(40), primary_key=True),
                Column("foo", String(30), primary_key=True),
                Column("bar", String(30)),
                implicit_returning=implicit_returning,
            ),
            {"id": "hi", "foo": "thisisfoo", "bar": "thisisbar"},
            {"id": "hi", "foo": "thisisfoo", "bar": "thisisbar"},
        )

    @testing.requires.sequences
    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_four(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t4",
                metadata,
                Column(
                    "id",
                    Integer,
                    normalize_sequence(
                        config, Sequence("t4_id_seq", optional=True)
                    ),
                    primary_key=True,
                ),
                Column("foo", String(30), primary_key=True),
                Column("bar", String(30), server_default="hi"),
                implicit_returning=implicit_returning,
            ),
            {"foo": "hi", "id": 1},
            {"id": 1, "foo": "hi", "bar": "hi"},
        )

    @testing.requires.sequences
    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_four_a(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t4",
                metadata,
                Column(
                    "id",
                    Integer,
                    normalize_sequence(config, Sequence("t4_id_seq")),
                    primary_key=True,
                ),
                Column("foo", String(30)),
                implicit_returning=implicit_returning,
            ),
            {"foo": "hi"},
            {"id": 1, "foo": "hi"},
        )

    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_five(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t5",
                metadata,
                Column("id", String(10), primary_key=True),
                Column("bar", String(30), server_default="hi"),
                implicit_returning=implicit_returning,
            ),
            {"id": "id1"},
            {"id": "id1", "bar": "hi"},
        )

    @testing.requires.supports_autoincrement_w_composite_pk
    @testing.combinations(
        (True, testing.requires.insert_returning),
        (False,),
        argnames="implicit_returning",
    )
    def test_lastrow_accessor_six(
        self, metadata, connection, implicit_returning
    ):
        self._test_lastrow_accessor(
            connection,
            Table(
                "t6",
                metadata,
                Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                ),
                Column("bar", Integer, primary_key=True),
                implicit_returning=implicit_returning,
            ),
            {"bar": 0},
            {"id": 1, "bar": 0},
        )

    # TODO: why not in the sqlite suite?
    @testing.only_on("sqlite+pysqlite")
    def test_lastrowid_zero(self, metadata, connection):
        from sqlalchemy.dialects import sqlite

        class ExcCtx(sqlite.base.SQLiteExecutionContext):
            def get_lastrowid(self):
                return 0

        t = Table(
            "t",
            self.metadata,
            Column("x", Integer, primary_key=True),
            Column("y", Integer),
            implicit_returning=False,
        )
        t.create(connection)
        with mock.patch.object(
            connection.dialect, "execution_ctx_cls", ExcCtx
        ):
            r = connection.execute(t.insert().values(y=5))
            eq_(r.inserted_primary_key, (0,))

    @testing.requires.supports_autoincrement_w_composite_pk
    def test_misordered_lastrow(self, connection, metadata):
        related = Table(
            "related",
            metadata,
            Column("id", Integer, primary_key=True),
            mysql_engine="MyISAM",
            mariadb_engine="MyISAM",
        )
        t6 = Table(
            "t6",
            metadata,
            Column(
                "manual_id",
                Integer,
                ForeignKey("related.id"),
                primary_key=True,
            ),
            Column(
                "auto_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            mysql_engine="MyISAM",
            mariadb_engine="MyISAM",
        )

        metadata.create_all(connection)
        r = connection.execute(related.insert().values(id=12))
        id_ = r.inserted_primary_key[0]
        eq_(id_, 12)

        r = connection.execute(t6.insert().values(manual_id=id_))
        eq_(r.inserted_primary_key, (12, 1))

    def test_implicit_id_insert_select_columns(self, connection):
        users = self.tables.users
        stmt = users.insert().from_select(
            (users.c.user_id, users.c.user_name),
            users.select().where(users.c.user_id == 20),
        )

        r = connection.execute(stmt)
        eq_(r.inserted_primary_key, (None,))

    def test_implicit_id_insert_select_keys(self, connection):
        users = self.tables.users
        stmt = users.insert().from_select(
            ["user_id", "user_name"],
            users.select().where(users.c.user_id == 20),
        )

        r = connection.execute(stmt)
        eq_(r.inserted_primary_key, (None,))

    @testing.requires.empty_inserts
    @testing.requires.insert_returning
    def test_no_inserted_pk_on_returning(
        self, connection, close_result_when_finished
    ):
        users = self.tables.users
        result = connection.execute(
            users.insert().returning(users.c.user_id, users.c.user_name)
        )
        close_result_when_finished(result)

        assert_raises_message(
            exc.InvalidRequestError,
            r"Can't call inserted_primary_key when returning\(\) is used.",
            getattr,
            result,
            "inserted_primary_key",
        )


class TableInsertTest(fixtures.TablesTest):
    """test for consistent insert behavior across dialects
    regarding the inline() method, values() method, lower-case 't' tables.

    """

    run_create_tables = "each"
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column(
                "id",
                Integer,
                normalize_sequence(config, Sequence("t_id_seq")),
                primary_key=True,
            ),
            Column("data", String(50)),
            Column("x", Integer),
        )

        Table(
            "foo_no_seq",
            metadata,
            # note this will have full AUTO INCREMENT on MariaDB
            # whereas "foo" will not due to sequence support
            Column(
                "id",
                Integer,
                primary_key=True,
            ),
            Column("data", String(50)),
            Column("x", Integer),
        )

    def _fixture(self, types=True):
        if types:
            t = sql.table(
                "foo",
                sql.column("id", Integer),
                sql.column("data", String),
                sql.column("x", Integer),
            )
        else:
            t = sql.table(
                "foo", sql.column("id"), sql.column("data"), sql.column("x")
            )
        return t

    def _test(
        self,
        connection,
        stmt,
        row,
        returning=None,
        inserted_primary_key=False,
        table=None,
        parameters=None,
    ):
        if parameters is not None:
            r = connection.execute(stmt, parameters)
        else:
            r = connection.execute(stmt)

        if returning:
            returned = r.first()
            eq_(returned, returning)
        elif inserted_primary_key is not False:
            eq_(r.inserted_primary_key, inserted_primary_key)

        if table is None:
            table = self.tables.foo

        eq_(connection.execute(table.select()).first(), row)

    def _test_multi(self, connection, stmt, rows, data):
        connection.execute(stmt, rows)
        eq_(
            connection.execute(
                self.tables.foo.select().order_by(self.tables.foo.c.id)
            ).all(),
            data,
        )

    @testing.requires.sequences
    def test_explicit_sequence(self, connection):
        t = self._fixture()
        self._test(
            connection,
            t.insert().values(
                id=func.next_value(
                    normalize_sequence(config, Sequence("t_id_seq"))
                ),
                data="data",
                x=5,
            ),
            (testing.db.dialect.default_sequence_base, "data", 5),
        )

    def test_uppercase(self, connection):
        t = self.tables.foo
        self._test(
            connection,
            t.insert().values(id=1, data="data", x=5),
            (1, "data", 5),
            inserted_primary_key=(1,),
        )

    def test_uppercase_inline(self, connection):
        t = self.tables.foo
        self._test(
            connection,
            t.insert().inline().values(id=1, data="data", x=5),
            (1, "data", 5),
            inserted_primary_key=(1,),
        )

    @testing.crashes(
        "mssql+pyodbc",
        "Pyodbc + SQL Server + Py3K, some decimal handling issue",
    )
    def test_uppercase_inline_implicit(self, connection):
        t = self.tables.foo
        self._test(
            connection,
            t.insert().inline().values(data="data", x=5),
            (1, "data", 5),
            inserted_primary_key=(None,),
        )

    def test_uppercase_implicit(self, connection):
        t = self.tables.foo
        self._test(
            connection,
            t.insert().values(data="data", x=5),
            (testing.db.dialect.default_sequence_base, "data", 5),
            inserted_primary_key=(testing.db.dialect.default_sequence_base,),
        )

    def test_uppercase_direct_params(self, connection):
        t = self.tables.foo
        self._test(
            connection,
            t.insert().values(id=1, data="data", x=5),
            (1, "data", 5),
            inserted_primary_key=(1,),
        )

    @testing.requires.insert_returning
    def test_uppercase_direct_params_returning(self, connection):
        t = self.tables.foo
        self._test(
            connection,
            t.insert().values(id=1, data="data", x=5).returning(t.c.id, t.c.x),
            (1, "data", 5),
            returning=(1, 5),
        )

    @testing.requires.sql_expressions_inserted_as_primary_key
    def test_sql_expr_lastrowid(self, connection):
        # see also test.orm.test_unitofwork.py
        # ClauseAttributesTest.test_insert_pk_expression
        t = self.tables.foo_no_seq
        self._test(
            connection,
            t.insert().values(id=literal(5) + 10, data="data", x=5),
            (15, "data", 5),
            inserted_primary_key=(15,),
            table=self.tables.foo_no_seq,
        )

    def test_direct_params(self, connection):
        t = self._fixture()
        self._test(
            connection,
            t.insert().values(id=1, data="data", x=5),
            (1, "data", 5),
            inserted_primary_key=(),
        )

    @testing.requires.insert_returning
    def test_direct_params_returning(self, connection):
        t = self._fixture()
        self._test(
            connection,
            t.insert().values(id=1, data="data", x=5).returning(t.c.id, t.c.x),
            (testing.db.dialect.default_sequence_base, "data", 5),
            returning=(testing.db.dialect.default_sequence_base, 5),
        )

    # there's a non optional Sequence in the metadata, which if the dialect
    # supports sequences, it means the CREATE TABLE should *not* have
    # autoincrement, so the INSERT below would fail because the "t" fixture
    # does not indicate the Sequence
    @testing.fails_if(testing.requires.sequences)
    @testing.requires.emulated_lastrowid
    def test_implicit_pk(self, connection):
        t = self._fixture()
        self._test(
            connection,
            t.insert().values(data="data", x=5),
            (testing.db.dialect.default_sequence_base, "data", 5),
            inserted_primary_key=(),
        )

    @testing.fails_if(testing.requires.sequences)
    @testing.requires.emulated_lastrowid
    def test_implicit_pk_multi_rows(self, connection):
        t = self._fixture()
        self._test_multi(
            connection,
            t.insert(),
            [
                {"data": "d1", "x": 5},
                {"data": "d2", "x": 6},
                {"data": "d3", "x": 7},
            ],
            [(1, "d1", 5), (2, "d2", 6), (3, "d3", 7)],
        )

    @testing.fails_if(testing.requires.sequences)
    @testing.requires.emulated_lastrowid
    def test_implicit_pk_inline(self, connection):
        t = self._fixture()
        self._test(
            connection,
            t.insert().inline().values(data="data", x=5),
            (testing.db.dialect.default_sequence_base, "data", 5),
            inserted_primary_key=(),
        )

    @testing.requires.database_discards_null_for_autoincrement
    def test_explicit_null_pk_values_db_ignores_it(self, connection):
        """test new use case in #7998"""

        # NOTE: this use case uses cursor.lastrowid on SQLite, MySQL, MariaDB,
        # however when SQLAlchemy 2.0 adds support for RETURNING to SQLite
        # and MariaDB, it should work there as well.

        t = self.tables.foo_no_seq
        self._test(
            connection,
            t.insert().values(id=None, data="data", x=5),
            (testing.db.dialect.default_sequence_base, "data", 5),
            inserted_primary_key=(testing.db.dialect.default_sequence_base,),
            table=t,
        )

    @testing.requires.database_discards_null_for_autoincrement
    def test_explicit_null_pk_params_db_ignores_it(self, connection):
        """test new use case in #7998"""

        # NOTE: this use case uses cursor.lastrowid on SQLite, MySQL, MariaDB,
        # however when SQLAlchemy 2.0 adds support for RETURNING to SQLite
        # and MariaDB, it should work there as well.

        t = self.tables.foo_no_seq
        self._test(
            connection,
            t.insert(),
            (testing.db.dialect.default_sequence_base, "data", 5),
            inserted_primary_key=(testing.db.dialect.default_sequence_base,),
            table=t,
            parameters=dict(id=None, data="data", x=5),
        )


class InsertManyValuesTest(fixtures.RemovesEvents, fixtures.TablesTest):
    __sparse_driver_backend__ = True
    __requires__ = ("insertmanyvalues",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", String(50)),
            Column("y", String(50)),
            Column("z", Integer, server_default="5"),
        )

        Table(
            "Unitéble2",
            metadata,
            Column("méil", Integer, primary_key=True),
            Column("\u6e2c\u8a66", Integer),
        )

        Table(
            "extra_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x_value", String(50)),
            Column("y_value", String(50)),
        )
        Table(
            "uniq_cons",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50), unique=True),
        )

    @testing.variation("use_returning", [True, False])
    def test_returning_integrity_error(self, connection, use_returning):
        """test for #11532"""

        stmt = self.tables.uniq_cons.insert()
        if use_returning:
            stmt = stmt.returning(self.tables.uniq_cons.c.id)

        # pymssql thought it would be funny to use OperationalError for
        # a unique key violation.
        with expect_raises((exc.IntegrityError, exc.OperationalError)):
            connection.execute(
                stmt, [{"data": "the data"}, {"data": "the data"}]
            )

    def test_insert_unicode_keys(self, connection):
        table = self.tables["Unitéble2"]

        stmt = table.insert().returning(table.c["méil"])

        connection.execute(
            stmt,
            [
                {"méil": 1, "\u6e2c\u8a66": 1},
                {"méil": 2, "\u6e2c\u8a66": 2},
                {"méil": 3, "\u6e2c\u8a66": 3},
            ],
        )

        eq_(connection.execute(table.select()).all(), [(1, 1), (2, 2), (3, 3)])

    @testing.variation("preserve_rowcount", [True, False])
    def test_insert_returning_values(self, connection, preserve_rowcount):
        t = self.tables.data

        conn = connection
        page_size = conn.dialect.insertmanyvalues_page_size or 100
        data = [
            {"x": "x%d" % i, "y": "y%d" % i}
            for i in range(1, page_size * 2 + 27)
        ]
        if preserve_rowcount:
            eo = {"preserve_rowcount": True}
        else:
            eo = {}

        result = conn.execute(
            t.insert().returning(t.c.x, t.c.y), data, execution_options=eo
        )

        eq_([tup[0] for tup in result.cursor.description], ["x", "y"])
        eq_(result.keys(), ["x", "y"])
        assert t.c.x in result.keys()
        assert t.c.id not in result.keys()
        assert not result._soft_closed
        assert isinstance(
            result.cursor_strategy,
            _cursor.FullyBufferedCursorFetchStrategy,
        )
        assert not result.closed
        eq_(result.mappings().all(), data)

        assert result._soft_closed
        # assert result.closed
        assert result.cursor is None

        if preserve_rowcount:
            eq_(result.rowcount, len(data))

    def test_insert_returning_preexecute_pk(self, metadata, connection):
        counter = itertools.count(1)

        t = Table(
            "t",
            self.metadata,
            Column(
                "id",
                Integer,
                primary_key=True,
                default=lambda: next(counter),
            ),
            Column("data", Integer),
        )
        metadata.create_all(connection)

        result = connection.execute(
            t.insert().return_defaults(),
            [{"data": 1}, {"data": 2}, {"data": 3}],
        )

        eq_(result.inserted_primary_key_rows, [(1,), (2,), (3,)])

    @testing.requires.ctes_on_dml
    @testing.variation("add_expr_returning", [True, False])
    def test_insert_w_bindparam_in_nested_insert(
        self, connection, add_expr_returning
    ):
        """test related to #9173"""

        data, extra_table = self.tables("data", "extra_table")

        inst = (
            extra_table.insert()
            .values(x_value="x", y_value="y")
            .returning(extra_table.c.id)
            .cte("inst")
        )

        stmt = (
            data.insert()
            .values(x="the x", z=select(inst.c.id).scalar_subquery())
            .add_cte(inst)
        )

        if add_expr_returning:
            stmt = stmt.returning(data.c.id, data.c.y + " returned y")
        else:
            stmt = stmt.returning(data.c.id)

        result = connection.execute(
            stmt,
            [
                {"y": "y1"},
                {"y": "y2"},
                {"y": "y3"},
            ],
        )

        result_rows = result.all()

        ids = [row[0] for row in result_rows]

        extra_row = connection.execute(
            select(extra_table).order_by(extra_table.c.id)
        ).one()
        extra_row_id = extra_row[0]
        eq_(extra_row, (extra_row_id, "x", "y"))
        eq_(
            connection.execute(select(data).order_by(data.c.id)).all(),
            [
                (ids[0], "the x", "y1", extra_row_id),
                (ids[1], "the x", "y2", extra_row_id),
                (ids[2], "the x", "y3", extra_row_id),
            ],
        )

    @testing.requires.provisioned_upsert
    def test_upsert_w_returning(self, connection):
        """test cases that will exercise SQL similar to that of
        test/orm/dml/test_bulk_statements.py

        """

        data = self.tables.data

        initial_data = [
            {"x": "x1", "y": "y1", "z": 4},
            {"x": "x2", "y": "y2", "z": 8},
        ]
        ids = connection.scalars(
            data.insert().returning(data.c.id), initial_data
        ).all()

        upsert_data = [
            {
                "id": ids[0],
                "x": "x1",
                "y": "y1",
            },
            {
                "id": 32,
                "x": "x19",
                "y": "y7",
            },
            {
                "id": ids[1],
                "x": "x5",
                "y": "y6",
            },
            {
                "id": 28,
                "x": "x9",
                "y": "y15",
            },
        ]

        stmt = provision.upsert(
            config,
            data,
            (data,),
            set_lambda=lambda inserted: {"x": inserted.x + " upserted"},
        )

        result = connection.execute(stmt, upsert_data)

        eq_(
            result.all(),
            [
                (ids[0], "x1 upserted", "y1", 4),
                (32, "x19", "y7", 5),
                (ids[1], "x5 upserted", "y2", 8),
                (28, "x9", "y15", 5),
            ],
        )

    @testing.combinations(True, False, argnames="use_returning")
    @testing.combinations(1, 2, argnames="num_embedded_params")
    @testing.combinations(True, False, argnames="use_whereclause")
    @testing.crashes(
        "+mariadbconnector",
        "returning crashes, regular executemany malfunctions",
    )
    def test_insert_w_bindparam_in_subq(
        self, connection, use_returning, num_embedded_params, use_whereclause
    ):
        """test #8639

        see also test_insert_w_bindparam_in_nested_insert

        """

        t = self.tables.data
        extra = self.tables.extra_table

        conn = connection
        connection.execute(
            extra.insert(),
            [
                {"x_value": "p1", "y_value": "yv1"},
                {"x_value": "p2", "y_value": "yv2"},
                {"x_value": "p1_p1", "y_value": "yv3"},
                {"x_value": "p2_p2", "y_value": "yv4"},
            ],
        )

        if num_embedded_params == 1:
            if use_whereclause:
                scalar_subq = select(bindparam("paramname")).scalar_subquery()
                params = [
                    {"paramname": "p1_p1", "y": "y1"},
                    {"paramname": "p2_p2", "y": "y2"},
                ]
            else:
                scalar_subq = (
                    select(extra.c.x_value)
                    .where(extra.c.y_value == bindparam("y_value"))
                    .scalar_subquery()
                )
                params = [
                    {"y_value": "yv3", "y": "y1"},
                    {"y_value": "yv4", "y": "y2"},
                ]

        elif num_embedded_params == 2:
            if use_whereclause:
                scalar_subq = (
                    select(
                        bindparam("paramname1", type_=String) + extra.c.x_value
                    )
                    .where(extra.c.y_value == bindparam("y_value"))
                    .scalar_subquery()
                )
                params = [
                    {"paramname1": "p1_", "y_value": "yv1", "y": "y1"},
                    {"paramname1": "p2_", "y_value": "yv2", "y": "y2"},
                ]
            else:
                scalar_subq = select(
                    bindparam("paramname1", type_=String)
                    + bindparam("paramname2", type_=String)
                ).scalar_subquery()
                params = [
                    {"paramname1": "p1_", "paramname2": "p1", "y": "y1"},
                    {"paramname1": "p2_", "paramname2": "p2", "y": "y2"},
                ]
        else:
            assert False

        stmt = t.insert().values(x=scalar_subq)
        if use_returning:
            stmt = stmt.returning(t.c["x", "y"])

        result = conn.execute(stmt, params)

        if use_returning:
            eq_(result.all(), [("p1_p1", "y1"), ("p2_p2", "y2")])

        result = conn.execute(select(t.c["x", "y"]))

        eq_(result.all(), [("p1_p1", "y1"), ("p2_p2", "y2")])

    @testing.variation("preserve_rowcount", [True, False])
    def test_insert_returning_defaults(self, connection, preserve_rowcount):
        t = self.tables.data

        if preserve_rowcount:
            conn = connection.execution_options(preserve_rowcount=True)
        else:
            conn = connection

        result = conn.execute(t.insert(), {"x": "x0", "y": "y0"})
        first_pk = result.inserted_primary_key[0]

        page_size = conn.dialect.insertmanyvalues_page_size or 100
        total_rows = page_size * 5 + 27
        data = [{"x": "x%d" % i, "y": "y%d" % i} for i in range(1, total_rows)]
        result = conn.execute(t.insert().returning(t.c.id, t.c.z), data)

        eq_(
            result.all(),
            [(pk, 5) for pk in range(1 + first_pk, total_rows + first_pk)],
        )

        if preserve_rowcount:
            eq_(result.rowcount, total_rows - 1)  # range starts from 1

    def test_insert_return_pks_default_values(self, connection):
        """test sending multiple, empty rows into an INSERT and getting primary
        key values back.

        This has to use a format that indicates at least one DEFAULT in
        multiple parameter sets, i.e. "INSERT INTO table (anycol) VALUES
        (DEFAULT) (DEFAULT) (DEFAULT) ... RETURNING col"

        if the database doesn't support this (like SQLite, mssql), it
        actually runs the statement that many times on the cursor.
        This is much less efficient, but is still more efficient than
        how it worked previously where we'd run the statement that many
        times anyway.

        There's ways to make it work for those, such as on SQLite
        we can use "INSERT INTO table (pk_col) VALUES (NULL) RETURNING pk_col",
        but that assumes an autoincrement pk_col, not clear how this
        could be produced generically.

        """
        t = self.tables.data

        conn = connection

        result = conn.execute(t.insert(), {"x": "x0", "y": "y0"})
        first_pk = result.inserted_primary_key[0]

        page_size = conn.dialect.insertmanyvalues_page_size or 100
        total_rows = page_size * 2 + 27
        data = [{} for i in range(1, total_rows)]
        result = conn.execute(t.insert().returning(t.c.id), data)

        eq_(
            result.all(),
            [(pk,) for pk in range(1 + first_pk, total_rows + first_pk)],
        )

    @testing.combinations(None, 100, 329, argnames="batchsize")
    @testing.combinations(
        "engine",
        "conn_execution_option",
        "exec_execution_option",
        "stmt_execution_option",
        argnames="paramtype",
    )
    def test_page_size_adjustment(self, testing_engine, batchsize, paramtype):
        t = self.tables.data

        if paramtype == "engine" and batchsize is not None:
            e = testing_engine(
                options={
                    "insertmanyvalues_page_size": batchsize,
                },
            )

            # sqlite, since this is a new engine, re-create the table
            if not testing.requires.independent_connections.enabled:
                t.create(e, checkfirst=True)
        else:
            e = testing.db

        totalnum = 1275
        data = [{"x": "x%d" % i, "y": "y%d" % i} for i in range(1, totalnum)]

        insert_count = 0

        with e.begin() as conn:

            @event.listens_for(conn, "before_cursor_execute")
            def go(conn, cursor, statement, parameters, context, executemany):
                nonlocal insert_count
                if statement.startswith("INSERT"):
                    insert_count += 1

            stmt = t.insert()
            if batchsize is None or paramtype == "engine":
                conn.execute(stmt.returning(t.c.id), data)
            elif paramtype == "conn_execution_option":
                conn = conn.execution_options(
                    insertmanyvalues_page_size=batchsize
                )
                conn.execute(stmt.returning(t.c.id), data)
            elif paramtype == "stmt_execution_option":
                stmt = stmt.execution_options(
                    insertmanyvalues_page_size=batchsize
                )
                conn.execute(stmt.returning(t.c.id), data)
            elif paramtype == "exec_execution_option":
                conn.execute(
                    stmt.returning(t.c.id),
                    data,
                    execution_options=dict(
                        insertmanyvalues_page_size=batchsize
                    ),
                )
            else:
                assert False

        assert_batchsize = batchsize or 1000
        eq_(
            insert_count,
            totalnum // assert_batchsize
            + (1 if totalnum % assert_batchsize else 0),
        )

    def test_disabled(self, testing_engine):
        e = testing_engine(
            options={"use_insertmanyvalues": False, "sqlite_share_pool": True},
        )
        totalnum = 1275
        data = [{"x": "x%d" % i, "y": "y%d" % i} for i in range(1, totalnum)]

        t = self.tables.data

        with e.begin() as conn:
            stmt = t.insert()
            with expect_raises_message(
                exc.StatementError,
                "with current server capabilities does not support "
                "INSERT..RETURNING when executemany",
            ):
                conn.execute(stmt.returning(t.c.id), data)


class IMVSentinelTest(fixtures.TestBase):
    __sparse_driver_backend__ = True

    __requires__ = ("insert_returning",)

    def _expect_downgrade_warnings(
        self,
        *,
        warn_for_downgrades,
        sort_by_parameter_order,
        separate_sentinel=False,
        server_autoincrement=False,
        client_side_pk=False,
        autoincrement_is_sequence=False,
        connection=None,
        expect_warnings_override=None,
    ):
        if connection:
            dialect = connection.dialect
        else:
            dialect = testing.db.dialect

        if (
            expect_warnings_override is not False
            and sort_by_parameter_order
            and warn_for_downgrades
            and dialect.use_insertmanyvalues
        ):
            if (
                not separate_sentinel
                and (
                    server_autoincrement
                    and (
                        not (
                            dialect.insertmanyvalues_implicit_sentinel  # noqa: E501
                            & InsertmanyvaluesSentinelOpts.ANY_AUTOINCREMENT
                        )
                        or (
                            autoincrement_is_sequence
                            and not (
                                dialect.insertmanyvalues_implicit_sentinel  # noqa: E501
                                & InsertmanyvaluesSentinelOpts.SEQUENCE
                            )
                        )
                    )
                )
                or (
                    not separate_sentinel
                    and not server_autoincrement
                    and not client_side_pk
                )
                or (expect_warnings_override is True)
            ):
                return expect_warnings(
                    "Batches were downgraded",
                )

        return contextlib.nullcontext()

    @testing.variation
    def sort_by_parameter_order(self):
        return [True, False]

    @testing.variation
    def warn_for_downgrades(self):
        return [True, False]

    @testing.variation
    def randomize_returning(self):
        return [True, False]

    @testing.requires.insertmanyvalues
    def test_fixture_randomizing(self, connection, metadata):
        t = Table(
            "t",
            metadata,
            Column("id", Integer, Identity(), primary_key=True),
            Column("data", String(50)),
        )
        metadata.create_all(connection)

        insertmanyvalues_fixture(connection, randomize_rows=True)

        results = set()

        for i in range(15):
            result = connection.execute(
                insert(t).returning(t.c.data, sort_by_parameter_order=False),
                [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}],
            )

            hashed_result = tuple(result.all())
            results.add(hashed_result)
            if len(results) > 1:
                return
        else:
            assert False, "got same order every time for 15 tries"

    @testing.only_on("postgresql>=13")
    @testing.variation("downgrade", [True, False])
    def test_fixture_downgraded(self, connection, metadata, downgrade):
        t = Table(
            "t",
            metadata,
            Column(
                "id",
                Uuid(),
                server_default=func.gen_random_uuid(),
                primary_key=True,
            ),
            Column("data", String(50)),
        )
        metadata.create_all(connection)

        r1 = connection.execute(
            insert(t).returning(t.c.data, sort_by_parameter_order=True),
            [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}],
        )
        eq_(r1.all(), [("d1",), ("d2",), ("d3",)])

        if downgrade:
            insertmanyvalues_fixture(connection, warn_on_downgraded=True)

            with self._expect_downgrade_warnings(
                warn_for_downgrades=True,
                sort_by_parameter_order=True,
            ):
                connection.execute(
                    insert(t).returning(
                        t.c.data, sort_by_parameter_order=True
                    ),
                    [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}],
                )
        else:
            # run a plain test to help ensure the fixture doesn't leak to
            # other tests
            r1 = connection.execute(
                insert(t).returning(t.c.data, sort_by_parameter_order=True),
                [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}],
            )
            eq_(r1.all(), [("d1",), ("d2",), ("d3",)])

    @testing.variation(
        "sequence_type",
        [
            ("sequence", testing.requires.sequences),
            ("identity", testing.requires.identity_columns),
        ],
    )
    @testing.variation("increment", ["positive", "negative", "implicit"])
    @testing.variation("explicit_sentinel", [True, False])
    def test_invalid_identities(
        self,
        metadata,
        connection,
        warn_for_downgrades,
        randomize_returning,
        sort_by_parameter_order,
        sequence_type: testing.Variation,
        increment: testing.Variation,
        explicit_sentinel,
    ):
        if sequence_type.sequence:
            seq_cls = functools.partial(Sequence, name="t1_id_seq")
        elif sequence_type.identity:
            seq_cls = Identity
        else:
            sequence_type.fail()

        if increment.implicit:
            sequence = seq_cls(start=1)
        elif increment.positive:
            sequence = seq_cls(start=1, increment=1)
        elif increment.negative:
            sequence = seq_cls(start=-1, increment=-1)
        else:
            increment.fail()

        t1 = Table(
            "t1",
            metadata,
            Column(
                "id",
                Integer,
                sequence,
                primary_key=True,
                insert_sentinel=bool(explicit_sentinel),
            ),
            Column("data", String(50)),
        )
        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = insert(t1).returning(
            t1.c.id,
            t1.c.data,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        data = [{"data": f"d{i}"} for i in range(10)]

        use_imv = testing.db.dialect.use_insertmanyvalues
        if (
            use_imv
            and increment.negative
            and explicit_sentinel
            and sort_by_parameter_order
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                rf"Can't use "
                rf"{'SEQUENCE' if sequence_type.sequence else 'IDENTITY'} "
                rf"default with negative increment",
            ):
                connection.execute(stmt, data)
            return
        elif (
            use_imv
            and explicit_sentinel
            and sort_by_parameter_order
            and sequence_type.sequence
            and not (
                testing.db.dialect.insertmanyvalues_implicit_sentinel
                & InsertmanyvaluesSentinelOpts.SEQUENCE
            )
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                r"Column t1.id can't be explicitly marked as a sentinel "
                r"column .* as the particular type of default generation",
            ):
                connection.execute(stmt, data)
            return

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
            server_autoincrement=not increment.negative,
            autoincrement_is_sequence=sequence_type.sequence,
        ):
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        if increment.negative:
            expected_data = [(-1 - i, f"d{i}") for i in range(10)]
        else:
            expected_data = [(i + 1, f"d{i}") for i in range(10)]

        eq_(
            coll(result),
            coll(expected_data),
        )

    @testing.requires.sequences
    @testing.variation("explicit_sentinel", [True, False])
    @testing.variation("sequence_actually_translates", [True, False])
    @testing.variation("the_table_translates", [True, False])
    def test_sequence_schema_translate(
        self,
        metadata,
        connection,
        explicit_sentinel,
        warn_for_downgrades,
        randomize_returning,
        sort_by_parameter_order,
        sequence_actually_translates,
        the_table_translates,
    ):
        """test #11157"""

        # so there's a bit of a bug which is that functions has_table()
        # and has_sequence() do not take schema translate map into account,
        # at all.   So on MySQL, where we dont have transactional DDL, the
        # DROP for Table / Sequence does not really work for all test runs
        # when the schema is set to a "to be translated" kind of name.
        # so, make a Table/Sequence with fixed schema name for the CREATE,
        # then use a different object for the test that has a translate
        # schema name
        Table(
            "t1",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("some_seq", start=1, schema=config.test_schema),
                primary_key=True,
                insert_sentinel=bool(explicit_sentinel),
            ),
            Column("data", String(50)),
            schema=config.test_schema if the_table_translates else None,
        )
        metadata.create_all(connection)

        if sequence_actually_translates:
            connection = connection.execution_options(
                schema_translate_map={
                    "should_be_translated": config.test_schema
                }
            )
            sequence = Sequence(
                "some_seq", start=1, schema="should_be_translated"
            )
        else:
            connection = connection.execution_options(
                schema_translate_map={"foo": "bar"}
            )
            sequence = Sequence("some_seq", start=1, schema=config.test_schema)

        m2 = MetaData()
        t1 = Table(
            "t1",
            m2,
            Column(
                "id",
                Integer,
                sequence,
                primary_key=True,
                insert_sentinel=bool(explicit_sentinel),
            ),
            Column("data", String(50)),
            schema=(
                "should_be_translated"
                if sequence_actually_translates and the_table_translates
                else config.test_schema if the_table_translates else None
            ),
        )

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = insert(t1).returning(
            t1.c.id,
            t1.c.data,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        data = [{"data": f"d{i}"} for i in range(10)]

        use_imv = testing.db.dialect.use_insertmanyvalues
        if (
            use_imv
            and explicit_sentinel
            and sort_by_parameter_order
            and not (
                testing.db.dialect.insertmanyvalues_implicit_sentinel
                & InsertmanyvaluesSentinelOpts.SEQUENCE
            )
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                r"Column t1.id can't be explicitly marked as a sentinel "
                r"column .* as the particular type of default generation",
            ):
                connection.execute(stmt, data)
            return

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
            server_autoincrement=True,
            autoincrement_is_sequence=True,
        ):
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        expected_data = [(i + 1, f"d{i}") for i in range(10)]

        eq_(
            coll(result),
            coll(expected_data),
        )

    @testing.combinations(
        Integer(),
        String(50),
        (ARRAY(Integer()), testing.requires.array_type),
        DateTime(),
        Uuid(),
        Uuid(native_uuid=False),
        argnames="datatype",
    )
    def test_inserts_w_all_nulls(
        self, connection, metadata, sort_by_parameter_order, datatype
    ):
        """this test is geared towards the INSERT..SELECT VALUES case,
        where if the VALUES have all NULL for some column, PostgreSQL assumes
        the datatype must be TEXT and throws for other table datatypes. So an
        additional layer of casts is applied to the SELECT p0,p1, p2... part of
        the statement for all datatypes unconditionally. Even though the VALUES
        clause also has bind casts for selected datatypes, this NULL handling
        is needed even for simple datatypes. We'd prefer not to render bind
        casts for all possible datatypes as that affects other kinds of
        statements as well and also is very verbose for insertmanyvalues.


        """
        t = Table(
            "t",
            metadata,
            Column("id", Integer, Identity(), primary_key=True),
            Column("data", datatype),
        )
        metadata.create_all(connection)
        result = connection.execute(
            insert(t).returning(
                t.c.id,
                sort_by_parameter_order=bool(sort_by_parameter_order),
            ),
            [{"data": None}, {"data": None}, {"data": None}],
        )
        eq_(set(result), {(1,), (2,), (3,)})

    @testing.variation("pk_type", ["autoinc", "clientside"])
    @testing.variation("add_sentinel", ["none", "clientside", "sentinel"])
    def test_imv_w_additional_values(
        self,
        metadata,
        connection,
        sort_by_parameter_order,
        pk_type: testing.Variation,
        randomize_returning,
        warn_for_downgrades,
        add_sentinel,
    ):
        if pk_type.autoinc:
            pk_col = Column("id", Integer(), Identity(), primary_key=True)
        elif pk_type.clientside:
            pk_col = Column("id", Uuid(), default=uuid.uuid4, primary_key=True)
        else:
            pk_type.fail()

        if add_sentinel.clientside:
            extra_col = insert_sentinel(
                "sentinel", type_=Uuid(), default=uuid.uuid4
            )
        elif add_sentinel.sentinel:
            extra_col = insert_sentinel("sentinel")
        else:
            extra_col = Column("sentinel", Integer())

        t1 = Table(
            "t1",
            metadata,
            pk_col,
            Column("data", String(30)),
            Column("moredata", String(30)),
            extra_col,
            Column(
                "has_server_default",
                String(50),
                server_default="some_server_default",
            ),
        )
        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = (
            insert(t1)
            .values(moredata="more data")
            .returning(
                t1.c.data,
                t1.c.moredata,
                t1.c.has_server_default,
                sort_by_parameter_order=bool(sort_by_parameter_order),
            )
        )
        data = [{"data": f"d{i}"} for i in range(10)]

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
            separate_sentinel=not add_sentinel.none,
            server_autoincrement=pk_type.autoinc,
            client_side_pk=pk_type.clientside,
        ):
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(
            coll(result),
            coll(
                [
                    (f"d{i}", "more data", "some_server_default")
                    for i in range(10)
                ]
            ),
        )

    def test_sentinel_incorrect_rowcount(
        self, metadata, connection, sort_by_parameter_order
    ):
        """test assertions to ensure sentinel values don't have duplicates"""

        uuids = [uuid.uuid4() for i in range(10)]

        # make some dupes
        uuids[3] = uuids[5]
        uuids[9] = uuids[5]

        t1 = Table(
            "data",
            metadata,
            Column("id", Integer, Identity(), primary_key=True),
            Column("data", String(50)),
            insert_sentinel(
                "uuids",
                Uuid(),
                default=functools.partial(next, iter(uuids)),
            ),
        )

        metadata.create_all(connection)

        stmt = insert(t1).returning(
            t1.c.data,
            t1.c.uuids,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        data = [{"data": f"d{i}"} for i in range(10)]

        if testing.db.dialect.use_insertmanyvalues and sort_by_parameter_order:
            with expect_raises_message(
                exc.InvalidRequestError,
                "Sentinel-keyed result set did not produce correct "
                "number of rows 10; produced 8.",
            ):
                connection.execute(stmt, data)
        else:
            result = connection.execute(stmt, data)
            eq_(
                set(result.all()),
                {(f"d{i}", uuids[i]) for i in range(10)},
            )

    @testing.variation("resolve_sentinel_values", [True, False])
    def test_sentinel_cant_match_keys(
        self,
        metadata,
        connection,
        sort_by_parameter_order,
        resolve_sentinel_values,
    ):
        """test assertions to ensure sentinel values passed in parameter
        structures can be identified when they come back in cursor.fetchall().

        Sentinels are now matched based on the data on the outside of the
        type, that is, before the bind, and after the result.

        """

        class UnsymmetricDataType(TypeDecorator):
            cache_ok = True
            impl = String

            def bind_expression(self, bindparam):
                return func.lower(bindparam)

            if resolve_sentinel_values:

                def process_result_value(self, value, dialect):
                    return value.replace("upper", "UPPER")

        t1 = Table(
            "data",
            metadata,
            Column("id", Integer, Identity(), primary_key=True),
            Column("data", String(50)),
            insert_sentinel("unsym", UnsymmetricDataType(10)),
        )

        metadata.create_all(connection)

        stmt = insert(t1).returning(
            t1.c.data,
            t1.c.unsym,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        data = [{"data": f"d{i}", "unsym": f"UPPER_d{i}"} for i in range(10)]

        if (
            testing.db.dialect.use_insertmanyvalues
            and sort_by_parameter_order
            and not resolve_sentinel_values
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                r"Can't match sentinel values in result set to parameter "
                r"sets; key 'UPPER_d.' was not found.",
            ):
                connection.execute(stmt, data)
        else:
            result = connection.execute(stmt, data)
            if resolve_sentinel_values:
                eq_(
                    set(result.all()),
                    {(f"d{i}", f"UPPER_d{i}") for i in range(10)},
                )
            else:
                eq_(
                    set(result.all()),
                    {(f"d{i}", f"upper_d{i}") for i in range(10)},
                )

    @testing.variation("add_insert_sentinel", [True, False])
    def test_sentinel_insert_default_pk_only(
        self,
        metadata,
        connection,
        sort_by_parameter_order,
        add_insert_sentinel,
    ):
        t1 = Table(
            "data",
            metadata,
            Column(
                "id",
                Integer,
                Identity(),
                insert_sentinel=bool(add_insert_sentinel),
                primary_key=True,
            ),
            Column("data", String(50)),
        )

        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection, randomize_rows=True, warn_on_downgraded=False
        )

        stmt = insert(t1).returning(
            t1.c.id,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        data = [{} for i in range(3)]

        if (
            testing.db.dialect.use_insertmanyvalues
            and add_insert_sentinel
            and sort_by_parameter_order
            and not (
                testing.db.dialect.insertmanyvalues_implicit_sentinel
                & InsertmanyvaluesSentinelOpts.ANY_AUTOINCREMENT
            )
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                "Column data.id can't be explicitly marked as a "
                f"sentinel column when using the {testing.db.dialect.name} "
                "dialect",
            ):
                connection.execute(stmt, data)
            return
        else:
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            # if we used a client side default function, or we had no sentinel
            # at all, we're sorted
            coll = list
        else:
            # otherwise we are not, we randomized the order in any case
            coll = set

        eq_(
            coll(result),
            coll(
                [
                    (1,),
                    (2,),
                    (3,),
                ]
            ),
        )

    @testing.only_on("postgresql>=13")
    @testing.variation("default_type", ["server_side", "client_side"])
    @testing.variation("add_insert_sentinel", [True, False])
    def test_no_sentinel_on_non_int_ss_function(
        self,
        metadata,
        connection,
        add_insert_sentinel,
        default_type,
        sort_by_parameter_order,
    ):
        t1 = Table(
            "data",
            metadata,
            Column(
                "id",
                Uuid(),
                server_default=(
                    func.gen_random_uuid()
                    if default_type.server_side
                    else None
                ),
                default=uuid.uuid4 if default_type.client_side else None,
                primary_key=True,
                insert_sentinel=bool(add_insert_sentinel),
            ),
            Column("data", String(50)),
        )

        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection, randomize_rows=True, warn_on_downgraded=False
        )

        stmt = insert(t1).returning(
            t1.c.data,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        data = [
            {"data": "d1"},
            {"data": "d2"},
            {"data": "d3"},
        ]

        if (
            default_type.server_side
            and add_insert_sentinel
            and sort_by_parameter_order
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                r"Column data.id can't be a sentinel column because it uses "
                r"an explicit server side default that's not the Identity\(\)",
            ):
                connection.execute(stmt, data)
            return
        else:
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            # if we used a client side default function, or we had no sentinel
            # at all, we're sorted
            coll = list
        else:
            # otherwise we are not, we randomized the order in any case
            coll = set

        eq_(
            coll(result),
            coll(
                [
                    ("d1",),
                    ("d2",),
                    ("d3",),
                ]
            ),
        )

    @testing.variation(
        "pk_type",
        [
            ("plain_autoinc", testing.requires.autoincrement_without_sequence),
            ("sequence", testing.requires.sequences),
            ("identity", testing.requires.identity_columns),
        ],
    )
    @testing.variation(
        "sentinel",
        [
            "none",  # passes because we automatically downgrade
            # for no sentinel col
            "implicit_not_omitted",
            "implicit_omitted",
            "explicit",
            "explicit_but_nullable",
            "default_uuid",
            "default_string_uuid",
            ("identity", testing.requires.multiple_identity_columns),
            ("sequence", testing.requires.sequences),
        ],
    )
    def test_sentinel_col_configurations(
        self,
        pk_type: testing.Variation,
        sentinel: testing.Variation,
        sort_by_parameter_order,
        randomize_returning,
        metadata,
        connection,
    ):
        if pk_type.plain_autoinc:
            pk_col = Column("id", Integer, primary_key=True)
        elif pk_type.sequence:
            pk_col = Column(
                "id",
                Integer,
                Sequence("result_id_seq", start=1),
                primary_key=True,
            )
        elif pk_type.identity:
            pk_col = Column("id", Integer, Identity(), primary_key=True)
        else:
            pk_type.fail()

        if sentinel.implicit_not_omitted or sentinel.implicit_omitted:
            _sentinel = insert_sentinel(
                "sentinel",
                omit_from_statements=bool(sentinel.implicit_omitted),
            )
        elif sentinel.explicit:
            _sentinel = Column(
                "some_uuid", Uuid(), nullable=False, insert_sentinel=True
            )
        elif sentinel.explicit_but_nullable:
            _sentinel = Column("some_uuid", Uuid(), insert_sentinel=True)
        elif sentinel.default_uuid or sentinel.default_string_uuid:
            _sentinel = Column(
                "some_uuid",
                Uuid(native_uuid=bool(sentinel.default_uuid)),
                insert_sentinel=True,
                default=uuid.uuid4,
            )
        elif sentinel.identity:
            _sentinel = Column(
                "some_identity",
                Integer,
                Identity(),
                insert_sentinel=True,
            )
        elif sentinel.sequence:
            _sentinel = Column(
                "some_identity",
                Integer,
                Sequence("some_id_seq", start=1),
                insert_sentinel=True,
            )
        else:
            _sentinel = Column("some_uuid", Uuid())

        t = Table("t", metadata, pk_col, Column("data", String(50)), _sentinel)

        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=True,
        )

        stmt = insert(t).returning(
            pk_col,
            t.c.data,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        if sentinel.explicit:
            data = [
                {"data": f"d{i}", "some_uuid": uuid.uuid4()}
                for i in range(150)
            ]
        else:
            data = [{"data": f"d{i}"} for i in range(150)]

        expect_sentinel_use = (
            sort_by_parameter_order
            and testing.db.dialect.insert_returning
            and testing.db.dialect.use_insertmanyvalues
        )

        if sentinel.explicit_but_nullable and expect_sentinel_use:
            with expect_raises_message(
                exc.InvalidRequestError,
                "Column t.some_uuid has been marked as a sentinel column "
                "with no default generation function; it at least needs to "
                "be marked nullable=False",
            ):
                connection.execute(stmt, data)
            return

        elif (
            expect_sentinel_use
            and sentinel.sequence
            and not (
                testing.db.dialect.insertmanyvalues_implicit_sentinel
                & InsertmanyvaluesSentinelOpts.SEQUENCE
            )
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                "Column t.some_identity can't be explicitly marked as a "
                f"sentinel column when using the {testing.db.dialect.name} "
                "dialect",
            ):
                connection.execute(stmt, data)
            return

        elif (
            sentinel.none
            and expect_sentinel_use
            and stmt.compile(
                dialect=testing.db.dialect
            )._get_sentinel_column_for_table(t)
            is None
        ):
            with expect_warnings(
                "Batches were downgraded for sorted INSERT",
            ):
                result = connection.execute(stmt, data)
        else:
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            eq_(list(result), [(i + 1, f"d{i}") for i in range(150)])
        else:
            eq_(set(result), {(i + 1, f"d{i}") for i in range(150)})

    @testing.variation(
        "return_type", ["include_sentinel", "default_only", "return_defaults"]
    )
    @testing.variation("add_sentinel_flag_to_col", [True, False])
    @testing.variation("native_uuid", [True, False])
    @testing.variation("as_uuid", [True, False])
    def test_sentinel_on_non_autoinc_primary_key(
        self,
        metadata,
        connection,
        return_type: testing.Variation,
        sort_by_parameter_order,
        randomize_returning,
        add_sentinel_flag_to_col,
        native_uuid,
        as_uuid,
    ):
        uuids = [uuid.uuid4() for i in range(10)]
        if not as_uuid:
            uuids = [str(u) for u in uuids]

        _some_uuids = iter(uuids)

        t1 = Table(
            "data",
            metadata,
            Column(
                "id",
                Uuid(native_uuid=bool(native_uuid), as_uuid=bool(as_uuid)),
                default=functools.partial(next, _some_uuids),
                primary_key=True,
                insert_sentinel=bool(add_sentinel_flag_to_col),
            ),
            Column("data", String(50)),
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=True,
        )

        if sort_by_parameter_order:
            collection_cls = list
        else:
            collection_cls = set

        metadata.create_all(connection)

        if sort_by_parameter_order:
            kw = {"sort_by_parameter_order": True}
        else:
            kw = {}

        if return_type.include_sentinel:
            stmt = t1.insert().returning(
                t1.c.id, t1.c.data, t1.c.has_server_default, **kw
            )
        elif return_type.default_only:
            stmt = t1.insert().returning(
                t1.c.data, t1.c.has_server_default, **kw
            )
        elif return_type.return_defaults:
            stmt = t1.insert().return_defaults(**kw)

        else:
            return_type.fail()

        r = connection.execute(
            stmt,
            [{"data": f"d{i}"} for i in range(1, 6)],
        )

        if return_type.include_sentinel:
            eq_(r.keys(), ["id", "data", "has_server_default"])
            eq_(
                collection_cls(r),
                collection_cls(
                    [
                        (uuids[i], f"d{i + 1}", "some_server_default")
                        for i in range(5)
                    ]
                ),
            )
        elif return_type.default_only:
            eq_(r.keys(), ["data", "has_server_default"])
            eq_(
                collection_cls(r),
                collection_cls(
                    [
                        (
                            f"d{i + 1}",
                            "some_server_default",
                        )
                        for i in range(5)
                    ]
                ),
            )
        elif return_type.return_defaults:
            eq_(r.keys(), ["has_server_default"])
            eq_(r.inserted_primary_key_rows, [(uuids[i],) for i in range(5)])
            eq_(
                r.returned_defaults_rows,
                [
                    ("some_server_default",),
                    ("some_server_default",),
                    ("some_server_default",),
                    ("some_server_default",),
                    ("some_server_default",),
                ],
            )
            eq_(r.all(), [])
        else:
            return_type.fail()

    @testing.variation("native_uuid", [True, False])
    @testing.variation("as_uuid", [True, False])
    def test_client_composite_pk(
        self,
        metadata,
        connection,
        randomize_returning,
        sort_by_parameter_order,
        warn_for_downgrades,
        native_uuid,
        as_uuid,
    ):
        uuids = [uuid.uuid4() for i in range(10)]
        if not as_uuid:
            uuids = [str(u) for u in uuids]

        t1 = Table(
            "data",
            metadata,
            Column(
                "id1",
                Uuid(as_uuid=bool(as_uuid), native_uuid=bool(native_uuid)),
                default=functools.partial(next, iter(uuids)),
                primary_key=True,
            ),
            Column(
                "id2",
                # note this is testing that plain populated PK cols
                # also qualify as sentinels since they have to be there
                String(30),
                primary_key=True,
            ),
            Column("data", String(50)),
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )
        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        result = connection.execute(
            insert(t1).returning(
                t1.c.id1,
                t1.c.id2,
                t1.c.data,
                t1.c.has_server_default,
                sort_by_parameter_order=bool(sort_by_parameter_order),
            ),
            [{"id2": f"id{i}", "data": f"d{i}"} for i in range(10)],
        )

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(
            coll(result),
            coll(
                [
                    (uuids[i], f"id{i}", f"d{i}", "some_server_default")
                    for i in range(10)
                ]
            ),
        )

    @testing.variation("add_sentinel", [True, False])
    @testing.variation(
        "set_identity", [(True, testing.requires.identity_columns), False]
    )
    def test_no_pk(
        self,
        metadata,
        connection,
        randomize_returning,
        sort_by_parameter_order,
        warn_for_downgrades,
        add_sentinel,
        set_identity,
    ):
        if set_identity:
            id_col = Column("id", Integer(), Identity())
        else:
            id_col = Column("id", Integer())

        uuids = [uuid.uuid4() for i in range(10)]

        sentinel_col = Column(
            "unique_id",
            Uuid,
            default=functools.partial(next, iter(uuids)),
            insert_sentinel=bool(add_sentinel),
        )
        t1 = Table(
            "nopk",
            metadata,
            id_col,
            Column("data", String(50)),
            sentinel_col,
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )
        metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = insert(t1).returning(
            t1.c.id,
            t1.c.data,
            t1.c.has_server_default,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )
        if not set_identity:
            data = [{"id": i + 1, "data": f"d{i}"} for i in range(10)]
        else:
            data = [{"data": f"d{i}"} for i in range(10)]

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
            separate_sentinel=add_sentinel,
        ):
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(
            coll(result),
            coll([(i + 1, f"d{i}", "some_server_default") for i in range(10)]),
        )

    @testing.variation("add_sentinel_to_col", [True, False])
    @testing.variation(
        "set_autoincrement", [True, (False, testing.skip_if("mariadb"))]
    )
    def test_hybrid_client_composite_pk(
        self,
        metadata,
        connection,
        randomize_returning,
        sort_by_parameter_order,
        warn_for_downgrades,
        add_sentinel_to_col,
        set_autoincrement,
    ):
        """test a pk that is part server generated part client generated.

        The server generated col by itself can be the sentinel.  if it's
        part of the PK and is autoincrement=True then it is automatically
        used as such.    if not, there's a graceful downgrade.

        """

        t1 = Table(
            "data",
            metadata,
            Column(
                "idint",
                Integer,
                Identity(),
                autoincrement=True if set_autoincrement else "auto",
                primary_key=True,
                insert_sentinel=bool(add_sentinel_to_col),
            ),
            Column(
                "idstr",
                String(30),
                primary_key=True,
            ),
            Column("data", String(50)),
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )

        no_autoincrement = (
            not testing.requires.supports_autoincrement_w_composite_pk.enabled  # noqa: E501
        )
        if set_autoincrement and no_autoincrement:
            with expect_raises_message(
                exc.CompileError,
                r".*SQLite does not support autoincrement for "
                "composite primary keys",
            ):
                metadata.create_all(connection)
            return
        else:
            metadata.create_all(connection)

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = insert(t1).returning(
            t1.c.idint,
            t1.c.idstr,
            t1.c.data,
            t1.c.has_server_default,
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )

        if no_autoincrement:
            data = [
                {"idint": i + 1, "idstr": f"id{i}", "data": f"d{i}"}
                for i in range(10)
            ]
        else:
            data = [{"idstr": f"id{i}", "data": f"d{i}"} for i in range(10)]

        if (
            testing.db.dialect.use_insertmanyvalues
            and add_sentinel_to_col
            and sort_by_parameter_order
            and not (
                testing.db.dialect.insertmanyvalues_implicit_sentinel
                & InsertmanyvaluesSentinelOpts.ANY_AUTOINCREMENT
            )
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                "Column data.idint can't be explicitly marked as a sentinel "
                "column when using the sqlite dialect",
            ):
                result = connection.execute(stmt, data)
            return

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
            separate_sentinel=not set_autoincrement and add_sentinel_to_col,
            server_autoincrement=set_autoincrement,
        ):
            result = connection.execute(stmt, data)

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(
            coll(result),
            coll(
                [
                    (i + 1, f"id{i}", f"d{i}", "some_server_default")
                    for i in range(10)
                ]
            ),
        )

    @testing.variation("composite_pk", [True, False])
    @testing.only_on(
        [
            "+psycopg",
            "+psycopg2",
            "+pysqlite",
            "+mysqlclient",
            "+cx_oracle",
            "+oracledb",
        ]
    )
    def test_failure_mode_if_i_dont_send_value(
        self, metadata, connection, sort_by_parameter_order, composite_pk
    ):
        """test that we get a regular integrity error if a required
        PK value was not sent, that is, imv does not get in the way

        """
        t1 = Table(
            "data",
            metadata,
            Column("id", String(30), primary_key=True),
            Column("data", String(50)),
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )
        if composite_pk:
            t1.append_column(Column("uid", Uuid(), default=uuid.uuid4))

        metadata.create_all(connection)

        with expect_warnings(
            r".*but has no Python-side or server-side default ",
        ):
            with expect_raises(exc.IntegrityError):
                connection.execute(
                    insert(t1).returning(
                        t1.c.id,
                        t1.c.data,
                        t1.c.has_server_default,
                        sort_by_parameter_order=bool(sort_by_parameter_order),
                    ),
                    [{"data": f"d{i}"} for i in range(10)],
                )

    @testing.variation("add_sentinel_flag_to_col", [True, False])
    @testing.variation(
        "return_type", ["include_sentinel", "default_only", "return_defaults"]
    )
    @testing.variation(
        "sentinel_type",
        [
            ("autoincrement", testing.requires.autoincrement_without_sequence),
            "identity",
            "sequence",
        ],
    )
    def test_implicit_autoincrement_sentinel(
        self,
        metadata,
        connection,
        return_type: testing.Variation,
        sort_by_parameter_order,
        randomize_returning,
        sentinel_type,
        add_sentinel_flag_to_col,
    ):
        if sentinel_type.identity:
            sentinel_args = [Identity()]
        elif sentinel_type.sequence:
            sentinel_args = [Sequence("id_seq", start=1)]
        else:
            sentinel_args = []
        t1 = Table(
            "data",
            metadata,
            Column(
                "id",
                Integer,
                *sentinel_args,
                primary_key=True,
                insert_sentinel=bool(add_sentinel_flag_to_col),
            ),
            Column("data", String(50)),
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=False,
        )

        if sort_by_parameter_order:
            collection_cls = list
        else:
            collection_cls = set

        metadata.create_all(connection)

        if sort_by_parameter_order:
            kw = {"sort_by_parameter_order": True}
        else:
            kw = {}

        if return_type.include_sentinel:
            stmt = t1.insert().returning(
                t1.c.id, t1.c.data, t1.c.has_server_default, **kw
            )
        elif return_type.default_only:
            stmt = t1.insert().returning(
                t1.c.data, t1.c.has_server_default, **kw
            )
        elif return_type.return_defaults:
            stmt = t1.insert().return_defaults(**kw)

        else:
            return_type.fail()

        if (
            testing.db.dialect.use_insertmanyvalues
            and add_sentinel_flag_to_col
            and sort_by_parameter_order
            and (
                not (
                    testing.db.dialect.insertmanyvalues_implicit_sentinel
                    & InsertmanyvaluesSentinelOpts.ANY_AUTOINCREMENT
                )
                or (
                    # currently a SQL Server case, we dont yet render a
                    # syntax for SQL Server sequence w/ deterministic
                    # ordering.   The INSERT..SELECT could be restructured
                    # further to support this at a later time however
                    # sequences with SQL Server are very unusual.
                    sentinel_type.sequence
                    and not (
                        testing.db.dialect.insertmanyvalues_implicit_sentinel
                        & InsertmanyvaluesSentinelOpts.SEQUENCE
                    )
                )
            )
        ):
            with expect_raises_message(
                exc.InvalidRequestError,
                "Column data.id can't be explicitly marked as a "
                f"sentinel column when using the {testing.db.dialect.name} "
                "dialect",
            ):
                connection.execute(
                    stmt,
                    [{"data": f"d{i}"} for i in range(1, 6)],
                )
            return
        else:
            r = connection.execute(
                stmt,
                [{"data": f"d{i}"} for i in range(1, 6)],
            )

        if return_type.include_sentinel:
            eq_(r.keys(), ["id", "data", "has_server_default"])
            eq_(
                collection_cls(r),
                collection_cls(
                    [(i, f"d{i}", "some_server_default") for i in range(1, 6)]
                ),
            )
        elif return_type.default_only:
            eq_(r.keys(), ["data", "has_server_default"])
            eq_(
                collection_cls(r),
                collection_cls(
                    [(f"d{i}", "some_server_default") for i in range(1, 6)]
                ),
            )
        elif return_type.return_defaults:
            eq_(r.keys(), ["id", "has_server_default"])
            eq_(
                collection_cls(r.inserted_primary_key_rows),
                collection_cls([(i + 1,) for i in range(5)]),
            )
            eq_(
                collection_cls(r.returned_defaults_rows),
                collection_cls(
                    [
                        (
                            1,
                            "some_server_default",
                        ),
                        (
                            2,
                            "some_server_default",
                        ),
                        (
                            3,
                            "some_server_default",
                        ),
                        (
                            4,
                            "some_server_default",
                        ),
                        (
                            5,
                            "some_server_default",
                        ),
                    ]
                ),
            )
            eq_(r.all(), [])
        else:
            return_type.fail()

    @testing.variation("pk_type", ["serverside", "clientside"])
    @testing.variation(
        "sentinel_type",
        [
            "use_pk",
            ("use_pk_explicit", testing.skip_if("sqlite")),
            "separate_uuid",
            "separate_sentinel",
        ],
    )
    @testing.requires.provisioned_upsert
    def test_upsert_downgrades(
        self,
        metadata,
        connection,
        pk_type: testing.Variation,
        sort_by_parameter_order,
        randomize_returning,
        sentinel_type,
        warn_for_downgrades,
    ):
        if pk_type.serverside:
            pk_col = Column(
                "id",
                Integer(),
                primary_key=True,
                insert_sentinel=bool(sentinel_type.use_pk_explicit),
            )
        elif pk_type.clientside:
            pk_col = Column(
                "id",
                Uuid(),
                default=uuid.uuid4,
                primary_key=True,
                insert_sentinel=bool(sentinel_type.use_pk_explicit),
            )
        else:
            pk_type.fail()

        if sentinel_type.separate_uuid:
            extra_col = Column(
                "sent_col",
                Uuid(),
                default=uuid.uuid4,
                insert_sentinel=True,
                nullable=False,
            )
        elif sentinel_type.separate_sentinel:
            extra_col = insert_sentinel("sent_col")
        else:
            extra_col = Column("sent_col", Integer)

        t1 = Table(
            "upsert_table",
            metadata,
            pk_col,
            Column("data", String(50)),
            extra_col,
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )
        metadata.create_all(connection)

        result = connection.execute(
            insert(t1).returning(
                t1.c.id, t1.c.data, sort_by_parameter_order=True
            ),
            [{"data": "d1"}, {"data": "d2"}],
        )
        d1d2 = list(result)

        if pk_type.serverside:
            new_ids = [10, 15, 3]
        elif pk_type.clientside:
            new_ids = [uuid.uuid4() for i in range(3)]
        else:
            pk_type.fail()

        upsert_data = [
            {"id": d1d2[0][0], "data": "d1 new"},
            {"id": new_ids[0], "data": "d10"},
            {"id": new_ids[1], "data": "d15"},
            {"id": d1d2[1][0], "data": "d2 new"},
            {"id": new_ids[2], "data": "d3"},
        ]

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = provision.upsert(
            config,
            t1,
            (t1.c.data, t1.c.has_server_default),
            set_lambda=lambda inserted: {
                "data": inserted.data + " upserted",
            },
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
        ):
            result = connection.execute(stmt, upsert_data)

        expected_data = [
            ("d1 new upserted", "some_server_default"),
            ("d10", "some_server_default"),
            ("d15", "some_server_default"),
            ("d2 new upserted", "some_server_default"),
            ("d3", "some_server_default"),
        ]
        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(coll(result), coll(expected_data))

    @testing.variation(
        "sentinel_type",
        [
            "use_pk",
            "separate_uuid",
            "separate_sentinel",
        ],
    )
    @testing.requires.provisioned_upsert
    def test_upsert_autoincrement_downgrades(
        self,
        metadata,
        connection,
        sort_by_parameter_order,
        randomize_returning,
        sentinel_type,
        warn_for_downgrades,
    ):
        pk_col = Column(
            "id", Integer, test_needs_autoincrement=True, primary_key=True
        )

        if sentinel_type.separate_uuid:
            extra_col = Column(
                "sent_col",
                Uuid(),
                default=uuid.uuid4,
                insert_sentinel=True,
                nullable=False,
            )
        elif sentinel_type.separate_sentinel:
            extra_col = insert_sentinel("sent_col")
        else:
            extra_col = Column("sent_col", Integer)

        t1 = Table(
            "upsert_table",
            metadata,
            pk_col,
            Column("otherid", Integer, unique=True),
            Column("data", String(50)),
            extra_col,
            Column(
                "has_server_default",
                String(30),
                server_default="some_server_default",
            ),
        )
        metadata.create_all(connection)

        result = connection.execute(
            insert(t1).returning(
                t1.c.id, t1.c.data, sort_by_parameter_order=True
            ),
            [{"otherid": 1, "data": "d1"}, {"otherid": 2, "data": "d2"}],
        )

        upsert_data = [
            {"otherid": 1, "data": "d1 new"},
            {"otherid": 3, "data": "d10"},
            {"otherid": 4, "data": "d15"},
            {"otherid": 2, "data": "d2 new"},
            {"otherid": 5, "data": "d3"},
        ]

        fixtures.insertmanyvalues_fixture(
            connection,
            randomize_rows=bool(randomize_returning),
            warn_on_downgraded=bool(warn_for_downgrades),
        )

        stmt = provision.upsert(
            config,
            t1,
            (t1.c.data, t1.c.has_server_default),
            set_lambda=lambda inserted: {
                "data": inserted.data + " upserted",
            },
            sort_by_parameter_order=bool(sort_by_parameter_order),
            index_elements=["otherid"],
        )

        with self._expect_downgrade_warnings(
            warn_for_downgrades=warn_for_downgrades,
            sort_by_parameter_order=sort_by_parameter_order,
            expect_warnings_override=(
                testing.against("mysql", "mariadb", "sqlite")
                or (testing.against("postgresql") and not sentinel_type.use_pk)
            ),
        ):
            result = connection.execute(stmt, upsert_data)

        if sentinel_type.use_pk and testing.against("postgresql"):
            expected_data = [
                ("d1 new upserted", "some_server_default"),
                ("d2 new upserted", "some_server_default"),
                ("d10", "some_server_default"),
                ("d15", "some_server_default"),
                ("d3", "some_server_default"),
            ]
        else:
            expected_data = [
                ("d1 new upserted", "some_server_default"),
                ("d10", "some_server_default"),
                ("d15", "some_server_default"),
                ("d2 new upserted", "some_server_default"),
                ("d3", "some_server_default"),
            ]

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(coll(result), coll(expected_data))

    def test_auto_downgraded_non_mvi_dialect(
        self,
        metadata,
        testing_engine,
        randomize_returning,
        warn_for_downgrades,
        sort_by_parameter_order,
    ):
        """Accommodate the case of the dialect that supports RETURNING, but
        does not support "multi values INSERT" syntax.

        These dialects should still provide insertmanyvalues/returning
        support, using downgraded batching.

        For now, we are still keeping this entire thing "opt in" by requiring
        that use_insertmanyvalues=True, which means we can't simplify the
        ORM by not worrying about dialects where ordering is available or
        not.

        However, dialects that use RETURNING, but don't support INSERT VALUES
        (..., ..., ...) can set themselves up like this::

            class MyDialect(DefaultDialect):
                use_insertmanyvalues = True
                supports_multivalues_insert = False

        This test runs for everyone **including** Oracle, where we
        exercise Oracle using "insertmanyvalues" without "multivalues_insert".

        """
        engine = testing_engine()
        engine.connect().close()

        engine.dialect.supports_multivalues_insert = False
        engine.dialect.use_insertmanyvalues = True

        uuids = [uuid.uuid4() for i in range(10)]

        t1 = Table(
            "t1",
            metadata,
            Column("id", Uuid(), default=functools.partial(next, iter(uuids))),
            Column("data", String(50)),
        )
        metadata.create_all(engine)

        with engine.connect() as conn:
            fixtures.insertmanyvalues_fixture(
                conn,
                randomize_rows=bool(randomize_returning),
                warn_on_downgraded=bool(warn_for_downgrades),
            )

            stmt = insert(t1).returning(
                t1.c.id,
                t1.c.data,
                sort_by_parameter_order=bool(sort_by_parameter_order),
            )
            data = [{"data": f"d{i}"} for i in range(10)]

            with self._expect_downgrade_warnings(
                warn_for_downgrades=warn_for_downgrades,
                sort_by_parameter_order=True,  # will warn even if not sorted
                connection=conn,
            ):
                result = conn.execute(stmt, data)

            expected_data = [(uuids[i], f"d{i}") for i in range(10)]
            if sort_by_parameter_order:
                coll = list
            else:
                coll = set

            eq_(coll(result), coll(expected_data))
