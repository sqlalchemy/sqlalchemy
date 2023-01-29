import itertools

from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import VARCHAR
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing import provision
from sqlalchemy.testing.provision import normalize_sequence
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class ExpectExpr:
    def __init__(self, element):
        self.element = element

    def __clause_element__(self):
        return self.element


class InsertExecTest(fixtures.TablesTest):
    __backend__ = True

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
    __backend__ = True

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
    __backend__ = True
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

    def test_insert_returning_values(self, connection):
        t = self.tables.data

        conn = connection
        page_size = conn.dialect.insertmanyvalues_page_size or 100
        data = [
            {"x": "x%d" % i, "y": "y%d" % i}
            for i in range(1, page_size * 2 + 27)
        ]
        result = conn.execute(t.insert().returning(t.c.x, t.c.y), data)

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
        """test cases that will execise SQL similar to that of
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
            lambda inserted: {"x": inserted.x + " upserted"},
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

    def test_insert_returning_defaults(self, connection):
        t = self.tables.data

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

    def test_insert_return_pks_default_values(self, connection):
        """test sending multiple, empty rows into an INSERT and getting primary
        key values back.

        This has to use a format that indicates at least one DEFAULT in
        multiple parameter sets, i.e. "INSERT INTO table (anycol) VALUES
        (DEFAULT) (DEFAULT) (DEFAULT) ... RETURNING col"

        if the database doesnt support this (like SQLite, mssql), it
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
            options={"use_insertmanyvalues": False},
            share_pool=True,
            transfer_staticpool=True,
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
