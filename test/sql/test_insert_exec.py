from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import VARCHAR
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
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
                    Sequence("t4_id_seq", optional=True),
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
                    Sequence("t4_id_seq"),
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
                Sequence("t_id_seq"),
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
                id=func.next_value(Sequence("t_id_seq")), data="data", x=5
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
