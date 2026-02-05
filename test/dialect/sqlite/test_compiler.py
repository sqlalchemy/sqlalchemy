"""SQLite-specific tests."""

from collections import OrderedDict
import contextlib

from sqlalchemy import and_
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.sqlite import base as sqlite
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.types import Boolean
from sqlalchemy.types import Integer
from sqlalchemy.types import String


def exec_sql(engine, sql, *args, **kwargs):
    # TODO: convert all tests to not use this
    with engine.begin() as conn:
        conn.exec_driver_sql(sql, *args, **kwargs)


class SQLTest(fixtures.TestBase, AssertsCompiledSQL):
    """Tests SQLite-dialect specific compilation."""

    __dialect__ = sqlite.dialect()

    def test_extract(self):
        t = sql.table("t", sql.column("col1"))
        mapping = {
            "month": "%m",
            "day": "%d",
            "year": "%Y",
            "second": "%S",
            "hour": "%H",
            "doy": "%j",
            "minute": "%M",
            "epoch": "%s",
            "dow": "%w",
            "week": "%W",
        }
        for field, subst in mapping.items():
            self.assert_compile(
                select(extract(field, t.c.col1)),
                "SELECT CAST(STRFTIME('%s', t.col1) AS "
                "INTEGER) AS anon_1 FROM t" % subst,
            )

    def test_plain_stringify_returning(self):
        t = Table(
            "t",
            MetaData(),
            Column("myid", Integer, primary_key=True),
            Column("name", String, server_default="some str"),
            Column("description", String, default=func.lower("hi")),
        )
        stmt = t.insert().values().return_defaults()
        eq_ignore_whitespace(
            str(stmt.compile(dialect=sqlite.SQLiteDialect())),
            "INSERT INTO t (description) VALUES (lower(?)) "
            "RETURNING myid, name, description",
        )

    def test_true_false(self):
        self.assert_compile(sql.false(), "0")
        self.assert_compile(sql.true(), "1")

    def test_is_distinct_from(self):
        self.assert_compile(
            sql.column("x").is_distinct_from(None), "x IS NOT NULL"
        )

        self.assert_compile(
            sql.column("x").is_not_distinct_from(False), "x IS 0"
        )

    def test_localtime(self):
        self.assert_compile(
            func.localtimestamp(), "DATETIME(CURRENT_TIMESTAMP, 'localtime')"
        )

    def test_constraints_with_schemas(self):
        metadata = MetaData()
        Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="master",
        )
        t2 = Table(
            "t2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("master.t1.id")),
            schema="master",
        )
        t3 = Table(
            "t3",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("master.t1.id")),
            schema="alternate",
        )
        t4 = Table(
            "t4",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("master.t1.id")),
        )

        # schema->schema, generate REFERENCES with no schema name
        self.assert_compile(
            schema.CreateTable(t2),
            "CREATE TABLE master.t2 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id), "
            "FOREIGN KEY(t1_id) REFERENCES t1 (id)"
            ")",
        )

        # schema->different schema, don't generate REFERENCES
        self.assert_compile(
            schema.CreateTable(t3),
            "CREATE TABLE alternate.t3 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id)"
            ")",
        )

        # same for local schema
        self.assert_compile(
            schema.CreateTable(t4),
            "CREATE TABLE t4 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id)"
            ")",
        )

    @testing.combinations(
        (
            Boolean(create_constraint=True),
            sql.false(),
            "BOOLEAN DEFAULT 0, CHECK (x IN (0, 1))",
        ),
        (
            String(),
            func.sqlite_version(),
            "VARCHAR DEFAULT (sqlite_version())",
        ),
        (Integer(), func.abs(-5) + 17, "INTEGER DEFAULT (abs(-5) + 17)"),
        (
            # test #12425
            String(),
            func.now(),
            "VARCHAR DEFAULT CURRENT_TIMESTAMP",
        ),
        (
            # test #12425
            String(),
            func.datetime(func.now(), "localtime"),
            "VARCHAR DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))",
        ),
        (
            # test #12425
            String(),
            text("datetime(CURRENT_TIMESTAMP, 'localtime')"),
            "VARCHAR DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))",
        ),
        (
            # default with leading spaces that should not be
            # parenthesized
            String,
            text("  'some default'"),
            "VARCHAR DEFAULT   'some default'",
        ),
        (String, text("'some default'"), "VARCHAR DEFAULT 'some default'"),
        argnames="datatype,default,expected",
    )
    def test_column_defaults_ddl(self, datatype, default, expected):
        t = Table(
            "t",
            MetaData(),
            Column(
                "x",
                datatype,
                server_default=default,
            ),
        )

        self.assert_compile(
            CreateTable(t),
            f"CREATE TABLE t (x {expected})",
        )

    def test_create_partial_index(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            sqlite_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )

        # test quoting and all that

        idx2 = Index(
            "test_idx2",
            tbl.c.data,
            sqlite_where=and_(tbl.c.data > "a", tbl.c.data < "b's"),
        )
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (data) "
            "WHERE data > 5 AND data < 10",
            dialect=sqlite.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) "
            "WHERE data > 'a' AND data < 'b''s'",
            dialect=sqlite.dialect(),
        )

    def test_no_autoinc_on_composite_pk(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer, primary_key=True, autoincrement=True),
            Column("y", Integer, primary_key=True),
        )
        assert_raises_message(
            exc.CompileError,
            "SQLite does not support autoincrement for composite",
            CreateTable(t).compile,
            dialect=sqlite.dialect(),
        )

    def test_in_tuple(self):
        compiled = (
            tuple_(column("q"), column("p"))
            .in_([(1, 2), (3, 4)])
            .compile(dialect=sqlite.dialect())
        )
        eq_(str(compiled), "(q, p) IN (__[POSTCOMPILE_param_1])")
        eq_(
            compiled._literal_execute_expanding_parameter(
                "param_1",
                compiled.binds["param_1"],
                compiled.binds["param_1"].value,
            ),
            (
                [
                    ("param_1_1_1", 1),
                    ("param_1_1_2", 2),
                    ("param_1_2_1", 3),
                    ("param_1_2_2", 4),
                ],
                "VALUES (?, ?), (?, ?)",
            ),
        )

    def test_create_table_without_rowid(self):
        m = MetaData()
        tbl = Table(
            "atable", m, Column("id", Integer), sqlite_with_rowid=False
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) WITHOUT ROWID",
        )

    def test_create_table_strict(self):
        m = MetaData()
        table = Table("atable", m, Column("id", Integer), sqlite_strict=True)
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE atable (id INTEGER) STRICT",
        )

    def test_create_table_without_rowid_strict(self):
        m = MetaData()
        table = Table(
            "atable",
            m,
            Column("id", Integer),
            sqlite_with_rowid=False,
            sqlite_strict=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE atable (id INTEGER) WITHOUT ROWID, STRICT",
        )


class OnConflictDDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = sqlite.dialect()

    def test_on_conflict_clause_column_not_null(self):
        c = Column(
            "test", Integer, nullable=False, sqlite_on_conflict_not_null="FAIL"
        )

        self.assert_compile(
            schema.CreateColumn(c),
            "test INTEGER NOT NULL ON CONFLICT FAIL",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_column_many_clause(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "test",
                Integer,
                nullable=False,
                primary_key=True,
                sqlite_on_conflict_not_null="FAIL",
                sqlite_on_conflict_primary_key="IGNORE",
            ),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n ("
            "test INTEGER NOT NULL ON CONFLICT FAIL, "
            "PRIMARY KEY (test) ON CONFLICT IGNORE)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_unique_constraint_from_column(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "x", String(30), unique=True, sqlite_on_conflict_unique="FAIL"
            ),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (x VARCHAR(30), UNIQUE (x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_unique_constraint(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column("id", Integer),
            Column("x", String(30)),
            UniqueConstraint("id", "x", sqlite_on_conflict="FAIL"),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (id INTEGER, x VARCHAR(30), "
            "UNIQUE (id, x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_primary_key(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "id",
                Integer,
                primary_key=True,
                sqlite_on_conflict_primary_key="FAIL",
            ),
            sqlite_autoincrement=True,
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (id INTEGER NOT NULL "
            "PRIMARY KEY ON CONFLICT FAIL AUTOINCREMENT)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_primary_key_constraint_from_column(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "x",
                String(30),
                sqlite_on_conflict_primary_key="FAIL",
                primary_key=True,
            ),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (x VARCHAR(30) NOT NULL, "
            "PRIMARY KEY (x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_check_constraint(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column("id", Integer),
            Column("x", Integer),
            CheckConstraint("id > x", sqlite_on_conflict="FAIL"),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (id INTEGER, x INTEGER, "
            "CHECK (id > x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_check_constraint_from_column(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "x",
                Integer,
                CheckConstraint("x > 1", sqlite_on_conflict="FAIL"),
            ),
        )

        assert_raises_message(
            exc.CompileError,
            "SQLite does not support on conflict "
            "clause for column check constraint",
            CreateTable(t).compile,
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_primary_key_constraint(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column("id", Integer),
            Column("x", String(30)),
            PrimaryKeyConstraint("id", "x", sqlite_on_conflict="FAIL"),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n ("
            "id INTEGER NOT NULL, "
            "x VARCHAR(30) NOT NULL, "
            "PRIMARY KEY (id, x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )


class RegexpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "sqlite"

    def setup_test(self):
        self.table = table(
            "mytable", column("myid", String), column("name", String)
        )

    def _only_on_py38_w_sqlite_39():
        """in python 3.9 and above you can actually do::

            @(testing.requires.python38 + testing.only_on("sqlite > 3.9"))
            def test_determinsitic_parameter(self): ...

        that'll be cool.  until then...

        """
        return testing.requires.python38 + testing.only_on("sqlite >= 3.9")

    @_only_on_py38_w_sqlite_39()
    def test_determinsitic_parameter(self):
        """for #9379, make sure that "deterministic=True" is used when we are
        on python 3.8 with modern SQLite version.

        For the case where we are not on py3.8 or not on modern sqlite version,
        the rest of the test suite confirms that connection still passes.

        """
        e = create_engine("sqlite://")

        @event.listens_for(e, "do_connect", retval=True)
        def _mock_connect(dialect, conn_rec, cargs, cparams):
            conn = e.dialect.loaded_dbapi.connect(":memory:")
            return mock.Mock(wraps=conn)

        c = e.connect()
        eq_(
            c.connection.driver_connection.create_function.mock_calls,
            [
                mock.call("regexp", 2, mock.ANY, deterministic=True),
                mock.call("floor", 1, mock.ANY, deterministic=True),
            ],
        )

    def test_regexp_match(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern"),
            "mytable.myid REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_regexp_match_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid REGEXP mytable.name",
            checkparams={},
        )

    def test_regexp_match_str(self):
        self.assert_compile(
            literal("string").regexp_match(self.table.c.name),
            "? REGEXP mytable.name",
            checkpositional=("string",),
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern"),
            "mytable.myid NOT REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid NOT REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_column(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid NOT REGEXP mytable.name",
            checkparams={},
        )

    def test_not_regexp_match_str(self):
        self.assert_compile(
            ~literal("string").regexp_match(self.table.c.name),
            "? NOT REGEXP mytable.name",
            checkpositional=("string",),
        )

    def test_regexp_replace(self):
        assert_raises_message(
            exc.CompileError,
            "sqlite dialect does not support regular expression replacements",
            self.table.c.myid.regexp_replace("pattern", "rep").compile,
            dialect=sqlite.dialect(),
        )


class OnConflictCompileTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "sqlite"

    @testing.combinations(
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=["id"], index_where=text("name = 'hi'")
            ),
            "ON CONFLICT (id) WHERE name = 'hi' DO NOTHING",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=["id"], index_where="name = 'hi'"
            ),
            exc.ArgumentError,
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=[users.c.id], index_where=users.c.name == "hi"
            ),
            "ON CONFLICT (id) WHERE name = __[POSTCOMPILE_name_1] DO NOTHING",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where=users.c.name == "hi",
            ),
            "ON CONFLICT (id) DO UPDATE SET name = ? " "WHERE users.name = ?",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where=text("name = 'hi'"),
            ),
            "ON CONFLICT (id) DO UPDATE SET name = ? " "WHERE name = 'hi'",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where="name = 'hi'",
            ),
            exc.ArgumentError,
        ),
        argnames="case,expected",
    )
    def test_assorted_arg_coercion(self, users, case, expected):
        stmt = insert(users)

        if isinstance(expected, type) and issubclass(expected, Exception):
            with expect_raises(expected):
                testing.resolve_lambda(case, stmt=stmt, users=users),
        else:
            self.assert_compile(
                testing.resolve_lambda(case, stmt=stmt, users=users),
                f"INSERT INTO users (id, name) VALUES (?, ?) {expected}",
            )

    @testing.combinations("control", "excluded", "dict", argnames="scenario")
    def test_set_excluded(self, scenario, users, users_w_key):
        """test #8014, sending all of .excluded to set"""

        if scenario == "control":

            stmt = insert(users)
            self.assert_compile(
                stmt.on_conflict_do_update(set_=stmt.excluded),
                "INSERT INTO users (id, name) VALUES (?, ?) ON CONFLICT  "
                "DO UPDATE SET id = excluded.id, name = excluded.name",
            )
        else:

            stmt = insert(users_w_key)

            if scenario == "excluded":
                self.assert_compile(
                    stmt.on_conflict_do_update(set_=stmt.excluded),
                    "INSERT INTO users_w_key (id, name) VALUES (?, ?) "
                    "ON CONFLICT  "
                    "DO UPDATE SET id = excluded.id, name = excluded.name",
                )
            else:
                self.assert_compile(
                    stmt.on_conflict_do_update(
                        set_={
                            "id": stmt.excluded.id,
                            "name_keyed": stmt.excluded.name_keyed,
                        }
                    ),
                    "INSERT INTO users_w_key (id, name) VALUES (?, ?) "
                    "ON CONFLICT  "
                    "DO UPDATE SET id = excluded.id, name = excluded.name",
                )

    def test_dont_consume_set_collection(self, users):
        stmt = insert(users).values(
            [
                {
                    "name": "spongebob",
                },
                {
                    "name": "sandy",
                },
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[users.c.name], set_=dict(name=stmt.excluded.name)
        )
        self.assert_compile(
            stmt,
            "INSERT INTO users (name) VALUES (?), (?) "
            "ON CONFLICT (name) DO UPDATE SET name = excluded.name",
        )
        stmt = stmt.returning(users)
        self.assert_compile(
            stmt,
            "INSERT INTO users (name) VALUES (?), (?) "
            "ON CONFLICT (name) DO UPDATE SET name = excluded.name "
            "RETURNING id, name",
        )

    def test_on_conflict_do_update_exotic_targets_six(self, users_xtra):
        users = users_xtra

        unique_partial_index = schema.Index(
            "idx_unique_partial_name",
            users_xtra.c.name,
            users_xtra.c.lets_index_this,
            unique=True,
            sqlite_where=users_xtra.c.lets_index_this == "unique_name",
        )

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=unique_partial_index.columns,
            index_where=unique_partial_index.dialect_options["sqlite"][
                "where"
            ],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        # this test illustrates that the index_where clause can't use
        # bound parameters, where we see below a literal_execute parameter is
        # used (will be sent as literal to the DBAPI).  SQLite otherwise
        # fails here with "(sqlite3.OperationalError) ON CONFLICT clause does
        # not match any PRIMARY KEY or UNIQUE constraint" if sent as a real
        # bind parameter.
        self.assert_compile(
            i,
            "INSERT INTO users_xtra (id, name, login_email, lets_index_this) "
            "VALUES (?, ?, ?, ?) ON CONFLICT (name, lets_index_this) "
            "WHERE lets_index_this = __[POSTCOMPILE_lets_index_this_1] "
            "DO UPDATE "
            "SET name = excluded.name, login_email = excluded.login_email",
        )

    @testing.variation(
        "path", ["unknown_columns", "whereclause", "indexwhere"]
    )
    def test_on_conflict_literal_binds(self, path: testing.Variation):
        """test for #13110"""

        metadata = MetaData()
        table_with_metadata = Table(
            "mytable",
            metadata,
            Column("myid", Integer, primary_key=True),
            Column("name", String(128)),
        )
        goofy_index = Index(
            "goofy_index",
            table_with_metadata.c.name,
            sqlite_where=table_with_metadata.c.name > "m",
        )

        i = insert(table_with_metadata).values(myid=1, name="foo")

        if path.unknown_columns:
            i = i.on_conflict_do_update(
                index_elements=["myid"],
                set_=OrderedDict(
                    [
                        ("name", "I'm a name"),
                        ("other_param", literal("this too")),
                    ]
                ),
            )
            expected = (
                "ON CONFLICT (myid) DO UPDATE SET name = "
                "'I''m a name', other_param = 'this too'"
            )
            warnings = testing.expect_warnings(
                "Additional column names not matching any column keys"
            )
        elif path.whereclause:
            i = i.on_conflict_do_update(
                index_elements=["myid"],
                set_={"name": "I'm a name"},
                where=table_with_metadata.c.name == "foo",
            )
            expected = (
                "ON CONFLICT (myid) DO UPDATE SET name = "
                "'I''m a name' WHERE mytable.name = 'foo'"
            )
            warnings = contextlib.nullcontext()
        elif path.indexwhere:
            i = i.on_conflict_do_update(
                index_elements=["myid"],
                set_={"name": "I'm a name"},
                index_where=goofy_index.dialect_options["sqlite"]["where"],
            )
            warnings = contextlib.nullcontext()
            expected = (
                "ON CONFLICT (myid) WHERE name > 'm' "
                "DO UPDATE SET name = 'I''m a name'"
            )
        else:
            path.fail()

        with warnings:
            self.assert_compile(
                i,
                "INSERT INTO mytable (myid, name) VALUES (1, 'foo')"
                f" {expected}",
                {},
                literal_binds=True,
            )

    @testing.fixture
    def users(self):
        metadata = MetaData()
        return Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

    @testing.fixture
    def users_w_key(self):
        metadata = MetaData()
        return Table(
            "users_w_key",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), key="name_keyed"),
        )

    @testing.fixture
    def users_xtra(self):
        metadata = MetaData()
        return Table(
            "users_xtra",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("login_email", String(50)),
            Column("lets_index_this", String(50)),
        )
