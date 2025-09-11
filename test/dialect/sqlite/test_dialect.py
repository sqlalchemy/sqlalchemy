"""SQLite-specific tests."""

import os

from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Computed
from sqlalchemy import create_engine
from sqlalchemy import DefaultClause
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import pool
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import types as sqltypes
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.sqlite import base as sqlite
from sqlalchemy.dialects.sqlite import pysqlite as pysqlite_dialect
from sqlalchemy.engine.url import make_url
from sqlalchemy.schema import CreateTable
from sqlalchemy.schema import FetchedValue
from sqlalchemy.sql.ddl import CreateTableAs
from sqlalchemy.sql.ddl import DropTable
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import combinations
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.types import Boolean
from sqlalchemy.types import Integer
from sqlalchemy.types import String


def exec_sql(engine, sql, *args, **kwargs):
    # TODO: convert all tests to not use this
    with engine.begin() as conn:
        conn.exec_driver_sql(sql, *args, **kwargs)


class DefaultsTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = "sqlite"
    __backend__ = True

    def test_default_reflection(self, connection, metadata):
        specs = [
            (String(3), '"foo"'),
            (sqltypes.NUMERIC(10, 2), "100.50"),
            (Integer, "5"),
            (Boolean, "False"),
        ]
        columns = [
            Column("c%i" % (i + 1), t[0], server_default=text(t[1]))
            for (i, t) in enumerate(specs)
        ]
        Table("t_defaults", metadata, *columns)
        metadata.create_all(connection)
        m2 = MetaData()
        rt = Table("t_defaults", m2, autoload_with=connection)
        expected = [c[1] for c in specs]
        for i, reflected in enumerate(rt.c):
            eq_(str(reflected.server_default.arg), expected[i])

    @testing.exclude(
        "sqlite",
        "<",
        (3, 3, 8),
        "sqlite3 changesets 3353 and 3440 modified "
        "behavior of default displayed in pragma "
        "table_info()",
    )
    def test_default_reflection_2(self):
        db = testing.db
        m = MetaData()
        expected = ["'my_default'", "0"]
        table = """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
            )"""
        try:
            exec_sql(db, table)
            rt = Table("r_defaults", m, autoload_with=db)
            for i, reflected in enumerate(rt.c):
                eq_(str(reflected.server_default.arg), expected[i])
        finally:
            exec_sql(db, "DROP TABLE r_defaults")

    def test_default_reflection_3(self):
        db = testing.db
        table = """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
            )"""
        try:
            exec_sql(db, table)
            m1 = MetaData()
            t1 = Table("r_defaults", m1, autoload_with=db)
            exec_sql(db, "DROP TABLE r_defaults")
            t1.create(db)
            m2 = MetaData()
            t2 = Table("r_defaults", m2, autoload_with=db)
            self.assert_compile(
                CreateTable(t2),
                "CREATE TABLE r_defaults (data VARCHAR(40) "
                "DEFAULT 'my_default', val INTEGER DEFAULT 0 "
                "NOT NULL)",
            )
        finally:
            exec_sql(db, "DROP TABLE r_defaults")

    @testing.provide_metadata
    def test_boolean_default(self):
        t = Table(
            "t",
            self.metadata,
            Column("x", Boolean, server_default=sql.false()),
        )
        t.create(testing.db)
        with testing.db.begin() as conn:
            conn.execute(t.insert())
            conn.execute(t.insert().values(x=True))
            eq_(
                conn.execute(t.select().order_by(t.c.x)).fetchall(),
                [(False,), (True,)],
            )

    @testing.provide_metadata
    def test_function_default(self):
        t = Table(
            "t",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("x", String(), server_default=func.lower("UPPERCASE")),
        )
        t.create(testing.db)
        with testing.db.begin() as conn:
            conn.execute(t.insert())
            conn.execute(t.insert().values(x="foobar"))
            eq_(
                conn.execute(select(t.c.x).order_by(t.c.id)).fetchall(),
                [("uppercase",), ("foobar",)],
            )

    @testing.provide_metadata
    def test_expression_with_function_default(self):
        t = Table(
            "t",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer(), server_default=func.abs(-5) + 17),
        )
        t.create(testing.db)
        with testing.db.begin() as conn:
            conn.execute(t.insert())
            conn.execute(t.insert().values(x=35))
            eq_(
                conn.execute(select(t.c.x).order_by(t.c.id)).fetchall(),
                [(22,), (35,)],
            )

    def test_old_style_default(self):
        """test non-quoted integer value on older sqlite pragma"""

        dialect = sqlite.dialect()
        info = dialect._get_column_info(
            "foo", "INTEGER", False, 3, False, False, False, None
        )
        eq_(info["default"], "3")


class DialectTest(
    fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL
):
    __only_on__ = "sqlite"
    __backend__ = True

    def test_3_7_16_warning(self):
        with expect_warnings(
            r"SQLite version \(3, 2, 8\) is older than 3.7.16, and "
            "will not support right nested joins"
        ):
            sqlite.dialect(
                dbapi=mock.Mock(
                    version_info=(2, 6, 0), sqlite_version_info=(3, 2, 8)
                )
            )

    @testing.only_on("sqlite+pysqlcipher")
    def test_pysqlcipher_connects(self):
        """test #6586"""
        str_url = str(testing.db.url)
        e = create_engine(str_url)

        with e.connect() as conn:
            eq_(conn.scalar(text("select 1")), 1)

    @testing.provide_metadata
    def test_extra_reserved_words(self, connection):
        """Tests reserved words in identifiers.

        'true', 'false', and 'column' are undocumented reserved words
        when used as column identifiers (as of 3.5.1).  Covering them
        here to ensure they remain in place if the dialect's
        reserved_words set is updated in the future."""

        t = Table(
            "reserved",
            self.metadata,
            Column("safe", Integer),
            Column("true", Integer),
            Column("false", Integer),
            Column("column", Integer),
            Column("exists", Integer),
        )
        self.metadata.create_all(connection)
        connection.execute(t.insert(), dict(safe=1))
        result = connection.execute(t.select())
        eq_(list(result), [(1, None, None, None, None)])

    @testing.provide_metadata
    def test_quoted_identifiers_functional_one(self):
        """Tests autoload of tables created with quoted column names."""

        metadata = self.metadata
        exec_sql(
            testing.db,
            """CREATE TABLE "django_content_type" (
            "id" integer NOT NULL PRIMARY KEY,
            "django_stuff" text NULL
        )
        """,
        )
        exec_sql(
            testing.db,
            """
        CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "content_type_id" integer NULL
                    REFERENCES "django_content_type" ("id"),
            "object_id" text NULL,
            "change_message" text NOT NULL
        )
        """,
        )
        table1 = Table("django_admin_log", metadata, autoload_with=testing.db)
        table2 = Table(
            "django_content_type", metadata, autoload_with=testing.db
        )
        j = table1.join(table2)
        assert j.onclause.compare(table1.c.content_type_id == table2.c.id)

    @testing.provide_metadata
    def test_quoted_identifiers_functional_two(self):
        """test the edgiest of edge cases, quoted table/col names
        that start and end with quotes.

        SQLite claims to have fixed this in
        https://www.sqlite.org/src/info/600482d161, however
        it still fails if the FK points to a table name that actually
        has quotes as part of its name.

        """

        metadata = self.metadata
        exec_sql(
            testing.db,
            r'''CREATE TABLE """a""" (
            """id""" integer NOT NULL PRIMARY KEY
        )
        ''',
        )

        # unfortunately, still can't do this; sqlite quadruples
        # up the quotes on the table name here for pragma foreign_key_list
        # exec_sql(testing.db,r'''
        # CREATE TABLE """b""" (
        #    """id""" integer NOT NULL PRIMARY KEY,
        #    """aid""" integer NULL
        #           REFERENCES """a""" ("""id""")
        # )
        # ''')

        table1 = Table(r'"a"', metadata, autoload_with=testing.db)
        assert '"id"' in table1.c

    @testing.provide_metadata
    def test_description_encoding(self, connection):
        t = Table(
            "x",
            self.metadata,
            Column("méil", Integer, primary_key=True),
            Column("\u6e2c\u8a66", Integer),
        )
        self.metadata.create_all(testing.db)

        result = connection.execute(t.select())
        assert "méil" in result.keys()
        assert "\u6e2c\u8a66" in result.keys()

    def test_pool_class(self):
        e = create_engine("sqlite+pysqlite://")
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine("sqlite+pysqlite:///:memory:")
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine(
            "sqlite+pysqlite:///file:foo.db?mode=memory&uri=true"
        )
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine("sqlite+pysqlite:///foo.db")
        # changed as of 2.0 #7490
        assert e.pool.__class__ is pool.QueuePool

    @combinations(
        (
            "sqlite:///foo.db",  # file path is absolute
            ([os.path.abspath("foo.db")], {"check_same_thread": False}),
        ),
        (
            "sqlite:////abs/path/to/foo.db",
            (
                [os.path.abspath("/abs/path/to/foo.db")],
                {"check_same_thread": False},
            ),
        ),
        ("sqlite://", ([":memory:"], {"check_same_thread": True})),
        (
            "sqlite:///?check_same_thread=true",
            ([":memory:"], {"check_same_thread": True}),
        ),
        (
            "sqlite:///file:path/to/database?"
            "check_same_thread=true&timeout=10&mode=ro&nolock=1&uri=true",
            (
                ["file:path/to/database?mode=ro&nolock=1"],
                {"check_same_thread": True, "timeout": 10.0, "uri": True},
            ),
        ),
        (
            "sqlite:///file:path/to/database?mode=ro&uri=true",
            (
                ["file:path/to/database?mode=ro"],
                {"uri": True, "check_same_thread": False},
            ),
        ),
        (
            "sqlite:///file:path/to/database?uri=true",
            (
                ["file:path/to/database"],
                {"uri": True, "check_same_thread": False},
            ),
        ),
    )
    def test_connect_args(self, url, expected):
        """test create_connect_args scenarios including support for uri=True"""

        d = pysqlite_dialect.dialect()
        url = make_url(url)
        eq_(d.create_connect_args(url), expected)

    @testing.combinations(
        ("no_persisted", "", "ignore"),
        ("persisted_none", "", None),
        ("persisted_true", " STORED", True),
        ("persisted_false", " VIRTUAL", False),
        id_="iaa",
    )
    def test_column_computed(self, text, persisted):
        m = MetaData()
        kwargs = {"persisted": persisted} if persisted != "ignore" else {}
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", **kwargs)),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (x INTEGER,"
            " y INTEGER GENERATED ALWAYS AS (x + 2)%s)" % text,
        )

    @testing.combinations(
        (func.localtimestamp(),),
        (func.now(),),
        (func.char_length("test"),),
        (func.aggregate_strings("abc", ","),),
        argnames="fn",
    )
    def test_builtin_functions_roundtrip(self, fn, connection):
        connection.execute(select(fn))


class AttachedDBTest(fixtures.TablesTest):
    __only_on__ = "sqlite"
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(self, metadata):
        meta = metadata

        Table("created", meta, Column("foo", Integer), Column("bar", String))
        Table("local_only", meta, Column("q", Integer), Column("p", Integer))

        Table(
            "created",
            meta,
            Column("id", Integer),
            Column("name", String),
            schema="test_schema",
        )

        Table(
            "another_created",
            meta,
            Column("bat", Integer),
            Column("hoho", String),
            schema="test_schema",
        )

    def test_no_tables(self, connection):
        tt = self.tables("test_schema.created", "test_schema.another_created")
        for t in tt:
            t.drop(connection)
        insp = inspect(connection)
        eq_(insp.get_table_names("test_schema"), [])

    def test_column_names(self, connection):
        insp = inspect(connection)
        eq_(
            [
                d["name"]
                for d in insp.get_columns("created", schema="test_schema")
            ],
            ["id", "name"],
        )
        eq_(
            [d["name"] for d in insp.get_columns("created", schema=None)],
            ["foo", "bar"],
        )

        with expect_raises(exc.NoSuchTableError):
            insp.get_columns("nonexistent", schema="test_schema")

        with expect_raises(exc.NoSuchTableError):
            insp.get_columns("another_created", schema=None)

        with expect_raises(exc.NoSuchTableError):
            insp.get_columns("local_only", schema="test_schema")

        eq_([d["name"] for d in insp.get_columns("local_only")], ["q", "p"])

    def test_table_names_present(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_table_names("test_schema")),
            {"created", "another_created"},
        )

    def test_table_names_system(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_table_names("test_schema")),
            {"created", "another_created"},
        )

    def test_schema_names(self, connection):
        insp = inspect(connection)
        eq_(insp.get_schema_names(), ["main", "test_schema"])

        # implicitly creates a "temp" schema
        connection.exec_driver_sql("select * from sqlite_temp_master")

        # we're not including it
        insp = inspect(connection)
        eq_(insp.get_schema_names(), ["main", "test_schema"])

    def test_reflect_system_table(self, connection):
        meta = MetaData()
        alt_master = Table(
            "sqlite_master",
            meta,
            autoload_with=connection,
            schema="test_schema",
        )
        assert len(alt_master.c) > 0

    def test_reflect_user_table(self, connection):
        m2 = MetaData()
        c2 = Table("created", m2, autoload_with=connection)
        eq_(len(c2.c), 2)

    def test_crud(self, connection):
        (ct,) = self.tables("test_schema.created")
        connection.execute(ct.insert(), {"id": 1, "name": "foo"})
        eq_(connection.execute(ct.select()).fetchall(), [(1, "foo")])

        connection.execute(ct.update(), {"id": 2, "name": "bar"})
        eq_(connection.execute(ct.select()).fetchall(), [(2, "bar")])
        connection.execute(ct.delete())
        eq_(connection.execute(ct.select()).fetchall(), [])

    def test_col_targeting(self, connection):
        (ct,) = self.tables("test_schema.created")

        connection.execute(ct.insert(), {"id": 1, "name": "foo"})
        row = connection.execute(ct.select()).first()
        eq_(row._mapping["id"], 1)
        eq_(row._mapping["name"], "foo")

    def test_col_targeting_union(self, connection):
        (ct,) = self.tables("test_schema.created")
        connection.execute(ct.insert(), {"id": 1, "name": "foo"})
        row = connection.execute(ct.select().union(ct.select())).first()
        eq_(row._mapping["id"], 1)
        eq_(row._mapping["name"], "foo")


class CreateTableAsDDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = sqlite.dialect()

    @testing.fixture
    def src_table(self):
        return table("src", column("id"), column("name"))

    @testing.fixture
    def src_two_tables(self):
        a = table("a", column("id"), column("name"))
        b = table("b", column("id"), column("name"))
        return a, b

    def test_schema_main(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst",
            schema="main",
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE main.dst AS SELECT src.id FROM src",
        )

    def test_temporary_no_schema(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id, src.c.name).select_from(src),
            "dst",
            temporary=True,
        )
        self.assert_compile(
            stmt,
            "CREATE TEMPORARY TABLE dst AS "
            "SELECT src.id, src.name FROM src",
        )

    def test_select_shape_where_order_limit(self, src_table):
        src = src_table
        sel = (
            select(src.c.id, src.c.name)
            .select_from(src)
            .where(src.c.id > literal(10))
            .order_by(src.c.name)
            .limit(5)
            .offset(0)
        )
        stmt = CreateTableAs(sel, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT src.id, src.name FROM src "
            "WHERE src.id > 10 ORDER BY src.name LIMIT 5 OFFSET 0",
        )

    def test_inline_binds(self, src_table):
        src = src_table
        sel = select(
            literal(1).label("x"), literal("a").label("y")
        ).select_from(src)
        stmt = CreateTableAs(sel, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS SELECT 1 AS x, 'a' AS y FROM src",
        )

    def test_explicit_temp_schema_without_keyword(self, src_table):
        # When not using temporary but schema is temp (any case), qualify
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst",
            schema="TEMP",
        )
        self.assert_compile(
            stmt,
            'CREATE TABLE "TEMP".dst AS SELECT src.id FROM src',
        )

    def test_if_not_exists(self, src_table):
        src = src_table
        stmt = CreateTableAs(
            select(src.c.id, src.c.name).select_from(src),
            "dst",
            if_not_exists=True,
        )
        self.assert_compile(
            stmt,
            "CREATE TABLE IF NOT EXISTS dst AS "
            "SELECT src.id, src.name FROM src",
        )

    def test_union_all_smoke(self, src_two_tables):
        # Proves CTAS wraps a UNION ALL and preserves compound ordering.
        a, b = src_two_tables
        u = (
            select(a.c.id)
            .select_from(a)
            .union_all(select(b.c.id).select_from(b))
            .order_by("id")  # order-by on the compound
            .limit(3)
        )
        stmt = CreateTableAs(u, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT a.id FROM a UNION ALL SELECT b.id FROM b "
            "ORDER BY id LIMIT 3 OFFSET 0",
        )

    def test_cte_smoke(self, src_two_tables):
        # Proves CTAS works with a WITH-CTE wrapper and labeled column.
        a, _ = src_two_tables
        cte = select(a.c.id.label("aid")).select_from(a).cte("u")
        stmt = CreateTableAs(select(cte.c.aid), "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "WITH u AS (SELECT a.id AS aid FROM a) "
            "SELECT u.aid FROM u",
        )

    def test_union_all_with_inlined_literals_smoke(self, src_two_tables):
        # Proves literal_binds=True behavior applies across branches.
        a, b = src_two_tables
        u = (
            select(literal(1).label("x"))
            .select_from(a)
            .union_all(select(literal("b").label("x")).select_from(b))
        )
        stmt = CreateTableAs(u, "dst")
        self.assert_compile(
            stmt,
            "CREATE TABLE dst AS "
            "SELECT 1 AS x FROM a UNION ALL SELECT 'b' AS x FROM b",
        )


class CreateTableAsSQLiteBehavior(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @testing.fixture
    def ctas_manager(self, connection):
        """Executes CreateTableAs and drops them after the test"""

        created_tables = []

        def execute_ctas(stmt: CreateTableAs):
            connection.execute(stmt)
            created_tables.append(stmt.generated_table)
            return stmt

        yield execute_ctas

        for t in created_tables:
            connection.execute(DropTable(t, if_exists=True))

    @testing.fixture
    def source_table(self, connection):
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS src (
                id INTEGER PRIMARY KEY,
                name TEXT
            )"""
        )
        connection.exec_driver_sql(
            "INSERT INTO src (name) VALUES ('a'), ('b')"
        )
        yield table("src", column("id"), column("name"))
        connection.exec_driver_sql("DROP TABLE IF EXISTS src")

    @testing.fixture
    def seeded_tables(self, connection):
        connection.exec_driver_sql("CREATE TABLE a (id INTEGER)")
        connection.exec_driver_sql("CREATE TABLE b (id INTEGER)")

        def seed(a_values, b_values):
            if a_values:
                stmt_a = text("INSERT INTO a (id) VALUES (:v)")
                connection.execute(stmt_a, [{"v": v} for v in a_values])
            if b_values:
                stmt_b = text("INSERT INTO b (id) VALUES (:v)")
                connection.execute(stmt_b, [{"v": v} for v in b_values])

        yield seed

        connection.exec_driver_sql("DROP TABLE a")
        connection.exec_driver_sql("DROP TABLE b")

    def test_create_table_as_creates_table_and_copies_rows(
        self, connection, source_table, ctas_manager
    ):
        src = source_table
        stmt = CreateTableAs(
            select(src.c.id, src.c.name).select_from(src),
            "dst",
        )
        ctas_manager(stmt)

        insp = inspect(connection)
        cols = insp.get_columns("dst")
        assert [c["name"] for c in cols] == ["id", "name"]

        # In SQLite CREATE TABLE AS does NOT carry over PK/constraints
        pk = insp.get_pk_constraint("dst")["constrained_columns"]
        assert pk == []

        # data copied
        count = connection.exec_driver_sql("SELECT COUNT(*) FROM dst").scalar()
        assert count == 2

    def test_if_not_exists_does_not_error(
        self, connection, source_table, ctas_manager
    ):
        src = source_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst",
            if_not_exists=True,
        )
        # first run creates; second run should not error
        ctas_manager(stmt)
        ctas_manager(stmt)

        exists = connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='dst'"
        ).fetchall()
        assert exists

    def test_temporary_with_temp_schema_ok(
        self, connection, source_table, ctas_manager
    ):
        src = source_table
        stmt = CreateTableAs(
            select(src.c.id).select_from(src),
            "dst_tmp",
            temporary=True,
            schema="temp",  # accepted; still emits CREATE TEMPORARY TABLE ...
        )
        ctas_manager(stmt)

        # verify it was created as a temp table
        assert (
            connection.exec_driver_sql(
                "SELECT name FROM sqlite_temp_master "
                "WHERE type='table' AND name='dst_tmp'"
            ).fetchone()
            is not None
        )

    def test_literal_inlining_inside_select(
        self, connection, source_table, ctas_manager
    ):
        src = source_table
        sel = select(
            (src.c.id + 1).label("id2"),
            literal("x").label("tag"),
        ).select_from(src)

        stmt = CreateTableAs(sel, "dst2")
        ctas_manager(stmt)

        rows = connection.exec_driver_sql(
            "SELECT COUNT(*), MIN(tag), MAX(tag) FROM dst2"
        ).fetchone()
        assert rows[0] == 2 and rows[1] == "x" and rows[2] == "x"

    def test_create_table_as_with_bind_param_executes(
        self, connection, source_table, ctas_manager
    ):
        src = source_table
        sel = (
            select(src.c.id, src.c.name)
            .select_from(src)
            .where(src.c.name == bindparam("p", value="a"))
        )

        stmt = CreateTableAs(sel, "dst_bind")
        ctas_manager(stmt)

        rows = connection.exec_driver_sql(
            "SELECT COUNT(*), MIN(name), MAX(name) FROM dst_bind"
        ).fetchone()
        assert rows[0] == 1 and rows[1] == "a" and rows[2] == "a"

    def test_compound_select_smoke(
        self, connection, seeded_tables, ctas_manager
    ):
        # UNION ALL + ORDER/LIMIT survives inside CTAS
        seeded_tables(a_values=[1, 3], b_values=[2, 4])

        sel = (
            select(text("id"))
            .select_from(text("a"))
            .union_all(select(text("id")).select_from(text("b")))
            .order_by(text("id"))
            .limit(3)
        )
        stmt = CreateTableAs(sel, "dst_union")
        ctas_manager(stmt)

        vals = [
            r[0]
            for r in connection.exec_driver_sql(
                "SELECT id FROM dst_union ORDER BY id"
            ).fetchall()
        ]
        assert vals == [1, 2, 3]


class InsertTest(fixtures.TestBase, AssertsExecutionResults):
    """Tests inserts and autoincrement."""

    __only_on__ = "sqlite"
    __backend__ = True

    # empty insert was added as of sqlite 3.3.8.

    def _test_empty_insert(self, connection, table, expect=1):
        try:
            table.create(connection)
            for wanted in expect, expect * 2:
                connection.execute(table.insert())
                rows = connection.execute(table.select()).fetchall()
                eq_(len(rows), wanted)
        finally:
            table.drop(connection)

    def test_empty_insert_pk1(self, connection):
        self._test_empty_insert(
            connection,
            Table(
                "a",
                MetaData(),
                Column("id", Integer, primary_key=True),
            ),
        )

    def test_empty_insert_pk2(self, connection):
        # now warns due to [ticket:3216]

        with expect_warnings(
            "Column 'b.x' is marked as a member of the "
            "primary key for table 'b'",
            "Column 'b.y' is marked as a member of the "
            "primary key for table 'b'",
        ):
            assert_raises(
                exc.IntegrityError,
                self._test_empty_insert,
                connection,
                Table(
                    "b",
                    MetaData(),
                    Column("x", Integer, primary_key=True),
                    Column("y", Integer, primary_key=True),
                ),
            )

    def test_empty_insert_pk2_fv(self, connection):
        assert_raises(
            exc.DBAPIError,
            self._test_empty_insert,
            connection,
            Table(
                "b",
                MetaData(),
                Column(
                    "x",
                    Integer,
                    primary_key=True,
                    server_default=FetchedValue(),
                ),
                Column(
                    "y",
                    Integer,
                    primary_key=True,
                    server_default=FetchedValue(),
                ),
            ),
        )

    def test_empty_insert_pk3(self, connection):
        # now warns due to [ticket:3216]
        with expect_warnings(
            "Column 'c.x' is marked as a member of the primary key for table"
        ):
            assert_raises(
                exc.IntegrityError,
                self._test_empty_insert,
                connection,
                Table(
                    "c",
                    MetaData(),
                    Column("x", Integer, primary_key=True),
                    Column(
                        "y", Integer, DefaultClause("123"), primary_key=True
                    ),
                ),
            )

    def test_empty_insert_pk3_fv(self, connection):
        assert_raises(
            exc.DBAPIError,
            self._test_empty_insert,
            connection,
            Table(
                "c",
                MetaData(),
                Column(
                    "x",
                    Integer,
                    primary_key=True,
                    server_default=FetchedValue(),
                ),
                Column("y", Integer, DefaultClause("123"), primary_key=True),
            ),
        )

    def test_empty_insert_pk4(self, connection):
        self._test_empty_insert(
            connection,
            Table(
                "d",
                MetaData(),
                Column("x", Integer, primary_key=True),
                Column("y", Integer, DefaultClause("123")),
            ),
        )

    def test_empty_insert_nopk1(self, connection):
        self._test_empty_insert(
            connection, Table("e", MetaData(), Column("id", Integer))
        )

    def test_empty_insert_nopk2(self, connection):
        self._test_empty_insert(
            connection,
            Table(
                "f",
                MetaData(),
                Column("x", Integer),
                Column("y", Integer),
            ),
        )

    @testing.provide_metadata
    def test_inserts_with_spaces(self, connection):
        tbl = Table(
            "tbl",
            self.metadata,
            Column("with space", Integer),
            Column("without", Integer),
        )
        tbl.create(connection)
        connection.execute(tbl.insert(), {"without": 123})
        eq_(connection.execute(tbl.select()).fetchall(), [(None, 123)])
        connection.execute(tbl.insert(), {"with space": 456})
        eq_(
            connection.execute(tbl.select()).fetchall(),
            [(None, 123), (456, None)],
        )


def full_text_search_missing():
    """Test if full text search is not implemented and return False if
    it is and True otherwise."""

    try:
        exec_sql(testing.db, "CREATE VIRTUAL TABLE t using FTS3;")
        exec_sql(testing.db, "DROP TABLE t;")
        return False
    except Exception:
        return True


metadata = cattable = matchtable = None


class MatchTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = "sqlite"
    __skip_if__ = (full_text_search_missing,)
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData()
        exec_sql(
            testing.db,
            """
        CREATE VIRTUAL TABLE cattable using FTS3 (
            id INTEGER NOT NULL,
            description VARCHAR(50),
            PRIMARY KEY (id)
        )
        """,
        )
        cattable = Table("cattable", metadata, autoload_with=testing.db)
        exec_sql(
            testing.db,
            """
        CREATE VIRTUAL TABLE matchtable using FTS3 (
            id INTEGER NOT NULL,
            title VARCHAR(200),
            category_id INTEGER NOT NULL,
            PRIMARY KEY (id)
        )
        """,
        )
        matchtable = Table("matchtable", metadata, autoload_with=testing.db)
        with testing.db.begin() as conn:
            metadata.create_all(conn)

            conn.execute(
                cattable.insert(),
                [
                    {"id": 1, "description": "Python"},
                    {"id": 2, "description": "Ruby"},
                ],
            )
            conn.execute(
                matchtable.insert(),
                [
                    {
                        "id": 1,
                        "title": "Agile Web Development with Rails",
                        "category_id": 2,
                    },
                    {"id": 2, "title": "Dive Into Python", "category_id": 1},
                    {
                        "id": 3,
                        "title": "Programming Matz's Ruby",
                        "category_id": 2,
                    },
                    {
                        "id": 4,
                        "title": "The Definitive Guide to Django",
                        "category_id": 1,
                    },
                    {
                        "id": 5,
                        "title": "Python in a Nutshell",
                        "category_id": 1,
                    },
                ],
            )

    @classmethod
    def teardown_test_class(cls):
        metadata.drop_all(testing.db)

    def test_expression(self):
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "matchtable.title MATCH ?",
            dialect=sqlite.dialect(),
        )

    def test_simple_match(self, connection):
        results = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_prefix_match(self, connection):
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match("nut*"))
        ).fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self, connection):
        results2 = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("nutshell OR ruby"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self, connection):
        results2 = connection.execute(
            matchtable.select().where(
                matchtable.c.title.match("python nutshell")
            )
        ).fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self, connection):
        results = connection.execute(
            matchtable.select()
            .where(
                and_(
                    cattable.c.id == matchtable.c.category_id,
                    cattable.c.description.match("Ruby"),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3], [r.id for r in results])


class AutoIncrementTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_sqlite_autoincrement(self):
        table = Table(
            "autoinctable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=None),
            sqlite_autoincrement=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE autoinctable (id INTEGER NOT "
            "NULL PRIMARY KEY AUTOINCREMENT, x INTEGER)",
            dialect=sqlite.dialect(),
        )

    def test_sqlite_autoincrement_constraint(self):
        table = Table(
            "autoinctable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=None),
            UniqueConstraint("x"),
            sqlite_autoincrement=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE autoinctable (id INTEGER NOT "
            "NULL PRIMARY KEY AUTOINCREMENT, x "
            "INTEGER, UNIQUE (x))",
            dialect=sqlite.dialect(),
        )

    def test_sqlite_no_autoincrement(self):
        table = Table(
            "noautoinctable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=None),
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE noautoinctable (id INTEGER "
            "NOT NULL, x INTEGER, PRIMARY KEY (id))",
            dialect=sqlite.dialect(),
        )

    def test_sqlite_autoincrement_int_affinity(self):
        class MyInteger(sqltypes.TypeDecorator):
            impl = Integer
            cache_ok = True

        table = Table(
            "autoinctable",
            MetaData(),
            Column("id", MyInteger, primary_key=True),
            sqlite_autoincrement=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE autoinctable (id INTEGER NOT "
            "NULL PRIMARY KEY AUTOINCREMENT)",
            dialect=sqlite.dialect(),
        )


class SavepointTest(fixtures.TablesTest):
    """test that savepoints work when we use the correct event setup"""

    __only_on__ = "sqlite"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String),
        )

    @classmethod
    def setup_bind(cls):
        engine = engines.testing_engine(options={"scope": "class"})

        @event.listens_for(engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable pysqlite's emitting of the BEGIN statement entirely.
            # also stops it from emitting COMMIT before any DDL.
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            conn.exec_driver_sql("BEGIN")

        return engine

    def test_nested_subtransaction_rollback(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        trans2.rollback()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (3,)],
        )
        connection.close()

    def test_nested_subtransaction_commit(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        trans2.commit()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,), (3,)],
        )
        connection.close()
