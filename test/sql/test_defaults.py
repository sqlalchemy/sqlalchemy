import datetime
import itertools

import sqlalchemy as sa
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import DateTime
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import Unicode
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import literal_column
from sqlalchemy.sql import select
from sqlalchemy.sql import text
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.types import TypeDecorator
from sqlalchemy.types import TypeEngine
from sqlalchemy.util import b
from sqlalchemy.util import u


class DDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_string(self):
        # note: that the datatype is an Integer here doesn't matter,
        # the server_default is interpreted independently of the
        # column's datatype.
        m = MetaData()
        t = Table("t", m, Column("x", Integer, server_default="5"))
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT '5')"
        )

    def test_string_w_quotes(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer, server_default="5'6"))
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT '5''6')"
        )

    def test_text(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer, server_default=text("5 + 8")))
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT 5 + 8)"
        )

    def test_text_w_quotes(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer, server_default=text("5 ' 8")))
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT 5 ' 8)"
        )

    def test_literal_binds_w_quotes(self):
        m = MetaData()
        t = Table(
            "t", m, Column("x", Integer, server_default=literal("5 ' 8"))
        )
        self.assert_compile(
            CreateTable(t), """CREATE TABLE t (x INTEGER DEFAULT '5 '' 8')"""
        )

    def test_text_literal_binds(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column(
                "x", Integer, server_default=text("q + :x1").bindparams(x1=7)
            ),
        )
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT q + 7)"
        )

    def test_sqlexpr(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column(
                "x",
                Integer,
                server_default=literal_column("a") + literal_column("b"),
            ),
        )
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT a + b)"
        )

    def test_literal_binds_plain(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer, server_default=literal("a") + literal("b")),
        )
        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (x INTEGER DEFAULT 'a' || 'b')"
        )

    def test_literal_binds_pgarray(self):
        from sqlalchemy.dialects.postgresql import ARRAY, array

        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", ARRAY(Integer), server_default=array([1, 2, 3])),
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER[] DEFAULT ARRAY[1, 2, 3])",
            dialect="postgresql",
        )


class DefaultObjectTest(fixtures.TestBase):
    def test_bad_arg_signature(self):
        ex_msg = (
            "ColumnDefault Python function takes zero "
            "or one positional arguments"
        )

        def fn1(x, y):
            pass

        def fn2(x, y, z=3):
            pass

        class fn3(object):
            def __init__(self, x, y):
                pass

        class FN4(object):
            def __call__(self, x, y):
                pass

        fn4 = FN4()

        for fn in fn1, fn2, fn3, fn4:
            assert_raises_message(
                sa.exc.ArgumentError, ex_msg, sa.ColumnDefault, fn
            )

    def test_arg_signature(self):
        def fn1():
            pass

        def fn2():
            pass

        def fn3(x=1):
            eq_(x, 1)

        def fn4(x=1, y=2, z=3):
            eq_(x, 1)

        fn5 = list

        class fn6a(object):
            def __init__(self, x):
                eq_(x, "context")

        class fn6b(object):
            def __init__(self, x, y=3):
                eq_(x, "context")

        class FN7(object):
            def __call__(self, x):
                eq_(x, "context")

        fn7 = FN7()

        class FN8(object):
            def __call__(self, x, y=3):
                eq_(x, "context")

        fn8 = FN8()

        for fn in fn1, fn2, fn3, fn4, fn5, fn6a, fn6b, fn7, fn8:
            c = sa.ColumnDefault(fn)
            c.arg("context")

    def _check_default_slots(self, tbl, name, *wanted):
        slots = [
            "default",
            "onupdate",
            "server_default",
            "server_onupdate",
        ]
        col = tbl.c[name]
        for slot in wanted:
            slots.remove(slot)
            assert getattr(col, slot) is not None, getattr(col, slot)
        for slot in slots:
            assert getattr(col, slot) is None, getattr(col, slot)

    def test_py_vs_server_default_detection_one(self):
        has_ = self._check_default_slots

        metadata = MetaData()
        tbl = Table(
            "default_test",
            metadata,
            # python function
            Column("col1", Integer, primary_key=True, default="1"),
            # python literal
            Column(
                "col2",
                String(20),
                default="imthedefault",
                onupdate="im the update",
            ),
            # preexecute expression
            Column(
                "col3",
                Integer,
                default=func.length("abcdef"),
                onupdate=func.length("abcdefghijk"),
            ),
            # SQL-side default from sql expression
            Column("col4", Integer, server_default="1"),
            # SQL-side default from literal expression
            Column("col5", Integer, server_default="1"),
            # preexecute + update timestamp
            Column(
                "col6",
                sa.Date,
                default=datetime.datetime.today,
                onupdate=datetime.datetime.today,
            ),
            Column("boolcol1", sa.Boolean, default=True),
            Column("boolcol2", sa.Boolean, default=False),
            # python function which uses ExecutionContext
            Column(
                "col7",
                Integer,
                default=lambda: 5,
                onupdate=lambda: 10,
            ),
            # python builtin
            Column(
                "col8",
                sa.Date,
                default=datetime.date.today,
                onupdate=datetime.date.today,
            ),
            Column("col9", String(20), default="py", server_default="ddl"),
        )

        has_(tbl, "col1", "default")
        has_(tbl, "col2", "default", "onupdate")
        has_(tbl, "col3", "default", "onupdate")
        has_(tbl, "col4", "server_default")
        has_(tbl, "col5", "server_default")
        has_(tbl, "col6", "default", "onupdate")
        has_(tbl, "boolcol1", "default")
        has_(tbl, "boolcol2", "default")
        has_(tbl, "col7", "default", "onupdate")
        has_(tbl, "col8", "default", "onupdate")
        has_(tbl, "col9", "default", "server_default")

    def test_py_vs_server_default_detection_two(self):
        has_ = self._check_default_slots

        metadata = MetaData()
        ColumnDefault, DefaultClause = sa.ColumnDefault, sa.DefaultClause

        tbl = Table(
            "t2",
            metadata,
            Column("col1", Integer, Sequence("foo")),
            Column(
                "col2", Integer, default=Sequence("foo"), server_default="y"
            ),
            Column("col3", Integer, Sequence("foo"), server_default="x"),
            Column("col4", Integer, ColumnDefault("x"), DefaultClause("y")),
            Column(
                "col5",
                Integer,
                ColumnDefault("x"),
                DefaultClause("y"),
                onupdate="z",
            ),
            Column(
                "col6",
                Integer,
                ColumnDefault("x"),
                server_default="y",
                onupdate="z",
            ),
            Column(
                "col7", Integer, default="x", server_default="y", onupdate="z"
            ),
            Column(
                "col8",
                Integer,
                server_onupdate="u",
                default="x",
                server_default="y",
                onupdate="z",
            ),
        )
        tbl.append_column(
            Column(
                "col4",
                Integer,
                ColumnDefault("x"),
                DefaultClause("y"),
                DefaultClause("y", for_update=True),
            ),
            replace_existing=True,
        )
        has_(tbl, "col1", "default")
        has_(tbl, "col2", "default", "server_default")
        has_(tbl, "col3", "default", "server_default")
        has_(tbl, "col4", "default", "server_default", "server_onupdate")
        has_(tbl, "col5", "default", "server_default", "onupdate")
        has_(tbl, "col6", "default", "server_default", "onupdate")
        has_(tbl, "col7", "default", "server_default", "onupdate")
        has_(
            tbl,
            "col8",
            "default",
            "server_default",
            "onupdate",
            "server_onupdate",
        )

    def test_no_embed_in_sql(self):
        """Using a DefaultGenerator, Sequence, DefaultClause
        in the columns, where clause of a select, or in the values
        clause of insert, update, raises an informative error"""

        t = Table("some_table", MetaData(), Column("id", Integer))
        for const in (
            sa.Sequence("y"),
            sa.ColumnDefault("y"),
            sa.DefaultClause("y"),
        ):
            assert_raises_message(
                sa.exc.ArgumentError,
                r"SQL expression for WHERE/HAVING role expected, "
                r"got (?:Sequence|ColumnDefault|DefaultClause)\('y'.*\)",
                t.select().where,
                const,
            )
            assert_raises_message(
                sa.exc.ArgumentError,
                "SQL expression element expected, got %s"
                % const.__class__.__name__,
                t.insert().values,
                col4=const,
            )
            assert_raises_message(
                sa.exc.ArgumentError,
                "SQL expression element expected, got %s"
                % const.__class__.__name__,
                t.update().values,
                col4=const,
            )


class DefaultRoundTripTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        default_generator = cls.default_generator = {"x": 50}

        def mydefault():
            default_generator["x"] += 1
            return default_generator["x"]

        def myupdate_with_ctx(ctx):
            conn = ctx.connection
            return conn.execute(sa.select(sa.text("13"))).scalar()

        def mydefault_using_connection(ctx):
            conn = ctx.connection
            return conn.execute(sa.select(sa.text("12"))).scalar()

        use_function_defaults = testing.against("postgresql", "mssql")
        is_oracle = testing.against("oracle")

        class MyClass(object):
            @classmethod
            def gen_default(cls, ctx):
                return "hi"

        class MyType(TypeDecorator):
            impl = String(50)

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value = "BIND" + value
                return value

        cls.f = 6
        cls.f2 = 11
        with testing.db.connect() as conn:
            currenttime = cls.currenttime = func.current_date(type_=sa.Date)
            if is_oracle:
                ts = conn.scalar(
                    sa.select(
                        func.trunc(
                            func.current_timestamp(),
                            sa.literal_column("'DAY'"),
                            type_=sa.Date,
                        )
                    )
                )
                currenttime = cls.currenttime = func.trunc(
                    currenttime, sa.literal_column("'DAY'"), type_=sa.Date
                )
                def1 = currenttime
                def2 = func.trunc(
                    sa.text("current_timestamp"),
                    sa.literal_column("'DAY'"),
                    type_=sa.Date,
                )

                deftype = sa.Date
            elif use_function_defaults:
                def1 = currenttime
                deftype = sa.Date
                if testing.against("mssql"):
                    def2 = sa.text("getdate()")
                else:
                    def2 = sa.text("current_date")
                ts = conn.scalar(func.current_date())
            else:
                def1 = def2 = "3"
                ts = 3
                deftype = Integer

            cls.ts = ts

        Table(
            "default_test",
            metadata,
            # python function
            Column("col1", Integer, primary_key=True, default=mydefault),
            # python literal
            Column(
                "col2",
                String(20),
                default="imthedefault",
                onupdate="im the update",
            ),
            # preexecute expression
            Column(
                "col3",
                Integer,
                default=func.length("abcdef"),
                onupdate=func.length("abcdefghijk"),
            ),
            # SQL-side default from sql expression
            Column("col4", deftype, server_default=def1),
            # SQL-side default from literal expression
            Column("col5", deftype, server_default=def2),
            # preexecute + update timestamp
            Column("col6", sa.Date, default=currenttime, onupdate=currenttime),
            Column("boolcol1", sa.Boolean, default=True),
            Column("boolcol2", sa.Boolean, default=False),
            # python function which uses ExecutionContext
            Column(
                "col7",
                Integer,
                default=mydefault_using_connection,
                onupdate=myupdate_with_ctx,
            ),
            # python builtin
            Column(
                "col8",
                sa.Date,
                default=datetime.date.today,
                onupdate=datetime.date.today,
            ),
            # combo
            Column("col9", String(20), default="py", server_default="ddl"),
            # python method w/ context
            Column("col10", String(20), default=MyClass.gen_default),
            # fixed default w/ type that has bound processor
            Column("col11", MyType(), default="foo"),
        )

    def teardown_test(self):
        self.default_generator["x"] = 50

    def test_standalone(self, connection):
        t = self.tables.default_test
        x = connection.execute(t.c.col1.default)
        y = connection.execute(t.c.col2.default)
        z = connection.execute(t.c.col3.default)
        assert 50 <= x <= 57
        eq_(y, "imthedefault")
        eq_(z, self.f)

    def test_insert(self, connection):
        t = self.tables.default_test

        r = connection.execute(t.insert())
        assert r.lastrow_has_defaults()
        eq_(
            set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]),
        )

        r = connection.execute(t.insert().inline())
        assert r.lastrow_has_defaults()
        eq_(
            set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]),
        )

        connection.execute(t.insert())

        ctexec = connection.execute(
            sa.select(self.currenttime.label("now"))
        ).scalar()
        result = connection.execute(t.select().order_by(t.c.col1))
        today = datetime.date.today()
        eq_(
            list(result),
            [
                (
                    x,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                )
                for x in range(51, 54)
            ],
        )

        connection.execute(t.insert(), dict(col9=None))

        # TODO: why are we looking at 'r' when we just executed something
        # else ?
        assert r.lastrow_has_defaults()

        eq_(
            set(r.context.postfetch_cols),
            set([t.c.col3, t.c.col5, t.c.col4, t.c.col6]),
        )

        eq_(
            list(connection.execute(t.select().where(t.c.col1 == 54))),
            [
                (
                    54,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    None,
                    "hi",
                    "BINDfoo",
                )
            ],
        )

    def test_insertmany(self, connection):
        t = self.tables.default_test

        connection.execute(t.insert(), [{}, {}, {}])

        ctexec = connection.scalar(self.currenttime)
        result = connection.execute(t.select().order_by(t.c.col1))
        today = datetime.date.today()
        eq_(
            list(result),
            [
                (
                    51,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
                (
                    52,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
                (
                    53,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
            ],
        )

    @testing.requires.multivalues_inserts
    def test_insert_multivalues(self, connection):
        t = self.tables.default_test

        connection.execute(t.insert().values([{}, {}, {}]))

        ctexec = connection.execute(self.currenttime).scalar()
        result = connection.execute(t.select().order_by(t.c.col1))
        today = datetime.date.today()
        eq_(
            list(result),
            [
                (
                    51,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
                (
                    52,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
                (
                    53,
                    "imthedefault",
                    self.f,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    12,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
            ],
        )

    def test_missing_many_param(self, connection):
        t = self.tables.default_test
        assert_raises_message(
            exc.StatementError,
            "A value is required for bind parameter 'col7', in parameter "
            "group 1",
            connection.execute,
            t.insert(),
            [
                {"col4": 7, "col7": 12, "col8": 19},
                {"col4": 7, "col8": 19},
                {"col4": 7, "col7": 12, "col8": 19},
            ],
        )

    def test_insert_values(self, connection):
        t = self.tables.default_test
        connection.execute(t.insert().values(col3=50))
        result = connection.execute(t.select().order_by(t.c.col1))
        eq_(50, result.first()._mapping["col3"])

    def test_updatemany(self, connection):
        t = self.tables.default_test

        connection.execute(t.insert(), [{}, {}, {}])

        connection.execute(
            t.update().where(t.c.col1 == sa.bindparam("pkval")),
            {"pkval": 51, "col7": None, "col8": None, "boolcol1": False},
        )

        connection.execute(
            t.update().where(t.c.col1 == sa.bindparam("pkval")),
            [{"pkval": 51}, {"pkval": 52}, {"pkval": 53}],
        )

        ctexec = connection.scalar(self.currenttime)
        today = datetime.date.today()
        result = connection.execute(t.select().order_by(t.c.col1))
        eq_(
            list(result),
            [
                (
                    51,
                    "im the update",
                    self.f2,
                    self.ts,
                    self.ts,
                    ctexec,
                    False,
                    False,
                    13,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
                (
                    52,
                    "im the update",
                    self.f2,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    13,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
                (
                    53,
                    "im the update",
                    self.f2,
                    self.ts,
                    self.ts,
                    ctexec,
                    True,
                    False,
                    13,
                    today,
                    "py",
                    "hi",
                    "BINDfoo",
                ),
            ],
        )

    def test_update(self, connection):
        t = self.tables.default_test
        r = connection.execute(t.insert())
        pk = r.inserted_primary_key[0]
        connection.execute(
            t.update().where(t.c.col1 == pk), dict(col4=None, col5=None)
        )
        ctexec = connection.scalar(self.currenttime)
        result = connection.execute(t.select().where(t.c.col1 == pk))
        result = result.first()
        eq_(
            result,
            (
                pk,
                "im the update",
                self.f2,
                None,
                None,
                ctexec,
                True,
                False,
                13,
                datetime.date.today(),
                "py",
                "hi",
                "BINDfoo",
            ),
        )

    def test_update_values(self, connection):
        t = self.tables.default_test
        r = connection.execute(t.insert())
        pk = r.inserted_primary_key[0]
        connection.execute(t.update().where(t.c.col1 == pk).values(col3=55))
        result = connection.execute(t.select().where(t.c.col1 == pk))
        row = result.first()
        eq_(55, row._mapping["col3"])


class FutureDefaultRoundTripTest(
    fixtures.FutureEngineMixin, DefaultRoundTripTest
):

    __backend__ = True


class CTEDefaultTest(fixtures.TablesTest):
    __requires__ = ("ctes", "returning", "ctes_on_dml")
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "q",
            metadata,
            Column("x", Integer, default=2),
            Column("y", Integer, onupdate=5),
            Column("z", Integer),
        )

        Table(
            "p",
            metadata,
            Column("s", Integer),
            Column("t", Integer),
            Column("u", Integer, onupdate=1),
        )

    @testing.combinations(
        ("update", "select", testing.requires.ctes_on_dml),
        ("delete", "select", testing.requires.ctes_on_dml),
        ("insert", "select", testing.requires.ctes_on_dml),
        ("select", "update"),
        ("select", "insert"),
        argnames="a, b",
    )
    def test_a_in_b(self, a, b, connection):
        q = self.tables.q
        p = self.tables.p

        conn = connection
        if a == "delete":
            conn.execute(q.insert().values(y=10, z=1))
            cte = q.delete().where(q.c.z == 1).returning(q.c.z).cte("c")
            expected = None
        elif a == "insert":
            cte = q.insert().values(z=1, y=10).returning(q.c.z).cte("c")
            expected = (2, 10)
        elif a == "update":
            conn.execute(q.insert().values(x=5, y=10, z=1))
            cte = (
                q.update()
                .where(q.c.z == 1)
                .values(x=7)
                .returning(q.c.z)
                .cte("c")
            )
            expected = (7, 5)
        elif a == "select":
            conn.execute(q.insert().values(x=5, y=10, z=1))
            cte = sa.select(q.c.z).cte("c")
            expected = (5, 10)

        if b == "select":
            conn.execute(p.insert().values(s=1))
            stmt = select(p.c.s, cte.c.z).where(p.c.s == cte.c.z)
        elif b == "insert":
            sel = select(1, cte.c.z)
            stmt = (
                p.insert().from_select(["s", "t"], sel).returning(p.c.s, p.c.t)
            )
        elif b == "delete":
            stmt = p.insert().values(s=1, t=cte.c.z).returning(p.c.s, cte.c.z)
        elif b == "update":
            conn.execute(p.insert().values(s=1))
            stmt = (
                p.update()
                .values(t=5)
                .where(p.c.s == cte.c.z)
                .returning(p.c.u, cte.c.z)
            )
        eq_(list(conn.execute(stmt)), [(1, 1)])

        eq_(conn.execute(select(q.c.x, q.c.y)).first(), expected)


class PKDefaultTest(fixtures.TablesTest):
    __requires__ = ("subqueries",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        t2 = Table("t2", metadata, Column("nextid", Integer))

        Table(
            "t1",
            metadata,
            Column(
                "id",
                Integer,
                primary_key=True,
                default=sa.select(func.max(t2.c.nextid)).scalar_subquery(),
            ),
            Column("data", String(30)),
        )

        Table(
            "date_table",
            metadata,
            Column(
                "date_id",
                DateTime(timezone=True),
                default=text("current_timestamp"),
                primary_key=True,
            ),
        )

    @testing.requires.returning
    def test_with_implicit_returning(self):
        self._test(True)

    def test_regular(self):
        self._test(False)

    def _test(self, returning):
        t2, t1, date_table = (
            self.tables.t2,
            self.tables.t1,
            self.tables.date_table,
        )

        if not returning and not testing.db.dialect.implicit_returning:
            engine = testing.db
        else:
            engine = engines.testing_engine(
                options={"implicit_returning": returning}
            )
        with engine.begin() as conn:
            conn.execute(t2.insert(), dict(nextid=1))
            r = conn.execute(t1.insert(), dict(data="hi"))
            eq_((1,), r.inserted_primary_key)

            conn.execute(t2.insert(), dict(nextid=2))
            r = conn.execute(t1.insert(), dict(data="there"))
            eq_((2,), r.inserted_primary_key)

            r = conn.execute(date_table.insert())
            assert isinstance(r.inserted_primary_key[0], datetime.datetime)


class PKIncrementTest(fixtures.TablesTest):
    run_define_tables = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "aitable",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("ai_id_seq", optional=True),
                primary_key=True,
            ),
            Column("int1", Integer),
            Column("str1", String(20)),
        )

    def test_autoincrement(self, connection):
        aitable = self.tables.aitable

        ids = set()
        rs = connection.execute(aitable.insert(), dict(int1=1))
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = connection.execute(aitable.insert(), dict(str1="row 2"))
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = connection.execute(aitable.insert(), dict(int1=3, str1="row 3"))
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = connection.execute(
            aitable.insert().values({"int1": func.length("four")})
        )
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        eq_(
            ids,
            set(
                range(
                    testing.db.dialect.default_sequence_base,
                    testing.db.dialect.default_sequence_base + 4,
                )
            ),
        )

        eq_(
            list(connection.execute(aitable.select().order_by(aitable.c.id))),
            [
                (testing.db.dialect.default_sequence_base, 1, None),
                (testing.db.dialect.default_sequence_base + 1, None, "row 2"),
                (testing.db.dialect.default_sequence_base + 2, 3, "row 3"),
                (testing.db.dialect.default_sequence_base + 3, 4, None),
            ],
        )


class EmptyInsertTest(fixtures.TestBase):
    __backend__ = True

    @testing.fails_on("oracle", "FIXME: unknown")
    def test_empty_insert(self, metadata, connection):
        t1 = Table(
            "t1",
            metadata,
            Column("is_true", Boolean, server_default=("1")),
        )
        metadata.create_all(connection)
        connection.execute(t1.insert())
        eq_(
            1,
            connection.scalar(select(func.count(text("*"))).select_from(t1)),
        )
        eq_(True, connection.scalar(t1.select()))


class AutoIncrementTest(fixtures.TestBase):

    __backend__ = True

    @testing.requires.empty_inserts
    def test_autoincrement_single_col(self, metadata, connection):
        single = Table(
            "single", self.metadata, Column("id", Integer, primary_key=True)
        )
        self.metadata.create_all(connection)

        r = connection.execute(single.insert())
        id_ = r.inserted_primary_key[0]
        eq_(id_, 1)
        eq_(connection.scalar(sa.select(single.c.id)), 1)

    def test_autoinc_detection_no_affinity(self):
        class MyType(TypeDecorator):
            impl = TypeEngine

        assert MyType()._type_affinity is None
        t = Table("x", MetaData(), Column("id", MyType(), primary_key=True))
        assert t._autoincrement_column is None

    def test_autoincrement_ignore_fk(self):
        m = MetaData()
        Table("y", m, Column("id", Integer(), primary_key=True))
        x = Table(
            "x",
            m,
            Column(
                "id",
                Integer(),
                ForeignKey("y.id"),
                autoincrement="ignore_fk",
                primary_key=True,
            ),
        )
        assert x._autoincrement_column is x.c.id

    def test_autoincrement_fk_disqualifies(self):
        m = MetaData()
        Table("y", m, Column("id", Integer(), primary_key=True))
        x = Table(
            "x",
            m,
            Column("id", Integer(), ForeignKey("y.id"), primary_key=True),
        )
        assert x._autoincrement_column is None

    @testing.only_on("sqlite")
    @testing.provide_metadata
    def test_non_autoincrement(self, connection):
        # sqlite INT primary keys can be non-unique! (only for ints)
        nonai = Table(
            "nonaitest",
            self.metadata,
            Column("id", Integer, autoincrement=False, primary_key=True),
            Column("data", String(20)),
        )
        nonai.create(connection)

        # just testing SQLite for now, it passes
        with expect_warnings(".*has no Python-side or server-side default.*"):
            # postgresql + mysql strict will fail on first row,
            # mysql in legacy mode fails on second row
            connection.execute(nonai.insert(), dict(data="row 1"))
            connection.execute(nonai.insert(), dict(data="row 2"))

    @testing.metadata_fixture(ddl="function")
    def dataset_no_autoinc(self, metadata):
        # plain autoincrement/PK table in the actual schema
        Table("x", metadata, Column("set_id", Integer, primary_key=True))

        # for the INSERT use a table with a Sequence
        # and autoincrement=False.  Using a ForeignKey
        # would have the same effect

        some_seq = Sequence("some_seq")

        dataset_no_autoinc = Table(
            "x",
            MetaData(),
            Column(
                "set_id",
                Integer,
                some_seq,
                primary_key=True,
                autoincrement=False,
            ),
        )
        return dataset_no_autoinc

    @testing.skip_if(testing.requires.sequences)
    def test_col_w_optional_sequence_non_autoinc_no_firing(
        self, dataset_no_autoinc, connection
    ):
        """this is testing that a Table which includes a Sequence, when
        run against a DB that does not support sequences, the Sequence
        does not get in the way.

        """
        dataset_no_autoinc.c.set_id.default.optional = True

        connection.execute(dataset_no_autoinc.insert())
        eq_(
            connection.scalar(
                select(func.count("*")).select_from(dataset_no_autoinc)
            ),
            1,
        )

    @testing.fails_if(testing.requires.sequences)
    def test_col_w_nonoptional_sequence_non_autoinc_no_firing(
        self, dataset_no_autoinc, connection
    ):
        """When the sequence is not optional and sequences are supported,
        the test fails because we didn't create the sequence.

        """
        dataset_no_autoinc.c.set_id.default.optional = False

        connection.execute(dataset_no_autoinc.insert())
        eq_(
            connection.scalar(
                select(func.count("*")).select_from(dataset_no_autoinc)
            ),
            1,
        )


class SpecialTypePKTest(fixtures.TestBase):

    """test process_result_value in conjunction with primary key columns.

    Also tests that "autoincrement" checks are against
    column.type._type_affinity, rather than the class of "type" itself.

    """

    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        class MyInteger(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                if value is None:
                    return None
                return int(value[4:])

            def process_result_value(self, value, dialect):
                if value is None:
                    return None
                return "INT_%d" % value

        cls.MyInteger = MyInteger

    @testing.provide_metadata
    def _run_test(self, *arg, **kw):
        metadata = self.metadata
        implicit_returning = kw.pop("implicit_returning", True)
        kw["primary_key"] = True
        if kw.get("autoincrement", True):
            kw["test_needs_autoincrement"] = True
        t = Table(
            "x",
            metadata,
            Column("y", self.MyInteger, *arg, **kw),
            Column("data", Integer),
            implicit_returning=implicit_returning,
        )

        with testing.db.begin() as conn:
            t.create(conn)
            r = conn.execute(t.insert().values(data=5))

            expected_result = "INT_" + str(
                testing.db.dialect.default_sequence_base
                if (arg and isinstance(arg[0], Sequence))
                else 1
            )

            # we don't pre-fetch 'server_default'.
            if "server_default" in kw and (
                not testing.db.dialect.implicit_returning
                or not implicit_returning
            ):
                eq_(r.inserted_primary_key, (None,))
            else:
                eq_(
                    r.inserted_primary_key,
                    (expected_result,),
                )

            eq_(
                conn.execute(t.select()).first(),
                (expected_result, 5),
            )

    def test_plain(self):
        # among other things, tests that autoincrement
        # is enabled.
        self._run_test()

    def test_literal_default_label(self):
        self._run_test(
            default=literal("INT_1", type_=self.MyInteger).label("foo")
        )

    def test_literal_default_no_label(self):
        self._run_test(default=literal("INT_1", type_=self.MyInteger))

    def test_literal_column_default_no_label(self):
        self._run_test(default=literal_column("1", type_=self.MyInteger))

    def test_sequence(self):
        self._run_test(Sequence("foo_seq"))

    def test_text_clause_default_no_type(self):
        self._run_test(default=text("1"))

    def test_server_default(self):
        self._run_test(server_default="1")

    def test_server_default_no_autoincrement(self):
        self._run_test(server_default="1", autoincrement=False)

    def test_clause(self):
        stmt = select(cast("INT_1", type_=self.MyInteger)).scalar_subquery()
        self._run_test(default=stmt)

    @testing.requires.returning
    def test_no_implicit_returning(self):
        self._run_test(implicit_returning=False)

    @testing.requires.returning
    def test_server_default_no_implicit_returning(self):
        self._run_test(server_default="1", autoincrement=False)


class ServerDefaultsOnPKTest(fixtures.TestBase):
    __backend__ = True

    @testing.provide_metadata
    def test_string_default_none_on_insert(self, connection):
        """Test that without implicit returning, we return None for
        a string server default.

        That is, we don't want to attempt to pre-execute "server_default"
        generically - the user should use a Python side-default for a case
        like this.   Testing that all backends do the same thing here.

        """

        metadata = self.metadata
        t = Table(
            "x",
            metadata,
            Column(
                "y", String(10), server_default="key_one", primary_key=True
            ),
            Column("data", String(10)),
            implicit_returning=False,
        )
        metadata.create_all(connection)
        r = connection.execute(t.insert(), dict(data="data"))
        eq_(r.inserted_primary_key, (None,))
        eq_(list(connection.execute(t.select())), [("key_one", "data")])

    @testing.requires.returning
    @testing.provide_metadata
    def test_string_default_on_insert_with_returning(self, connection):
        """With implicit_returning, we get a string PK default back no
        problem."""

        metadata = self.metadata
        t = Table(
            "x",
            metadata,
            Column(
                "y", String(10), server_default="key_one", primary_key=True
            ),
            Column("data", String(10)),
        )
        metadata.create_all(connection)
        r = connection.execute(t.insert(), dict(data="data"))
        eq_(r.inserted_primary_key, ("key_one",))
        eq_(list(connection.execute(t.select())), [("key_one", "data")])

    @testing.provide_metadata
    def test_int_default_none_on_insert(self, connection):
        metadata = self.metadata
        t = Table(
            "x",
            metadata,
            Column("y", Integer, server_default="5", primary_key=True),
            Column("data", String(10)),
            implicit_returning=False,
        )
        assert t._autoincrement_column is None
        metadata.create_all(connection)
        r = connection.execute(t.insert(), dict(data="data"))
        eq_(r.inserted_primary_key, (None,))
        if testing.against("sqlite"):
            eq_(list(connection.execute(t.select())), [(1, "data")])
        else:
            eq_(list(connection.execute(t.select())), [(5, "data")])

    @testing.provide_metadata
    def test_autoincrement_reflected_from_server_default(self, connection):
        metadata = self.metadata
        t = Table(
            "x",
            metadata,
            Column("y", Integer, server_default="5", primary_key=True),
            Column("data", String(10)),
            implicit_returning=False,
        )
        assert t._autoincrement_column is None
        metadata.create_all(connection)

        m2 = MetaData()
        t2 = Table("x", m2, autoload_with=connection, implicit_returning=False)
        assert t2._autoincrement_column is None

    @testing.provide_metadata
    def test_int_default_none_on_insert_reflected(self, connection):
        metadata = self.metadata
        Table(
            "x",
            metadata,
            Column("y", Integer, server_default="5", primary_key=True),
            Column("data", String(10)),
            implicit_returning=False,
        )
        metadata.create_all(connection)

        m2 = MetaData()
        t2 = Table("x", m2, autoload_with=connection, implicit_returning=False)

        r = connection.execute(t2.insert(), dict(data="data"))
        eq_(r.inserted_primary_key, (None,))
        if testing.against("sqlite"):
            eq_(list(connection.execute(t2.select())), [(1, "data")])
        else:
            eq_(list(connection.execute(t2.select())), [(5, "data")])

    @testing.requires.returning
    @testing.provide_metadata
    def test_int_default_on_insert_with_returning(self, connection):
        metadata = self.metadata
        t = Table(
            "x",
            metadata,
            Column("y", Integer, server_default="5", primary_key=True),
            Column("data", String(10)),
        )

        metadata.create_all(connection)
        r = connection.execute(t.insert(), dict(data="data"))
        eq_(r.inserted_primary_key, (5,))
        eq_(list(connection.execute(t.select())), [(5, "data")])


class UnicodeDefaultsTest(fixtures.TestBase):
    __backend__ = True

    def test_no_default(self):
        Column(Unicode(32))

    def test_unicode_default(self):
        default = u("foo")
        Column(Unicode(32), default=default)

    def test_nonunicode_default(self):
        default = b("foo")
        assert_raises_message(
            sa.exc.SAWarning,
            "Unicode column 'foobar' has non-unicode "
            "default value b?'foo' specified.",
            Column,
            "foobar",
            Unicode(32),
            default=default,
        )


class InsertFromSelectTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("data", metadata, Column("x", Integer), Column("y", Integer))

    @classmethod
    def insert_data(cls, connection):
        data = cls.tables.data

        connection.execute(
            data.insert(), [{"x": 2, "y": 5}, {"x": 7, "y": 12}]
        )

    @testing.provide_metadata
    def test_insert_from_select_override_defaults(self, connection):
        data = self.tables.data

        table = Table(
            "sometable",
            self.metadata,
            Column("x", Integer),
            Column("foo", Integer, default=12),
            Column("y", Integer),
        )

        table.create(connection)

        sel = select(data.c.x, data.c.y)

        ins = table.insert().from_select(["x", "y"], sel)
        connection.execute(ins)

        eq_(
            list(connection.execute(table.select().order_by(table.c.x))),
            [(2, 12, 5), (7, 12, 12)],
        )

    @testing.provide_metadata
    def test_insert_from_select_fn_defaults(self, connection):
        data = self.tables.data

        counter = itertools.count(1)

        def foo(ctx):
            return next(counter)

        table = Table(
            "sometable",
            self.metadata,
            Column("x", Integer),
            Column("foo", Integer, default=foo),
            Column("y", Integer),
        )

        table.create(connection)

        sel = select(data.c.x, data.c.y)

        ins = table.insert().from_select(["x", "y"], sel)
        connection.execute(ins)

        # counter is only called once!
        eq_(
            list(connection.execute(table.select().order_by(table.c.x))),
            [(2, 1, 5), (7, 1, 12)],
        )


class CurrentParametersTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        def gen_default(context):
            pass

        Table(
            "some_table",
            metadata,
            Column("x", String(50), default=gen_default),
            Column("y", String(50)),
        )

    def _fixture(self, fn):
        def gen_default(context):
            fn(context)

        some_table = self.tables.some_table
        some_table.c.x.default.arg = gen_default
        return fn

    @testing.combinations(
        ("single", "attribute"),
        ("single", "method"),
        ("executemany", "attribute"),
        ("executemany", "method"),
        ("multivalues", "method", testing.requires.multivalues_inserts),
        argnames="exec_type, usemethod",
    )
    def test_parameters(self, exec_type, usemethod, connection):
        collect = mock.Mock()

        @self._fixture
        def fn(context):
            collect(context.get_current_parameters())

        table = self.tables.some_table
        if exec_type in ("multivalues", "executemany"):
            parameters = [{"y": "h1"}, {"y": "h2"}]
        else:
            parameters = [{"y": "hello"}]

        if exec_type == "multivalues":
            stmt, params = table.insert().values(parameters), {}
        else:
            stmt, params = table.insert(), parameters

        connection.execute(stmt, params)
        eq_(
            collect.mock_calls,
            [mock.call({"y": param["y"], "x": None}) for param in parameters],
        )
