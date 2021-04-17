from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.compiler import deregister
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateColumn
from sqlalchemy.schema import CreateTable
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.expression import BindParameter
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.sql.expression import ColumnClause
from sqlalchemy.sql.expression import Executable
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.sql.expression import Select
from sqlalchemy.sql.sqltypes import NULLTYPE
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.types import TypeEngine


class UserDefinedTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_column(self):
        class MyThingy(ColumnClause):
            def __init__(self, arg=None):
                super(MyThingy, self).__init__(arg or "MYTHINGY!")

        @compiles(MyThingy)
        def visit_thingy(thingy, compiler, **kw):
            return ">>%s<<" % thingy.name

        self.assert_compile(
            select(column("foo"), MyThingy()), "SELECT foo, >>MYTHINGY!<<"
        )

        self.assert_compile(
            select(MyThingy("x"), MyThingy("y")).where(MyThingy() == 5),
            "SELECT >>x<<, >>y<< WHERE >>MYTHINGY!<< = :MYTHINGY!_1",
        )

    def test_create_column_skip(self):
        @compiles(CreateColumn)
        def skip_xmin(element, compiler, **kw):
            if element.element.name == "xmin":
                return None
            else:
                return compiler.visit_create_column(element, **kw)

        t = Table(
            "t",
            MetaData(),
            Column("a", Integer),
            Column("xmin", Integer),
            Column("c", Integer),
        )

        self.assert_compile(
            CreateTable(t), "CREATE TABLE t (a INTEGER, c INTEGER)"
        )

    def test_types(self):
        class MyType(TypeEngine):
            pass

        @compiles(MyType, "sqlite")
        def visit_sqlite_type(type_, compiler, **kw):
            return "SQLITE_FOO"

        @compiles(MyType, "postgresql")
        def visit_pg_type(type_, compiler, **kw):
            return "POSTGRES_FOO"

        from sqlalchemy.dialects.sqlite import base as sqlite
        from sqlalchemy.dialects.postgresql import base as postgresql

        self.assert_compile(MyType(), "SQLITE_FOO", dialect=sqlite.dialect())

        self.assert_compile(
            MyType(), "POSTGRES_FOO", dialect=postgresql.dialect()
        )

    def test_no_compile_for_col_label(self):
        class MyThingy(FunctionElement):
            pass

        @compiles(MyThingy)
        def visit_thingy(thingy, compiler, **kw):
            raise Exception(
                "unfriendly exception, dont catch this, dont run this"
            )

        @compiles(MyThingy, "postgresql")
        def visit_thingy_pg(thingy, compiler, **kw):
            return "mythingy"

        subq = select(MyThingy("text")).subquery()

        stmt = select(subq)

        self.assert_compile(
            stmt,
            "SELECT anon_2.anon_1 FROM (SELECT mythingy AS anon_1) AS anon_2",
            dialect="postgresql",
        )

    def test_stateful(self):
        class MyThingy(ColumnClause):
            def __init__(self):
                super(MyThingy, self).__init__("MYTHINGY!")

        @compiles(MyThingy)
        def visit_thingy(thingy, compiler, **kw):
            if not hasattr(compiler, "counter"):
                compiler.counter = 0
            compiler.counter += 1
            return str(compiler.counter)

        self.assert_compile(
            select(column("foo"), MyThingy()).order_by(desc(MyThingy())),
            "SELECT foo, 1 ORDER BY 2 DESC",
        )

        self.assert_compile(
            select(MyThingy(), MyThingy()).where(MyThingy() == 5),
            "SELECT 1, 2 WHERE 3 = :MYTHINGY!_1",
        )

    def test_callout_to_compiler(self):
        class InsertFromSelect(ClauseElement):
            def __init__(self, table, select):
                self.table = table
                self.select = select

        @compiles(InsertFromSelect)
        def visit_insert_from_select(element, compiler, **kw):
            return "INSERT INTO %s (%s)" % (
                compiler.process(element.table, asfrom=True),
                compiler.process(element.select),
            )

        t1 = table("mytable", column("x"), column("y"), column("z"))
        self.assert_compile(
            InsertFromSelect(t1, select(t1).where(t1.c.x > 5)),
            "INSERT INTO mytable (SELECT mytable.x, mytable.y, mytable.z "
            "FROM mytable WHERE mytable.x > :x_1)",
        )

    def test_no_default_but_has_a_visit(self):
        class MyThingy(ColumnClause):
            pass

        @compiles(MyThingy, "postgresql")
        def visit_thingy(thingy, compiler, **kw):
            return "mythingy"

        eq_(str(MyThingy("x")), "x")

    def test_no_default_has_no_visit(self):
        class MyThingy(TypeEngine):
            pass

        @compiles(MyThingy, "postgresql")
        def visit_thingy(thingy, compiler, **kw):
            return "mythingy"

        assert_raises_message(
            exc.UnsupportedCompilationError,
            "<class 'test.ext.test_compiler..*MyThingy'> "
            "construct has no default compilation handler.",
            str,
            MyThingy(),
        )

    @testing.combinations((True,), (False,))
    def test_no_default_proxy_generation(self, named):
        class my_function(FunctionElement):
            if named:
                name = "my_function"
            type = Numeric()

        @compiles(my_function, "sqlite")
        def sqlite_my_function(element, compiler, **kw):
            return "my_function(%s)" % compiler.process(element.clauses, **kw)

        t1 = table("t1", column("q"))
        stmt = select(my_function(t1.c.q))

        self.assert_compile(
            stmt,
            "SELECT my_function(t1.q) AS my_function_1 FROM t1"
            if named
            else "SELECT my_function(t1.q) AS anon_1 FROM t1",
            dialect="sqlite",
        )

        if named:
            eq_(stmt.selected_columns.keys(), ["my_function"])
        else:
            eq_(stmt.selected_columns.keys(), ["_no_label"])

    def test_no_default_message(self):
        class MyThingy(ClauseElement):
            pass

        @compiles(MyThingy, "postgresql")
        def visit_thingy(thingy, compiler, **kw):
            return "mythingy"

        assert_raises_message(
            exc.UnsupportedCompilationError,
            "Compiler .*StrSQLCompiler.* can't .* "
            "<class 'test.ext.test_compiler..*MyThingy'> "
            "construct has no default compilation handler.",
            str,
            MyThingy(),
        )

    def test_default_subclass(self):
        from sqlalchemy.types import ARRAY

        class MyArray(ARRAY):
            pass

        @compiles(MyArray, "sqlite")
        def sl_array(elem, compiler, **kw):
            return "array"

        self.assert_compile(
            MyArray(Integer), "INTEGER[]", dialect="postgresql"
        )

    def test_annotations(self):
        """test that annotated clause constructs use the
        decorated class' compiler.

        """

        t1 = table("t1", column("c1"), column("c2"))

        dispatch = Select._compiler_dispatch
        try:

            @compiles(Select)
            def compile_(element, compiler, **kw):
                return "OVERRIDE"

            s1 = select(t1)
            self.assert_compile(s1, "OVERRIDE")
            self.assert_compile(s1._annotate({}), "OVERRIDE")
        finally:
            Select._compiler_dispatch = dispatch
            if hasattr(Select, "_compiler_dispatcher"):
                del Select._compiler_dispatcher

    def test_dialect_specific(self):
        class AddThingy(DDLElement):
            __visit_name__ = "add_thingy"

        class DropThingy(DDLElement):
            __visit_name__ = "drop_thingy"

        @compiles(AddThingy, "sqlite")
        def visit_add_thingy_sqlite(thingy, compiler, **kw):
            return "ADD SPECIAL SL THINGY"

        @compiles(AddThingy)
        def visit_add_thingy(thingy, compiler, **kw):
            return "ADD THINGY"

        @compiles(DropThingy)
        def visit_drop_thingy(thingy, compiler, **kw):
            return "DROP THINGY"

        self.assert_compile(AddThingy(), "ADD THINGY")

        self.assert_compile(DropThingy(), "DROP THINGY")

        from sqlalchemy.dialects.sqlite import base

        self.assert_compile(
            AddThingy(), "ADD SPECIAL SL THINGY", dialect=base.dialect()
        )

        self.assert_compile(
            DropThingy(), "DROP THINGY", dialect=base.dialect()
        )

        @compiles(DropThingy, "sqlite")
        def visit_drop_thingy_sqlite(thingy, compiler, **kw):
            return "DROP SPECIAL SL THINGY"

        self.assert_compile(
            DropThingy(), "DROP SPECIAL SL THINGY", dialect=base.dialect()
        )

        self.assert_compile(DropThingy(), "DROP THINGY")

    def test_functions(self):
        from sqlalchemy.dialects import postgresql

        class MyUtcFunction(FunctionElement):
            pass

        @compiles(MyUtcFunction)
        def visit_myfunc(element, compiler, **kw):
            return "utcnow()"

        @compiles(MyUtcFunction, "postgresql")
        def visit_myfunc_pg(element, compiler, **kw):
            return "timezone('utc', current_timestamp)"

        self.assert_compile(
            MyUtcFunction(), "utcnow()", use_default_dialect=True
        )
        self.assert_compile(
            MyUtcFunction(),
            "timezone('utc', current_timestamp)",
            dialect=postgresql.dialect(),
        )

    def test_functions_args_noname(self):
        class myfunc(FunctionElement):
            pass

        @compiles(myfunc)
        def visit_myfunc(element, compiler, **kw):
            return "myfunc%s" % (compiler.process(element.clause_expr, **kw),)

        self.assert_compile(myfunc(), "myfunc()")

        self.assert_compile(myfunc(column("x"), column("y")), "myfunc(x, y)")

    def test_function_calls_base(self):
        from sqlalchemy.dialects import mssql

        class greatest(FunctionElement):
            type = Numeric()
            name = "greatest"

        @compiles(greatest)
        def default_greatest(element, compiler, **kw):
            return compiler.visit_function(element)

        @compiles(greatest, "mssql")
        def case_greatest(element, compiler, **kw):
            arg1, arg2 = list(element.clauses)
            return "CASE WHEN %s > %s THEN %s ELSE %s END" % (
                compiler.process(arg1),
                compiler.process(arg2),
                compiler.process(arg1),
                compiler.process(arg2),
            )

        self.assert_compile(
            greatest("a", "b"),
            "greatest(:greatest_1, :greatest_2)",
            use_default_dialect=True,
        )
        self.assert_compile(
            greatest("a", "b"),
            "CASE WHEN :greatest_1 > :greatest_2 "
            "THEN :greatest_1 ELSE :greatest_2 END",
            dialect=mssql.dialect(),
        )

    def test_function_subclasses_one(self):
        class Base(FunctionElement):
            name = "base"

        class Sub1(Base):
            name = "sub1"

        class Sub2(Base):
            name = "sub2"

        @compiles(Base)
        def visit_base(element, compiler, **kw):
            return element.name

        @compiles(Sub1)
        def visit_sub1(element, compiler, **kw):
            return "FOO" + element.name

        self.assert_compile(
            select(Sub1(), Sub2()),
            "SELECT FOOsub1 AS sub1_1, sub2 AS sub2_1",
            use_default_dialect=True,
        )

    def test_function_subclasses_two(self):
        class Base(FunctionElement):
            name = "base"

        class Sub1(Base):
            name = "sub1"

        @compiles(Base)
        def visit_base(element, compiler, **kw):
            return element.name

        class Sub2(Base):
            name = "sub2"

        class SubSub1(Sub1):
            name = "subsub1"

        self.assert_compile(
            select(Sub1(), Sub2(), SubSub1()),
            "SELECT sub1 AS sub1_1, sub2 AS sub2_1, subsub1 AS subsub1_1",
            use_default_dialect=True,
        )

        @compiles(Sub1)
        def visit_sub1(element, compiler, **kw):
            return "FOO" + element.name

        self.assert_compile(
            select(Sub1(), Sub2(), SubSub1()),
            "SELECT FOOsub1 AS sub1_1, sub2 AS sub2_1, "
            "FOOsubsub1 AS subsub1_1",
            use_default_dialect=True,
        )

    def _test_result_map_population(self, expression):
        lc1 = literal_column("1")
        lc2 = literal_column("2")
        stmt = select(lc1, expression, lc2)

        compiled = stmt.compile()
        eq_(
            compiled._result_columns,
            [
                ("1", "1", (lc1, "1", "1"), NULLTYPE),
                (None, None, (expression,), NULLTYPE),
                ("2", "2", (lc2, "2", "2"), NULLTYPE),
            ],
        )

    def test_result_map_population_explicit(self):
        class not_named_max(ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            # explicit add
            kw["add_to_result_map"](None, None, (element,), NULLTYPE)
            return "max(a)"

        nnm = not_named_max()
        self._test_result_map_population(nnm)

    def test_result_map_population_implicit(self):
        class not_named_max(ColumnElement):
            name = "not_named_max"

        @compiles(not_named_max)
        def visit_max(element, compiler, **kw):
            # we don't add to keymap here; compiler should be doing it
            return "max(a)"

        nnm = not_named_max()
        self._test_result_map_population(nnm)


class DefaultOnExistingTest(fixtures.TestBase, AssertsCompiledSQL):
    """Test replacement of default compilation on existing constructs."""

    __dialect__ = "default"

    def teardown_test(self):
        for cls in (Select, BindParameter):
            deregister(cls)

    def test_select(self):
        t1 = table("t1", column("c1"), column("c2"))

        @compiles(Select, "sqlite")
        def compile_(element, compiler, **kw):
            return "OVERRIDE"

        s1 = select(t1)
        self.assert_compile(s1, "SELECT t1.c1, t1.c2 FROM t1")

        from sqlalchemy.dialects.sqlite import base as sqlite

        self.assert_compile(s1, "OVERRIDE", dialect=sqlite.dialect())

    def test_binds_in_select(self):
        t = table("t", column("a"), column("b"), column("c"))

        @compiles(BindParameter)
        def gen_bind(element, compiler, **kw):
            return "BIND(%s)" % compiler.visit_bindparam(element, **kw)

        self.assert_compile(
            t.select().where(t.c.c == 5),
            "SELECT t.a, t.b, t.c FROM t WHERE t.c = BIND(:c_1)",
            use_default_dialect=True,
        )

    def test_binds_in_dml(self):
        t = table("t", column("a"), column("b"), column("c"))

        @compiles(BindParameter)
        def gen_bind(element, compiler, **kw):
            return "BIND(%s)" % compiler.visit_bindparam(element, **kw)

        self.assert_compile(
            t.insert(),
            "INSERT INTO t (a, b) VALUES (BIND(:a), BIND(:b))",
            {"a": 1, "b": 2},
            use_default_dialect=True,
        )


class ExecuteTest(fixtures.TablesTest):
    """test that Executable constructs work at a rudimentary level."""

    __requires__ = ("standard_cursor_sql",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
        )

    @testing.fixture()
    def insert_fixture(self):
        class MyInsert(Executable, ClauseElement):
            pass

        @compiles(MyInsert)
        def _run_myinsert(element, compiler, **kw):
            return "INSERT INTO some_table (id, data) VALUES(1, 'some data')"

        return MyInsert

    @testing.fixture()
    def select_fixture(self):
        class MySelect(Executable, ClauseElement):
            pass

        @compiles(MySelect)
        def _run_myinsert(element, compiler, **kw):
            return "SELECT id, data FROM some_table"

        return MySelect

    def test_insert(self, connection, insert_fixture):
        connection.execute(insert_fixture())

        some_table = self.tables.some_table
        eq_(connection.scalar(select(some_table.c.data)), "some data")

    def test_insert_session(self, connection, insert_fixture):
        with Session(connection) as session:
            session.execute(insert_fixture())

        some_table = self.tables.some_table

        eq_(connection.scalar(select(some_table.c.data)), "some data")

    def test_select(self, connection, select_fixture):
        some_table = self.tables.some_table

        connection.execute(some_table.insert().values(id=1, data="some data"))
        result = connection.execute(select_fixture())

        eq_(result.first(), (1, "some data"))

    def test_select_session(self, connection, select_fixture):
        some_table = self.tables.some_table

        connection.execute(some_table.insert().values(id=1, data="some data"))

        with Session(connection) as session:
            result = session.execute(select_fixture())

            eq_(result.first(), (1, "some data"))
