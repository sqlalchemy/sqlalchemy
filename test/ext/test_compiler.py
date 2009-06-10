from sqlalchemy import *
from sqlalchemy.sql.expression import ClauseElement, ColumnClause
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import table, column
from sqlalchemy.test import *

class UserDefinedTest(TestBase, AssertsCompiledSQL):

    def test_column(self):

        class MyThingy(ColumnClause):
            def __init__(self, arg= None):
                super(MyThingy, self).__init__(arg or 'MYTHINGY!')

        @compiles(MyThingy)
        def visit_thingy(thingy, compiler, **kw):
            return ">>%s<<" % thingy.name

        self.assert_compile(
            select([column('foo'), MyThingy()]),
            "SELECT foo, >>MYTHINGY!<<"
        )

        self.assert_compile(
            select([MyThingy('x'), MyThingy('y')]).where(MyThingy() == 5),
            "SELECT >>x<<, >>y<< WHERE >>MYTHINGY!<< = :MYTHINGY!_1"
        )

    def test_stateful(self):
        class MyThingy(ColumnClause):
            def __init__(self):
                super(MyThingy, self).__init__('MYTHINGY!')

        @compiles(MyThingy)
        def visit_thingy(thingy, compiler, **kw):
            if not hasattr(compiler, 'counter'):
                compiler.counter = 0
            compiler.counter += 1
            return str(compiler.counter)

        self.assert_compile(
            select([column('foo'), MyThingy()]).order_by(desc(MyThingy())),
            "SELECT foo, 1 ORDER BY 2 DESC"
        )

        self.assert_compile(
            select([MyThingy(), MyThingy()]).where(MyThingy() == 5),
            "SELECT 1, 2 WHERE 3 = :MYTHINGY!_1"
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
                compiler.process(element.select)
            )

        t1 = table("mytable", column('x'), column('y'), column('z'))
        self.assert_compile(
            InsertFromSelect(
                t1,
                select([t1]).where(t1.c.x>5)
            ),
            "INSERT INTO mytable (SELECT mytable.x, mytable.y, mytable.z FROM mytable WHERE mytable.x > :x_1)"
        )

    def test_dialect_specific(self):
        class AddThingy(ClauseElement):
            __visit_name__ = 'add_thingy'

        class DropThingy(ClauseElement):
            __visit_name__ = 'drop_thingy'

        @compiles(AddThingy, 'sqlite')
        def visit_add_thingy(thingy, compiler, **kw):
            return "ADD SPECIAL SL THINGY"

        @compiles(AddThingy)
        def visit_add_thingy(thingy, compiler, **kw):
            return "ADD THINGY"

        @compiles(DropThingy)
        def visit_drop_thingy(thingy, compiler, **kw):
            return "DROP THINGY"

        self.assert_compile(AddThingy(),
            "ADD THINGY"
        )

        self.assert_compile(DropThingy(),
            "DROP THINGY"
        )

        from sqlalchemy.databases import sqlite as base
        self.assert_compile(AddThingy(),
            "ADD SPECIAL SL THINGY",
            dialect=base.dialect()
        )

        self.assert_compile(DropThingy(),
            "DROP THINGY",
            dialect=base.dialect()
        )

        @compiles(DropThingy, 'sqlite')
        def visit_drop_thingy(thingy, compiler, **kw):
            return "DROP SPECIAL SL THINGY"

        self.assert_compile(DropThingy(),
            "DROP SPECIAL SL THINGY",
            dialect=base.dialect()
        )

        self.assert_compile(DropThingy(),
            "DROP THINGY",
        )

