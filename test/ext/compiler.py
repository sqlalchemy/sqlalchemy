import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.sql.expression import ClauseElement, ColumnClause
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext.compiler import UserDefinedCompiler
from sqlalchemy.ext import compiler
from sqlalchemy.sql import table, column
from testlib import *
import gc

class UserDefinedTest(TestBase, AssertsCompiledSQL):

    def test_column(self):

        class MyThingy(ColumnClause):
            __visit_name__ = 'thingy'

            def __init__(self, arg= None):
                super(MyThingy, self).__init__(arg or 'MYTHINGY!')

        class MyCompiler(UserDefinedCompiler):
            compile_elements = [MyThingy]

            def visit_thingy(self, thingy, **kw):
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
            __visit_name__ = 'thingy'

            def __init__(self):
                super(MyThingy, self).__init__('MYTHINGY!')

        class MyCompiler(UserDefinedCompiler):
            compile_elements = [MyThingy]

            def __init__(self, parent_compiler):
                UserDefinedCompiler.__init__(self, parent_compiler)
                self.counter = 0

            def visit_thingy(self, thingy, **kw):
                self.counter += 1
                return str(self.counter)

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
            __visit_name__ = 'insert_from_select'
            def __init__(self, table, select):
                self.table = table
                self.select = select

        class MyCompiler(UserDefinedCompiler):
            compile_elements = [InsertFromSelect]

            def visit_insert_from_select(self, element):
                return "INSERT INTO %s (%s)" % (
                    self.process(element.table, asfrom=True),
                    self.process(element.select)
                )

        t1 = table("mytable", column('x'), column('y'), column('z'))
        self.assert_compile(
            InsertFromSelect(
                t1,
                select([t1]).where(t1.c.x>5)
            ),
            "INSERT INTO mytable (SELECT mytable.x, mytable.y, mytable.z FROM mytable WHERE mytable.x > :x_1)"
        )

    def test_ddl(self):
        class AddThingy(DDLElement):
            __visit_name__ = 'add_thingy'

        class DropThingy(DDLElement):
            __visit_name__ = 'drop_thingy'

        class MyCompiler(UserDefinedCompiler):
            compile_elements = [AddThingy, DropThingy]

            def visit_add_thingy(self, thingy, **kw):
                return "ADD THINGY"

            def visit_drop_thingy(self, thingy, **kw):
                return "DROP THINGY"

        class MySqliteCompiler(MyCompiler):
            dialect = 'sqlite'

            def visit_add_thingy(self, thingy, **kw):
                return "ADD SPECIAL SL THINGY"

        self.assert_compile(AddThingy(),
            "ADD THINGY"
        )

        self.assert_compile(DropThingy(),
            "DROP THINGY"
        )

        from sqlalchemy.dialects.postgres import base
        self.assert_compile(AddThingy(),
            "ADD SPECIAL PG THINGY",
            dialect=base.dialect()
        )

        self.assert_compile(DropThingy(),
            "DROP THINGY",
            dialect=base.dialect()
        )


if __name__ == '__main__':
    testenv.main()
