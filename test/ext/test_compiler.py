from sqlalchemy import *
from sqlalchemy.types import TypeEngine
from sqlalchemy.sql.expression import ClauseElement, ColumnClause,\
                                    FunctionElement, Select
from sqlalchemy.schema import DDLElement
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
    
    def test_types(self):
        class MyType(TypeEngine):
            pass
        
        @compiles(MyType, 'sqlite')
        def visit_type(type, compiler, **kw):
            return "SQLITE_FOO"

        @compiles(MyType, 'postgresql')
        def visit_type(type, compiler, **kw):
            return "POSTGRES_FOO"

        from sqlalchemy.dialects.sqlite import base as sqlite
        from sqlalchemy.dialects.postgresql import base as postgresql

        self.assert_compile(
            MyType(),
            "SQLITE_FOO",
            dialect=sqlite.dialect()
        )

        self.assert_compile(
            MyType(),
            "POSTGRES_FOO",
            dialect=postgresql.dialect()
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
            "INSERT INTO mytable (SELECT mytable.x, mytable.y, mytable.z "
            "FROM mytable WHERE mytable.x > :x_1)"
        )
    
    def test_annotations(self):
        """test that annotated clause constructs use the 
        decorated class' compiler.
        
        """
        t1 = table('t1', column('c1'), column('c2'))
        
        dispatch = Select._compiler_dispatch
        try:
            @compiles(Select)
            def compile(element, compiler, **kw):
                return "OVERRIDE"
            
            s1 = select([t1])
            self.assert_compile(
                s1, "OVERRIDE"
            )
            self.assert_compile(
                s1._annotate({}),
                "OVERRIDE"
            )
        finally:
            Select._compiler_dispatch = dispatch
            if hasattr(Select, '_compiler_dispatcher'):
                del Select._compiler_dispatcher
            
    def test_default_on_existing(self):
        """test that the existing compiler function remains
        as 'default' when overriding the compilation of an
        existing construct."""
        

        t1 = table('t1', column('c1'), column('c2'))
        
        dispatch = Select._compiler_dispatch
        try:
            
            @compiles(Select, 'sqlite')
            def compile(element, compiler, **kw):
                return "OVERRIDE"
            
            s1 = select([t1])
            self.assert_compile(
                s1, "SELECT t1.c1, t1.c2 FROM t1",
            )

            from sqlalchemy.dialects.sqlite import base as sqlite
            self.assert_compile(
                s1, "OVERRIDE",
                dialect=sqlite.dialect()
            )
        finally:
            Select._compiler_dispatch = dispatch
            if hasattr(Select, '_compiler_dispatcher'):
                del Select._compiler_dispatcher
        
    def test_dialect_specific(self):
        class AddThingy(DDLElement):
            __visit_name__ = 'add_thingy'

        class DropThingy(DDLElement):
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

        from sqlalchemy.dialects.sqlite import base
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

    def test_functions(self):
        from sqlalchemy.dialects.postgresql import base as postgresql
        
        class MyUtcFunction(FunctionElement):
            pass
            
        @compiles(MyUtcFunction)
        def visit_myfunc(element, compiler, **kw):
            return "utcnow()"
            
        @compiles(MyUtcFunction, 'postgresql')
        def visit_myfunc(element, compiler, **kw):
            return "timezone('utc', current_timestamp)"
            
        self.assert_compile(
            MyUtcFunction(),
            "utcnow()",
            use_default_dialect=True
        )
        self.assert_compile(
            MyUtcFunction(),
            "timezone('utc', current_timestamp)",
            dialect=postgresql.dialect()
        )
    
    def test_subclasses_one(self):
        class Base(FunctionElement):
            name = 'base'
        
        class Sub1(Base):
            name = 'sub1'

        class Sub2(Base):
            name = 'sub2'
        
        @compiles(Base)
        def visit_base(element, compiler, **kw):
            return element.name

        @compiles(Sub1)
        def visit_base(element, compiler, **kw):
            return "FOO" + element.name

        self.assert_compile(
            select([Sub1(), Sub2()]),
            'SELECT FOOsub1, sub2',
            use_default_dialect=True
        )
    
    def test_subclasses_two(self):
        class Base(FunctionElement):
            name = 'base'
        
        class Sub1(Base):
            name = 'sub1'

        @compiles(Base)
        def visit_base(element, compiler, **kw):
            return element.name

        class Sub2(Base):
            name = 'sub2'
        
        class SubSub1(Sub1):
            name = 'subsub1'
            
        self.assert_compile(
            select([Sub1(), Sub2(), SubSub1()]),
            'SELECT sub1, sub2, subsub1',
            use_default_dialect=True
        )

        @compiles(Sub1)
        def visit_base(element, compiler, **kw):
            return "FOO" + element.name

        self.assert_compile(
            select([Sub1(), Sub2(), SubSub1()]),
            'SELECT FOOsub1, sub2, FOOsubsub1',
            use_default_dialect=True
        )
        