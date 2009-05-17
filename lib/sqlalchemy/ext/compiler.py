"""Provides an API for creation of custom ClauseElements and compilers.

Synopsis
========

Usage involves the creation of one or more :class:`~sqlalchemy.sql.expression.ClauseElement`
subclasses and one or more callables defining its compilation::

    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.sql.expression import ColumnClause
    
    class MyColumn(ColumnClause):
        pass
            
    @compiles(MyColumn)
    def compile_mycolumn(element, compiler, **kw):
        return "[%s]" % element.name
        
Above, ``MyColumn`` extends :class:`~sqlalchemy.sql.expression.ColumnClause`, the
base expression element for column objects.  The ``compiles`` decorator registers
itself with the ``MyColumn`` class so that it is invoked when the object 
is compiled to a string::

    from sqlalchemy import select
    
    s = select([MyColumn('x'), MyColumn('y')])
    print str(s)
    
Produces::

    SELECT [x], [y]

Compilers can also be made dialect-specific.  The appropriate compiler will be invoked
for the dialect in use::

    from sqlalchemy.schema import DDLElement  # this is a SQLA 0.6 construct

    class AlterColumn(DDLElement):

        def __init__(self, column, cmd):
            self.column = column
            self.cmd = cmd

    @compiles(AlterColumn)
    def visit_alter_column(element, compiler, **kw):
        return "ALTER COLUMN %s ..." % element.column.name

    @compiles(AlterColumn, 'postgres')
    def visit_alter_column(element, compiler, **kw):
        return "ALTER TABLE %s ALTER COLUMN %s ..." % (element.table.name, element.column.name)

The second ``visit_alter_table`` will be invoked when any ``postgres`` dialect is used.

The ``compiler`` argument is the :class:`~sqlalchemy.engine.base.Compiled` object
in use.  This object can be inspected for any information about the in-progress 
compilation, including ``compiler.dialect``, ``compiler.statement`` etc.
The :class:`~sqlalchemy.sql.compiler.SQLCompiler` and :class:`~sqlalchemy.sql.compiler.DDLCompiler` (DDLCompiler is 0.6. only)
both include a ``process()`` method which can be used for compilation of embedded attributes::

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

    insert = InsertFromSelect(t1, select([t1]).where(t1.c.x>5))
    print insert
    
Produces::

    "INSERT INTO mytable (SELECT mytable.x, mytable.y, mytable.z FROM mytable WHERE mytable.x > :x_1)"


"""

def compiles(class_, *specs):
    def decorate(fn):
        existing = getattr(class_, '_compiler_dispatcher', None)
        if not existing:
            existing = _dispatcher()

            # TODO: why is the lambda needed ?
            setattr(class_, '_compiler_dispatch', lambda *arg, **kw: existing(*arg, **kw))
            setattr(class_, '_compiler_dispatcher', existing)
        
        if specs:
            for s in specs:
                existing.specs[s] = fn
        else:
            existing.specs['default'] = fn
        return fn
    return decorate
    
class _dispatcher(object):
    def __init__(self):
        self.specs = {}
    
    def __call__(self, element, compiler, **kw):
        # TODO: yes, this could also switch off of DBAPI in use.
        fn = self.specs.get(compiler.dialect.name, None)
        if not fn:
            fn = self.specs['default']
        return fn(element, compiler, **kw)
        
