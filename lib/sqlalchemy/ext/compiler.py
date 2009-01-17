"""Provides an API for creation of custom ClauseElements and compilers.

Synopsis
========

Usage involves the creation of one or more :class:`~sqlalchemy.sql.expression.ClauseElement`
subclasses and a :class:`~UserDefinedCompiler` class::

    from sqlalchemy.ext.compiler import UserDefinedCompiler
    from sqlalchemy.sql.expression import ColumnClause
    
    class MyColumn(ColumnClause):
        __visit_name__ = 'mycolumn'
        
        def __init__(self, text):
            ColumnClause.__init__(self, text)
            
    class MyCompiler(UserDefinedCompiler):
        compile_elements = [MyColumn]
        
        def visit_mycolumn(self, element, **kw):
            return "[%s]" % element.name
            
Above, ``MyColumn`` extends :class:`~sqlalchemy.sql.expression.ColumnClause`, the
base expression element for column objects.  The ``MyCompiler`` class registers
itself with the ``MyColumn`` class so that it is invoked when the object 
is compiled to a string::

    from sqlalchemy import select
    
    s = select([MyColumn('x'), MyColumn('y')])
    print str(s)
    
Produces::

    SELECT [x], [y]

User defined compilers are associated with the :class:`~sqlalchemy.engine.Compiled`
object that is responsible for the current compile, and can compile sub elements using
the :meth:`UserDefinedCompiler.process` method::

    class InsertFromSelect(ClauseElement):
        __visit_name__ = 'insert_from_select'
        def __init__(self, table, select):
            self.table = table
            self.select = select

    class MyCompiler(UserDefinedCompiler):
        compile_elements = [InsertFromSelect]
    
        def visit_insert_from_select(self, element, **kw):
            return "INSERT INTO %s (%s)" % (
                self.process(element.table, asfrom=True),
                self.process(element.select)
            )

A single compiler can be made to service any number of elements as in this DDL example::

    from sqlalchemy.schema import DDLElement
    class AlterTable(DDLElement):
        __visit_name__ = 'alter_table'
        
        def __init__(self, table, cmd):
            self.table = table
            self.cmd = cmd

    class AlterColumn(DDLElement):
        __visit_name__ = 'alter_column'

        def __init__(self, column, cmd):
            self.column = column
            self.cmd = cmd

    class AlterCompiler(UserDefinedCompiler):
        compile_elements = [AlterTable, AlterColumn]
        
        def visit_alter_table(self, element, **kw):
            return "ALTER TABLE %s ..." % element.table.name

        def visit_alter_column(self, element, **kw):
            return "ALTER COLUMN %s ..." % element.column.name

Compilers can also be made dialect-specific.  The appropriate compiler will be invoked
for the dialect in use::
    
    class PGAlterCompiler(AlterCompiler):
        compile_elements = [AlterTable, AlterColumn]
        dialect = 'postgres'
        
        def visit_alter_table(self, element, **kw):
            return "ALTER PG TABLE %s ..." % element.table.name

The above compiler will be invoked when any ``postgres`` dialect is used. Note
that it extends the ``AlterCompiler`` so that the ``AlterColumn`` construct
will be serviced by the generic ``AlterCompiler.visit_alter_column()`` method. 
Subclassing is not required for dialect-specific compilers, but is recommended.

"""
from sqlalchemy import util
from sqlalchemy.engine.base import Compiled
import weakref

def _spawn_compiler(clauseelement, compiler):
    if not hasattr(compiler, '_user_compilers'):
        compiler._user_compilers = {}
    try:
        return compiler._user_compilers[clauseelement._user_compiler_registry]
    except KeyError:
        registry = clauseelement._user_compiler_registry
        cls = registry.get_compiler_cls(compiler.dialect)
        compiler._user_compilers[registry] = user_compiler = cls(compiler)
        return user_compiler

class _CompilerRegistry(object):
    def __init__(self):
        self.user_compilers = {}
        
    def get_compiler_cls(self, dialect):
        if dialect.name in self.user_compilers:
            return self.user_compilers[dialect.name]
        else:
            return self.user_compilers['*']

class _UserDefinedMeta(type):
    def __init__(cls, classname, bases, dict_):
        if cls.compile_elements:
            if not hasattr(cls.compile_elements[0], '_user_compiler_registry'):
                registry = _CompilerRegistry()
                def compiler_dispatch(element, visitor, **kw):
                    compiler = _spawn_compiler(element, visitor)
                    return getattr(compiler, 'visit_%s' % element.__visit_name__)(element, **kw)
                
                for elem in cls.compile_elements:
                    if hasattr(elem, '_user_compiler_registry'):
                        raise exceptions.InvalidRequestError("Detected an existing UserDefinedCompiler registry on class %r" % elem)
                    elem._user_compiler_registry = registry
                    elem._compiler_dispatch = compiler_dispatch
            else:
                registry = cls.compile_elements[0]._user_compiler_registry
        
            if hasattr(cls, 'dialect'):
                registry.user_compilers[cls.dialect] = cls
            else:
                registry.user_compilers['*'] = cls
        return type.__init__(cls, classname, bases, dict_)

class UserDefinedCompiler(Compiled):
    __metaclass__ = _UserDefinedMeta
    compile_elements = []
    
    def __init__(self, parent_compiler):
        Compiled.__init__(self, parent_compiler.dialect, parent_compiler.statement, parent_compiler.bind)
        self.compiler = weakref.ref(parent_compiler)
        
    def compile(self):
        raise NotImplementedError()

    def process(self, obj, **kwargs):
        return obj._compiler_dispatch(self.compiler(), **kwargs)

    def __str__(self):
        return self.compiler().string or ''
    