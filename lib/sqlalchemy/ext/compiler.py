# ext/compiler.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

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

Above, ``MyColumn`` extends :class:`~sqlalchemy.sql.expression.ColumnClause`,
the base expression element for named column objects. The ``compiles``
decorator registers itself with the ``MyColumn`` class so that it is invoked
when the object is compiled to a string::

    from sqlalchemy import select

    s = select([MyColumn('x'), MyColumn('y')])
    print str(s)

Produces::

    SELECT [x], [y]

Dialect-specific compilation rules
==================================

Compilers can also be made dialect-specific. The appropriate compiler will be
invoked for the dialect in use::

    from sqlalchemy.schema import DDLElement

    class AlterColumn(DDLElement):

        def __init__(self, column, cmd):
            self.column = column
            self.cmd = cmd

    @compiles(AlterColumn)
    def visit_alter_column(element, compiler, **kw):
        return "ALTER COLUMN %s ..." % element.column.name

    @compiles(AlterColumn, 'postgresql')
    def visit_alter_column(element, compiler, **kw):
        return "ALTER TABLE %s ALTER COLUMN %s ..." % (element.table.name, element.column.name)

The second ``visit_alter_table`` will be invoked when any ``postgresql`` dialect is used.

Compiling sub-elements of a custom expression construct
=======================================================

The ``compiler`` argument is the :class:`~sqlalchemy.engine.base.Compiled`
object in use. This object can be inspected for any information about the
in-progress compilation, including ``compiler.dialect``,
``compiler.statement`` etc. The :class:`~sqlalchemy.sql.compiler.SQLCompiler`
and :class:`~sqlalchemy.sql.compiler.DDLCompiler` both include a ``process()``
method which can be used for compilation of embedded attributes::

    from sqlalchemy.sql.expression import Executable, ClauseElement

    class InsertFromSelect(Executable, ClauseElement):
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

Cross Compiling between SQL and DDL compilers
---------------------------------------------

SQL and DDL constructs are each compiled using different base compilers - ``SQLCompiler``
and ``DDLCompiler``.   A common need is to access the compilation rules of SQL expressions
from within a DDL expression. The ``DDLCompiler`` includes an accessor ``sql_compiler`` for this reason, such as below where we generate a CHECK
constraint that embeds a SQL expression::

    @compiles(MyConstraint)
    def compile_my_constraint(constraint, ddlcompiler, **kw):
        return "CONSTRAINT %s CHECK (%s)" % (
            constraint.name,
            ddlcompiler.sql_compiler.process(constraint.expression)
        )

Changing the default compilation of existing constructs
=======================================================

The compiler extension applies just as well to the existing constructs.  When overriding
the compilation of a built in SQL construct, the @compiles decorator is invoked upon
the appropriate class (be sure to use the class, i.e. ``Insert`` or ``Select``, instead of the creation function such as ``insert()`` or ``select()``).

Within the new compilation function, to get at the "original" compilation routine,
use the appropriate visit_XXX method - this because compiler.process() will call upon the 
overriding routine and cause an endless loop.   Such as, to add "prefix" to all insert statements::

    from sqlalchemy.sql.expression import Insert

    @compiles(Insert)
    def prefix_inserts(insert, compiler, **kw):
        return compiler.visit_insert(insert.prefix_with("some prefix"), **kw)

The above compiler will prefix all INSERT statements with "some prefix" when compiled.

.. _type_compilation_extension:

Changing Compilation of Types
=============================

``compiler`` works for types, too, such as below where we implement the MS-SQL specific 'max' keyword for ``String``/``VARCHAR``::

    @compiles(String, 'mssql')
    @compiles(VARCHAR, 'mssql')
    def compile_varchar(element, compiler, **kw):
        if element.length == 'max':
            return "VARCHAR('max')"
        else:
            return compiler.visit_VARCHAR(element, **kw)

    foo = Table('foo', metadata,
        Column('data', VARCHAR('max'))
    )

Subclassing Guidelines
======================

A big part of using the compiler extension is subclassing SQLAlchemy expression constructs.  To make this easier, the expression and schema packages feature a set of "bases" intended for common tasks.  A synopsis is as follows:

* :class:`~sqlalchemy.sql.expression.ClauseElement` - This is the root
  expression class. Any SQL expression can be derived from this base, and is
  probably the best choice for longer constructs such as specialized INSERT
  statements.
 
* :class:`~sqlalchemy.sql.expression.ColumnElement` - The root of all
  "column-like" elements. Anything that you'd place in the "columns" clause of
  a SELECT statement (as well as order by and group by) can derive from this -
  the object will automatically have Python "comparison" behavior.

  :class:`~sqlalchemy.sql.expression.ColumnElement` classes want to have a
  ``type`` member which is expression's return type.  This can be established
  at the instance level in the constructor, or at the class level if its
  generally constant::

      class timestamp(ColumnElement):
          type = TIMESTAMP()
 
* :class:`~sqlalchemy.sql.expression.FunctionElement` - This is a hybrid of a
  ``ColumnElement`` and a "from clause" like object, and represents a SQL
  function or stored procedure type of call. Since most databases support
  statements along the line of "SELECT FROM <some function>"
  ``FunctionElement`` adds in the ability to be used in the FROM clause of a
  ``select()`` construct::

      from sqlalchemy.sql.expression import FunctionElement

      class coalesce(FunctionElement):
          name = 'coalesce'

      @compiles(coalesce)
      def compile(element, compiler, **kw):
          return "coalesce(%s)" % compiler.process(element.clauses)

      @compiles(coalesce, 'oracle')
      def compile(element, compiler, **kw):
          if len(element.clauses) > 2:
              raise TypeError("coalesce only supports two arguments on Oracle")
          return "nvl(%s)" % compiler.process(element.clauses)

* :class:`~sqlalchemy.schema.DDLElement` - The root of all DDL expressions,
  like CREATE TABLE, ALTER TABLE, etc. Compilation of ``DDLElement``
  subclasses is issued by a ``DDLCompiler`` instead of a ``SQLCompiler``.
  ``DDLElement`` also features ``Table`` and ``MetaData`` event hooks via the
  ``execute_at()`` method, allowing the construct to be invoked during CREATE
  TABLE and DROP TABLE sequences.

* :class:`~sqlalchemy.sql.expression.Executable` - This is a mixin which should be
  used with any expression class that represents a "standalone" SQL statement that
  can be passed directly to an ``execute()`` method.  It is already implicit 
  within ``DDLElement`` and ``FunctionElement``.

"""

def compiles(class_, *specs):
    def decorate(fn):
        existing = class_.__dict__.get('_compiler_dispatcher', None)
        existing_dispatch = class_.__dict__.get('_compiler_dispatch')
        if not existing:
            existing = _dispatcher()

            if existing_dispatch:
                existing.specs['default'] = existing_dispatch

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

