# sql.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the base components of SQL expression trees.

All components are derived from a common base class
[sqlalchemy.sql.expression#ClauseElement].  Common behaviors are organized
based on class hierarchies, in some cases via mixins.

All object construction from this package occurs via functions which
in some cases will construct composite ``ClauseElement`` structures
together, and in other cases simply return a single ``ClauseElement``
constructed directly.  The function interface affords a more "DSL-ish"
feel to constructing SQL expressions and also allows future class
reorganizations.

Even though classes are not constructed directly from the outside,
most classes which have additional public methods are considered to be
public (i.e. have no leading underscore).  Other classes which are
"semi-public" are marked with a single leading underscore; these
classes usually have few or no public methods and are less guaranteed
to stay the same in future releases.
"""

import re
import datetime
import warnings
from itertools import chain
from sqlalchemy import util, exceptions
from sqlalchemy.sql import operators, visitors
from sqlalchemy import types as sqltypes

functions, schema = None, None
DefaultDialect, ClauseAdapter = None, None

__all__ = [
    'Alias', 'ClauseElement',
    'ColumnCollection', 'ColumnElement',
    'CompoundSelect', 'Delete', 'FromClause', 'Insert', 'Join',
    'Select', 'Selectable', 'TableClause', 'Update', 'alias', 'and_', 'asc',
    'between', 'bindparam', 'case', 'cast', 'column', 'delete',
    'desc', 'distinct', 'except_', 'except_all', 'exists', 'extract', 'func',
    'modifier',
    'insert', 'intersect', 'intersect_all', 'join', 'literal',
    'literal_column', 'not_', 'null', 'or_', 'outparam', 'outerjoin', 'select',
    'subquery', 'table', 'text', 'union', 'union_all', 'update', ]



def desc(column):
    """Return a descending ``ORDER BY`` clause element.

    e.g.::

      order_by = [desc(table1.mycol)]
    """
    return _UnaryExpression(column, modifier=operators.desc_op)

def asc(column):
    """Return an ascending ``ORDER BY`` clause element.

    e.g.::

      order_by = [asc(table1.mycol)]
    """
    return _UnaryExpression(column, modifier=operators.asc_op)

def outerjoin(left, right, onclause=None, **kwargs):
    """Return an ``OUTER JOIN`` clause element.

    The returned object is an instance of [sqlalchemy.sql.expression#Join].

    Similar functionality is also available via the ``outerjoin()``
    method on any [sqlalchemy.sql.expression#FromClause].

    left
      The left side of the join.

    right
      The right side of the join.

    onclause
      Optional criterion for the ``ON`` clause, is derived from
      foreign key relationships established between left and right
      otherwise.

    To chain joins together, use the ``join()`` or ``outerjoin()``
    methods on the resulting ``Join`` object.
    """

    return Join(left, right, onclause, isouter = True, **kwargs)

def join(left, right, onclause=None, **kwargs):
    """Return a ``JOIN`` clause element (regular inner join).

    The returned object is an instance of [sqlalchemy.sql.expression#Join].

    Similar functionality is also available via the ``join()`` method
    on any [sqlalchemy.sql.expression#FromClause].

    left
      The left side of the join.

    right
      The right side of the join.

    onclause
      Optional criterion for the ``ON`` clause, is derived from
      foreign key relationships established between left and right
      otherwise.

    To chain joins together, use the ``join()`` or ``outerjoin()``
    methods on the resulting ``Join`` object.
    """

    return Join(left, right, onclause, **kwargs)

def select(columns=None, whereclause=None, from_obj=[], **kwargs):
    """Returns a ``SELECT`` clause element.

    Similar functionality is also available via the ``select()``
    method on any [sqlalchemy.sql.expression#FromClause].

    The returned object is an instance of [sqlalchemy.sql.expression#Select].

    All arguments which accept ``ClauseElement`` arguments also accept
    string arguments, which will be converted as appropriate into
    either ``text()`` or ``literal_column()`` constructs.

    columns
      A list of ``ClauseElement`` objects, typically ``ColumnElement``
      objects or subclasses, which will form the columns clause of the
      resulting statement.  For all members which are instances of
      ``Selectable``, the individual ``ColumnElement`` members of the
      ``Selectable`` will be added individually to the columns clause.
      For example, specifying a ``Table`` instance will result in all
      the contained ``Column`` objects within to be added to the
      columns clause.

      This argument is not present on the form of ``select()``
      available on ``Table``.

    whereclause
      A ``ClauseElement`` expression which will be used to form the
      ``WHERE`` clause.

    from_obj
      A list of ``ClauseElement`` objects which will be added to the
      ``FROM`` clause of the resulting statement.  Note that "from"
      objects are automatically located within the columns and
      whereclause ClauseElements.  Use this parameter to explicitly
      specify "from" objects which are not automatically locatable.
      This could include ``Table`` objects that aren't otherwise
      present, or ``Join`` objects whose presence will supercede that
      of the ``Table`` objects already located in the other clauses.

    \**kwargs
      Additional parameters include:

      prefixes
        a list of strings or ``ClauseElement`` objects to include
        directly after the SELECT keyword in the generated statement,
        for dialect-specific query features.

      distinct=False
        when ``True``, applies a ``DISTINCT`` qualifier to the columns
        clause of the resulting statement.

      use_labels=False
        when ``True``, the statement will be generated using labels
        for each column in the columns clause, which qualify each
        column with its parent table's (or aliases) name so that name
        conflicts between columns in different tables don't occur.
        The format of the label is <tablename>_<column>.  The "c"
        collection of the resulting ``Select`` object will use these
        names as well for targeting column members.

      for_update=False
        when ``True``, applies ``FOR UPDATE`` to the end of the
        resulting statement.  Certain database dialects also support
        alternate values for this parameter, for example mysql
        supports "read" which translates to ``LOCK IN SHARE MODE``,
        and oracle supports "nowait" which translates to ``FOR UPDATE
        NOWAIT``.

      correlate=True
        indicates that this ``Select`` object should have its
        contained ``FromClause`` elements "correlated" to an enclosing
        ``Select`` object.  This means that any ``ClauseElement``
        instance within the "froms" collection of this ``Select``
        which is also present in the "froms" collection of an
        enclosing select will not be rendered in the ``FROM`` clause
        of this select statement.

      group_by
        a list of ``ClauseElement`` objects which will comprise the
        ``GROUP BY`` clause of the resulting select.

      having
        a ``ClauseElement`` that will comprise the ``HAVING`` clause
        of the resulting select when ``GROUP BY`` is used.

      order_by
        a scalar or list of ``ClauseElement`` objects which will
        comprise the ``ORDER BY`` clause of the resulting select.

      limit=None
        a numerical value which usually compiles to a ``LIMIT``
        expression in the resulting select.  Databases that don't
        support ``LIMIT`` will attempt to provide similar
        functionality.

      offset=None
        a numeric value which usually compiles to an ``OFFSET``
        expression in the resulting select.  Databases that don't
        support ``OFFSET`` will attempt to provide similar
        functionality.

      bind=None
        an ``Engine`` or ``Connection`` instance to which the
        resulting ``Select ` object will be bound.  The ``Select``
        object will otherwise automatically bind to whatever
        ``Connectable`` instances can be located within its contained
        ``ClauseElement`` members.

      scalar=False
        deprecated.  Use select(...).as_scalar() to create a "scalar
        column" proxy for an existing Select object.
    """

    if 'scalar' in kwargs:
        util.warn_deprecated('scalar option is deprecated; see docs for details')
    scalar = kwargs.pop('scalar', False)
    s = Select(columns, whereclause=whereclause, from_obj=from_obj, **kwargs)
    if scalar:
        return s.as_scalar()
    else:
        return s

def subquery(alias, *args, **kwargs):
    """Return an [sqlalchemy.sql.expression#Alias] object derived from a [sqlalchemy.sql.expression#Select].

    name
      alias name

    \*args, \**kwargs

      all other arguments are delivered to the [sqlalchemy.sql.expression#select()]
      function.
    """

    return Select(*args, **kwargs).alias(alias)

def insert(table, values=None, inline=False, **kwargs):
    """Return an [sqlalchemy.sql.expression#Insert] clause element.

    Similar functionality is available via the ``insert()`` method on
    [sqlalchemy.schema#Table].

    table
      The table to be inserted into.

    values
      A dictionary which specifies the column specifications of the
      ``INSERT``, and is optional.  If left as None, the column
      specifications are determined from the bind parameters used
      during the compile phase of the ``INSERT`` statement.  If the
      bind parameters also are None during the compile phase, then the
      column specifications will be generated from the full list of
      table columns.

    inline
      if True, SQL defaults will be compiled 'inline' into the statement
      and not pre-executed.

    If both `values` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within `values` on a per-key basis.

    The keys within `values` can be either ``Column`` objects or their
    string identifiers.  Each key may reference one of:

    * a literal data value (i.e. string, number, etc.);
    * a Column object;
    * a SELECT statement.

    If a ``SELECT`` statement is specified which references this
    ``INSERT`` statement's table, the statement will be correlated
    against the ``INSERT`` statement.
    """

    return Insert(table, values, inline=inline, **kwargs)

def update(table, whereclause=None, values=None, inline=False, **kwargs):
    """Return an [sqlalchemy.sql.expression#Update] clause element.

    Similar functionality is available via the ``update()`` method on
    [sqlalchemy.schema#Table].

    table
      The table to be updated.

    whereclause
      A ``ClauseElement`` describing the ``WHERE`` condition of the
      ``UPDATE`` statement.

    values
      A dictionary which specifies the ``SET`` conditions of the
      ``UPDATE``, and is optional. If left as None, the ``SET``
      conditions are determined from the bind parameters used during
      the compile phase of the ``UPDATE`` statement.  If the bind
      parameters also are None during the compile phase, then the
      ``SET`` conditions will be generated from the full list of table
      columns.

    inline
      if True, SQL defaults will be compiled 'inline' into the statement
      and not pre-executed.


    If both `values` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within `values` on a per-key basis.

    The keys within `values` can be either ``Column`` objects or their
    string identifiers. Each key may reference one of:

    * a literal data value (i.e. string, number, etc.);
    * a Column object;
    * a SELECT statement.

    If a ``SELECT`` statement is specified which references this
    ``UPDATE`` statement's table, the statement will be correlated
    against the ``UPDATE`` statement.
    """

    return Update(table, whereclause=whereclause, values=values, inline=inline, **kwargs)

def delete(table, whereclause = None, **kwargs):
    """Return a [sqlalchemy.sql.expression#Delete] clause element.

    Similar functionality is available via the ``delete()`` method on
    [sqlalchemy.schema#Table].

    table
      The table to be updated.

    whereclause
      A ``ClauseElement`` describing the ``WHERE`` condition of the
      ``UPDATE`` statement.
    """

    return Delete(table, whereclause, **kwargs)

def and_(*clauses):
    """Join a list of clauses together using the ``AND`` operator.

    The ``&`` operator is also overloaded on all
    [sqlalchemy.sql.expression#_CompareMixin] subclasses to produce the same
    result.
    """
    if len(clauses) == 1:
        return clauses[0]
    return ClauseList(operator=operators.and_, *clauses)

def or_(*clauses):
    """Join a list of clauses together using the ``OR`` operator.

    The ``|`` operator is also overloaded on all
    [sqlalchemy.sql.expression#_CompareMixin] subclasses to produce the same
    result.
    """

    if len(clauses) == 1:
        return clauses[0]
    return ClauseList(operator=operators.or_, *clauses)

def not_(clause):
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The ``~`` operator is also overloaded on all
    [sqlalchemy.sql.expression#_CompareMixin] subclasses to produce the same
    result.
    """

    return operators.inv(clause)

def distinct(expr):
    """Return a ``DISTINCT`` clause."""

    return _UnaryExpression(expr, operator=operators.distinct_op)

def between(ctest, cleft, cright):
    """Return a ``BETWEEN`` predicate clause.

    Equivalent of SQL ``clausetest BETWEEN clauseleft AND clauseright``.

    The ``between()`` method on all [sqlalchemy.sql.expression#_CompareMixin] subclasses
    provides similar functionality.
    """

    ctest = _literal_as_binds(ctest)
    return _BinaryExpression(ctest, ClauseList(_literal_as_binds(cleft, type_=ctest.type), _literal_as_binds(cright, type_=ctest.type), operator=operators.and_, group=False), operators.between_op)


def case(whens, value=None, else_=None):
    """Produce a ``CASE`` statement.

    whens
      A sequence of pairs to be translated into "when / then" clauses.

    value
      Optional for simple case statements.

    else\_
      Optional as well, for case defaults.
    """

    whenlist = [ClauseList('WHEN', c, 'THEN', r, operator=None)
                for (c,r) in whens]
    if not else_ is None:
        whenlist.append(ClauseList('ELSE', else_, operator=None))
    if whenlist:
        type = list(whenlist[-1])[-1].type
    else:
        type = None
    cc = _CalculatedClause(None, 'CASE', value, type_=type, operator=None, group_contents=False, *whenlist + ['END'])
    return cc

def cast(clause, totype, **kwargs):
    """Return a ``CAST`` function.

    Equivalent of SQL ``CAST(clause AS totype)``.

    Use with a [sqlalchemy.types#TypeEngine] subclass, i.e::

      cast(table.c.unit_price * table.c.qty, Numeric(10,4))

    or::

      cast(table.c.timestamp, DATE)
    """

    return _Cast(clause, totype, **kwargs)

def extract(field, expr):
    """Return the clause ``extract(field FROM expr)``."""

    expr = _BinaryExpression(text(field), expr, operators.from_)
    return func.extract(expr)

def exists(*args, **kwargs):
    """Return an ``EXISTS`` clause as applied to a [sqlalchemy.sql.expression#Select] object.

    The resulting [sqlalchemy.sql.expression#_Exists] object can be executed by
    itself or used as a subquery within an enclosing select.

    \*args, \**kwargs
      all arguments are sent directly to the [sqlalchemy.sql.expression#select()]
      function to produce a ``SELECT`` statement.
    """

    return _Exists(*args, **kwargs)

def union(*selects, **kwargs):
    """Return a ``UNION`` of multiple selectables.

    The returned object is an instance of [sqlalchemy.sql.expression#CompoundSelect].

    A similar ``union()`` method is available on all
    [sqlalchemy.sql.expression#FromClause] subclasses.

    \*selects
      a list of [sqlalchemy.sql.expression#Select] instances.

    \**kwargs
       available keyword arguments are the same as those of
       [sqlalchemy.sql.expression#select()].
    """

    return _compound_select('UNION', *selects, **kwargs)

def union_all(*selects, **kwargs):
    """Return a ``UNION ALL`` of multiple selectables.

    The returned object is an instance of [sqlalchemy.sql.expression#CompoundSelect].

    A similar ``union_all()`` method is available on all
    [sqlalchemy.sql.expression#FromClause] subclasses.

    \*selects
      a list of [sqlalchemy.sql.expression#Select] instances.

    \**kwargs
      available keyword arguments are the same as those of
      [sqlalchemy.sql.expression#select()].
    """

    return _compound_select('UNION ALL', *selects, **kwargs)

def except_(*selects, **kwargs):
    """Return an ``EXCEPT`` of multiple selectables.

    The returned object is an instance of [sqlalchemy.sql.expression#CompoundSelect].

    \*selects
      a list of [sqlalchemy.sql.expression#Select] instances.

    \**kwargs
      available keyword arguments are the same as those of
      [sqlalchemy.sql.expression#select()].
    """
    return _compound_select('EXCEPT', *selects, **kwargs)

def except_all(*selects, **kwargs):
    """Return an ``EXCEPT ALL`` of multiple selectables.

    The returned object is an instance of [sqlalchemy.sql.expression#CompoundSelect].

    \*selects
      a list of [sqlalchemy.sql.expression#Select] instances.

    \**kwargs
      available keyword arguments are the same as those of
      [sqlalchemy.sql.expression#select()].
    """
    return _compound_select('EXCEPT ALL', *selects, **kwargs)

def intersect(*selects, **kwargs):
    """Return an ``INTERSECT`` of multiple selectables.

    The returned object is an instance of [sqlalchemy.sql.expression#CompoundSelect].

    \*selects
      a list of [sqlalchemy.sql.expression#Select] instances.

    \**kwargs
      available keyword arguments are the same as those of
      [sqlalchemy.sql.expression#select()].
    """
    return _compound_select('INTERSECT', *selects, **kwargs)

def intersect_all(*selects, **kwargs):
    """Return an ``INTERSECT ALL`` of multiple selectables.

    The returned object is an instance of [sqlalchemy.sql.expression#CompoundSelect].

    \*selects
      a list of [sqlalchemy.sql.expression#Select] instances.

    \**kwargs
      available keyword arguments are the same as those of
      [sqlalchemy.sql.expression#select()].
    """
    return _compound_select('INTERSECT ALL', *selects, **kwargs)

def alias(selectable, alias=None):
    """Return an [sqlalchemy.sql.expression#Alias] object.

    An ``Alias`` represents any [sqlalchemy.sql.expression#FromClause] with
    an alternate name assigned within SQL, typically using the ``AS``
    clause when generated, e.g. ``SELECT * FROM table AS aliasname``.

    Similar functionality is available via the ``alias()`` method
    available on all ``FromClause`` subclasses.

      selectable
        any ``FromClause`` subclass, such as a table, select
        statement, etc..

      alias
        string name to be assigned as the alias.  If ``None``, a
        random name will be generated.
    """

    return Alias(selectable, alias=alias)


def literal(value, type_=None):
    """Return a literal clause, bound to a bind parameter.

    Literal clauses are created automatically when non-
    ``ClauseElement`` objects (such as strings, ints, dates, etc.) are
    used in a comparison operation with a
    [sqlalchemy.sql.expression#_CompareMixin] subclass, such as a ``Column``
    object.  Use this function to force the generation of a literal
    clause, which will be created as a
    [sqlalchemy.sql.expression#_BindParamClause] with a bound value.

    value
      the value to be bound.  Can be any Python object supported by
      the underlying DB-API, or is translatable via the given type
      argument.

    type\_
      an optional [sqlalchemy.types#TypeEngine] which will provide
      bind-parameter translation for this literal.
    """

    return _BindParamClause(None, value, type_=type_, unique=True)

def label(name, obj):
    """Return a [sqlalchemy.sql.expression#_Label] object for the given [sqlalchemy.sql.expression#ColumnElement].

    A label changes the name of an element in the columns clause of a
    ``SELECT`` statement, typically via the ``AS`` SQL keyword.

    This functionality is more conveniently available via the
    ``label()`` method on ``ColumnElement``.

    name
      label name

    obj
      a ``ColumnElement``.
    """

    return _Label(name, obj)

def column(text, type_=None):
    """Return a textual column clause, as would be in the columns clause of a ``SELECT`` statement.

    The object returned is an instance of [sqlalchemy.sql.expression#_ColumnClause],
    which represents the "syntactical" portion of the schema-level
    [sqlalchemy.schema#Column] object.

    text
      the name of the column.  Quoting rules will be applied to the
      clause like any other column name.  For textual column
      constructs that are not to be quoted, use the
      [sqlalchemy.sql.expression#literal_column()] function.

    type\_
      an optional [sqlalchemy.types#TypeEngine] object which will
      provide result-set translation for this column.

    """

    return _ColumnClause(text, type_=type_)

def literal_column(text, type_=None):
    """Return a textual column clause, as would be in the columns clause of a ``SELECT`` statement.

    The object returned is an instance of [sqlalchemy.sql.expression#_ColumnClause],
    which represents the "syntactical" portion of the schema-level
    [sqlalchemy.schema#Column] object.

    text
      the name of the column.  Quoting rules will not be applied to
      the column.  For textual column constructs that should be quoted
      like any other column construct, use the
      [sqlalchemy.sql.expression#column()] function.

    type
      an optional [sqlalchemy.types#TypeEngine] object which will
      provide result-set translation for this column.
    """

    return _ColumnClause(text, type_=type_, is_literal=True)

def table(name, *columns):
    """Return a [sqlalchemy.sql.expression#Table] object.

    This is a primitive version of the [sqlalchemy.schema#Table] object,
    which is a subclass of this object.
    """

    return TableClause(name, *columns)

def bindparam(key, value=None, shortname=None, type_=None, unique=False):
    """Create a bind parameter clause with the given key.

    value
      a default value for this bind parameter.  a bindparam with a
      value is called a ``value-based bindparam``.

    type
      a sqlalchemy.types.TypeEngine object indicating the type of this
      bind param, will invoke type-specific bind parameter processing

    shortname
      deprecated.

    unique
      if True, bind params sharing the same name will have their
      underlying ``key`` modified to a uniquely generated name.
      mostly useful with value-based bind params.
    """

    if isinstance(key, _ColumnClause):
        return _BindParamClause(key.name, value, type_=key.type, unique=unique, shortname=shortname)
    else:
        return _BindParamClause(key, value, type_=type_, unique=unique, shortname=shortname)

def outparam(key, type_=None):
    """Create an 'OUT' parameter for usage in functions (stored procedures), for databases which support them.

    The ``outparam`` can be used like a regular function parameter.
    The "output" value will be available from the
    [sqlalchemy.engine#ResultProxy] object via its ``out_parameters``
    attribute, which returns a dictionary containing the values.
    """

    return _BindParamClause(key, None, type_=type_, unique=False, isoutparam=True)

def text(text, bind=None, *args, **kwargs):
    """Create literal text to be inserted into a query.

    When constructing a query from a ``select()``, ``update()``,
    ``insert()`` or ``delete()``, using plain strings for argument
    values will usually result in text objects being created
    automatically.  Use this function when creating textual clauses
    outside of other ``ClauseElement`` objects, or optionally wherever
    plain text is to be used.

    text
      the text of the SQL statement to be created.  use ``:<param>``
      to specify bind parameters; they will be compiled to their
      engine-specific format.

    bind
      an optional connection or engine to be used for this text query.

    bindparams
      a list of ``bindparam()`` instances which can be used to define
      the types and/or initial values for the bind parameters within
      the textual statement; the keynames of the bindparams must match
      those within the text of the statement.  The types will be used
      for pre-processing on bind values.

    typemap
      a dictionary mapping the names of columns represented in the
      ``SELECT`` clause of the textual statement to type objects,
      which will be used to perform post-processing on columns within
      the result set (for textual statements that produce result
      sets).
    """

    return _TextClause(text, bind=bind, *args, **kwargs)

def null():
    """Return a ``_Null`` object, which compiles to ``NULL`` in a sql statement."""

    return _Null()

class _FunctionGenerator(object):
    """Generate ``_Function`` objects based on getattr calls."""

    def __init__(self, **opts):
        self.__names = []
        self.opts = opts

    def __getattr__(self, name):
        # passthru __ attributes; fixes pydoc
        if name.startswith('__'):
            try:
                return self.__dict__[name]
            except KeyError:
                raise AttributeError(name)

        elif name.startswith('_'):
            name = name[0:-1]
        f = _FunctionGenerator(**self.opts)
        f.__names = list(self.__names) + [name]
        return f

    def __call__(self, *c, **kwargs):
        o = self.opts.copy()
        o.update(kwargs)
        if len(self.__names) == 1:
            global functions
            if functions is None:
                from sqlalchemy.sql import functions
            func = getattr(functions, self.__names[-1].lower(), None)
            if func is not None:
                return func(*c, **o)

        return _Function(self.__names[-1], packagenames=self.__names[0:-1], *c, **o)

func = _FunctionGenerator()

# TODO: use UnaryExpression for this instead ?
modifier = _FunctionGenerator(group=False)

def _clone(element):
    return element._clone()

def _compound_select(keyword, *selects, **kwargs):
    return CompoundSelect(keyword, *selects, **kwargs)

def _is_literal(element):
    return not isinstance(element, ClauseElement)

def _literal_as_text(element):
    if isinstance(element, Operators):
        return element.expression_element()
    elif _is_literal(element):
        return _TextClause(unicode(element))
    else:
        return element

def _literal_as_column(element):
    if isinstance(element, Operators):
        return element.clause_element()
    elif _is_literal(element):
        return literal_column(str(element))
    else:
        return element

def _literal_as_binds(element, name=None, type_=None):
    if isinstance(element, Operators):
        return element.expression_element()
    elif _is_literal(element):
        if element is None:
            return null()
        else:
            return _BindParamClause(name, element, type_=type_, unique=True)
    else:
        return element

def _corresponding_column_or_error(fromclause, column, require_embedded=False):
    c = fromclause.corresponding_column(column, require_embedded=require_embedded)
    if not c:
        raise exceptions.InvalidRequestError("Given column '%s', attached to table '%s', failed to locate a corresponding column from table '%s'" % (str(column), str(getattr(column, 'table', None)), fromclause.description))
    return c

def _selectable(element):
    if hasattr(element, '__selectable__'):
        return element.__selectable__()
    elif isinstance(element, Selectable):
        return element
    else:
        raise exceptions.ArgumentError("Object '%s' is not a Selectable and does not implement `__selectable__()`" % repr(element))

def is_column(col):
    """True if ``col`` is an instance of ``ColumnElement``."""
    return isinstance(col, ColumnElement)


class _FigureVisitName(type):
    def __init__(cls, clsname, bases, dict):
        if not '__visit_name__' in cls.__dict__:
            m = re.match(r'_?(\w+?)(?:Expression|Clause|Element|$)', clsname)
            x = m.group(1)
            x = re.sub(r'(?!^)[A-Z]', lambda m:'_'+m.group(0).lower(), x)
            cls.__visit_name__ = x.lower()
        super(_FigureVisitName, cls).__init__(clsname, bases, dict)

class ClauseElement(object):
    """Base class for elements of a programmatically constructed SQL expression."""
    __metaclass__ = _FigureVisitName

    def _clone(self):
        """Create a shallow copy of this ClauseElement.

        This method may be used by a generative API.  Its also used as
        part of the "deep" copy afforded by a traversal that combines
        the _copy_internals() method.
        """
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()

        # this is a marker that helps to "equate" clauses to each other
        # when a Select returns its list of FROM clauses.  the cloning
        # process leaves around a lot of remnants of the previous clause
        # typically in the form of column expressions still attached to the
        # old table.
        c._is_clone_of = self

        return c

    def _cloned_set(self):
        f = self
        while f is not None:
            yield f
            f = getattr(f, '_is_clone_of', None)
    _cloned_set = property(_cloned_set)

    def _get_from_objects(self, **modifiers):
        """Return objects represented in this ``ClauseElement`` that
        should be added to the ``FROM`` list of a query, when this
        ``ClauseElement`` is placed in the column clause of a
        ``Select`` statement.
        """

        raise NotImplementedError(repr(self))

    def unique_params(self, *optionaldict, **kwargs):
        """Return a copy with ``bindparam()`` elments replaced.

        Same functionality as ``params()``, except adds `unique=True`
        to affected bind parameters so that multiple statements can be
        used.
        """

        return self._params(True, optionaldict, kwargs)

    def params(self, *optionaldict, **kwargs):
        """Return a copy with ``bindparam()`` elments replaced.

        Returns a copy of this ClauseElement with ``bindparam()``
        elements replaced with values taken from the given dictionary::

          >>> clause = column('x') + bindparam('foo')
          >>> print clause.compile().params
          {'foo':None}
          >>> print clause.params({'foo':7}).compile().params
          {'foo':7}
        """

        return self._params(False, optionaldict, kwargs)

    def _params(self, unique, optionaldict, kwargs):
        if len(optionaldict) == 1:
            kwargs.update(optionaldict[0])
        elif len(optionaldict) > 1:
            raise exceptions.ArgumentError("params() takes zero or one positional dictionary argument")

        def visit_bindparam(bind):
            if bind.key in kwargs:
                bind.value = kwargs[bind.key]
            if unique:
                bind._convert_to_unique()
        return visitors.traverse(self, visit_bindparam=visit_bindparam, clone=True)

    def compare(self, other):
        """Compare this ClauseElement to the given ClauseElement.

        Subclasses should override the default behavior, which is a
        straight identity comparison.
        """

        return self is other

    def _copy_internals(self, clone=_clone):
        """Reassign internal elements to be clones of themselves.

        Called during a copy-and-traverse operation on newly
        shallow-copied elements to create a deep copy.
        """

        pass

    def get_children(self, **kwargs):
        """Return immediate child elements of this ``ClauseElement``.

        This is used for visit traversal.

        \**kwargs may contain flags that change the collection that is
        returned, for example to return a subset of items in order to
        cut down on larger traversals, or to return child items from a
        different context (such as schema-level collections instead of
        clause-level).
        """
        return []

    def self_group(self, against=None):
        return self

    def supports_execution(self):
        """Return True if this clause element represents a complete executable statement."""

        return False

    def bind(self):
        """Returns the Engine or Connection to which this ClauseElement is bound, or None if none found."""

        try:
            if self._bind is not None:
                return self._bind
        except AttributeError:
            pass
        for f in self._get_from_objects():
            if f is self:
                continue
            engine = f.bind
            if engine is not None:
                return engine
        else:
            return None
    bind = property(bind)

    def execute(self, *multiparams, **params):
        """Compile and execute this ``ClauseElement``."""

        e = self.bind
        if e is None:
            raise exceptions.InvalidRequestError("This Compiled object is not bound to any Engine or Connection.")
        return e.execute_clauseelement(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Compile and execute this ``ClauseElement``, returning the result's scalar representation."""

        return self.execute(*multiparams, **params).scalar()

    def compile(self, bind=None, column_keys=None, compiler=None, dialect=None, inline=False):
        """Compile this SQL expression.

        Uses the given ``Compiler``, or the given ``AbstractDialect``
        or ``Engine`` to create a ``Compiler``.  If no `compiler`
        arguments are given, tries to use the underlying ``Engine``
        this ``ClauseElement`` is bound to to create a ``Compiler``,
        if any.

        Finally, if there is no bound ``Engine``, uses an
        ``DefaultDialect`` to create a default ``Compiler``.

        `parameters` is a dictionary representing the default bind
        parameters to be used with the statement.  If `parameters` is
        a list, it is assumed to be a list of dictionaries and the
        first dictionary in the list is used with which to compile
        against.

        The bind parameters can in some cases determine the output of
        the compilation, such as for ``UPDATE`` and ``INSERT``
        statements the bind parameters that are present determine the
        ``SET`` and ``VALUES`` clause of those statements.
        """

        if compiler is None:
            if dialect is not None:
                compiler = dialect.statement_compiler(dialect, self, column_keys=column_keys, inline=inline)
            elif bind is not None:
                compiler = bind.statement_compiler(self, column_keys=column_keys, inline=inline)
            elif self.bind is not None:
                compiler = self.bind.statement_compiler(self, column_keys=column_keys, inline=inline)

        if compiler is None:
            global DefaultDialect
            if DefaultDialect is None:
                from sqlalchemy.engine.default import DefaultDialect
            dialect = DefaultDialect()
            compiler = dialect.statement_compiler(dialect, self, column_keys=column_keys, inline=inline)
        compiler.compile()
        return compiler

    def __str__(self):
        return unicode(self.compile()).encode('ascii', 'backslashreplace')

    def __and__(self, other):
        return and_(self, other)

    def __or__(self, other):
        return or_(self, other)

    def __invert__(self):
        return self._negate()

    def _negate(self):
        if hasattr(self, 'negation_clause'):
            return self.negation_clause
        else:
            return _UnaryExpression(self.self_group(against=operators.inv), operator=operators.inv, negate=None)

    def __repr__(self):
        friendly = getattr(self, 'description', None)
        if friendly is None:
            return object.__repr__(self)
        else:
            return '<%s.%s at 0x%x; %s>' % (
                self.__module__, self.__class__.__name__, id(self), friendly)


class Operators(object):
    def __and__(self, other):
        return self.operate(operators.and_, other)

    def __or__(self, other):
        return self.operate(operators.or_, other)

    def __invert__(self):
        return self.operate(operators.inv)

    def op(self, opstring):
        def op(b):
            return self.operate(operators.op, opstring, b)
        return op

    def clause_element(self):
        raise NotImplementedError()

    def operate(self, op, *other, **kwargs):
        raise NotImplementedError()

    def reverse_operate(self, op, other, **kwargs):
        raise NotImplementedError()

class ColumnOperators(Operators):
    """Defines comparison and math operations."""

    def __lt__(self, other):
        return self.operate(operators.lt, other)

    def __le__(self, other):
        return self.operate(operators.le, other)

    def __eq__(self, other):
        return self.operate(operators.eq, other)

    def __ne__(self, other):
        return self.operate(operators.ne, other)

    def __gt__(self, other):
        return self.operate(operators.gt, other)

    def __ge__(self, other):
        return self.operate(operators.ge, other)

    def concat(self, other):
        return self.operate(operators.concat_op, other)

    def like(self, other):
        return self.operate(operators.like_op, other)

    def in_(self, *other):
        return self.operate(operators.in_op, other)

    def startswith(self, other):
        return self.operate(operators.startswith_op, other)

    def endswith(self, other):
        return self.operate(operators.endswith_op, other)

    def contains(self, other):
        return self.operate(operators.contains_op, other)

    def desc(self):
        return self.operate(operators.desc_op)

    def asc(self):
        return self.operate(operators.asc_op)

    def __radd__(self, other):
        return self.reverse_operate(operators.add, other)

    def __rsub__(self, other):
        return self.reverse_operate(operators.sub, other)

    def __rmul__(self, other):
        return self.reverse_operate(operators.mul, other)

    def __rdiv__(self, other):
        return self.reverse_operate(operators.div, other)

    def between(self, cleft, cright):
        return self.operate(operators.between_op, cleft, cright)

    def distinct(self):
        return self.operate(operators.distinct_op)

    def __add__(self, other):
        return self.operate(operators.add, other)

    def __sub__(self, other):
        return self.operate(operators.sub, other)

    def __mul__(self, other):
        return self.operate(operators.mul, other)

    def __div__(self, other):
        return self.operate(operators.div, other)

    def __mod__(self, other):
        return self.operate(operators.mod, other)

    def __truediv__(self, other):
        return self.operate(operators.truediv, other)

class _CompareMixin(ColumnOperators):
    """Defines comparison and math operations for ``ClauseElement`` instances."""

    def __compare(self, op, obj, negate=None):
        if obj is None or isinstance(obj, _Null):
            if op == operators.eq:
                return _BinaryExpression(self.expression_element(), null(), operators.is_, negate=operators.isnot)
            elif op == operators.ne:
                return _BinaryExpression(self.expression_element(), null(), operators.isnot, negate=operators.is_)
            else:
                raise exceptions.ArgumentError("Only '='/'!=' operators can be used with NULL")
        else:
            obj = self._check_literal(obj)
        return _BinaryExpression(self.expression_element(), obj, op, type_=sqltypes.Boolean, negate=negate)

    def __operate(self, op, obj):
        obj = self._check_literal(obj)

        type_ = self._compare_type(obj)

        return _BinaryExpression(self.expression_element(), obj, type_.adapt_operator(op), type_=type_)

    # a mapping of operators with the method they use, along with their negated
    # operator for comparison operators
    operators = {
        operators.add : (__operate,),
        operators.mul : (__operate,),
        operators.sub : (__operate,),
        operators.div : (__operate,),
        operators.mod : (__operate,),
        operators.truediv : (__operate,),
        operators.lt : (__compare, operators.ge),
        operators.le : (__compare, operators.gt),
        operators.ne : (__compare, operators.eq),
        operators.gt : (__compare, operators.le),
        operators.ge : (__compare, operators.lt),
        operators.eq : (__compare, operators.ne),
        operators.like_op : (__compare, operators.notlike_op),
    }

    def operate(self, op, *other):
        o = _CompareMixin.operators[op]
        return o[0](self, op, other[0], *o[1:])

    def reverse_operate(self, op, other):
        return self._bind_param(other).operate(op, self)

    def in_(self, *other):
        return self._in_impl(operators.in_op, operators.notin_op, *other)

    def _in_impl(self, op, negate_op, *other):
        # Handle old style *args argument passing
        if len(other) != 1 or not isinstance(other[0], Selectable) and (not hasattr(other[0], '__iter__') or isinstance(other[0], basestring)):
            util.warn_deprecated('passing in_ arguments as varargs is deprecated, in_ takes a single argument that is a sequence or a selectable')
            seq_or_selectable = other
        else:
            seq_or_selectable = other[0]

        if isinstance(seq_or_selectable, Selectable):
            return self.__compare( op, seq_or_selectable, negate=negate_op)

        # Handle non selectable arguments as sequences
        args = []
        for o in seq_or_selectable:
            if not _is_literal(o):
                if not isinstance( o, _CompareMixin):
                    raise exceptions.InvalidRequestError( "in() function accepts either a list of non-selectable values, or a selectable: "+repr(o) )
            else:
                o = self._bind_param(o)
            args.append(o)

        if len(args) == 0:
            # Special case handling for empty IN's
            return _Grouping(case([(self.__eq__(None), text('NULL'))], else_=text('0')).__eq__(text('1')))

        return self.__compare(op, ClauseList(*args).self_group(against=op), negate=negate_op)

    def startswith(self, other):
        """Produce the clause ``LIKE '<other>%'``"""

        perc = isinstance(other, basestring) and '%' or literal('%', type_=sqltypes.String)
        return self.__compare(operators.like_op, other + perc)

    def endswith(self, other):
        """Produce the clause ``LIKE '%<other>'``"""

        if isinstance(other, basestring):
            po = '%' + other
        else:
            po = literal('%', type_=sqltypes.String) + other
            po.type = sqltypes.to_instance(sqltypes.String)     #force!
        return self.__compare(operators.like_op, po)

    def contains(self, other):
        """Produce the clause ``LIKE '%<other>%'``"""

        if isinstance(other, basestring):
            po = '%' + other + '%'
        else:
            perc = literal('%', type_=sqltypes.String)
            po = perc + other + perc
            po.type = sqltypes.to_instance(sqltypes.String)     #force!
        return self.__compare(operators.like_op, po)

    def label(self, name):
        """Produce a column label, i.e. ``<columnname> AS <name>``.

        if 'name' is None, an anonymous label name will be generated.
        """
        return _Label(name, self, self.type)

    def desc(self):
        """Produce a DESC clause, i.e. ``<columnname> DESC``"""

        return desc(self)

    def asc(self):
        """Produce a ASC clause, i.e. ``<columnname> ASC``"""

        return asc(self)

    def distinct(self):
        """Produce a DISTINCT clause, i.e. ``DISTINCT <columnname>``"""
        return _UnaryExpression(self, operator=operators.distinct_op)

    def between(self, cleft, cright):
        """Produce a BETWEEN clause, i.e. ``<column> BETWEEN <cleft> AND <cright>``"""

        return _BinaryExpression(self, ClauseList(self._check_literal(cleft), self._check_literal(cright), operator=operators.and_, group=False), operators.between_op)

    def op(self, operator):
        """produce a generic operator function.

        e.g.::

          somecolumn.op("*")(5)

        produces::

          somecolumn * 5

        operator
          a string which will be output as the infix operator between
          this ``ClauseElement`` and the expression passed to the
          generated function.
        """
        return lambda other: self.__operate(operator, other)

    def _bind_param(self, obj):
        return _BindParamClause(None, obj, type_=self.type, unique=True)

    def _check_literal(self, other):
        if isinstance(other, _BindParamClause) and isinstance(other.type, sqltypes.NullType):
            other.type = self.type
            return other
        elif isinstance(other, Operators):
            return other.expression_element()
        elif _is_literal(other):
            return self._bind_param(other)
        else:
            return other

    def clause_element(self):
        """Allow ``_CompareMixins`` to return the underlying ``ClauseElement``, for non-``ClauseElement`` ``_CompareMixins``."""
        return self

    def expression_element(self):
        """Allow ``_CompareMixins`` to return the appropriate object to be used in expressions."""

        return self

    def _compare_type(self, obj):
        """Allow subclasses to override the type used in constructing
        ``_BinaryExpression`` objects.

        Default return value is the type of the given object.
        """

        return obj.type

class ColumnElement(ClauseElement, _CompareMixin):
    """Represent an element that is usable within the "column clause" portion of a ``SELECT`` statement.

    This includes columns associated with tables, aliases, and
    subqueries, expressions, function calls, SQL keywords such as
    ``NULL``, literals, etc.  ``ColumnElement`` is the ultimate base
    class for all such elements.

    ``ColumnElement`` supports the ability to be a *proxy* element,
    which indicates that the ``ColumnElement`` may be associated with
    a ``Selectable`` which was derived from another ``Selectable``.
    An example of a "derived" ``Selectable`` is an ``Alias`` of a
    ``Table``.

    A ``ColumnElement``, by subclassing the ``_CompareMixin`` mixin
    class, provides the ability to generate new ``ClauseElement``
    objects using Python expressions.  See the ``_CompareMixin``
    docstring for more details.
    """

    primary_key = False
    foreign_keys = []

    def base_columns(self):
        if hasattr(self, '_base_columns'):
            return self._base_columns
        self._base_columns = util.Set([c for c in self.proxy_set if not hasattr(c, 'proxies')])
        return self._base_columns
    base_columns = property(base_columns)

    def proxy_set(self):
        if hasattr(self, '_proxy_set'):
            return self._proxy_set
        s = util.Set([self])
        if hasattr(self, 'proxies'):
            for c in self.proxies:
                s = s.union(c.proxy_set)
        self._proxy_set = s
        return s
    proxy_set = property(proxy_set)

    def shares_lineage(self, othercolumn):
        """Return True if the given ``ColumnElement`` has a common ancestor to this ``ColumnElement``.
        """
        return len(self.proxy_set.intersection(othercolumn.proxy_set)) > 0

    def _make_proxy(self, selectable, name=None):
        """Create a new ``ColumnElement`` representing this
        ``ColumnElement`` as it appears in the select list of a
        descending selectable.

        The default implementation returns a ``_ColumnClause`` if a
        name is given, else just returns self.
        """

        if name is not None:
            co = _ColumnClause(name, selectable)
            co.proxies = [self]
            selectable.columns[name]= co
            return co
        else:
            return self

class ColumnCollection(util.OrderedProperties):
    """An ordered dictionary that stores a list of ColumnElement
    instances.

    Overrides the ``__eq__()`` method to produce SQL clauses between
    sets of correlated columns.
    """

    def __init__(self, *cols):
        super(ColumnCollection, self).__init__()
        [self.add(c) for c in cols]

    def __str__(self):
        return repr([str(c) for c in self])

    def replace(self, column):
        """add the given column to this collection, removing unaliased versions of this column
           as well as existing columns with the same key.

            e.g.::

                t = Table('sometable', Column('col1', Integer))
                t.replace_unalised(Column('col1', Integer, key='columnone'))

            will remove the original 'col1' from the collection, and add
            the new column under the name 'columnname'.

           Used by schema.Column to override columns during table reflection.
        """

        if column.name in self and column.key != column.name:
            other = self[column.name]
            if other.name == other.key:
                del self[other.name]
        util.OrderedProperties.__setitem__(self, column.key, column)

    def add(self, column):
        """Add a column to this collection.

        The key attribute of the column will be used as the hash key
        for this dictionary.
        """

        self[column.key] = column

    def __setitem__(self, key, value):
        if key in self:
            # this warning is primarily to catch select() statements which have conflicting
            # column names in their exported columns collection
            existing = self[key]
            if not existing.shares_lineage(value):
                table = getattr(existing, 'table', None) and existing.table.description
                warnings.warn(RuntimeWarning("Column %r on table %r being replaced by another column with the same key.  Consider use_labels for select() statements."  % (key, table)))
        util.OrderedProperties.__setitem__(self, key, value)

    def remove(self, column):
        del self[column.key]

    def extend(self, iter):
        for c in iter:
            self.add(c)

    def __eq__(self, other):
        l = []
        for c in other:
            for local in self:
                if c.shares_lineage(local):
                    l.append(c==local)
        return and_(*l)

    def __contains__(self, other):
        if not isinstance(other, basestring):
            raise exceptions.ArgumentError("__contains__ requires a string argument")
        return util.OrderedProperties.__contains__(self, other)

    def contains_column(self, col):
        # have to use a Set here, because it will compare the identity
        # of the column, not just using "==" for comparison which will always return a
        # "True" value (i.e. a BinaryClause...)
        return col in util.Set(self)

class ColumnSet(util.OrderedSet):
    def contains_column(self, col):
        return col in self

    def extend(self, cols):
        for col in cols:
            self.add(col)

    def __add__(self, other):
        return list(self) + list(other)

    def __eq__(self, other):
        l = []
        for c in other:
            for local in self:
                if c.shares_lineage(local):
                    l.append(c==local)
        return and_(*l)

class Selectable(ClauseElement):
    """mark a class as being selectable"""

class FromClause(Selectable):
    """Represent an element that can be used within the ``FROM`` clause of a ``SELECT`` statement."""

    __visit_name__ = 'fromclause'
    named_with_column=False
    _hide_froms = []

    def __init__(self):
        self.oid_column = None

    def _get_from_objects(self, **modifiers):
        return []

    def default_order_by(self):
        return [self.oid_column]

    def count(self, whereclause=None, **params):
        """return a SELECT COUNT generated against this ``FromClause``."""

        if self.primary_key:
            col = list(self.primary_key)[0]
        else:
            col = list(self.columns)[0]
        return select([func.count(col).label('tbl_row_count')], whereclause, from_obj=[self], **params)

    def select(self, whereclauses = None, **params):
        """return a SELECT of this ``FromClause``."""

        return select([self], whereclauses, **params)

    def join(self, right, *args, **kwargs):
        """return a join of this ``FromClause`` against another ``FromClause``."""

        return Join(self, right, *args, **kwargs)

    def outerjoin(self, right, *args, **kwargs):
        """return an outer join of this ``FromClause`` against another ``FromClause``."""

        return Join(self, right, isouter=True, *args, **kwargs)

    def alias(self, name=None):
        """return an alias of this ``FromClause`` against another ``FromClause``."""

        return Alias(self, name)

    def is_derived_from(self, fromclause):
        """Return True if this FromClause is 'derived' from the given FromClause.

        An example would be an Alias of a Table is derived from that Table.
        """
        return fromclause in util.Set(self._cloned_set)

    def replace_selectable(self, old, alias):
      """replace all occurences of FromClause 'old' with the given Alias object, returning a copy of this ``FromClause``."""

      global ClauseAdapter
      if ClauseAdapter is None:
          from sqlalchemy.sql.util import ClauseAdapter
      return ClauseAdapter(alias).traverse(self, clone=True)

    def corresponding_column(self, column, require_embedded=False):
        """Given a ``ColumnElement``, return the exported ``ColumnElement``
        object from this ``Selectable`` which corresponds to that
        original ``Column`` via a common anscestor column.

        column
          the target ``ColumnElement`` to be matched

        require_embedded
          only return corresponding columns for the given
          ``ColumnElement``, if the given ``ColumnElement`` is
          actually present within a sub-element of this
          ``FromClause``.  Normally the column will match if it merely
          shares a common anscestor with one of the exported columns
          of this ``FromClause``.
        """

        # dont dig around if the column is locally present
        if self.c.contains_column(column):
            return column

        col, intersect = None, None
        target_set = column.proxy_set
        for c in self.c + [self.oid_column]:
            i = c.proxy_set.intersection(target_set)
            if i and \
                (not require_embedded or c.proxy_set.issuperset(target_set)) and \
                (intersect is None or len(i) > len(intersect)):
                col, intersect = c, i
        return col

    def description(self):
        """a brief description of this FromClause.

        Used primarily for error message formatting.
        """
        return getattr(self, 'name', self.__class__.__name__ + " object")
    description = property(description)

    def _clone_from_clause(self):
        # delete all the "generated" collections of columns for a
        # newly cloned FromClause, so that they will be re-derived
        # from the item.  this is because FromClause subclasses, when
        # cloned, need to reestablish new "proxied" columns that are
        # linked to the new item
        for attr in ('_columns', '_primary_key' '_foreign_keys', '_oid_column', '_embedded_columns', '_all_froms'):
            if hasattr(self, attr):
                delattr(self, attr)

    def _expr_attr_func(name):
        def attr(self):
            try:
                return getattr(self, name)
            except AttributeError:
                self._export_columns()
                return getattr(self, name)
        return property(attr)

    columns = c = _expr_attr_func('_columns')
    primary_key = _expr_attr_func('_primary_key')
    foreign_keys = _expr_attr_func('_foreign_keys')

    def _export_columns(self, columns=None):
        """Initialize column collections."""

        if hasattr(self, '_columns') and columns is None:
            return
        self._columns = ColumnCollection()
        self._primary_key = ColumnSet()
        self._foreign_keys = util.Set()

        if columns is None:
            columns = self._flatten_exportable_columns()
        for co in columns:
            cp = self._proxy_column(co)

    def _flatten_exportable_columns(self):
        """Return the list of ColumnElements represented within this FromClause's _exportable_columns"""
        export = self._exportable_columns()
        for column in export:
            if isinstance(column, Selectable):
                for co in column.columns:
                    yield co
            elif isinstance(column, ColumnElement):
                yield column
            else:
                continue

    def _exportable_columns(self):
        return []

    def _proxy_column(self, column):
        return column._make_proxy(self)

class _TextFromClause(FromClause):
    __visit_name__ = 'fromclause'

    def __init__(self, text):
        self.name = text

class _BindParamClause(ClauseElement, _CompareMixin):
    """Represent a bind parameter.

    Public constructor is the ``bindparam()`` function.
    """

    __visit_name__ = 'bindparam'

    def __init__(self, key, value, type_=None, unique=False, isoutparam=False, shortname=None):
        """Construct a _BindParamClause.

        key
          the key for this bind param.  Will be used in the generated
          SQL statement for dialects that use named parameters.  This
          value may be modified when part of a compilation operation,
          if other ``_BindParamClause`` objects exist with the same
          key, or if its length is too long and truncation is
          required.

        value
          Initial value for this bind param.  This value may be
          overridden by the dictionary of parameters sent to statement
          compilation/execution.

        shortname
          deprecated.

        type\_
          A ``TypeEngine`` object that will be used to pre-process the
          value corresponding to this ``_BindParamClause`` at
          execution time.

        unique
          if True, the key name of this BindParamClause will be
          modified if another ``_BindParamClause`` of the same name
          already has been located within the containing
          ``ClauseElement``.

        isoutparam
          if True, the parameter should be treated like a stored procedure "OUT"
          parameter.
        """

        if unique:
            self.key = "{ANON %d %s}" % (id(self), key or 'param')
        else:
            self.key = key or "{ANON %d param}" % id(self)
        self._orig_key = key or 'param'
        self.unique = unique
        self.value = value
        self.isoutparam = isoutparam
        self.shortname = shortname

        if type_ is None:
            self.type = sqltypes.type_map.get(type(value), sqltypes.NullType)()
        elif isinstance(type_, type):
            self.type = type_()
        else:
            self.type = type_

    def _clone(self):
        c = ClauseElement._clone(self)
        if self.unique:
            c.key = "{ANON %d %s}" % (id(c), c._orig_key or 'param')
        return c

    def _convert_to_unique(self):
        if not self.unique:
            self.unique=True
            self.key = "{ANON %d %s}" % (id(self), self._orig_key or 'param')

    def _get_from_objects(self, **modifiers):
        return []

    def bind_processor(self, dialect):
        return self.type.dialect_impl(dialect).bind_processor(dialect)

    def _compare_type(self, obj):
        if not isinstance(self.type, sqltypes.NullType):
            return self.type
        else:
            return obj.type

    def compare(self, other):
        """Compare this ``_BindParamClause`` to the given clause.

        Since ``compare()`` is meant to compare statement syntax, this
        method returns True if the two ``_BindParamClauses`` have just
        the same type.
        """

        return isinstance(other, _BindParamClause) and other.type.__class__ == self.type.__class__

    def __repr__(self):
        return "_BindParamClause(%s, %s, type_=%s)" % (repr(self.key), repr(self.value), repr(self.type))

class _TypeClause(ClauseElement):
    """Handle a type keyword in a SQL statement.

    Used by the ``Case`` statement.
    """

    __visit_name__ = 'typeclause'

    def __init__(self, type):
        self.type = type

    def _get_from_objects(self, **modifiers):
        return []

class _TextClause(ClauseElement):
    """Represent a literal SQL text fragment.

    Public constructor is the ``text()`` function.
    """

    __visit_name__ = 'textclause'

    _bind_params_regex = re.compile(r'(?<![:\w\x5c]):(\w+)(?!:)', re.UNICODE)

    def __init__(self, text = "", bind=None, bindparams=None, typemap=None):
        self._bind = bind
        self.bindparams = {}
        self.typemap = typemap
        if typemap is not None:
            for key in typemap.keys():
                typemap[key] = sqltypes.to_instance(typemap[key])

        def repl(m):
            self.bindparams[m.group(1)] = bindparam(m.group(1))
            return ":%s" % m.group(1)

        # scan the string and search for bind parameter names, add them
        # to the list of bindparams
        self.text = self._bind_params_regex.sub(repl, text)
        if bindparams is not None:
            for b in bindparams:
                self.bindparams[b.key] = b

    def type(self):
        if self.typemap is not None and len(self.typemap) == 1:
            return list(self.typemap)[0]
        else:
            return None
    type = property(type)

    def _copy_internals(self, clone=_clone):
        self.bindparams = dict([(b.key, clone(b)) for b in self.bindparams.values()])

    def get_children(self, **kwargs):
        return self.bindparams.values()

    def _get_from_objects(self, **modifiers):
        return []

    def supports_execution(self):
        return True

    def _table_iterator(self):
        return iter([])

class _Null(ColumnElement):
    """Represent the NULL keyword in a SQL statement.

    Public constructor is the ``null()`` function.
    """

    def __init__(self):
        ColumnElement.__init__(self)
        self.type = sqltypes.NULLTYPE

    def _get_from_objects(self, **modifiers):
        return []

class ClauseList(ClauseElement):
    """Describe a list of clauses, separated by an operator.

    By default, is comma-separated, such as a column listing.
    """
    __visit_name__ = 'clauselist'

    def __init__(self, *clauses, **kwargs):
        self.clauses = []
        self.operator = kwargs.pop('operator', operators.comma_op)
        self.group = kwargs.pop('group', True)
        self.group_contents = kwargs.pop('group_contents', True)
        for c in clauses:
            if c is None:
                continue
            self.append(c)

    def __iter__(self):
        return iter(self.clauses)
    def __len__(self):
        return len(self.clauses)

    def append(self, clause):
        # TODO: not sure if i like the 'group_contents' flag.  need to
        # define the difference between a ClauseList of ClauseLists,
        # and a "flattened" ClauseList of ClauseLists.  flatten()
        # method ?
        if self.group_contents:
            self.clauses.append(_literal_as_text(clause).self_group(against=self.operator))
        else:
            self.clauses.append(_literal_as_text(clause))

    def _copy_internals(self, clone=_clone):
        self.clauses = [clone(clause) for clause in self.clauses]

    def get_children(self, **kwargs):
        return self.clauses

    def _get_from_objects(self, **modifiers):
        f = []
        for c in self.clauses:
            f += c._get_from_objects(**modifiers)
        return f

    def self_group(self, against=None):
        if self.group and self.operator != against and operators.is_precedent(self.operator, against):
            return _Grouping(self)
        else:
            return self

    def compare(self, other):
        """Compare this ``ClauseList`` to the given ``ClauseList``,
        including a comparison of all the clause items.
        """

        if not isinstance(other, ClauseList) and len(self.clauses) == 1:
            return self.clauses[0].compare(other)
        elif isinstance(other, ClauseList) and len(self.clauses) == len(other.clauses):
            for i in range(0, len(self.clauses)):
                if not self.clauses[i].compare(other.clauses[i]):
                    return False
            else:
                return self.operator == other.operator
        else:
            return False

class _CalculatedClause(ColumnElement):
    """Describe a calculated SQL expression that has a type, like ``CASE``.

    Extends ``ColumnElement`` to provide column-level comparison
    operators.
    """

    __visit_name__ = 'calculatedclause'

    def __init__(self, name, *clauses, **kwargs):
        self.name = name
        self.type = sqltypes.to_instance(kwargs.get('type_', None))
        self._bind = kwargs.get('bind', None)
        self.group = kwargs.pop('group', True)
        clauses = ClauseList(operator=kwargs.get('operator', None), group_contents=kwargs.get('group_contents', True), *clauses)
        if self.group:
            self.clause_expr = clauses.self_group()
        else:
            self.clause_expr = clauses

    def key(self):
        return self.name or '_calc_'
    key = property(key)

    def _copy_internals(self, clone=_clone):
        self.clause_expr = clone(self.clause_expr)

    def clauses(self):
        if isinstance(self.clause_expr, _Grouping):
            return self.clause_expr.elem
        else:
            return self.clause_expr
    clauses = property(clauses)

    def get_children(self, **kwargs):
        return self.clause_expr,

    def _get_from_objects(self, **modifiers):
        return self.clauses._get_from_objects(**modifiers)

    def _bind_param(self, obj):
        return _BindParamClause(self.name, obj, type_=self.type, unique=True)

    def select(self):
        return select([self])

    def scalar(self):
        return select([self]).execute().scalar()

    def execute(self):
        return select([self]).execute()

    def _compare_type(self, obj):
        return self.type

class _Function(_CalculatedClause, FromClause):
    """Describe a SQL function.

    Extends ``_CalculatedClause``, turn the *clauselist* into function
    arguments, also adds a `packagenames` argument.
    """

    def __init__(self, name, *clauses, **kwargs):
        self.packagenames = kwargs.get('packagenames', None) or []
        self.oid_column = None
        self.name = name
        self._bind = kwargs.get('bind', None)
        args = [_literal_as_binds(c, self.name) for c in clauses]
        self.clause_expr = ClauseList(operator=operators.comma_op, group_contents=True, *args).self_group()
        self.type = sqltypes.to_instance(kwargs.get('type_', None))

    def key(self):
        return self.name
    key = property(key)

    def columns(self):
        return [self]
    columns = property(columns)

    def _copy_internals(self, clone=_clone):
        _CalculatedClause._copy_internals(self, clone=clone)
        self._clone_from_clause()

    def get_children(self, **kwargs):
        return _CalculatedClause.get_children(self, **kwargs)


class _Cast(ColumnElement):

    def __init__(self, clause, totype, **kwargs):
        ColumnElement.__init__(self)
        if not hasattr(clause, 'label'):
            clause = literal(clause)
        self.type = sqltypes.to_instance(totype)
        self.clause = clause
        self.typeclause = _TypeClause(self.type)

    def _copy_internals(self, clone=_clone):
        self.clause = clone(self.clause)
        self.typeclause = clone(self.typeclause)

    def get_children(self, **kwargs):
        return self.clause, self.typeclause

    def _get_from_objects(self, **modifiers):
        return self.clause._get_from_objects(**modifiers)

    def _make_proxy(self, selectable, name=None):
        if name is not None:
            co = _ColumnClause(name, selectable, type_=self.type)
            co.proxies = [self]
            selectable.columns[name]= co
            return co
        else:
            return self


class _UnaryExpression(ColumnElement):
    def __init__(self, element, operator=None, modifier=None, type_=None, negate=None):
        ColumnElement.__init__(self)
        self.operator = operator
        self.modifier = modifier

        self.element = _literal_as_text(element).self_group(against=self.operator or self.modifier)
        self.type = sqltypes.to_instance(type_)
        self.negate = negate

    def _get_from_objects(self, **modifiers):
        return self.element._get_from_objects(**modifiers)

    def _copy_internals(self, clone=_clone):
        self.element = clone(self.element)

    def get_children(self, **kwargs):
        return self.element,

    def compare(self, other):
        """Compare this ``_UnaryExpression`` against the given ``ClauseElement``."""

        return (
            isinstance(other, _UnaryExpression) and
            self.operator == other.operator and
            self.modifier == other.modifier and
            self.element.compare(other.element)
        )

    def _negate(self):
        if self.negate is not None:
            return _UnaryExpression(self.element, operator=self.negate, negate=self.operator, modifier=self.modifier, type_=self.type)
        else:
            return super(_UnaryExpression, self)._negate()

    def self_group(self, against):
        if self.operator and operators.is_precedent(self.operator, against):
            return _Grouping(self)
        else:
            return self


class _BinaryExpression(ColumnElement):
    """Represent an expression that is ``LEFT <operator> RIGHT``."""

    def __init__(self, left, right, operator, type_=None, negate=None):
        ColumnElement.__init__(self)
        self.left = _literal_as_text(left).self_group(against=operator)
        self.right = _literal_as_text(right).self_group(against=operator)
        self.operator = operator
        self.type = sqltypes.to_instance(type_)
        self.negate = negate

    def _get_from_objects(self, **modifiers):
        return self.left._get_from_objects(**modifiers) + self.right._get_from_objects(**modifiers)

    def _copy_internals(self, clone=_clone):
        self.left = clone(self.left)
        self.right = clone(self.right)

    def get_children(self, **kwargs):
        return self.left, self.right

    def compare(self, other):
        """Compare this ``_BinaryExpression`` against the given ``_BinaryExpression``."""

        return (
            isinstance(other, _BinaryExpression) and
            self.operator == other.operator and
            (
                self.left.compare(other.left) and
                self.right.compare(other.right) or
                (
                    operators.is_commutative(self.operator) and
                    self.left.compare(other.right) and
                    self.right.compare(other.left)
                )
            )
        )

    def self_group(self, against=None):
        # use small/large defaults for comparison so that unknown
        # operators are always parenthesized
        if self.operator != against and operators.is_precedent(self.operator, against):
            return _Grouping(self)
        else:
            return self

    def _negate(self):
        if self.negate is not None:
            return _BinaryExpression(self.left, self.right, self.negate, negate=self.operator, type_=self.type)
        else:
            return super(_BinaryExpression, self)._negate()

class _Exists(_UnaryExpression):
    __visit_name__ = _UnaryExpression.__visit_name__

    def __init__(self, *args, **kwargs):
        kwargs['correlate'] = True
        s = select(*args, **kwargs).as_scalar().self_group()
        _UnaryExpression.__init__(self, s, operator=operators.exists)

    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def correlate(self, fromclause):
        e = self._clone()
        e.element = self.element.correlate(fromclause).self_group()
        return e

    def where(self, clause):
        e = self._clone()
        e.element = self.element.where(clause).self_group()
        return e

class Join(FromClause):
    """represent a ``JOIN`` construct between two ``FromClause`` elements.

    The public constructor function for ``Join`` is the module-level
    ``join()`` function, as well as the ``join()`` method available
    off all ``FromClause`` subclasses.
    """

    def __init__(self, left, right, onclause=None, isouter = False):
        self.left = _selectable(left)
        self.right = _selectable(right).self_group()

        self.oid_column = self.left.oid_column
        if onclause is None:
            self.onclause = self._match_primaries(self.left, self.right)
        else:
            self.onclause = onclause
        self.isouter = isouter
        self.__folded_equivalents = None

    def _export_columns(self):
        if hasattr(self, '_columns'):
            return
        self._columns = ColumnCollection()
        self._foreign_keys = util.Set()

        columns = list(self._flatten_exportable_columns())
        self.__init_primary_key(columns)
        for co in columns:
            cp = self._proxy_column(co)

    def __init_primary_key(self, columns):
        global schema
        if schema is None:
            from sqlalchemy import schema
        pkcol = util.Set([c for c in columns if c.primary_key])

        equivs = {}
        def add_equiv(a, b):
            for x, y in ((a, b), (b, a)):
                if x in equivs:
                    equivs[x].add(y)
                else:
                    equivs[x] = util.Set([y])

        def visit_binary(binary):
            if binary.operator == operators.eq and isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column):
                add_equiv(binary.left, binary.right)
        visitors.traverse(self.onclause, visit_binary=visit_binary)

        for col in pkcol:
            for fk in col.foreign_keys:
                if fk.column in pkcol:
                    add_equiv(col, fk.column)

        omit = util.Set()
        for col in pkcol:
            p = col
            for c in equivs.get(col, util.Set()):
                if p.references(c) or (c.primary_key and not p.primary_key):
                    omit.add(p)
                    p = c

        self._primary_key = ColumnSet(pkcol.difference(omit))

    def description(self):
        return "Join object on %s(%d) and %s(%d)" % (self.left.description, id(self.left), self.right.description, id(self.right))
    description = property(description)

    def self_group(self, against=None):
        return _FromGrouping(self)

    def _exportable_columns(self):
        return [c for c in self.left.columns] + [c for c in self.right.columns]

    def _proxy_column(self, column):
        self._columns[column._label] = column
        for f in column.foreign_keys:
            self._foreign_keys.add(f)
        return column

    def _copy_internals(self, clone=_clone):
        self._clone_from_clause()
        self.left = clone(self.left)
        self.right = clone(self.right)
        self.onclause = clone(self.onclause)
        self.__folded_equivalents = None

    def get_children(self, **kwargs):
        return self.left, self.right, self.onclause

    def _match_primaries(self, primary, secondary):
        crit = []
        constraints = util.Set()
        for fk in secondary.foreign_keys:
            col = fk.get_referent(primary)
            if col:
                crit.append(col == fk.parent)
                constraints.add(fk.constraint)
        if primary is not secondary:
            for fk in primary.foreign_keys:
                col = fk.get_referent(secondary)
                if col:
                    crit.append(col == fk.parent)
                    constraints.add(fk.constraint)
        if len(crit) == 0:
            raise exceptions.ArgumentError(
                "Can't find any foreign key relationships "
                "between '%s' and '%s'" % (primary.description, secondary.description))
        elif len(constraints) > 1:
            raise exceptions.ArgumentError(
                "Can't determine join between '%s' and '%s'; "
                "tables have more than one foreign key "
                "constraint relationship between them. "
                "Please specify the 'onclause' of this "
                "join explicitly." % (primary.description, secondary.description))
        elif len(crit) == 1:
            return (crit[0])
        else:
            return and_(*crit)

    def _folded_equivalents(self, equivs=None):
        """Returns the column list of this Join with all equivalently-named,
        equated columns folded into one column, where 'equated' means they are
        equated to each other in the ON clause of this join.
        """

        if self.__folded_equivalents is not None:
            return self.__folded_equivalents
        if equivs is None:
            equivs = util.Set()
        class LocateEquivs(visitors.NoColumnVisitor):
            def visit_binary(self, binary):
                if binary.operator == operators.eq and binary.left.name == binary.right.name:
                    equivs.add(binary.right)
                    equivs.add(binary.left)
        LocateEquivs().traverse(self.onclause)
        collist = []
        if isinstance(self.left, Join):
            left = self.left._folded_equivalents(equivs)
        else:
            left = list(self.left.columns)
        if isinstance(self.right, Join):
            right = self.right._folded_equivalents(equivs)
        else:
            right = list(self.right.columns)
        used = util.Set()
        for c in left + right:
            if c in equivs:
                if c.name not in used:
                    collist.append(c)
                    used.add(c.name)
            else:
                collist.append(c)
        self.__folded_equivalents = collist
        return self.__folded_equivalents
    folded_equivalents = property(_folded_equivalents)

    def select(self, whereclause = None, fold_equivalents=False, **kwargs):
        """Create a ``Select`` from this ``Join``.

        whereclause
          the WHERE criterion that will be sent to the ``select()``
          function

        fold_equivalents
          based on the join criterion of this ``Join``, do not include
          repeat column names in the column list of the resulting
          select, for columns that are calculated to be "equivalent"
          based on the join criterion of this ``Join``. This will
          recursively apply to any joins directly nested by this one
          as well.

        \**kwargs
          all other kwargs are sent to the underlying ``select()`` function.
          See the ``select()`` module level function for details.
        """

        if fold_equivalents:
            collist = self.folded_equivalents
        else:
            collist = [self.left, self.right]

        return select(collist, whereclause, from_obj=[self], **kwargs)

    def bind(self):
        return self.left.bind or self.right.bind
    bind = property(bind)

    def alias(self, name=None):
        """Create a ``Select`` out of this ``Join`` clause and return an ``Alias`` of it.

        The ``Select`` is not correlating.
        """

        return self.select(use_labels=True, correlate=False).alias(name)

    def _hide_froms(self):
        return chain(*[x.left._get_from_objects() + x.right._get_from_objects() for x in self._cloned_set])
    _hide_froms = property(_hide_froms)

    def _get_from_objects(self, **modifiers):
        return [self] + self.onclause._get_from_objects(**modifiers) + self.left._get_from_objects(**modifiers) + self.right._get_from_objects(**modifiers)

class Alias(FromClause):
    """Represents an table or selectable alias (AS).

    Represents an alias, as typically applied to any table or
    sub-select within a SQL statement using the ``AS`` keyword (or
    without the keyword on certain databases such as Oracle).

    This object is constructed from the ``alias()`` module level
    function as well as the ``alias()`` method available on all
    ``FromClause`` subclasses.
    """

    named_with_column = True

    def __init__(self, selectable, alias=None):
        baseselectable = selectable
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.selectable
        self.original = baseselectable
        self.selectable = selectable
        if alias is None:
            if self.original.named_with_column:
                alias = getattr(self.original, 'name', None)
            alias = '{ANON %d %s}' % (id(self), alias or 'anon')
        self.name = alias
        if self.selectable.oid_column is not None:
            self.oid_column = self.selectable.oid_column._make_proxy(self)
        else:
            self.oid_column = None

    def description(self):
        return self.name.encode('ascii', 'backslashreplace')
    description = property(description)

    def is_derived_from(self, fromclause):
        if fromclause in util.Set(self._cloned_set):
            return True
        return self.selectable.is_derived_from(fromclause)

    def supports_execution(self):
        return self.original.supports_execution()

    def _table_iterator(self):
        return self.original._table_iterator()

    def _exportable_columns(self):
        #return self.selectable._exportable_columns()
        return self.selectable.columns

    def _copy_internals(self, clone=_clone):
       self._clone_from_clause()
       self.selectable = _clone(self.selectable)
       baseselectable = self.selectable
       while isinstance(baseselectable, Alias):
           baseselectable = baseselectable.selectable
       self.original = baseselectable

    def get_children(self, column_collections=True, aliased_selectables=True, **kwargs):
        if column_collections:
            for c in self.c:
                yield c
        if aliased_selectables:
            yield self.selectable

    def _get_from_objects(self, **modifiers):
        return [self]

    def bind(self):
        return self.selectable.bind
    bind = property(bind)

class _ColumnElementAdapter(ColumnElement):
    """Adapts a ClauseElement which may or may not be a
    ColumnElement subclass itself into an object which
    acts like a ColumnElement.
    """

    def __init__(self, elem):
        self.elem = elem
        self.type = getattr(elem, 'type', None)

    def key(self):
        return self.elem.key
    key = property(key)

    def _label(self):
        return self.elem._label
    _label = property(_label)

    def _copy_internals(self, clone=_clone):
        self.elem = clone(self.elem)

    def get_children(self, **kwargs):
        return self.elem,

    def _get_from_objects(self, **modifiers):
        return self.elem._get_from_objects(**modifiers)

    def __getattr__(self, attr):
        return getattr(self.elem, attr)

    def __getstate__(self):
        return {'elem':self.elem, 'type':self.type}

    def __setstate__(self, state):
        self.elem = state['elem']
        self.type = state['type']

class _Grouping(_ColumnElementAdapter):
    """Represent a grouping within a column expression"""
    pass

class _FromGrouping(FromClause):
    """Represent a grouping of a FROM clause"""
    __visit_name__ = 'grouping'

    def __init__(self, elem):
        self.elem = elem

    def columns(self):
        return self.elem.columns
    columns = c = property(columns)

    def _hide_froms(self):
        return self.elem._hide_froms
    _hide_froms = property(_hide_froms)

    def get_children(self, **kwargs):
        return self.elem,

    def _copy_internals(self, clone=_clone):
        self.elem = clone(self.elem)

    def _get_from_objects(self, **modifiers):
        return self.elem._get_from_objects(**modifiers)

    def __getattr__(self, attr):
        return getattr(self.elem, attr)

class _Label(ColumnElement):
    """Represents a column label (AS).

    Represent a label, as typically applied to any column-level
    element using the ``AS`` sql keyword.

    This object is constructed from the ``label()`` module level
    function as well as the ``label()`` method available on all
    ``ColumnElement`` subclasses.
    """

    def __init__(self, name, obj, type_=None):
        while isinstance(obj, _Label):
            obj = obj.obj
        self.name = name or "{ANON %d %s}" % (id(self), getattr(obj, 'name', 'anon'))
        self.obj = obj.self_group(against=operators.as_)
        self.type = sqltypes.to_instance(type_ or getattr(obj, 'type', None))

    def key(self):
        return self.name
    key = property(key)

    def _label(self):
        return self.name
    _label = property(_label)

    def _proxy_attr(name):
        def attr(self):
            return getattr(self.obj, name)
        return property(attr)

    proxies = _proxy_attr('proxies')
    base_columns = _proxy_attr('base_columns')
    proxy_set = _proxy_attr('proxy_set')
    primary_key = _proxy_attr('primary_key')
    foreign_keys = _proxy_attr('foreign_keys')

    def expression_element(self):
        return self.obj

    def get_children(self, **kwargs):
        return self.obj,

    def _copy_internals(self, clone=_clone):
        self.obj = clone(self.obj)

    def _get_from_objects(self, **modifiers):
        return self.obj._get_from_objects(**modifiers)

    def _make_proxy(self, selectable, name = None):
        if isinstance(self.obj, (Selectable, ColumnElement)):
            return self.obj._make_proxy(selectable, name=self.name)
        else:
            return column(self.name)._make_proxy(selectable=selectable)

class _ColumnClause(ColumnElement):
    """Represents a generic column expression from any textual string.

    This includes columns associated with tables, aliases and select
    statements, but also any arbitrary text.  May or may not be bound
    to an underlying ``Selectable``.  ``_ColumnClause`` is usually
    created publically via the ``column()`` function or the
    ``literal_column()`` function.

    text
      the text of the element.

    selectable
      parent selectable.

    type
      ``TypeEngine`` object which can associate this ``_ColumnClause``
      with a type.

    is_literal
      if True, the ``_ColumnClause`` is assumed to be an exact
      expression that will be delivered to the output with no quoting
      rules applied regardless of case sensitive settings.  the
      ``literal_column()`` function is usually used to create such a
      ``_ColumnClause``.
    """

    def __init__(self, text, selectable=None, type_=None, _is_oid=False, is_literal=False):
        ColumnElement.__init__(self)
        self.key = self.name = text
        self.table = selectable
        self.type = sqltypes.to_instance(type_)
        self._is_oid = _is_oid
        self.__label = None
        self.is_literal = is_literal

    def description(self):
        return self.name.encode('ascii', 'backslashreplace')
    description = property(description)

    def _clone(self):
        # ColumnClause is immutable
        return self

    def _label(self):
        """Generate a 'label' string for this column.
        """

        # for a "literal" column, we've no idea what the text is
        # therefore no 'label' can be automatically generated
        if self.is_literal:
            return None
        if self.__label is None:
            if self.table is not None and self.table.named_with_column:
                self.__label = self.table.name + "_" + self.name
                counter = 1
                while self.__label in self.table.c:
                    self.__label = self.__label + "_%d" % counter
                    counter += 1
            else:
                self.__label = self.name
        return self.__label

    _label = property(_label)

    def label(self, name):
        # if going off the "__label" property and its None, we have
        # no label; return self
        if name is None:
            return self
        else:
            return super(_ColumnClause, self).label(name)

    def _get_from_objects(self, **modifiers):
        if self.table is not None:
            return [self.table]
        else:
            return []

    def _bind_param(self, obj):
        return _BindParamClause(self._label, obj, type_=self.type, unique=True)

    def _make_proxy(self, selectable, name = None):
        # propigate the "is_literal" flag only if we are keeping our name,
        # otherwise its considered to be a label
        is_literal = self.is_literal and (name is None or name == self.name)
        c = _ColumnClause(name or self.name, selectable=selectable, _is_oid=self._is_oid, type_=self.type, is_literal=is_literal)
        c.proxies = [self]
        if not self._is_oid:
            selectable.columns[c.name] = c
        return c

    def _compare_type(self, obj):
        return self.type

class TableClause(FromClause):
    """Represents a "table" construct.

    Note that this represents tables only as another syntactical
    construct within SQL expressions; it does not provide schema-level
    functionality.
    """

    named_with_column = True

    def __init__(self, name, *columns):
        super(TableClause, self).__init__()
        self.name = self.fullname = name
        self.oid_column = _ColumnClause('oid', self, _is_oid=True)
        self._export_columns(columns)

    def description(self):
        return self.name.encode('ascii', 'backslashreplace')
    description = property(description)

    def _clone(self):
        # TableClause is immutable
        return self

    def append_column(self, c):
        self._columns[c.name] = c
        c.table = self

    def _proxy_column(self, c):
        self.append_column(c)
        return c

    def get_children(self, column_collections=True, **kwargs):
        if column_collections:
            return [c for c in self.c]
        else:
            return []

    def _exportable_columns(self):
        raise NotImplementedError()

    def count(self, whereclause=None, **params):
        if self.primary_key:
            col = list(self.primary_key)[0]
        else:
            col = list(self.columns)[0]
        return select([func.count(col).label('tbl_row_count')], whereclause, from_obj=[self], **params)

    def join(self, right, *args, **kwargs):
        return Join(self, right, *args, **kwargs)

    def outerjoin(self, right, *args, **kwargs):
        return Join(self, right, isouter = True, *args, **kwargs)

    def alias(self, name=None):
        return Alias(self, name)

    def select(self, whereclause = None, **params):
        return select([self], whereclause, **params)

    def insert(self, values=None, inline=False, **kwargs):
        return insert(self, values=values, inline=inline, **kwargs)

    def update(self, whereclause=None, values=None, inline=False, **kwargs):
        return update(self, whereclause=whereclause, values=values, inline=inline, **kwargs)

    def delete(self, whereclause=None):
        return delete(self, whereclause)

    def _get_from_objects(self, **modifiers):
        return [self]


class _SelectBaseMixin(object):
    """Base class for ``Select`` and ``CompoundSelects``."""

    def __init__(self, use_labels=False, for_update=False, limit=None, offset=None, order_by=None, group_by=None, bind=None):
        self.use_labels = use_labels
        self.for_update = for_update
        self._limit = limit
        self._offset = offset
        self._bind = bind

        self._order_by_clause = ClauseList(*util.to_list(order_by, []))
        self._group_by_clause = ClauseList(*util.to_list(group_by, []))

    def as_scalar(self):
        """return a 'scalar' representation of this selectable, which can be used
        as a column expression.

        Typically, a select statement which has only one column in its columns clause
        is eligible to be used as a scalar expression.

        The returned object is an instance of [sqlalchemy.sql.expression#_ScalarSelect].
        """

        return _ScalarSelect(self)

    def apply_labels(self):
        """set the 'labels' flag on this selectable.

        This will result in column expressions being generated using labels against their table
        name, such as "SELECT somecolumn AS tablename_somecolumn".  This allows selectables which
        contain multiple FROM clauses to produce a unique set of column names regardless of name conflicts
        among the individual FROM clauses.
        """

        s = self._generate()
        s.use_labels = True
        return s

    def label(self, name):
        """return a 'scalar' representation of this selectable, embedded as a subquery
        with a label.

        See also ``as_scalar()``.
        """

        return self.as_scalar().label(name)

    def supports_execution(self):
        """part of the ClauseElement contract; returns ``True`` in all cases for this class."""

        return True

    def _generate(self):
        s = self._clone()
        s._clone_from_clause()
        return s

    def limit(self, limit):
        """return a new selectable with the given LIMIT criterion applied."""

        s = self._generate()
        s._limit = limit
        return s

    def offset(self, offset):
        """return a new selectable with the given OFFSET criterion applied."""

        s = self._generate()
        s._offset = offset
        return s

    def order_by(self, *clauses):
        """return a new selectable with the given list of ORDER BY criterion applied.

        The criterion will be appended to any pre-existing ORDER BY criterion.
        """

        s = self._generate()
        s.append_order_by(*clauses)
        return s

    def group_by(self, *clauses):
        """return a new selectable with the given list of GROUP BY criterion applied.

        The criterion will be appended to any pre-existing GROUP BY criterion.
        """

        s = self._generate()
        s.append_group_by(*clauses)
        return s

    def append_order_by(self, *clauses):
        """Append the given ORDER BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing ORDER BY criterion.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        if len(clauses) == 1 and clauses[0] is None:
            self._order_by_clause = ClauseList()
        else:
            if getattr(self, '_order_by_clause', None):
                clauses = list(self._order_by_clause) + list(clauses)
            self._order_by_clause = ClauseList(*clauses)

    def append_group_by(self, *clauses):
        """Append the given GROUP BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing GROUP BY criterion.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        if len(clauses) == 1 and clauses[0] is None:
            self._group_by_clause = ClauseList()
        else:
            if getattr(self, '_group_by_clause', None):
                clauses = list(self._group_by_clause) + list(clauses)
            self._group_by_clause = ClauseList(*clauses)

    def select(self, whereclauses = None, **params):
        """return a SELECT of this selectable.

        This has the effect of embeddeding this select into a subquery that is selected
        from.
        """
        return select([self], whereclauses, **params)

    def _get_from_objects(self, is_where=False, **modifiers):
        if is_where:
            return []
        else:
            return [self]

class _ScalarSelect(_Grouping):
    __visit_name__ = 'grouping'

    def __init__(self, elem):
        self.elem = elem
        cols = list(elem.inner_columns)
        if len(cols) != 1:
            raise exceptions.InvalidRequestError("Scalar select can only be created from a Select object that has exactly one column expression.")
        self.type = cols[0].type

    def columns(self):
        raise exceptions.InvalidRequestError("Scalar Select expression has no columns; use this object directly within a column-level expression.")
    columns = c = property(columns)

    def self_group(self, **kwargs):
        return self

    def _make_proxy(self, selectable, name):
        return list(self.inner_columns)[0]._make_proxy(selectable, name)

    def _get_from_objects(self, **modifiers):
        return []

class CompoundSelect(_SelectBaseMixin, FromClause):
    def __init__(self, keyword, *selects, **kwargs):
        self._should_correlate = kwargs.pop('correlate', False)
        self.keyword = keyword
        self.selects = []

        # some DBs do not like ORDER BY in the inner queries of a UNION, etc.
        for n, s in enumerate(selects):
            if s._order_by_clause:
                s = s.order_by(None)
            # unions group from left to right, so don't group first select
            if n:
                self.selects.append(s.self_group(self))
            else:
                self.selects.append(s)

        self._col_map = {}

        _SelectBaseMixin.__init__(self, **kwargs)

        for s in self.selects:
            if s.oid_column:
                self.oid_column = self._proxy_column(s.oid_column)


    def self_group(self, against=None):
        return _FromGrouping(self)

    def _exportable_columns(self):
        for s in self.selects:
            for c in s.c:
                yield c

    def _proxy_column(self, column):
        existing = self._col_map.get(column.name, None)
        if existing is not None:
            existing.proxies.append(column)
            return existing
        else:
            if self.use_labels:
                col = column._make_proxy(self, name=column._label)
            else:
                col = column._make_proxy(self)
            self._col_map[col.name] = col
            return col

    def _copy_internals(self, clone=_clone):
        self._clone_from_clause()
        self._col_map = {}
        self.selects = [clone(s) for s in self.selects]
        for attr in ('_order_by_clause', '_group_by_clause'):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr)))

    def get_children(self, column_collections=True, **kwargs):
        return (column_collections and list(self.c) or []) + \
            [self._order_by_clause, self._group_by_clause] + list(self.selects)

    def _table_iterator(self):
        for s in self.selects:
            for t in s._table_iterator():
                yield t

    def bind(self):
        for s in self.selects:
            e = s.bind
            if e:
                return e
        else:
            return None
    bind = property(bind)

class Select(_SelectBaseMixin, FromClause):
    """Represents a ``SELECT`` statement.

    Select statements support appendable clauses, as well as the
    ability to execute themselves and return a result set.
    """

    def __init__(self, columns, whereclause=None, from_obj=None, distinct=False, having=None, correlate=True, prefixes=None, **kwargs):
        """Construct a Select object.

        The public constructor for Select is the
        [sqlalchemy.sql.expression#select()] function; see that function for
        argument descriptions.

        Additional generative and mutator methods are available on the
        [sqlalchemy.sql.expression#_SelectBaseMixin] superclass.
        """

        self._should_correlate = correlate
        self._distinct = distinct

        # NOTE: the _generate()
        # operation creates a *shallow* copy of the object, so append_XXX() methods,
        # usually called via a generative method, create a copy of each collection
        # by default

        self.__correlate = util.Set()
        self._having = None
        self._prefixes = []

        if columns:
            self._raw_columns = [
                isinstance(c, _ScalarSelect) and c.self_group(against=operators.comma_op) or c
                for c in
                [_literal_as_column(c) for c in columns]
            ]
        else:
            self._raw_columns = []

        if from_obj:
            self._froms = util.Set([
                _is_literal(f) and _TextFromClause(f) or f
                for f in util.to_list(from_obj)
            ])
        else:
            self._froms = util.Set()

        if whereclause:
            self._whereclause = _literal_as_text(whereclause)
        else:
            self._whereclause = None

        if having:
            self._having = _literal_as_text(having)
        else:
            self._having = None

        if prefixes:
            self._prefixes = [_literal_as_text(p) for p in prefixes]
        else:
            self._prefixes = []

        _SelectBaseMixin.__init__(self, **kwargs)

    def _get_display_froms(self, existing_froms=None):
        """Return the full list of 'from' clauses to be displayed.

        Takes into account a set of existing froms which may be
        rendered in the FROM clause of enclosing selects; this Select
        may want to leave those absent if it is automatically
        correlating.
        """

        froms = util.OrderedSet()

        for col in self._raw_columns:
            froms.update(col._get_from_objects())

        if self._whereclause is not None:
            froms.update(self._whereclause._get_from_objects(is_where=True))

        if self._froms:
            froms.update(self._froms)

        for f in froms:
            froms.difference_update(f._hide_froms)

        if len(froms) > 1:
            if self.__correlate:
                froms.difference_update(self.__correlate)
            if self._should_correlate and existing_froms is not None:
                froms.difference_update(existing_froms)

            if not froms:
                raise exceptions.InvalidRequestError("Select statement '%s' is overcorrelated; returned no 'from' clauses" % str(self.__dont_correlate()))
        return froms

    froms = property(_get_display_froms, doc="""Return a list of all FromClause elements which will be applied to the FROM clause of the resulting statement.""")

    def type(self):
        raise exceptions.InvalidRequestError("Select objects don't have a type.  Call as_scalar() on this Select object to return a 'scalar' version of this Select.")
    type = property(type)

    def locate_all_froms(self):
        """return a Set of all FromClause elements referenced by this Select.

        This set is a superset of that returned by the ``froms`` property, which
        is specifically for those FromClause elements that would actually be rendered.
        """
        if hasattr(self, '_all_froms'):
            return self._all_froms

        froms = util.Set()
        for col in self._raw_columns:
            for f in col._get_from_objects():
                froms.add(f)

        if self._whereclause is not None:
            for f in self._whereclause._get_from_objects(is_where=True):
                froms.add(f)

        for elem in self._froms:
            froms.add(elem)
            for f in elem._get_from_objects():
                froms.add(f)
        self._all_froms = froms
        return froms

    def inner_columns(self):
        """a collection of all ColumnElement expressions which would
        be rendered into the columns clause of the resulting SELECT statement.
        """

        for c in self._raw_columns:
            if isinstance(c, Selectable):
                for co in c.columns:
                    yield co
            else:
                yield c
    inner_columns = property(inner_columns)

    def is_derived_from(self, fromclause):
        if self in util.Set(fromclause._cloned_set):
            return True

        for f in self.locate_all_froms():
            if f.is_derived_from(fromclause):
                return True
        return False

    def _copy_internals(self, clone=_clone):
        self._clone_from_clause()
        self._recorrelate_froms([(f, clone(f)) for f in self._froms])
        self._raw_columns = [clone(c) for c in self._raw_columns]
        for attr in ('_whereclause', '_having', '_order_by_clause', '_group_by_clause'):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr)))

    def get_children(self, column_collections=True, **kwargs):
        """return child elements as per the ClauseElement specification."""

        return (column_collections and list(self.columns) or []) + \
            list(self.locate_all_froms()) + \
            [x for x in (self._whereclause, self._having, self._order_by_clause, self._group_by_clause) if x is not None]

    def _recorrelate_froms(self, froms):
        newcorrelate = util.Set()
        newfroms = util.Set()
        oldfroms = util.Set(self._froms)
        for old, new in froms:
            if old in self.__correlate:
                newcorrelate.add(new)
                self.__correlate.remove(old)
            if old in oldfroms:
                newfroms.add(new)
                oldfroms.remove(old)
        self.__correlate = self.__correlate.union(newcorrelate)
        self._froms = [f for f in oldfroms.union(newfroms)]

    def column(self, column):
        """return a new select() construct with the given column expression added to its columns clause."""

        s = self._generate()
        s.append_column(column)
        return s

    def where(self, whereclause):
        """return a new select() construct with the given expression added to its WHERE clause, joined
        to the existing clause via AND, if any."""

        s = self._generate()
        s.append_whereclause(whereclause)
        return s

    def having(self, having):
        """return a new select() construct with the given expression added to its HAVING clause, joined
        to the existing clause via AND, if any."""

        s = self._generate()
        s.append_having(having)
        return s

    def distinct(self):
        """return a new select() construct which will apply DISTINCT to its columns clause."""

        s = self._generate()
        s._distinct = True
        return s

    def prefix_with(self, clause):
        """return a new select() construct which will apply the given expression to the start of its
        columns clause, not using any commas."""

        s = self._generate()
        s.append_prefix(clause)
        return s

    def select_from(self, fromclause):
        """return a new select() construct with the given FROM expression applied to its list of
        FROM objects."""

        s = self._generate()
        s.append_from(fromclause)
        return s

    def __dont_correlate(self):
        s = self._generate()
        s._should_correlate = False
        return s

    def correlate(self, fromclause):
        """return a new select() construct which will correlate the given FROM clause to that
        of an enclosing select(), if a match is found.

        By "match", the given fromclause must be present in this select's list of FROM objects
        and also present in an enclosing select's list of FROM objects.

        Calling this method turns off the select's default behavior of "auto-correlation".  Normally,
        select() auto-correlates all of its FROM clauses to those of an embedded select when
        compiled.

        If the fromclause is None, the select() will not correlate to anything.
        """

        s = self._generate()
        s._should_correlate=False
        if fromclause is None:
            s.__correlate = util.Set()
        else:
            s.append_correlation(fromclause)
        return s

    def append_correlation(self, fromclause, _copy_collection=True):
        """append the given correlation expression to this select() construct.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        if not _copy_collection:
            self.__correlate.add(fromclause)
        else:
            self.__correlate = util.Set(list(self.__correlate) + [fromclause])

    def append_column(self, column, _copy_collection=True):
        """append the given column expression to the columns clause of this select() construct.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        column = _literal_as_column(column)

        if isinstance(column, _ScalarSelect):
            column = column.self_group(against=operators.comma_op)

        if not _copy_collection:
            self._raw_columns.append(column)
        else:
            self._raw_columns = self._raw_columns + [column]

    def append_prefix(self, clause, _copy_collection=True):
        """append the given columns clause prefix expression to this select() construct.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        clause = _literal_as_text(clause)
        if not _copy_collection:
            self._prefixes.append(clause)
        else:
            self._prefixes = self._prefixes + [clause]

    def append_whereclause(self, whereclause):
        """append the given expression to this select() construct's WHERE criterion.

        The expression will be joined to existing WHERE criterion via AND.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        if self._whereclause  is not None:
            self._whereclause = and_(self._whereclause, _literal_as_text(whereclause))
        else:
            self._whereclause = _literal_as_text(whereclause)

    def append_having(self, having):
        """append the given expression to this select() construct's HAVING criterion.

        The expression will be joined to existing HAVING criterion via AND.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        if self._having is not None:
            self._having = and_(self._having, _literal_as_text(having))
        else:
            self._having = _literal_as_text(having)

    def append_from(self, fromclause, _copy_collection=True):
        """append the given FromClause expression to this select() construct's FROM clause.

        Note that this mutates the Select construct such that derived attributes,
        such as the "primary_key", "oid_column", and child "froms" collection may
        be invalid if they have already been initialized.  Consider the generative
        form of this method instead to prevent this issue.
        """

        if _is_literal(fromclause):
            fromclause = _TextFromClause(fromclause)

        if not _copy_collection:
            self._froms.add(fromclause)
        else:
            self._froms = util.Set(list(self._froms) + [fromclause])

    def _exportable_columns(self):
        return [c for c in self._raw_columns if isinstance(c, (Selectable, ColumnElement))]

    def _proxy_column(self, column):
        if self.use_labels:
            return column._make_proxy(self, name=column._label)
        else:
            return column._make_proxy(self)

    def self_group(self, against=None):
        """return a 'grouping' construct as per the ClauseElement specification.

        This produces an element that can be embedded in an expression.  Note that
        this method is called automatically as needed when constructing expressions.
        """

        if isinstance(against, CompoundSelect):
            return self
        return _FromGrouping(self)

    def oid_column(self):
        if hasattr(self, '_oid_column'):
            return self._oid_column

        proxies = []
        for f in self.locate_all_froms():
            if f is self:
                continue
            oid = f.oid_column
            if oid is not None:
                proxies.append(oid)

        if proxies:
            # create a proxied column which will act as a proxy
            # for every OID we've located...
            col = self._proxy_column(proxies[0])
            col.proxies = proxies
            self._oid_column = col
            return col
        else:
            self._oid_column = None
            return self._oid_column
    oid_column = property(oid_column)

    def union(self, other, **kwargs):
        """return a SQL UNION of this select() construct against the given selectable."""

        return union(self, other, **kwargs)

    def union_all(self, other, **kwargs):
        """return a SQL UNION ALL of this select() construct against the given selectable."""

        return union_all(self, other, **kwargs)

    def except_(self, other, **kwargs):
        """return a SQL EXCEPT of this select() construct against the given selectable."""

        return except_(self, other, **kwargs)

    def except_all(self, other, **kwargs):
        """return a SQL EXCEPT ALL of this select() construct against the given selectable."""

        return except_all(self, other, **kwargs)

    def intersect(self, other, **kwargs):
        """return a SQL INTERSECT of this select() construct against the given selectable."""

        return intersect(self, other, **kwargs)

    def intersect_all(self, other, **kwargs):
        """return a SQL INTERSECT ALL of this select() construct against the given selectable."""

        return intersect_all(self, other, **kwargs)

    def _table_iterator(self):
        for t in visitors.NoColumnVisitor().iterate(self):
            if isinstance(t, TableClause):
                yield t

    def bind(self):
        if self._bind is not None:
            return self._bind
        for f in self._froms:
            if f is self:
                continue
            e = f.bind
            if e is not None:
                self._bind = e
                return e
        # look through the columns (largely synomous with looking
        # through the FROMs except in the case of _CalculatedClause/_Function)
        for c in self._exportable_columns():
            if getattr(c, 'table', None) is self:
                continue
            e = c.bind
            if e is not None:
                self._bind = e
                return e
        return None
    bind = property(bind)

class _UpdateBase(ClauseElement):
    """Form the base for ``INSERT``, ``UPDATE``, and ``DELETE`` statements."""

    def supports_execution(self):
        return True

    def _table_iterator(self):
        return iter([self.table])

    def _process_colparams(self, parameters):

        if parameters is None:
            return None

        if isinstance(parameters, (list, tuple)):
            pp = {}
            for i, c in enumerate(self.table.c):
                pp[c.key] = parameters[i]
            return pp
        else:
            return parameters

    def bind(self):
        return self.table.bind
    bind = property(bind)

class Insert(_UpdateBase):
    def __init__(self, table, values=None, inline=False, **kwargs):
        self.table = table
        self.select = None
        self.inline=inline
        self.parameters = self._process_colparams(values)

        self.kwargs = kwargs

    def get_children(self, **kwargs):
        if self.select is not None:
            return self.select,
        else:
            return ()

    def _copy_internals(self, clone=_clone):
        self.parameters = self.parameters.copy()

    def values(self, v):
        if len(v) == 0:
            return self
        u = self._clone()
        if u.parameters is None:
            u.parameters = u._process_colparams(v)
        else:
            u.parameters = self.parameters.copy()
            u.parameters.update(u._process_colparams(v))
        return u

class Update(_UpdateBase):
    def __init__(self, table, whereclause, values=None, inline=False, **kwargs):
        self.table = table
        self._whereclause = whereclause
        self.inline = inline
        self.parameters = self._process_colparams(values)

        self.kwargs = kwargs

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return self._whereclause,
        else:
            return ()

    def _copy_internals(self, clone=_clone):
        self._whereclause = clone(self._whereclause)
        self.parameters = self.parameters.copy()

    def values(self, v):
        if len(v) == 0:
            return self
        u = self._clone()
        if u.parameters is None:
            u.parameters = u._process_colparams(v)
        else:
            u.parameters = self.parameters.copy()
            u.parameters.update(u._process_colparams(v))
        return u

class Delete(_UpdateBase):
    def __init__(self, table, whereclause):
        self.table = table
        self._whereclause = whereclause

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return self._whereclause,
        else:
            return ()

    def _copy_internals(self, clone=_clone):
        self._whereclause = clone(self._whereclause)

class _IdentifiedClause(ClauseElement):
    def __init__(self, ident):
        self.ident = ident
    def supports_execution(self):
        return True

class SavepointClause(_IdentifiedClause):
    pass

class RollbackToSavepointClause(_IdentifiedClause):
    pass

class ReleaseSavepointClause(_IdentifiedClause):
    pass
