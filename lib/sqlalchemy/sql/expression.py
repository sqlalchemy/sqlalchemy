# expression.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the base components of SQL expression trees.

All components are derived from a common base class
:class:`ClauseElement`.  Common behaviors are organized
based on class hierarchies, in some cases via mixins.

All object construction from this package occurs via functions which
in some cases will construct composite :class:`ClauseElement` structures
together, and in other cases simply return a single :class:`ClauseElement`
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

import itertools, re
from operator import attrgetter

from sqlalchemy import util, exc #, types as sqltypes
from sqlalchemy.sql import operators
from sqlalchemy.sql.visitors import Visitable, cloned_traverse
import operator

functions, sql_util, sqltypes = None, None, None
DefaultDialect = None

__all__ = [
    'Alias', 'ClauseElement',
    'ColumnCollection', 'ColumnElement',
    'CompoundSelect', 'Delete', 'FromClause', 'Insert', 'Join',
    'Select', 'Selectable', 'TableClause', 'Update', 'alias', 'and_', 'asc',
    'between', 'bindparam', 'case', 'cast', 'column', 'delete',
    'desc', 'distinct', 'except_', 'except_all', 'exists', 'extract', 'func',
    'modifier', 'collate',
    'insert', 'intersect', 'intersect_all', 'join', 'label', 'literal',
    'literal_column', 'not_', 'null', 'or_', 'outparam', 'outerjoin', 'select',
    'subquery', 'table', 'text', 'tuple_', 'union', 'union_all', 'update', ]

PARSE_AUTOCOMMIT = util._symbol('PARSE_AUTOCOMMIT')

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

def outerjoin(left, right, onclause=None):
    """Return an ``OUTER JOIN`` clause element.

    The returned object is an instance of :class:`Join`.

    Similar functionality is also available via the :func:`outerjoin()`
    method on any :class:`FromClause`.

    left
      The left side of the join.

    right
      The right side of the join.

    onclause
      Optional criterion for the ``ON`` clause, is derived from
      foreign key relationships established between left and right
      otherwise.

    To chain joins together, use the :func:`join()` or :func:`outerjoin()`
    methods on the resulting :class:`Join` object.

    """
    return Join(left, right, onclause, isouter=True)

def join(left, right, onclause=None, isouter=False):
    """Return a ``JOIN`` clause element (regular inner join).

    The returned object is an instance of :class:`Join`.

    Similar functionality is also available via the :func:`join()` method
    on any :class:`FromClause`.

    left
      The left side of the join.

    right
      The right side of the join.

    onclause
      Optional criterion for the ``ON`` clause, is derived from
      foreign key relationships established between left and right
      otherwise.

    To chain joins together, use the :func:`join()` or :func:`outerjoin()`
    methods on the resulting :class:`Join` object.

    """
    return Join(left, right, onclause, isouter)

def select(columns=None, whereclause=None, from_obj=[], **kwargs):
    """Returns a ``SELECT`` clause element.

    Similar functionality is also available via the :func:`select()`
    method on any :class:`FromClause`.

    The returned object is an instance of :class:`Select`.

    All arguments which accept :class:`ClauseElement` arguments also accept
    string arguments, which will be converted as appropriate into
    either :func:`text()` or :func:`literal_column()` constructs.

    columns
      A list of :class:`ClauseElement` objects, typically :class:`ColumnElement`
      objects or subclasses, which will form the columns clause of the
      resulting statement.  For all members which are instances of
      :class:`Selectable`, the individual :class:`ColumnElement` members of the
      :class:`Selectable` will be added individually to the columns clause.
      For example, specifying a :class:`~sqlalchemy.schema.Table` instance will result in all
      the contained :class:`~sqlalchemy.schema.Column` objects within to be added to the
      columns clause.

      This argument is not present on the form of :func:`select()`
      available on :class:`~sqlalchemy.schema.Table`.

    whereclause
      A :class:`ClauseElement` expression which will be used to form the
      ``WHERE`` clause.

    from_obj
      A list of :class:`ClauseElement` objects which will be added to the
      ``FROM`` clause of the resulting statement.  Note that "from"
      objects are automatically located within the columns and
      whereclause ClauseElements.  Use this parameter to explicitly
      specify "from" objects which are not automatically locatable.
      This could include :class:`~sqlalchemy.schema.Table` objects that aren't otherwise
      present, or :class:`Join` objects whose presence will supercede that
      of the :class:`~sqlalchemy.schema.Table` objects already located in the other clauses.

    \**kwargs
      Additional parameters include:

      autocommit
        Deprecated.  Use .execution_options(autocommit=<True|False>)
        to set the autocommit option.

      prefixes
        a list of strings or :class:`ClauseElement` objects to include
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
        collection of the resulting :class:`Select` object will use these
        names as well for targeting column members.

      for_update=False
        when ``True``, applies ``FOR UPDATE`` to the end of the
        resulting statement.  Certain database dialects also support
        alternate values for this parameter, for example mysql
        supports "read" which translates to ``LOCK IN SHARE MODE``,
        and oracle supports "nowait" which translates to ``FOR UPDATE
        NOWAIT``.

      correlate=True
        indicates that this :class:`Select` object should have its
        contained :class:`FromClause` elements "correlated" to an enclosing
        :class:`Select` object.  This means that any :class:`ClauseElement`
        instance within the "froms" collection of this :class:`Select`
        which is also present in the "froms" collection of an
        enclosing select will not be rendered in the ``FROM`` clause
        of this select statement.

      group_by
        a list of :class:`ClauseElement` objects which will comprise the
        ``GROUP BY`` clause of the resulting select.

      having
        a :class:`ClauseElement` that will comprise the ``HAVING`` clause
        of the resulting select when ``GROUP BY`` is used.

      order_by
        a scalar or list of :class:`ClauseElement` objects which will
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
        :class:`ClauseElement` members.

    """
    return Select(columns, whereclause=whereclause, from_obj=from_obj, **kwargs)

def subquery(alias, *args, **kwargs):
    """Return an :class:`Alias` object derived 
    from a :class:`Select`.

    name
      alias name

    \*args, \**kwargs

      all other arguments are delivered to the
      :func:`select` function.

    """
    return Select(*args, **kwargs).alias(alias)

def insert(table, values=None, inline=False, **kwargs):
    """Return an :class:`Insert` clause element.

    Similar functionality is available via the :func:`insert()` method on
    :class:`~sqlalchemy.schema.Table`.

    :param table: The table to be inserted into.

    :param values: A dictionary which specifies the column specifications of the
      ``INSERT``, and is optional.  If left as None, the column
      specifications are determined from the bind parameters used
      during the compile phase of the ``INSERT`` statement.  If the
      bind parameters also are None during the compile phase, then the
      column specifications will be generated from the full list of
      table columns.  Note that the :meth:`~Insert.values()` generative method
      may also be used for this.

    :param prefixes: A list of modifier keywords to be inserted between INSERT
      and INTO. Alternatively, the :meth:`~Insert.prefix_with` generative method
      may be used.

    :param inline: if True, SQL defaults will be compiled 'inline' into the
      statement and not pre-executed.

    If both `values` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within `values` on a per-key basis.

    The keys within `values` can be either :class:`~sqlalchemy.schema.Column` objects or their
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
    """Return an :class:`Update` clause element.

    Similar functionality is available via the :func:`update()` method on
    :class:`~sqlalchemy.schema.Table`.

    :param table: The table to be updated.

    :param whereclause: A :class:`ClauseElement` describing the ``WHERE`` condition
      of the ``UPDATE`` statement. Note that the :meth:`~Update.where()`
      generative method may also be used for this.

    :param values:
      A dictionary which specifies the ``SET`` conditions of the
      ``UPDATE``, and is optional. If left as None, the ``SET``
      conditions are determined from the bind parameters used during
      the compile phase of the ``UPDATE`` statement.  If the bind
      parameters also are None during the compile phase, then the
      ``SET`` conditions will be generated from the full list of table
      columns.  Note that the :meth:`~Update.values()` generative method may
      also be used for this.

    :param inline:
      if True, SQL defaults will be compiled 'inline' into the statement
      and not pre-executed.

    If both `values` and compile-time bind parameters are present, the
    compile-time bind parameters override the information specified
    within `values` on a per-key basis.

    The keys within `values` can be either :class:`~sqlalchemy.schema.Column` objects or their
    string identifiers. Each key may reference one of:

    * a literal data value (i.e. string, number, etc.);
    * a Column object;
    * a SELECT statement.

    If a ``SELECT`` statement is specified which references this
    ``UPDATE`` statement's table, the statement will be correlated
    against the ``UPDATE`` statement.

    """
    return Update(
            table, 
            whereclause=whereclause, 
            values=values, 
            inline=inline, 
            **kwargs)

def delete(table, whereclause = None, **kwargs):
    """Return a :class:`Delete` clause element.

    Similar functionality is available via the :func:`delete()` method on
    :class:`~sqlalchemy.schema.Table`.

    :param table: The table to be updated.

    :param whereclause: A :class:`ClauseElement` describing the ``WHERE``
      condition of the ``UPDATE`` statement. Note that the :meth:`~Delete.where()`
      generative method may be used instead.

    """
    return Delete(table, whereclause, **kwargs)

def and_(*clauses):
    """Join a list of clauses together using the ``AND`` operator.

    The ``&`` operator is also overloaded on all
    :class:`_CompareMixin` subclasses to produce the
    same result.

    """
    if len(clauses) == 1:
        return clauses[0]
    return BooleanClauseList(operator=operators.and_, *clauses)

def or_(*clauses):
    """Join a list of clauses together using the ``OR`` operator.

    The ``|`` operator is also overloaded on all
    :class:`_CompareMixin` subclasses to produce the
    same result.

    """
    if len(clauses) == 1:
        return clauses[0]
    return BooleanClauseList(operator=operators.or_, *clauses)

def not_(clause):
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The ``~`` operator is also overloaded on all
    :class:`_CompareMixin` subclasses to produce the
    same result.

    """
    return operators.inv(_literal_as_binds(clause))

def distinct(expr):
    """Return a ``DISTINCT`` clause."""
    expr = _literal_as_binds(expr)
    return _UnaryExpression(expr, operator=operators.distinct_op, type_=expr.type)

def between(ctest, cleft, cright):
    """Return a ``BETWEEN`` predicate clause.

    Equivalent of SQL ``clausetest BETWEEN clauseleft AND clauseright``.

    The :func:`between()` method on all
    :class:`_CompareMixin` subclasses provides
    similar functionality.

    """
    ctest = _literal_as_binds(ctest)
    return ctest.between(cleft, cright)


def case(whens, value=None, else_=None):
    """Produce a ``CASE`` statement.

    whens
      A sequence of pairs, or alternatively a dict,
      to be translated into "WHEN / THEN" clauses.

    value
      Optional for simple case statements, produces
      a column expression as in "CASE <expr> WHEN ..."

    else\_
      Optional as well, for case defaults produces
      the "ELSE" portion of the "CASE" statement.

    The expressions used for THEN and ELSE,
    when specified as strings, will be interpreted
    as bound values. To specify textual SQL expressions
    for these, use the literal_column(<string>) or 
    text(<string>) construct. 

    The expressions used for the WHEN criterion
    may only be literal strings when "value" is
    present, i.e. CASE table.somecol WHEN "x" THEN "y".
    Otherwise, literal strings are not accepted
    in this position, and either the text(<string>)
    or literal(<string>) constructs must be used to
    interpret raw string values.

    Usage examples::

      case([(orderline.c.qty > 100, item.c.specialprice),
            (orderline.c.qty > 10, item.c.bulkprice)
          ], else_=item.c.regularprice)
      case(value=emp.c.type, whens={
              'engineer': emp.c.salary * 1.1,
              'manager':  emp.c.salary * 3,
          })

    Using :func:`literal_column()`, to allow for databases that
    do not support bind parameters in the ``then`` clause.  The type
    can be specified which determines the type of the :func:`case()` construct
    overall::
    
        case([(orderline.c.qty > 100, literal_column("'greaterthan100'", String)),
              (orderline.c.qty > 10, literal_column("'greaterthan10'", String))
            ], else_=literal_column("'lethan10'", String))

    """
    
    return _Case(whens, value=value, else_=else_)

def cast(clause, totype, **kwargs):
    """Return a ``CAST`` function.

    Equivalent of SQL ``CAST(clause AS totype)``.

    Use with a :class:`~sqlalchemy.types.TypeEngine` subclass, i.e::

      cast(table.c.unit_price * table.c.qty, Numeric(10,4))

    or::

      cast(table.c.timestamp, DATE)

    """
    return _Cast(clause, totype, **kwargs)

def extract(field, expr):
    """Return the clause ``extract(field FROM expr)``."""

    return _Extract(field, expr)

def collate(expression, collation):
    """Return the clause ``expression COLLATE collation``."""

    expr = _literal_as_binds(expression)
    return _BinaryExpression(
        expr, 
        _literal_as_text(collation), 
        operators.collate, type_=expr.type)

def exists(*args, **kwargs):
    """Return an ``EXISTS`` clause as applied to a :class:`Select` object.

    Calling styles are of the following forms::

        # use on an existing select()
        s = select([table.c.col1]).where(table.c.col2==5)
        s = exists(s)

        # construct a select() at once
        exists(['*'], **select_arguments).where(criterion)

        # columns argument is optional, generates "EXISTS (SELECT *)"
        # by default.
        exists().where(table.c.col2==5)

    """
    return _Exists(*args, **kwargs)

def union(*selects, **kwargs):
    """Return a ``UNION`` of multiple selectables.

    The returned object is an instance of
    :class:`CompoundSelect`.

    A similar :func:`union()` method is available on all
    :class:`FromClause` subclasses.

    \*selects
      a list of :class:`Select` instances.

    \**kwargs
       available keyword arguments are the same as those of
       :func:`select`.

    """
    return CompoundSelect(CompoundSelect.UNION, *selects, **kwargs)

def union_all(*selects, **kwargs):
    """Return a ``UNION ALL`` of multiple selectables.

    The returned object is an instance of
    :class:`CompoundSelect`.

    A similar :func:`union_all()` method is available on all
    :class:`FromClause` subclasses.

    \*selects
      a list of :class:`Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.UNION_ALL, *selects, **kwargs)

def except_(*selects, **kwargs):
    """Return an ``EXCEPT`` of multiple selectables.

    The returned object is an instance of
    :class:`CompoundSelect`.

    \*selects
      a list of :class:`Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.EXCEPT, *selects, **kwargs)

def except_all(*selects, **kwargs):
    """Return an ``EXCEPT ALL`` of multiple selectables.

    The returned object is an instance of
    :class:`CompoundSelect`.

    \*selects
      a list of :class:`Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.EXCEPT_ALL, *selects, **kwargs)

def intersect(*selects, **kwargs):
    """Return an ``INTERSECT`` of multiple selectables.

    The returned object is an instance of
    :class:`CompoundSelect`.

    \*selects
      a list of :class:`Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.INTERSECT, *selects, **kwargs)

def intersect_all(*selects, **kwargs):
    """Return an ``INTERSECT ALL`` of multiple selectables.

    The returned object is an instance of
    :class:`CompoundSelect`.

    \*selects
      a list of :class:`Select` instances.

    \**kwargs
      available keyword arguments are the same as those of
      :func:`select`.

    """
    return CompoundSelect(CompoundSelect.INTERSECT_ALL, *selects, **kwargs)

def alias(selectable, alias=None):
    """Return an :class:`Alias` object.

    An :class:`Alias` represents any :class:`FromClause`
    with an alternate name assigned within SQL, typically using the ``AS``
    clause when generated, e.g. ``SELECT * FROM table AS aliasname``.

    Similar functionality is available via the :func:`alias()` method
    available on all :class:`FromClause` subclasses.

      selectable
        any :class:`FromClause` subclass, such as a table, select
        statement, etc..

      alias
        string name to be assigned as the alias.  If ``None``, a
        random name will be generated.

    """
    return Alias(selectable, alias=alias)


def literal(value, type_=None):
    """Return a literal clause, bound to a bind parameter.

    Literal clauses are created automatically when non- :class:`ClauseElement`
    objects (such as strings, ints, dates, etc.) are used in a comparison
    operation with a :class:`_CompareMixin`
    subclass, such as a :class:`~sqlalchemy.schema.Column` object. Use this function to force the
    generation of a literal clause, which will be created as a
    :class:`_BindParamClause` with a bound value.

    :param value: the value to be bound. Can be any Python object supported by
        the underlying DB-API, or is translatable via the given type argument.

    :param type\_: an optional :class:`~sqlalchemy.types.TypeEngine` which
        will provide bind-parameter translation for this literal.

    """
    return _BindParamClause(None, value, type_=type_, unique=True)

def tuple_(*expr):
    """Return a SQL tuple.   
    
    Main usage is to produce a composite IN construct::
    
        tuple_(table.c.col1, table.c.col2).in_(
            [(1, 2), (5, 12), (10, 19)]
        )
    
    """
    return _Tuple(*expr)
    
def label(name, obj):
    """Return a :class:`_Label` object for the
    given :class:`ColumnElement`.

    A label changes the name of an element in the columns clause of a
    ``SELECT`` statement, typically via the ``AS`` SQL keyword.

    This functionality is more conveniently available via the
    :func:`label()` method on :class:`ColumnElement`.

    name
      label name

    obj
      a :class:`ColumnElement`.

    """
    return _Label(name, obj)

def column(text, type_=None):
    """Return a textual column clause, as would be in the columns clause of a
    ``SELECT`` statement.

    The object returned is an instance of
    :class:`ColumnClause`, which represents the
    "syntactical" portion of the schema-level
    :class:`~sqlalchemy.schema.Column` object.

    text
      the name of the column.  Quoting rules will be applied to the
      clause like any other column name.  For textual column
      constructs that are not to be quoted, use the
      :func:`literal_column` function.

    type\_
      an optional :class:`~sqlalchemy.types.TypeEngine` object which will
      provide result-set translation for this column.

    """
    return ColumnClause(text, type_=type_)

def literal_column(text, type_=None):
    """Return a textual column expression, as would be in the columns
    clause of a ``SELECT`` statement.

    The object returned supports further expressions in the same way as any
    other column object, including comparison, math and string operations.
    The type\_ parameter is important to determine proper expression behavior
    (such as, '+' means string concatenation or numerical addition based on
    the type).

    text
      the text of the expression; can be any SQL expression.  Quoting rules
      will not be applied.  To specify a column-name expression which should
      be subject to quoting rules, use the
      :func:`column` function.

    type\_
      an optional :class:`~sqlalchemy.types.TypeEngine` object which will
      provide result-set translation and additional expression semantics for
      this column. If left as None the type will be NullType.

    """
    return ColumnClause(text, type_=type_, is_literal=True)

def table(name, *columns):
    """Return a :class:`TableClause` object.

    This is a primitive version of the :class:`~sqlalchemy.schema.Table` object,
    which is a subclass of this object.

    """
    return TableClause(name, *columns)

def bindparam(key, value=None, type_=None, unique=False, required=False):
    """Create a bind parameter clause with the given key.

    value
      a default value for this bind parameter.  a bindparam with a
      value is called a ``value-based bindparam``.

    type\_
      a sqlalchemy.types.TypeEngine object indicating the type of this
      bind param, will invoke type-specific bind parameter processing

    unique
      if True, bind params sharing the same name will have their
      underlying ``key`` modified to a uniquely generated name.
      mostly useful with value-based bind params.

    required
      A value is required at execution time.
      
    """
    if isinstance(key, ColumnClause):
        return _BindParamClause(key.name, value, type_=key.type, 
                                unique=unique, required=required)
    else:
        return _BindParamClause(key, value, type_=type_, 
                                unique=unique, required=required)

def outparam(key, type_=None):
    """Create an 'OUT' parameter for usage in functions (stored procedures),
    for databases which support them.

    The ``outparam`` can be used like a regular function parameter.
    The "output" value will be available from the
    :class:`~sqlalchemy.engine.ResultProxy` object via its ``out_parameters``
    attribute, which returns a dictionary containing the values.

    """
    return _BindParamClause(
                key, None, type_=type_, unique=False, isoutparam=True)

def text(text, bind=None, *args, **kwargs):
    """Create literal text to be inserted into a query.

    When constructing a query from a :func:`select()`, :func:`update()`,
    :func:`insert()` or :func:`delete()`, using plain strings for argument
    values will usually result in text objects being created
    automatically.  Use this function when creating textual clauses
    outside of other :class:`ClauseElement` objects, or optionally wherever
    plain text is to be used.

    text
      the text of the SQL statement to be created.  use ``:<param>``
      to specify bind parameters; they will be compiled to their
      engine-specific format.

    bind
      an optional connection or engine to be used for this text query.

    autocommit=True
      Deprecated.  Use .execution_options(autocommit=<True|False>)
      to set the autocommit option.

    bindparams
      a list of :func:`bindparam()` instances which can be used to define
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
    """Return a :class:`_Null` object, which compiles to ``NULL`` in a sql
    statement.
    
    """
    return _Null()

class _FunctionGenerator(object):
    """Generate :class:`Function` objects based on getattr calls."""

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

        elif name.endswith('_'):
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
            if func is not None and \
                    isinstance(func, type) and \
                    issubclass(func, Function):
                return func(*c, **o)

        return Function(
                    self.__names[-1], packagenames=self.__names[0:-1], *c, **o)

# "func" global - i.e. func.count()
func = _FunctionGenerator()

# "modifier" global - i.e. modifier.distinct
# TODO: use UnaryExpression for this instead ?
modifier = _FunctionGenerator(group=False)

class _generated_label(unicode):
    """A unicode subclass used to identify dynamically generated names."""

def _escape_for_generated(x):
    if isinstance(x, _generated_label):
        return x
    else:
        return x.replace('%', '%%')
        
def _clone(element):
    return element._clone()

def _expand_cloned(elements):
    """expand the given set of ClauseElements to be the set of all 'cloned'
    predecessors.
    
    """
    return itertools.chain(*[x._cloned_set for x in elements])

def _select_iterables(elements):
    """expand tables into individual columns in the 
    given list of column expressions.
    
    """
    return itertools.chain(*[c._select_iterable for c in elements])
    
def _cloned_intersection(a, b):
    """return the intersection of sets a and b, counting
    any overlap between 'cloned' predecessors.

    The returned set is in terms of the enties present within 'a'.

    """
    all_overlap = set(_expand_cloned(a)).intersection(_expand_cloned(b))
    return set(elem for elem in a if all_overlap.intersection(elem._cloned_set))


def _is_literal(element):
    return not isinstance(element, Visitable) and \
            not hasattr(element, '__clause_element__')

def _from_objects(*elements):
    return itertools.chain(*[element._from_objects for element in elements])

def _labeled(element):
    if not hasattr(element, 'name'):
        return element.label(None)
    else:
        return element

def _column_as_key(element):
    if isinstance(element, basestring):
        return element
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    return element.key
    
def _literal_as_text(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        return _TextClause(unicode(element))
    else:
        return element

def _clause_element_as_expr(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    else:
        return element
        
def _literal_as_column(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        return literal_column(str(element))
    else:
        return element

def _literal_as_binds(element, name=None, type_=None):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        if element is None:
            return null()
        else:
            return _BindParamClause(name, element, type_=type_, unique=True)
    else:
        return element

def _type_from_args(args):
    for a in args:
        if not isinstance(a.type, sqltypes.NullType):
            return a.type
    else:
        return sqltypes.NullType

def _no_literals(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        raise exc.ArgumentError("Ambiguous literal: %r.  Use the 'text()' "
                                "function to indicate a SQL expression "
                                "literal, or 'literal()' to indicate a "
                                "bound value." % element)
    else:
        return element

def _corresponding_column_or_error(fromclause, column, require_embedded=False):
    c = fromclause.corresponding_column(column,
            require_embedded=require_embedded)
    if c is None:
        raise exc.InvalidRequestError(
                "Given column '%s', attached to table '%s', "
                "failed to locate a corresponding column from table '%s'"
                % 
                (column, 
                    getattr(column, 'table', None),fromclause.description)
                )
    return c

@util.decorator
def _generative(fn, *args, **kw):
    """Mark a method as generative."""

    self = args[0]._generate()
    fn(self, *args[1:], **kw)
    return self


def is_column(col):
    """True if ``col`` is an instance of :class:`ColumnElement`."""
    
    return isinstance(col, ColumnElement)


class ClauseElement(Visitable):
    """Base class for elements of a programmatically constructed SQL
    expression.
    
    """
    __visit_name__ = 'clause'

    _annotations = {}
    supports_execution = False
    _from_objects = []
    _bind = None
    
    def _clone(self):
        """Create a shallow copy of this ClauseElement.

        This method may be used by a generative API.  Its also used as
        part of the "deep" copy afforded by a traversal that combines
        the _copy_internals() method.

        """
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        c.__dict__.pop('_cloned_set', None)

        # this is a marker that helps to "equate" clauses to each other
        # when a Select returns its list of FROM clauses.  the cloning
        # process leaves around a lot of remnants of the previous clause
        # typically in the form of column expressions still attached to the
        # old table.
        c._is_clone_of = self

        return c

    @property
    def _constructor(self):
        """return the 'constructor' for this ClauseElement.
        
        This is for the purposes for creating a new object of 
        this type.   Usually, its just the element's __class__.  
        However, the "Annotated" version of the object overrides
        to return the class of its proxied element.

        """
        return self.__class__

    @util.memoized_property
    def _cloned_set(self):
        """Return the set consisting all cloned anscestors of this
        ClauseElement.

        Includes this ClauseElement.  This accessor tends to be used for
        FromClause objects to identify 'equivalent' FROM clauses, regardless
        of transformative operations.

        """
        s = util.column_set()
        f = self
        while f is not None:
            s.add(f)
            f = getattr(f, '_is_clone_of', None)
        return s

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('_is_clone_of', None)
        return d
    
    if util.jython:
        def __hash__(self):
            """Return a distinct hash code.

            ClauseElements may have special equality comparisons which
            makes us rely on them having unique hash codes for use in
            hash-based collections. Stock __hash__ doesn't guarantee
            unique values on platforms with moving GCs.
            """
            return id(self)
        
    def _annotate(self, values):
        """return a copy of this ClauseElement with the given annotations
        dictionary.
        
        """
        global sql_util
        if sql_util is None:
            from sqlalchemy.sql import util as sql_util
        return sql_util.Annotated(self, values)

    def _deannotate(self):
        """return a copy of this ClauseElement with an empty annotations
        dictionary.
        
        """
        return self._clone()

    def unique_params(self, *optionaldict, **kwargs):
        """Return a copy with :func:`bindparam()` elments replaced.

        Same functionality as ``params()``, except adds `unique=True`
        to affected bind parameters so that multiple statements can be
        used.

        """
        return self._params(True, optionaldict, kwargs)

    def params(self, *optionaldict, **kwargs):
        """Return a copy with :func:`bindparam()` elments replaced.

        Returns a copy of this ClauseElement with :func:`bindparam()`
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
            raise exc.ArgumentError(
                "params() takes zero or one positional dictionary argument")

        def visit_bindparam(bind):
            if bind.key in kwargs:
                bind.value = kwargs[bind.key]
            if unique:
                bind._convert_to_unique()
        return cloned_traverse(self, {}, {'bindparam':visit_bindparam})

    def compare(self, other, **kw):
        """Compare this ClauseElement to the given ClauseElement.

        Subclasses should override the default behavior, which is a
        straight identity comparison.
        
        \**kw are arguments consumed by subclass compare() methods and
        may be used to modify the criteria for comparison.
        (see :class:`ColumnElement`)

        """
        return self is other

    def _copy_internals(self, clone=_clone):
        """Reassign internal elements to be clones of themselves.

        Called during a copy-and-traverse operation on newly
        shallow-copied elements to create a deep copy.

        """
        pass

    def get_children(self, **kwargs):
        """Return immediate child elements of this :class:`ClauseElement`.

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

    # TODO: remove .bind as a method from the root ClauseElement.
    # we should only be deriving binds from FromClause elements
    # and certain SchemaItem subclasses.
    # the "search_for_bind" functionality can still be used by
    # execute(), however.
    @property
    def bind(self):
        """Returns the Engine or Connection to which this ClauseElement is
        bound, or None if none found.
        
        """
        if self._bind is not None:
            return self._bind

        for f in _from_objects(self):
            if f is self:
                continue
            engine = f.bind
            if engine is not None:
                return engine
        else:
            return None

    def execute(self, *multiparams, **params):
        """Compile and execute this :class:`ClauseElement`."""

        e = self.bind
        if e is None:
            label = getattr(self, 'description', self.__class__.__name__)
            msg = ('This %s is not bound and does not support direct '
                   'execution. Supply this statement to a Connection or '
                   'Engine for execution. Or, assign a bind to the statement '
                   'or the Metadata of its underlying tables to enable '
                   'implicit execution via this method.' % label)
            raise exc.UnboundExecutionError(msg)
        return e._execute_clauseelement(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Compile and execute this :class:`ClauseElement`, returning the result's
        scalar representation.
        
        """
        return self.execute(*multiparams, **params).scalar()

    def compile(self, bind=None, dialect=None, **kw):
        """Compile this SQL expression.

        The return value is a :class:`~sqlalchemy.engine.Compiled` object.
        Calling ``str()`` or ``unicode()`` on the returned value will yield a
        string representation of the result. The
        :class:`~sqlalchemy.engine.Compiled` object also can return a
        dictionary of bind parameter names and values
        using the ``params`` accessor.

        :param bind: An ``Engine`` or ``Connection`` from which a
            ``Compiled`` will be acquired. This argument takes precedence over
            this :class:`ClauseElement`'s bound engine, if any.

        :param column_keys: Used for INSERT and UPDATE statements, a list of
            column names which should be present in the VALUES clause of the
            compiled statement. If ``None``, all columns from the target table
            object are rendered.

        :param dialect: A ``Dialect`` instance frmo which a ``Compiled``
            will be acquired. This argument takes precedence over the `bind`
            argument as well as this :class:`ClauseElement`'s bound engine, if any.

        :param inline: Used for INSERT statements, for a dialect which does
            not support inline retrieval of newly generated primary key
            columns, will force the expression used to create the new primary
            key value to be rendered inline within the INSERT statement's
            VALUES clause. This typically refers to Sequence execution but may
            also refer to any server-side default generation function
            associated with a primary key `Column`.

        """
        
        if not dialect:
            if bind:
                dialect = bind.dialect
            elif self.bind:
                dialect = self.bind.dialect
                bind = self.bind
            else:
                global DefaultDialect
                if DefaultDialect is None:
                    from sqlalchemy.engine.default import DefaultDialect
                dialect = DefaultDialect()
        compiler = self._compiler(dialect, bind=bind, **kw)
        compiler.compile()
        return compiler
    
    def _compiler(self, dialect, **kw):
        """Return a compiler appropriate for this ClauseElement, given a Dialect."""
        
        return dialect.statement_compiler(dialect, self, **kw)
        
    def __str__(self):
        # Py3K
        #return unicode(self.compile())
        # Py2K
        return unicode(self.compile()).encode('ascii', 'backslashreplace')
        # end Py2K

    def __and__(self, other):
        return and_(self, other)

    def __or__(self, other):
        return or_(self, other)

    def __invert__(self):
        return self._negate()

    def __nonzero__(self):
       raise TypeError("Boolean value of this clause is not defined")

    def _negate(self):
        if hasattr(self, 'negation_clause'):
            return self.negation_clause
        else:
            return _UnaryExpression(
                        self.self_group(against=operators.inv), 
                        operator=operators.inv, 
                        negate=None)

    def __repr__(self):
        friendly = getattr(self, 'description', None)
        if friendly is None:
            return object.__repr__(self)
        else:
            return '<%s.%s at 0x%x; %s>' % (
                self.__module__, self.__class__.__name__, id(self), friendly)


class _Immutable(object):
    """mark a ClauseElement as 'immutable' when expressions are cloned."""

    def unique_params(self, *optionaldict, **kwargs):
        raise NotImplementedError("Immutable objects do not support copying")

    def params(self, *optionaldict, **kwargs):
        raise NotImplementedError("Immutable objects do not support copying")

    def _clone(self):
        return self

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

    def operate(self, op, *other, **kwargs):
        raise NotImplementedError(str(op))

    def reverse_operate(self, op, other, **kwargs):
        raise NotImplementedError(str(op))

class ColumnOperators(Operators):
    """Defines comparison and math operations."""

    timetuple = None
    """Hack, allows datetime objects to be compared on the LHS."""

    def __lt__(self, other):
        return self.operate(operators.lt, other)

    def __le__(self, other):
        return self.operate(operators.le, other)

    __hash__ = Operators.__hash__
    
    def __eq__(self, other):
        return self.operate(operators.eq, other)

    def __ne__(self, other):
        return self.operate(operators.ne, other)

    def __gt__(self, other):
        return self.operate(operators.gt, other)

    def __ge__(self, other):
        return self.operate(operators.ge, other)

    def __neg__(self):
        return self.operate(operators.neg)

    def concat(self, other):
        return self.operate(operators.concat_op, other)

    def like(self, other, escape=None):
        return self.operate(operators.like_op, other, escape=escape)

    def ilike(self, other, escape=None):
        return self.operate(operators.ilike_op, other, escape=escape)

    def in_(self, other):
        return self.operate(operators.in_op, other)

    def startswith(self, other, **kwargs):
        return self.operate(operators.startswith_op, other, **kwargs)

    def endswith(self, other, **kwargs):
        return self.operate(operators.endswith_op, other, **kwargs)

    def contains(self, other, **kwargs):
        return self.operate(operators.contains_op, other, **kwargs)

    def match(self, other, **kwargs):
        return self.operate(operators.match_op, other, **kwargs)

    def desc(self):
        return self.operate(operators.desc_op)

    def asc(self):
        return self.operate(operators.asc_op)

    def collate(self, collation):
        return self.operate(operators.collate, collation)

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

    def __rtruediv__(self, other):
        return self.reverse_operate(operators.truediv, other)

class _CompareMixin(ColumnOperators):
    """Defines comparison and math operations for :class:`ClauseElement` instances."""

    def __compare(self, op, obj, negate=None, reverse=False, **kwargs):
        if obj is None or isinstance(obj, _Null):
            if op == operators.eq:
                return _BinaryExpression(self, null(), operators.is_, negate=operators.isnot)
            elif op == operators.ne:
                return _BinaryExpression(self, null(), operators.isnot, negate=operators.is_)
            else:
                raise exc.ArgumentError("Only '='/'!=' operators can be used with NULL")
        else:
            obj = self._check_literal(op, obj)

        if reverse:
            return _BinaryExpression(obj, 
                            self, 
                            op, 
                            type_=sqltypes.BOOLEANTYPE, 
                            negate=negate, modifiers=kwargs)
        else:
            return _BinaryExpression(self, 
                            obj, 
                            op, 
                            type_=sqltypes.BOOLEANTYPE, 
                            negate=negate, modifiers=kwargs)

    def __operate(self, op, obj, reverse=False):
        obj = self._check_literal(op, obj)
        
        if reverse:
            left, right = obj, self
        else:
            left, right = self, obj
        
        if left.type is None:
            op, result_type = sqltypes.NULLTYPE._adapt_expression(op, right.type)
        elif right.type is None:
            op, result_type = left.type._adapt_expression(op, sqltypes.NULLTYPE)
        else:    
            op, result_type = left.type._adapt_expression(op, right.type)
        
        return _BinaryExpression(left, right, op, type_=result_type)
        

    # a mapping of operators with the method they use, along with their negated
    # operator for comparison operators
    operators = {
        operators.add : (__operate,),
        operators.mul : (__operate,),
        operators.sub : (__operate,),
        # Py2K
        operators.div : (__operate,),
        # end Py2K
        operators.mod : (__operate,),
        operators.truediv : (__operate,),
        operators.lt : (__compare, operators.ge),
        operators.le : (__compare, operators.gt),
        operators.ne : (__compare, operators.eq),
        operators.gt : (__compare, operators.le),
        operators.ge : (__compare, operators.lt),
        operators.eq : (__compare, operators.ne),
        operators.like_op : (__compare, operators.notlike_op),
        operators.ilike_op : (__compare, operators.notilike_op),
    }

    def operate(self, op, *other, **kwargs):
        o = _CompareMixin.operators[op]
        return o[0](self, op, other[0], *o[1:], **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        o = _CompareMixin.operators[op]
        return o[0](self, op, other, reverse=True, *o[1:], **kwargs)

    def in_(self, other):
        return self._in_impl(operators.in_op, operators.notin_op, other)

    def _in_impl(self, op, negate_op, seq_or_selectable):
        seq_or_selectable = _clause_element_as_expr(seq_or_selectable)
            
        if isinstance(seq_or_selectable, _ScalarSelect):
            return self.__compare( op, seq_or_selectable, negate=negate_op)

        elif isinstance(seq_or_selectable, _SelectBaseMixin):
            # TODO: if we ever want to support (x, y, z) IN (select x, y, z from table),
            # we would need a multi-column version of as_scalar() to produce a multi-
            # column selectable that does not export itself as a FROM clause
            return self.__compare( op, seq_or_selectable.as_scalar(), negate=negate_op)

        elif isinstance(seq_or_selectable, (Selectable, _TextClause)):
            return self.__compare( op, seq_or_selectable, negate=negate_op)
        
            
        # Handle non selectable arguments as sequences
        args = []
        for o in seq_or_selectable:
            if not _is_literal(o):
                if not isinstance( o, _CompareMixin):
                    raise exc.InvalidRequestError( 
                        "in() function accepts either a list of non-selectable values, "
                        "or a selectable: %r" % o)
            else:
                o = self._bind_param(op, o)
            args.append(o)

        if len(args) == 0:
            # Special case handling for empty IN's, behave like comparison 
            # against zero row selectable.  We use != to build the 
            # contradiction as it handles NULL values appropriately, i.e.
            # "not (x IN ())" should not return NULL values for x.
            util.warn("The IN-predicate on \"%s\" was invoked with an empty sequence. "
                       "This results in a contradiction, which nonetheless can be "
                       "expensive to evaluate.  Consider alternative strategies for "
                       "improved performance." % self)
                       
            return self != self

        return self.__compare(op, ClauseList(*args).self_group(against=op), negate=negate_op)

    def __neg__(self):
        return _UnaryExpression(self, operator=operators.neg)
        
    def startswith(self, other, escape=None):
        """Produce the clause ``LIKE '<other>%'``"""

        # use __radd__ to force string concat behavior
        return self.__compare(
            operators.like_op,
            literal_column("'%'", type_=sqltypes.String).__radd__(
                                self._check_literal(operators.like_op, other)
                            ),
            escape=escape)

    def endswith(self, other, escape=None):
        """Produce the clause ``LIKE '%<other>'``"""

        return self.__compare(
            operators.like_op,
            literal_column("'%'", type_=sqltypes.String) + 
                self._check_literal(operators.like_op, other),
            escape=escape)

    def contains(self, other, escape=None):
        """Produce the clause ``LIKE '%<other>%'``"""

        return self.__compare(
            operators.like_op,
            literal_column("'%'", type_=sqltypes.String) +
                self._check_literal(operators.like_op, other) +
                literal_column("'%'", type_=sqltypes.String),
            escape=escape)

    def match(self, other):
        """Produce a MATCH clause, i.e. ``MATCH '<other>'``

        The allowed contents of ``other`` are database backend specific.

        """
        return self.__compare(operators.match_op, self._check_literal(operators.match_op, other))

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
        return _UnaryExpression(self, operator=operators.distinct_op, type_=self.type)

    def between(self, cleft, cright):
        """Produce a BETWEEN clause, i.e. ``<column> BETWEEN <cleft> AND <cright>``"""

        return _BinaryExpression(
                self,
                ClauseList(
                    self._check_literal(operators.and_, cleft),
                    self._check_literal(operators.and_, cright),
                    operator=operators.and_,
                    group=False),
                operators.between_op)

    def collate(self, collation):
        """Produce a COLLATE clause, i.e. ``<column> COLLATE utf8_bin``"""

        return collate(self, collation)

    def op(self, operator):
        """produce a generic operator function.

        e.g.::

          somecolumn.op("*")(5)

        produces::

          somecolumn * 5

        
        :param operator: a string which will be output as the infix operator between
          this :class:`ClauseElement` and the expression passed to the
          generated function.

        This function can also be used to make bitwise operators explicit. For example::

          somecolumn.op('&')(0xff)

        is a bitwise AND of the value in somecolumn.
          
        """
        return lambda other: self.__operate(operator, other)

    def _bind_param(self, operator, obj):
        return _BindParamClause(None, obj, 
                                    _compared_to_operator=operator, 
                                    _compared_to_type=self.type, unique=True)

    def _check_literal(self, operator, other):
        if isinstance(other, _BindParamClause) and \
            isinstance(other.type, sqltypes.NullType):
            # TODO: perhaps we should not mutate the incoming bindparam()
            # here and instead make a copy of it.  this might
            # be the only place that we're mutating an incoming construct.
            other.type = self.type
            return other
        elif hasattr(other, '__clause_element__'):
            return other.__clause_element__()
        elif not isinstance(other, ClauseElement):
            return self._bind_param(operator, other)
        elif isinstance(other, (_SelectBaseMixin, Alias)):
            return other.as_scalar()
        else:
            return other


class ColumnElement(ClauseElement, _CompareMixin):
    """Represent an element that is usable within the "column clause" portion of a ``SELECT`` statement.

    This includes columns associated with tables, aliases, and
    subqueries, expressions, function calls, SQL keywords such as
    ``NULL``, literals, etc.  :class:`ColumnElement` is the ultimate base
    class for all such elements.

    :class:`ColumnElement` supports the ability to be a *proxy* element,
    which indicates that the :class:`ColumnElement` may be associated with
    a :class:`Selectable` which was derived from another :class:`Selectable`.
    An example of a "derived" :class:`Selectable` is an :class:`Alias` of a
    :class:`~sqlalchemy.schema.Table`.

    A :class:`ColumnElement`, by subclassing the :class:`_CompareMixin` mixin
    class, provides the ability to generate new :class:`ClauseElement`
    objects using Python expressions.  See the :class:`_CompareMixin`
    docstring for more details.

    """

    __visit_name__ = 'column'
    primary_key = False
    foreign_keys = []
    quote = None
    _label = None
    
    @property
    def _select_iterable(self):
        return (self, )

    @util.memoized_property
    def base_columns(self):
        return util.column_set(c for c in self.proxy_set
                                     if not hasattr(c, 'proxies'))

    @util.memoized_property
    def proxy_set(self):
        s = util.column_set([self])
        if hasattr(self, 'proxies'):
            for c in self.proxies:
                s.update(c.proxy_set)
        return s

    def shares_lineage(self, othercolumn):
        """Return True if the given :class:`ColumnElement` 
        has a common ancestor to this :class:`ColumnElement`."""

        return bool(self.proxy_set.intersection(othercolumn.proxy_set))

    def _make_proxy(self, selectable, name=None):
        """Create a new :class:`ColumnElement` representing this
        :class:`ColumnElement` as it appears in the select list of a
        descending selectable.

        """

        if name:
            co = ColumnClause(name, selectable, type_=getattr(self, 'type', None))
        else:
            name = str(self)
            co = ColumnClause(self.anon_label, selectable, type_=getattr(self, 'type', None))

        co.proxies = [self]
        selectable.columns[name] = co
        return co

    def compare(self, other, use_proxies=False, equivalents=None, **kw):
        """Compare this ColumnElement to another.
        
        Special arguments understood:
        
        :param use_proxies: when True, consider two columns that
          share a common base column as equivalent (i.e. shares_lineage())
        
        :param equivalents: a dictionary of columns as keys mapped to sets
          of columns.  If the given "other" column is present in this dictionary,
          if any of the columns in the correponding set() pass the comparison 
          test, the result is True.  This is used to expand the comparison to
          other columns that may be known to be equivalent to this one via 
          foreign key or other criterion.

        """
        to_compare = (other, )
        if equivalents and other in equivalents:
            to_compare = equivalents[other].union(to_compare)

        for oth in to_compare:
            if use_proxies and self.shares_lineage(oth):
                return True
            elif oth is self:
                return True
        else:
            return False

    @util.memoized_property
    def anon_label(self):
        """provides a constant 'anonymous label' for this ColumnElement.

        This is a label() expression which will be named at compile time.
        The same label() is returned each time anon_label is called so
        that expressions can reference anon_label multiple times, producing
        the same label name at compile time.

        the compiler uses this function automatically at compile time
        for expressions that are known to be 'unnamed' like binary
        expressions and function calls.

        """
        return _generated_label("%%(%d %s)s" % (id(self), getattr(self, 'name', 'anon')))

class ColumnCollection(util.OrderedProperties):
    """An ordered dictionary that stores a list of ColumnElement
    instances.

    Overrides the ``__eq__()`` method to produce SQL clauses between
    sets of correlated columns.

    """

    def __init__(self, *cols):
        super(ColumnCollection, self).__init__()
        self.update((c.key, c) for c in cols)

    def __str__(self):
        return repr([str(c) for c in self])

    def replace(self, column):
        """add the given column to this collection, removing unaliased
           versions of this column  as well as existing columns with the
           same key.

            e.g.::

                t = Table('sometable', metadata, Column('col1', Integer))
                t.columns.replace(Column('col1', Integer, key='columnone'))

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
            # this warning is primarily to catch select() statements which
            # have conflicting column names in their exported columns collection
            existing = self[key]
            if not existing.shares_lineage(value):
                util.warn(("Column %r on table %r being replaced by another "
                           "column with the same key.  Consider use_labels "
                           "for select() statements.")  % (key, getattr(existing, 'table', None)))
        util.OrderedProperties.__setitem__(self, key, value)

    def remove(self, column):
        del self[column.key]

    def extend(self, iter):
        for c in iter:
            self.add(c)

    __hash__ = None
    
    def __eq__(self, other):
        l = []
        for c in other:
            for local in self:
                if c.shares_lineage(local):
                    l.append(c==local)
        return and_(*l)

    def __contains__(self, other):
        if not isinstance(other, basestring):
            raise exc.ArgumentError("__contains__ requires a string argument")
        return util.OrderedProperties.__contains__(self, other)

    def contains_column(self, col):
        # have to use a Set here, because it will compare the identity
        # of the column, not just using "==" for comparison which will always return a
        # "True" value (i.e. a BinaryClause...)
        return col in util.column_set(self)

class ColumnSet(util.ordered_column_set):
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

    def __hash__(self):
        return hash(tuple(x for x in self))

class Selectable(ClauseElement):
    """mark a class as being selectable"""
    __visit_name__ = 'selectable'

class FromClause(Selectable):
    """Represent an element that can be used within the ``FROM`` 
    clause of a ``SELECT`` statement.
    
    """
    __visit_name__ = 'fromclause'
    named_with_column = False
    _hide_froms = []
    quote = None
    schema = None

    def count(self, whereclause=None, **params):
        """return a SELECT COUNT generated against this :class:`FromClause`."""

        if self.primary_key:
            col = list(self.primary_key)[0]
        else:
            col = list(self.columns)[0]
        return select(
                    [func.count(col).label('tbl_row_count')], 
                    whereclause, 
                    from_obj=[self], 
                    **params)

    def select(self, whereclause=None, **params):
        """return a SELECT of this :class:`FromClause`."""

        return select([self], whereclause, **params)

    def join(self, right, onclause=None, isouter=False):
        """return a join of this :class:`FromClause` against another :class:`FromClause`."""

        return Join(self, right, onclause, isouter)

    def outerjoin(self, right, onclause=None):
        """return an outer join of this :class:`FromClause` against another :class:`FromClause`."""

        return Join(self, right, onclause, True)

    def alias(self, name=None):
        """return an alias of this :class:`FromClause`.
        
        For table objects, this has the effect of the table being rendered
        as ``tablename AS aliasname`` in a SELECT statement.  
        For select objects, the effect is that of creating a named
        subquery, i.e. ``(select ...) AS aliasname``.
        The :func:`alias()` method is the general way to create
        a "subquery" out of an existing SELECT.
        
        The ``name`` parameter is optional, and if left blank an 
        "anonymous" name will be generated at compile time, guaranteed
        to be unique against other anonymous constructs used in the
        same statement.
        
        """

        return Alias(self, name)

    def is_derived_from(self, fromclause):
        """Return True if this FromClause is 'derived' from the given FromClause.

        An example would be an Alias of a Table is derived from that Table.

        """
        return fromclause in self._cloned_set

    def replace_selectable(self, old, alias):
        """replace all occurences of FromClause 'old' with the given Alias 
        object, returning a copy of this :class:`FromClause`.
        
        """

        global sql_util
        if sql_util is None:
            from sqlalchemy.sql import util as sql_util
        return sql_util.ClauseAdapter(alias).traverse(self)

    def correspond_on_equivalents(self, column, equivalents):
        """Return corresponding_column for the given column, or if None
        search for a match in the given dictionary.
        
        """
        col = self.corresponding_column(column, require_embedded=True)
        if col is None and col in equivalents:
            for equiv in equivalents[col]:
                nc = self.corresponding_column(equiv, require_embedded=True)
                if nc:
                    return nc
        return col

    def corresponding_column(self, column, require_embedded=False):
        """Given a :class:`ColumnElement`, return the exported :class:`ColumnElement`
        object from this :class:`Selectable` which corresponds to that
        original :class:`~sqlalchemy.schema.Column` via a common anscestor column.

        :param column: the target :class:`ColumnElement` to be matched

        :param require_embedded: only return corresponding columns for the given
          :class:`ColumnElement`, if the given :class:`ColumnElement` is
          actually present within a sub-element of this
          :class:`FromClause`.  Normally the column will match if it merely
          shares a common anscestor with one of the exported columns
          of this :class:`FromClause`.

        """
        # dont dig around if the column is locally present
        if self.c.contains_column(column):
            return column

        col, intersect = None, None
        target_set = column.proxy_set
        cols = self.c
        for c in cols:
            i = target_set.intersection(itertools.chain(*[p._cloned_set for p in c.proxy_set]))
            
            if i and \
                (not require_embedded or c.proxy_set.issuperset(target_set)):
                
                if col is None:
                    # no corresponding column yet, pick this one.
                    col, intersect = c, i
                elif len(i) > len(intersect):
                    # 'c' has a larger field of correspondence than 'col'.
                    # i.e. selectable.c.a1_x->a1.c.x->table.c.x matches
                    # a1.c.x->table.c.x better than 
                    # selectable.c.x->table.c.x does.
                    col, intersect = c, i
                elif i == intersect:
                    # they have the same field of correspondence.
                    # see which proxy_set has fewer columns in it, which indicates
                    # a closer relationship with the root column. Also take into
                    # account the "weight" attribute which CompoundSelect() uses to
                    # give higher precedence to columns based on vertical position
                    # in the compound statement, and discard columns that have no
                    # reference to the target column (also occurs with
                    # CompoundSelect)
                    col_distance = util.reduce(operator.add, 
                                        [sc._annotations.get('weight', 1) 
                                            for sc in col.proxy_set 
                                            if sc.shares_lineage(column)]
                                    )
                    c_distance = util.reduce(operator.add, 
                                        [sc._annotations.get('weight', 1) 
                                            for sc in c.proxy_set 
                                            if sc.shares_lineage(column)]
                                    )
                    if c_distance < col_distance:
                        col, intersect = c, i
        return col

    @property
    def description(self):
        """a brief description of this FromClause.

        Used primarily for error message formatting.

        """
        return getattr(self, 'name', self.__class__.__name__ + " object")

    def _reset_exported(self):
        """delete memoized collections when a FromClause is cloned."""

        for attr in ('_columns', '_primary_key' '_foreign_keys', 'locate_all_froms'):
            self.__dict__.pop(attr, None)

    @util.memoized_property
    def _columns(self):
        """Return the collection of Column objects contained by this FromClause."""

        self._export_columns()
        return self._columns

    @util.memoized_property
    def _primary_key(self):
        """Return the collection of Column objects which comprise the
        primary key of this FromClause."""

        self._export_columns()
        return self._primary_key

    @util.memoized_property
    def _foreign_keys(self):
        """Return the collection of ForeignKey objects which this
        FromClause references."""

        self._export_columns()
        return self._foreign_keys

    columns = property(attrgetter('_columns'), doc=_columns.__doc__)
    primary_key = property(
                    attrgetter('_primary_key'), 
                    doc=_primary_key.__doc__)
    foreign_keys = property(
                    attrgetter('_foreign_keys'), 
                    doc=_foreign_keys.__doc__)

    # synonyms for 'columns'
    c = _select_iterable = property(attrgetter('columns'), doc=_columns.__doc__)

    def _export_columns(self):
        """Initialize column collections."""

        self._columns = ColumnCollection()
        self._primary_key = ColumnSet()
        self._foreign_keys = set()
        self._populate_column_collection()

    def _populate_column_collection(self):
        pass

class _BindParamClause(ColumnElement):
    """Represent a bind parameter.

    Public constructor is the :func:`bindparam()` function.

    """

    __visit_name__ = 'bindparam'
    quote = None

    def __init__(self, key, value, type_=None, unique=False, 
                            isoutparam=False, required=False, 
                            _compared_to_operator=None,
                            _compared_to_type=None):
        """Construct a _BindParamClause.

        key
          the key for this bind param.  Will be used in the generated
          SQL statement for dialects that use named parameters.  This
          value may be modified when part of a compilation operation,
          if other :class:`_BindParamClause` objects exist with the same
          key, or if its length is too long and truncation is
          required.

        value
          Initial value for this bind param.  This value may be
          overridden by the dictionary of parameters sent to statement
          compilation/execution.

        type\_
          A ``TypeEngine`` object that will be used to pre-process the
          value corresponding to this :class:`_BindParamClause` at
          execution time.

        unique
          if True, the key name of this BindParamClause will be
          modified if another :class:`_BindParamClause` of the same name
          already has been located within the containing
          :class:`ClauseElement`.
        
        required
          a value is required at execution time.
          
        isoutparam
          if True, the parameter should be treated like a stored procedure "OUT"
          parameter.

        """
        if unique:
            self.key = _generated_label("%%(%d %s)s" % (id(self), key or 'param'))
        else:
            self.key = key or _generated_label("%%(%d param)s" % id(self))
        self._orig_key = key or 'param'
        self.unique = unique
        self.value = value
        self.isoutparam = isoutparam
        self.required = required
        
        if type_ is None:
            if _compared_to_type is not None:
                self.type = _compared_to_type._coerce_compared_value(_compared_to_operator, value)
            else:
                self.type = sqltypes.type_map.get(type(value), sqltypes.NULLTYPE)
        elif isinstance(type_, type):
            self.type = type_()
        else:
            self.type = type_
            
    def _clone(self):
        c = ClauseElement._clone(self)
        if self.unique:
            c.key = _generated_label("%%(%d %s)s" % (id(c), c._orig_key or 'param'))
        return c

    def _convert_to_unique(self):
        if not self.unique:
            self.unique = True
            self.key = _generated_label("%%(%d %s)s" % (id(self),
                                                        self._orig_key or 'param'))

    def bind_processor(self, dialect):
        return self.type.dialect_impl(dialect).bind_processor(dialect)

    def compare(self, other, **kw):
        """Compare this :class:`_BindParamClause` to the given clause."""
        
        return isinstance(other, _BindParamClause) and \
                    self.type._compare_type_affinity(other.type) and \
                    self.value == other.value

    def __getstate__(self):
        """execute a deferred value for serialization purposes."""

        d = self.__dict__.copy()
        v = self.value
        if util.callable(v):
            v = v()
        d['value'] = v
        return d

    def __repr__(self):
        return "_BindParamClause(%r, %r, type_=%r)" % (
            self.key, self.value, self.type
            )

class _TypeClause(ClauseElement):
    """Handle a type keyword in a SQL statement.

    Used by the ``Case`` statement.

    """

    __visit_name__ = 'typeclause'

    def __init__(self, type):
        self.type = type


class _Generative(object):
    """Allow a ClauseElement to generate itself via the
    @_generative decorator.
    
    """
    
    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s


class Executable(_Generative):
    """Mark a ClauseElement as supporting execution.
    
    :class:`Executable` is a superclass for all "statement" types
    of objects, including :func:`select`, :func:`delete`, :func:`update`,
    :func:`insert`, :func:`text`.
     
    """

    supports_execution = True
    _execution_options = util.frozendict()

    @_generative
    def execution_options(self, **kw):
        """ Set non-SQL options for the statement which take effect during execution.
        
        Current options include:
        
        * autocommit - when True, a COMMIT will be invoked after execution 
          when executed in 'autocommit' mode, i.e. when an explicit transaction
          is not begun on the connection.   Note that DBAPI connections by
          default are always in a transaction - SQLAlchemy uses rules applied
          to different kinds of statements to determine if COMMIT will be invoked
          in order to provide its "autocommit" feature.  Typically, all 
          INSERT/UPDATE/DELETE statements as well as CREATE/DROP statements 
          have autocommit behavior enabled; SELECT constructs do not.  Use this
          option when invokving a SELECT or other specific SQL construct
          where COMMIT is desired (typically when calling stored procedures
          and such).
          
        * stream_results - indicate to the dialect that results should be 
          "streamed" and not pre-buffered, if possible.  This is a limitation
          of many DBAPIs.  The flag is currently understood only by the
          psycopg2 dialect.

        * compiled_cache - a dictionary where :class:`Compiled` objects
          will be cached when the :class:`Connection` compiles a clause 
          expression into a dialect- and parameter-specific 
          :class:`Compiled` object.   It is the user's responsibility to
          manage the size of this dictionary, which will have keys
          corresponding to the dialect, clause element, the column
          names within the VALUES or SET clause of an INSERT or UPDATE, 
          as well as the "batch" mode for an INSERT or UPDATE statement.
          The format of this dictionary is not guaranteed to stay the
          same in future releases.
          
          This option is usually more appropriate
          to use via the 
          :meth:`sqlalchemy.engine.base.Connection.execution_options()`
          method of :class:`Connection`, rather than upon individual 
          statement objects, though the effect is the same.
          
        See also:
        
            :meth:`sqlalchemy.engine.base.Connection.execution_options()`

            :meth:`sqlalchemy.orm.query.Query.execution_options()`
            
        """
        self._execution_options = self._execution_options.union(kw)

# legacy, some outside users may be calling this
_Executable = Executable
    
class _TextClause(Executable, ClauseElement):
    """Represent a literal SQL text fragment.

    Public constructor is the :func:`text()` function.

    """

    __visit_name__ = 'textclause'

    _bind_params_regex = re.compile(r'(?<![:\w\x5c]):(\w+)(?!:)', re.UNICODE)
    _execution_options = Executable._execution_options.union({'autocommit':PARSE_AUTOCOMMIT})
    
    @property
    def _select_iterable(self):
        return (self,)

    _hide_froms = []

    def __init__(self, text = "", bind=None, 
                    bindparams=None, typemap=None, 
                    autocommit=None):
        self._bind = bind
        self.bindparams = {}
        self.typemap = typemap

        if autocommit is not None:
            util.warn_deprecated("autocommit on text() is deprecated.  "
                                        "Use .execution_options(autocommit=True)")
            self._execution_options = self._execution_options.union({'autocommit':autocommit})
        
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
                
    @property
    def type(self):
        if self.typemap is not None and len(self.typemap) == 1:
            return list(self.typemap)[0]
        else:
            return None

    def self_group(self, against=None):
        if against is operators.in_op:
            return _Grouping(self)
        else:
            return self

    def _copy_internals(self, clone=_clone):
        self.bindparams = dict((b.key, clone(b))
                               for b in self.bindparams.values())

    def get_children(self, **kwargs):
        return self.bindparams.values()


class _Null(ColumnElement):
    """Represent the NULL keyword in a SQL statement.

    Public constructor is the :func:`null()` function.

    """

    __visit_name__ = 'null'

    def __init__(self):
        self.type = sqltypes.NULLTYPE


class ClauseList(ClauseElement):
    """Describe a list of clauses, separated by an operator.

    By default, is comma-separated, such as a column listing.

    """
    __visit_name__ = 'clauselist'

    def __init__(self, *clauses, **kwargs):
        self.operator = kwargs.pop('operator', operators.comma_op)
        self.group = kwargs.pop('group', True)
        self.group_contents = kwargs.pop('group_contents', True)
        if self.group_contents:
            self.clauses = [
                _literal_as_text(clause).self_group(against=self.operator)
                for clause in clauses if clause is not None]
        else:
            self.clauses = [
                _literal_as_text(clause)
                for clause in clauses if clause is not None]
    
    @util.memoized_property
    def type(self):
        if self.clauses:
            return self.clauses[0].type
        else:
            return sqltypes.NULLTYPE
            
    def __iter__(self):
        return iter(self.clauses)

    def __len__(self):
        return len(self.clauses)

    @property
    def _select_iterable(self):
        return iter(self)

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

    @property
    def _from_objects(self):
        return list(itertools.chain(*[c._from_objects for c in self.clauses]))

    def self_group(self, against=None):
        if self.group and self.operator is not against and \
                operators.is_precedent(self.operator, against):
            return _Grouping(self)
        else:
            return self

    def compare(self, other, **kw):
        """Compare this :class:`ClauseList` to the given :class:`ClauseList`,
        including a comparison of all the clause items.

        """
        if not isinstance(other, ClauseList) and len(self.clauses) == 1:
            return self.clauses[0].compare(other, **kw)
        elif isinstance(other, ClauseList) and len(self.clauses) == len(other.clauses):
            for i in range(0, len(self.clauses)):
                if not self.clauses[i].compare(other.clauses[i], **kw):
                    return False
            else:
                return self.operator == other.operator
        else:
            return False

class BooleanClauseList(ClauseList, ColumnElement):
    __visit_name__ = 'clauselist'

    def __init__(self, *clauses, **kwargs):
        super(BooleanClauseList, self).__init__(*clauses, **kwargs)
        self.type = sqltypes.to_instance(kwargs.get('type_', sqltypes.Boolean))

    @property
    def _select_iterable(self):
        return (self, )

class _Tuple(ClauseList, ColumnElement):
    
    def __init__(self, *clauses, **kw):
        clauses = [_literal_as_binds(c) for c in clauses]
        super(_Tuple, self).__init__(*clauses, **kw)
        self.type = _type_from_args(clauses)

    @property
    def _select_iterable(self):
        return (self, )

    def _bind_param(self, operator, obj):
        return _Tuple(*[
            _BindParamClause(None, o, _compared_to_operator=operator,
                             _compared_to_type=self.type, unique=True)
            for o in obj
        ]).self_group()
    

class _Case(ColumnElement):
    __visit_name__ = 'case'

    def __init__(self, whens, value=None, else_=None):
        try:
            whens = util.dictlike_iteritems(whens)
        except TypeError:
            pass

        if value is not None:
            whenlist = [
                (_literal_as_binds(c).self_group(), _literal_as_binds(r)) for (c, r) in whens
            ]
        else:
            whenlist = [
                (_no_literals(c).self_group(), _literal_as_binds(r)) for (c, r) in whens
            ]

        if whenlist:
            type_ = list(whenlist[-1])[-1].type
        else:
            type_ = None

        if value is None:
            self.value = None
        else:
            self.value = _literal_as_binds(value)

        self.type = type_
        self.whens = whenlist
        if else_ is not None:
            self.else_ = _literal_as_binds(else_)
        else:
            self.else_ = None

    def _copy_internals(self, clone=_clone):
        if self.value is not None:
            self.value = clone(self.value)
        self.whens = [(clone(x), clone(y)) for x, y in self.whens]
        if self.else_ is not None:
            self.else_ = clone(self.else_)

    def get_children(self, **kwargs):
        if self.value is not None:
            yield self.value
        for x, y in self.whens:
            yield x
            yield y
        if self.else_ is not None:
            yield self.else_ 

    @property
    def _from_objects(self):
        return list(itertools.chain(*[x._from_objects for x in self.get_children()]))

class FunctionElement(Executable, ColumnElement, FromClause):
    """Base for SQL function-oriented constructs."""
    
    def __init__(self, *clauses, **kwargs):
        args = [_literal_as_binds(c, self.name) for c in clauses]
        self.clause_expr = ClauseList(
                                operator=operators.comma_op,
                                 group_contents=True, *args).\
                                 self_group()

    @property
    def columns(self):
        return [self]

    @util.memoized_property
    def clauses(self):
        return self.clause_expr.element

    @property
    def _from_objects(self):
        return self.clauses._from_objects

    def get_children(self, **kwargs):
        return self.clause_expr, 

    def _copy_internals(self, clone=_clone):
        self.clause_expr = clone(self.clause_expr)
        self._reset_exported()
        util.reset_memoized(self, 'clauses')

    def select(self):
        s = select([self])
        if self._execution_options:
            s = s.execution_options(**self._execution_options)
        return s

    def scalar(self):
        return self.select().execute().scalar()

    def execute(self):
        return self.select().execute()

    def _bind_param(self, operator, obj):
        return _BindParamClause(None, obj, _compared_to_operator=operator, 
                                _compared_to_type=self.type, unique=True)

    
class Function(FunctionElement):
    """Describe a named SQL function."""

    __visit_name__ = 'function'

    def __init__(self, name, *clauses, **kw):
        self.packagenames = kw.pop('packagenames', None) or []
        self.name = name
        self._bind = kw.get('bind', None)
        self.type = sqltypes.to_instance(kw.get('type_', None))
        
        FunctionElement.__init__(self, *clauses, **kw)

    def _bind_param(self, operator, obj):
        return _BindParamClause(self.name, obj, _compared_to_operator=operator, 
                                _compared_to_type=self.type, unique=True)


class _Cast(ColumnElement):

    __visit_name__ = 'cast'

    def __init__(self, clause, totype, **kwargs):
        self.type = sqltypes.to_instance(totype)
        self.clause = _literal_as_binds(clause, None)
        self.typeclause = _TypeClause(self.type)

    def _copy_internals(self, clone=_clone):
        self.clause = clone(self.clause)
        self.typeclause = clone(self.typeclause)

    def get_children(self, **kwargs):
        return self.clause, self.typeclause

    @property
    def _from_objects(self):
        return self.clause._from_objects


class _Extract(ColumnElement):

    __visit_name__ = 'extract'

    def __init__(self, field, expr, **kwargs):
        self.type = sqltypes.Integer()
        self.field = field
        self.expr = _literal_as_binds(expr, None)

    def _copy_internals(self, clone=_clone):
        self.expr = clone(self.expr)

    def get_children(self, **kwargs):
        return self.expr,

    @property
    def _from_objects(self):
        return self.expr._from_objects


class _UnaryExpression(ColumnElement):

    __visit_name__ = 'unary'

    def __init__(self, element, operator=None, modifier=None, type_=None, negate=None):
        self.operator = operator
        self.modifier = modifier

        self.element = _literal_as_text(element).self_group(against=self.operator or self.modifier)
        self.type = sqltypes.to_instance(type_)
        self.negate = negate

    @property
    def _from_objects(self):
        return self.element._from_objects

    def _copy_internals(self, clone=_clone):
        self.element = clone(self.element)

    def get_children(self, **kwargs):
        return self.element,

    def compare(self, other, **kw):
        """Compare this :class:`_UnaryExpression` against the given :class:`ClauseElement`."""

        return (
            isinstance(other, _UnaryExpression) and
            self.operator == other.operator and
            self.modifier == other.modifier and
            self.element.compare(other.element, **kw)
        )

    def _negate(self):
        if self.negate is not None:
            return _UnaryExpression(
                self.element,
                operator=self.negate,
                negate=self.operator,
                modifier=self.modifier,
                type_=self.type)
        else:
            return super(_UnaryExpression, self)._negate()

    def self_group(self, against=None):
        if self.operator and operators.is_precedent(self.operator, against):
            return _Grouping(self)
        else:
            return self


class _BinaryExpression(ColumnElement):
    """Represent an expression that is ``LEFT <operator> RIGHT``."""

    __visit_name__ = 'binary'

    def __init__(self, left, right, operator, type_=None, negate=None, modifiers=None):
        self.left = _literal_as_text(left).self_group(against=operator)
        self.right = _literal_as_text(right).self_group(against=operator)
        self.operator = operator
        self.type = sqltypes.to_instance(type_)
        self.negate = negate
        if modifiers is None:
            self.modifiers = {}
        else:
            self.modifiers = modifiers
    
    def __nonzero__(self):
        try:
            return self.operator(hash(self.left), hash(self.right))
        except:
            raise TypeError("Boolean value of this clause is not defined")
        
    @property
    def _from_objects(self):
        return self.left._from_objects + self.right._from_objects

    def _copy_internals(self, clone=_clone):
        self.left = clone(self.left)
        self.right = clone(self.right)

    def get_children(self, **kwargs):
        return self.left, self.right

    def compare(self, other, **kw):
        """Compare this :class:`_BinaryExpression` against the 
        given :class:`_BinaryExpression`."""

        return (
            isinstance(other, _BinaryExpression) and
            self.operator == other.operator and
            (
                self.left.compare(other.left, **kw) and
                self.right.compare(other.right, **kw) or
                (
                    operators.is_commutative(self.operator) and
                    self.left.compare(other.right, **kw) and
                    self.right.compare(other.left, **kw)
                )
            )
        )

    def self_group(self, against=None):
        # use small/large defaults for comparison so that unknown
        # operators are always parenthesized
        if self.operator is not against and operators.is_precedent(self.operator, against):
            return _Grouping(self)
        else:
            return self

    def _negate(self):
        if self.negate is not None:
            return _BinaryExpression(
                self.left,
                self.right,
                self.negate,
                negate=self.operator,
                type_=sqltypes.BOOLEANTYPE,
                modifiers=self.modifiers)
        else:
            return super(_BinaryExpression, self)._negate()

class _Exists(_UnaryExpression):
    __visit_name__ = _UnaryExpression.__visit_name__
    _from_objects = []

    def __init__(self, *args, **kwargs):
        if args and isinstance(args[0], (_SelectBaseMixin, _ScalarSelect)):
            s = args[0]
        else:
            if not args:
                args = ([literal_column('*')],)
            s = select(*args, **kwargs).as_scalar().self_group()

        _UnaryExpression.__init__(self, s, operator=operators.exists, type_=sqltypes.Boolean)

    def select(self, whereclause=None, **params):
        return select([self], whereclause, **params)

    def correlate(self, fromclause):
        e = self._clone()
        e.element = self.element.correlate(fromclause).self_group()
        return e

    def select_from(self, clause):
        """return a new exists() construct with the given expression set as its FROM
        clause.
        
        """
        e = self._clone()
        e.element = self.element.select_from(clause).self_group()
        return e

    def where(self, clause):
        """return a new exists() construct with the given expression added to its WHERE
        clause, joined to the existing clause via AND, if any.
        
        """
        e = self._clone()
        e.element = self.element.where(clause).self_group()
        return e

class Join(FromClause):
    """represent a ``JOIN`` construct between two :class:`FromClause` elements.

    The public constructor function for :class:`Join` is the module-level
    :func:`join()` function, as well as the :func:`join()` method available
    off all :class:`FromClause` subclasses.

    """
    __visit_name__ = 'join'

    def __init__(self, left, right, onclause=None, isouter=False):
        self.left = _literal_as_text(left)
        self.right = _literal_as_text(right).self_group()

        if onclause is None:
            self.onclause = self._match_primaries(self.left, self.right)
        else:
            self.onclause = onclause

        self.isouter = isouter
        self.__folded_equivalents = None

    @property
    def description(self):
        return "Join object on %s(%d) and %s(%d)" % (
            self.left.description,
            id(self.left),
            self.right.description,
            id(self.right))

    def is_derived_from(self, fromclause):
        return fromclause is self or \
                self.left.is_derived_from(fromclause) or\
                self.right.is_derived_from(fromclause)

    def self_group(self, against=None):
        return _FromGrouping(self)

    def _populate_column_collection(self):
        columns = [c for c in self.left.columns] + [c for c in self.right.columns]

        global sql_util
        if not sql_util:
            from sqlalchemy.sql import util as sql_util
        self._primary_key.extend(sql_util.reduce_columns(
                (c for c in columns if c.primary_key), self.onclause))
        self._columns.update((col._label, col) for col in columns)
        self._foreign_keys.update(itertools.chain(*[col.foreign_keys for col in columns]))

    def _copy_internals(self, clone=_clone):
        self._reset_exported()
        self.left = clone(self.left)
        self.right = clone(self.right)
        self.onclause = clone(self.onclause)
        self.__folded_equivalents = None

    def get_children(self, **kwargs):
        return self.left, self.right, self.onclause

    def _match_primaries(self, left, right):
        global sql_util
        if not sql_util:
            from sqlalchemy.sql import util as sql_util
        if isinstance(left, Join):
            left_right = left.right
        else:
            left_right = None
        return sql_util.join_condition(left, right, a_subset=left_right)

    def select(self, whereclause=None, fold_equivalents=False, **kwargs):
        """Create a :class:`Select` from this :class:`Join`.

        :param whereclause: the WHERE criterion that will be sent to 
          the :func:`select()` function

        :param fold_equivalents: based on the join criterion of this 
          :class:`Join`, do not include
          repeat column names in the column list of the resulting
          select, for columns that are calculated to be "equivalent"
          based on the join criterion of this :class:`Join`. This will
          recursively apply to any joins directly nested by this one
          as well.

        :param \**kwargs: all other kwargs are sent to the 
          underlying :func:`select()` function.

        """
        global sql_util
        if not sql_util:
            from sqlalchemy.sql import util as sql_util
        if fold_equivalents:
            collist = sql_util.folded_equivalents(self)
        else:
            collist = [self.left, self.right]

        return select(collist, whereclause, from_obj=[self], **kwargs)

    @property
    def bind(self):
        return self.left.bind or self.right.bind

    def alias(self, name=None):
        """Create a :class:`Select` out of this :class:`Join` clause and return an :class:`Alias` of it.

        The :class:`Select` is not correlating.

        """
        return self.select(use_labels=True, correlate=False).alias(name)

    @property
    def _hide_froms(self):
        return itertools.chain(*[_from_objects(x.left, x.right) for x in self._cloned_set])

    @property
    def _from_objects(self):
        return [self] + \
                self.onclause._from_objects + \
                self.left._from_objects + \
                self.right._from_objects

class Alias(FromClause):
    """Represents an table or selectable alias (AS).

    Represents an alias, as typically applied to any table or
    sub-select within a SQL statement using the ``AS`` keyword (or
    without the keyword on certain databases such as Oracle).

    This object is constructed from the :func:`alias()` module level
    function as well as the :func:`alias()` method available on all
    :class:`FromClause` subclasses.

    """

    __visit_name__ = 'alias'
    named_with_column = True

    def __init__(self, selectable, alias=None):
        baseselectable = selectable
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.element
        self.original = baseselectable
        self.supports_execution = baseselectable.supports_execution
        if self.supports_execution:
            self._execution_options = baseselectable._execution_options
        self.element = selectable
        if alias is None:
            if self.original.named_with_column:
                alias = getattr(self.original, 'name', None)
            alias = _generated_label('%%(%d %s)s' % (id(self), alias or 'anon'))
        self.name = alias

    @property
    def description(self):
        # Py3K
        #return self.name
        # Py2K
        return self.name.encode('ascii', 'backslashreplace')
        # end Py2K

    def as_scalar(self):
        try:
            return self.element.as_scalar()
        except AttributeError:
            raise AttributeError("Element %s does not support 'as_scalar()'" % self.element)
        
    def is_derived_from(self, fromclause):
        if fromclause in self._cloned_set:
            return True
        return self.element.is_derived_from(fromclause)

    def _populate_column_collection(self):
        for col in self.element.columns:
            col._make_proxy(self)

    def _copy_internals(self, clone=_clone):
        self._reset_exported()
        self.element = _clone(self.element)
        baseselectable = self.element
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.element
        self.original = baseselectable

    def get_children(self, column_collections=True, aliased_selectables=True, **kwargs):
        if column_collections:
            for c in self.c:
                yield c
        if aliased_selectables:
            yield self.element

    @property
    def _from_objects(self):
        return [self]

    @property
    def bind(self):
        return self.element.bind


class _Grouping(ColumnElement):
    """Represent a grouping within a column expression"""

    __visit_name__ = 'grouping'

    def __init__(self, element):
        self.element = element
        self.type = getattr(element, 'type', None)

    @property
    def _label(self):
        return getattr(self.element, '_label', None) or self.anon_label

    def _copy_internals(self, clone=_clone):
        self.element = clone(self.element)

    def get_children(self, **kwargs):
        return self.element,

    @property
    def _from_objects(self):
        return self.element._from_objects

    def __getattr__(self, attr):
        return getattr(self.element, attr)

    def __getstate__(self):
        return {'element':self.element, 'type':self.type}

    def __setstate__(self, state):
        self.element = state['element']
        self.type = state['type']

class _FromGrouping(FromClause):
    """Represent a grouping of a FROM clause"""
    __visit_name__ = 'grouping'

    def __init__(self, element):
        self.element = element

    @property
    def columns(self):
        return self.element.columns

    @property
    def _hide_froms(self):
        return self.element._hide_froms

    def get_children(self, **kwargs):
        return self.element,

    def _copy_internals(self, clone=_clone):
        self.element = clone(self.element)

    @property
    def _from_objects(self):
        return self.element._from_objects

    def __getattr__(self, attr):
        return getattr(self.element, attr)

    def __getstate__(self):
        return {'element':self.element}

    def __setstate__(self, state):
        self.element = state['element']

class _Label(ColumnElement):
    """Represents a column label (AS).

    Represent a label, as typically applied to any column-level
    element using the ``AS`` sql keyword.

    This object is constructed from the :func:`label()` module level
    function as well as the :func:`label()` method available on all
    :class:`ColumnElement` subclasses.

    """

    __visit_name__ = 'label'

    def __init__(self, name, element, type_=None):
        while isinstance(element, _Label):
            element = element.element
        self.name = self.key = self._label = name or \
                                _generated_label("%%(%d %s)s" % (
                                    id(self), getattr(element, 'name', 'anon'))
                                )
        self._element = element
        self._type = type_
        self.quote = element.quote

    @util.memoized_property
    def type(self):
        return sqltypes.to_instance(
                    self._type or getattr(self._element, 'type', None)
                )

    @util.memoized_property
    def element(self):
        return self._element.self_group(against=operators.as_)

    def _proxy_attr(name):
        get = attrgetter(name)
        def attr(self):
            return get(self.element)
        return property(attr)

    proxies = _proxy_attr('proxies')
    base_columns = _proxy_attr('base_columns')
    proxy_set = _proxy_attr('proxy_set')
    primary_key = _proxy_attr('primary_key')
    foreign_keys = _proxy_attr('foreign_keys')

    def get_children(self, **kwargs):
        return self.element,

    def _copy_internals(self, clone=_clone):
        self.element = clone(self.element)

    @property
    def _from_objects(self):
        return self.element._from_objects

    def _make_proxy(self, selectable, name = None):
        if isinstance(self.element, (Selectable, ColumnElement)):
            e = self.element._make_proxy(selectable, name=self.name)
        else:
            e = column(self.name)._make_proxy(selectable=selectable)
        e.proxies.append(self)
        return e

class ColumnClause(_Immutable, ColumnElement):
    """Represents a generic column expression from any textual string.

    This includes columns associated with tables, aliases and select
    statements, but also any arbitrary text.  May or may not be bound
    to an underlying :class:`Selectable`.  :class:`ColumnClause` is usually
    created publically via the :func:`column()` function or the
    :func:`literal_column()` function.

    text
      the text of the element.

    selectable
      parent selectable.

    type
      ``TypeEngine`` object which can associate this :class:`ColumnClause`
      with a type.

    is_literal
      if True, the :class:`ColumnClause` is assumed to be an exact
      expression that will be delivered to the output with no quoting
      rules applied regardless of case sensitive settings.  the
      :func:`literal_column()` function is usually used to create such a
      :class:`ColumnClause`.

    """
    __visit_name__ = 'column'

    onupdate = default = server_default = server_onupdate = None

    def __init__(self, text, selectable=None, type_=None, is_literal=False):
        self.key = self.name = text
        self.table = selectable
        self.type = sqltypes.to_instance(type_)
        self.is_literal = is_literal

    @util.memoized_property
    def description(self):
        # Py3K
        #return self.name
        # Py2K
        return self.name.encode('ascii', 'backslashreplace')
        # end Py2K

    @util.memoized_property
    def _label(self):
        if self.is_literal:
            return None

        elif self.table is not None and self.table.named_with_column:
            if getattr(self.table, 'schema', None):
                label = self.table.schema.replace('.', '_') + "_" + \
                            _escape_for_generated(self.table.name) + "_" + \
                            _escape_for_generated(self.name)
            else:
                label = _escape_for_generated(self.table.name) + "_" + \
                            _escape_for_generated(self.name)

            # ensure the label name doesn't conflict with that
            # of an existing column
            if label in self.table.c:
                _label = label
                counter = 1
                while _label in self.table.c:
                    _label = label + "_" + str(counter)
                    counter += 1
                label = _label

            return _generated_label(label)

        else:
            return self.name

    def label(self, name):
        if name is None:
            return self
        else:
            return super(ColumnClause, self).label(name)

    @property
    def _from_objects(self):
        if self.table is not None:
            return [self.table]
        else:
            return []

    def _bind_param(self, operator, obj):
        return _BindParamClause(self.name, obj, _compared_to_operator=operator, 
                                _compared_to_type=self.type, unique=True)

    def _make_proxy(self, selectable, name=None, attach=True):
        # propagate the "is_literal" flag only if we are keeping our name,
        # otherwise its considered to be a label
        is_literal = self.is_literal and (name is None or name == self.name)
        c = self._constructor(
                    name or self.name, 
                    selectable=selectable, 
                    type_=self.type, 
                    is_literal=is_literal
                )
        c.proxies = [self]
        if attach:
            selectable.columns[c.name] = c
        return c

class TableClause(_Immutable, FromClause):
    """Represents a "table" construct.

    Note that this represents tables only as another syntactical
    construct within SQL expressions; it does not provide schema-level
    functionality.

    """

    __visit_name__ = 'table'

    named_with_column = True
    
    def __init__(self, name, *columns):
        super(TableClause, self).__init__()
        self.name = self.fullname = name
        self._columns = ColumnCollection()
        self._primary_key = ColumnSet()
        self._foreign_keys = set()
        for c in columns:
            self.append_column(c)

    def _export_columns(self):
        raise NotImplementedError()

    @util.memoized_property
    def description(self):
        # Py3K
        #return self.name
        # Py2K
        return self.name.encode('ascii', 'backslashreplace')
        # end Py2K

    def append_column(self, c):
        self._columns[c.name] = c
        c.table = self

    def get_children(self, column_collections=True, **kwargs):
        if column_collections:
            return [c for c in self.c]
        else:
            return []

    def count(self, whereclause=None, **params):
        """return a SELECT COUNT generated against this :class:`TableClause`."""
        
        if self.primary_key:
            col = list(self.primary_key)[0]
        else:
            col = list(self.columns)[0]
        return select(
                    [func.count(col).label('tbl_row_count')], 
                    whereclause, 
                    from_obj=[self], 
                    **params)

    def insert(self, values=None, inline=False, **kwargs):
        """Generate an :func:`insert()` construct."""

        return insert(self, values=values, inline=inline, **kwargs)

    def update(self, whereclause=None, values=None, inline=False, **kwargs):
        """Generate an :func:`update()` construct."""

        return update(self, whereclause=whereclause, 
                            values=values, inline=inline, **kwargs)

    def delete(self, whereclause=None, **kwargs):
        """Generate a :func:`delete()` construct."""

        return delete(self, whereclause, **kwargs)

    @property
    def _from_objects(self):
        return [self]

class _SelectBaseMixin(Executable):
    """Base class for :class:`Select` and ``CompoundSelects``."""

    def __init__(self,
            use_labels=False,
            for_update=False,
            limit=None,
            offset=None,
            order_by=None,
            group_by=None,
            bind=None,
            autocommit=None):
        self.use_labels = use_labels
        self.for_update = for_update
        if autocommit is not None:
            util.warn_deprecated("autocommit on select() is deprecated.  "
                                        "Use .execution_options(autocommit=True)")
            self._execution_options = self._execution_options.union({'autocommit':autocommit})
        self._limit = limit
        self._offset = offset
        self._bind = bind

        self._order_by_clause = ClauseList(*util.to_list(order_by) or [])
        self._group_by_clause = ClauseList(*util.to_list(group_by) or [])

    def as_scalar(self):
        """return a 'scalar' representation of this selectable, which can be
        used as a column expression.

        Typically, a select statement which has only one column in its columns
        clause is eligible to be used as a scalar expression.

        The returned object is an instance of 
        :class:`_ScalarSelect`.

        """
        return _ScalarSelect(self)

    @_generative
    def apply_labels(self):
        """return a new selectable with the 'use_labels' flag set to True.

        This will result in column expressions being generated using labels
        against their table name, such as "SELECT somecolumn AS
        tablename_somecolumn". This allows selectables which contain multiple
        FROM clauses to produce a unique set of column names regardless of
        name conflicts among the individual FROM clauses.

        """
        self.use_labels = True

    def label(self, name):
        """return a 'scalar' representation of this selectable, embedded as a
        subquery with a label.

        See also ``as_scalar()``.

        """
        return self.as_scalar().label(name)

    @_generative
    @util.deprecated(message="autocommit() is deprecated. "
                        "Use .execution_options(autocommit=True)")
    def autocommit(self):
        """return a new selectable with the 'autocommit' flag set to True."""
        
        self._execution_options = self._execution_options.union({'autocommit':True})

    def _generate(self):
        """Override the default _generate() method to also clear out exported collections."""
        
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        s._reset_exported()
        return s

    @_generative
    def limit(self, limit):
        """return a new selectable with the given LIMIT criterion applied."""

        self._limit = limit

    @_generative
    def offset(self, offset):
        """return a new selectable with the given OFFSET criterion applied."""

        self._offset = offset

    @_generative
    def order_by(self, *clauses):
        """return a new selectable with the given list of ORDER BY criterion applied.

        The criterion will be appended to any pre-existing ORDER BY criterion.

        """
        self.append_order_by(*clauses)

    @_generative
    def group_by(self, *clauses):
        """return a new selectable with the given list of GROUP BY criterion applied.

        The criterion will be appended to any pre-existing GROUP BY criterion.

        """
        self.append_group_by(*clauses)

    def append_order_by(self, *clauses):
        """Append the given ORDER BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing ORDER BY criterion.

        """
        if len(clauses) == 1 and clauses[0] is None:
            self._order_by_clause = ClauseList()
        else:
            if getattr(self, '_order_by_clause', None) is not None:
                clauses = list(self._order_by_clause) + list(clauses)
            self._order_by_clause = ClauseList(*clauses)

    def append_group_by(self, *clauses):
        """Append the given GROUP BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing GROUP BY criterion.

        """
        if len(clauses) == 1 and clauses[0] is None:
            self._group_by_clause = ClauseList()
        else:
            if getattr(self, '_group_by_clause', None) is not None:
                clauses = list(self._group_by_clause) + list(clauses)
            self._group_by_clause = ClauseList(*clauses)

    @property
    def _from_objects(self):
        return [self]


class _ScalarSelect(_Grouping):
    _from_objects = []

    def __init__(self, element):
        self.element = element
        cols = list(element.c)
        self.type = cols[0].type

    @property
    def columns(self):
        raise exc.InvalidRequestError("Scalar Select expression has no columns; "
                    "use this object directly within a column-level expression.")
    c  = columns

    def self_group(self, **kwargs):
        return self

    def _make_proxy(self, selectable, name):
        return list(self.inner_columns)[0]._make_proxy(selectable, name)

class CompoundSelect(_SelectBaseMixin, FromClause):
    """Forms the basis of ``UNION``, ``UNION ALL``, and other 
        SELECT-based set operations."""

    __visit_name__ = 'compound_select'

    UNION = util.symbol('UNION')
    UNION_ALL = util.symbol('UNION ALL')
    EXCEPT = util.symbol('EXCEPT')
    EXCEPT_ALL = util.symbol('EXCEPT ALL')
    INTERSECT = util.symbol('INTERSECT')
    INTERSECT_ALL = util.symbol('INTERSECT ALL')
    
    def __init__(self, keyword, *selects, **kwargs):
        self._should_correlate = kwargs.pop('correlate', False)
        self.keyword = keyword
        self.selects = []

        numcols = None

        # some DBs do not like ORDER BY in the inner queries of a UNION, etc.
        for n, s in enumerate(selects):
            s = _clause_element_as_expr(s)
            
            if not numcols:
                numcols = len(s.c)
            elif len(s.c) != numcols:
                raise exc.ArgumentError(
                        "All selectables passed to CompoundSelect must "
                        "have identical numbers of columns; select #%d has %d columns,"
                        " select #%d has %d" %
                        (1, len(self.selects[0].c), n+1, len(s.c))
                )

            self.selects.append(s.self_group(self))

        _SelectBaseMixin.__init__(self, **kwargs)

    def self_group(self, against=None):
        return _FromGrouping(self)

    def is_derived_from(self, fromclause):
        for s in self.selects:
            if s.is_derived_from(fromclause):
                return True
        return False

    def _populate_column_collection(self):
        for cols in zip(*[s.c for s in self.selects]):
            # this is a slightly hacky thing - the union exports a column that
            # resembles just that of the *first* selectable.  to get at a "composite" column,
            # particularly foreign keys, you have to dig through the proxies collection 
            # which we generate below.  We may want to improve upon this,
            # such as perhaps _make_proxy can accept a list of other columns that
            # are "shared" - schema.column can then copy all the ForeignKeys in.
            # this would allow the union() to have all those fks too.
            proxy = cols[0]._make_proxy(
                            self, name=self.use_labels and cols[0]._label or None)
            
            # hand-construct the "proxies" collection to include all derived columns
            # place a 'weight' annotation corresponding to how low in the list of
            # select()s the column occurs, so that the corresponding_column() operation
            # can resolve conflicts
            proxy.proxies = [c._annotate({'weight':i + 1}) for i, c in enumerate(cols)]
            
    def _copy_internals(self, clone=_clone):
        self._reset_exported()
        self.selects = [clone(s) for s in self.selects]
        if hasattr(self, '_col_map'):
            del self._col_map
        for attr in ('_order_by_clause', '_group_by_clause'):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr)))

    def get_children(self, column_collections=True, **kwargs):
        return (column_collections and list(self.c) or []) + \
            [self._order_by_clause, self._group_by_clause] + list(self.selects)

    def bind(self):
        if self._bind:
            return self._bind
        for s in self.selects:
            e = s.bind
            if e:
                return e
        else:
            return None
    def _set_bind(self, bind):
        self._bind = bind
    bind = property(bind, _set_bind)

class Select(_SelectBaseMixin, FromClause):
    """Represents a ``SELECT`` statement.

    Select statements support appendable clauses, as well as the
    ability to execute themselves and return a result set.

    """

    __visit_name__ = 'select'
    
    _prefixes = ()
    _hints = util.frozendict()
    
    def __init__(self, 
                columns, 
                whereclause=None, 
                from_obj=None, 
                distinct=False, 
                having=None, 
                correlate=True, 
                prefixes=None, 
                **kwargs):
        """Construct a Select object.

        The public constructor for Select is the
        :func:`select` function; see that function for
        argument descriptions.

        Additional generative and mutator methods are available on the
        :class:`_SelectBaseMixin` superclass.

        """
        self._should_correlate = correlate
        self._distinct = distinct

        self._correlate = set()
        self._froms = util.OrderedSet()
        
        try:
            cols_present = bool(columns)
        except TypeError:
            raise exc.ArgumentError("columns argument to select() must "
                                "be a Python list or other iterable")
            
        if cols_present:
            self._raw_columns = []
            for c in columns:
                c = _literal_as_column(c)
                if isinstance(c, _ScalarSelect):
                    c = c.self_group(against=operators.comma_op)
                self._raw_columns.append(c)

            self._froms.update(_from_objects(*self._raw_columns))
        else:
            self._raw_columns = []

        if whereclause is not None:
            self._whereclause = _literal_as_text(whereclause)
            self._froms.update(_from_objects(self._whereclause))
        else:
            self._whereclause = None

        if from_obj is not None:
            for f in util.to_list(from_obj):
                if _is_literal(f):
                    self._froms.add(_TextClause(f))
                else:
                    self._froms.add(f)

        if having is not None:
            self._having = _literal_as_text(having)
        else:
            self._having = None

        if prefixes:
            self._prefixes = tuple([_literal_as_text(p) for p in prefixes])

        _SelectBaseMixin.__init__(self, **kwargs)

    def _get_display_froms(self, existing_froms=None):
        """Return the full list of 'from' clauses to be displayed.

        Takes into account a set of existing froms which may be
        rendered in the FROM clause of enclosing selects; this Select
        may want to leave those absent if it is automatically
        correlating.

        """
        froms = self._froms

        toremove = itertools.chain(*[f._hide_froms for f in froms])
        if toremove:
            froms = froms.difference(toremove)

        if len(froms) > 1 or self._correlate:
            if self._correlate:
                froms = froms.difference(_cloned_intersection(froms, self._correlate))

            if self._should_correlate and existing_froms:
                froms = froms.difference(_cloned_intersection(froms, existing_froms))

                if not len(froms):
                    raise exc.InvalidRequestError(
                            "Select statement '%s' returned no FROM clauses "
                            "due to auto-correlation; specify correlate(<tables>) "
                            "to control correlation manually." % self)

        return froms

    @property
    def froms(self):
        """Return the displayed list of FromClause elements."""

        return self._get_display_froms()
    
    @_generative
    def with_hint(self, selectable, text, dialect_name=None):
        """Add an indexing hint for the given selectable to this :class:`Select`.
        
        The text of the hint is written specific to a specific backend, and
        typically uses Python string substitution syntax to render the name
        of the table or alias, such as for Oracle::
        
            select([mytable]).with_hint(mytable, "+ index(%(name)s ix_mytable)")
            
        Would render SQL as::
        
            select /*+ index(mytable ix_mytable) */ ... from mytable
            
        The ``dialect_name`` option will limit the rendering of a particular hint
        to a particular backend.  Such as, to add hints for both Oracle and
        Sybase simultaneously::
        
            select([mytable]).\
                with_hint(mytable, "+ index(%(name)s ix_mytable)", 'oracle').\
                with_hint(mytable, "WITH INDEX ix_mytable", 'sybase')
        
        """
        if not dialect_name:
            dialect_name = '*'
        self._hints = self._hints.union({(selectable, dialect_name):text})
        
    @property
    def type(self):
        raise exc.InvalidRequestError("Select objects don't have a type.  "
                    "Call as_scalar() on this Select object "
                    "to return a 'scalar' version of this Select.")

    @util.memoized_instancemethod
    def locate_all_froms(self):
        """return a Set of all FromClause elements referenced by this Select.

        This set is a superset of that returned by the ``froms`` property, which
        is specifically for those FromClause elements that would actually be rendered.

        """
        return self._froms.union(_from_objects(*list(self._froms)))

    @property
    def inner_columns(self):
        """an iterator of all ColumnElement expressions which would
        be rendered into the columns clause of the resulting SELECT statement.

        """
        return _select_iterables(self._raw_columns)

    def is_derived_from(self, fromclause):
        if self in fromclause._cloned_set:
            return True

        for f in self.locate_all_froms():
            if f.is_derived_from(fromclause):
                return True
        return False

    def _copy_internals(self, clone=_clone):
        self._reset_exported()
        from_cloned = dict((f, clone(f))
                           for f in self._froms.union(self._correlate))
        self._froms = util.OrderedSet(from_cloned[f] for f in self._froms)
        self._correlate = set(from_cloned[f] for f in self._correlate)
        self._raw_columns = [clone(c) for c in self._raw_columns]
        for attr in ('_whereclause', '_having', '_order_by_clause', '_group_by_clause'):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr)))

    def get_children(self, column_collections=True, **kwargs):
        """return child elements as per the ClauseElement specification."""

        return (column_collections and list(self.columns) or []) + \
            self._raw_columns + list(self._froms) + \
            [x for x in 
                (self._whereclause, self._having, 
                    self._order_by_clause, self._group_by_clause) 
            if x is not None]

    @_generative
    def column(self, column):
        """return a new select() construct with the given column expression 
            added to its columns clause.
            
        """

        column = _literal_as_column(column)

        if isinstance(column, _ScalarSelect):
            column = column.self_group(against=operators.comma_op)

        self._raw_columns = self._raw_columns + [column]
        self._froms = self._froms.union(_from_objects(column))

    @_generative
    def with_only_columns(self, columns):
        """return a new select() construct with its columns clause replaced 
            with the given columns.
            
        """

        self._raw_columns = [
                isinstance(c, _ScalarSelect) and 
                c.self_group(against=operators.comma_op) or c
                for c in [_literal_as_column(c) for c in columns]
            ]

    @_generative
    def where(self, whereclause):
        """return a new select() construct with the given expression added to its 
        WHERE clause, joined to the existing clause via AND, if any.
        
        """

        self.append_whereclause(whereclause)

    @_generative
    def having(self, having):
        """return a new select() construct with the given expression added to its HAVING
          clause, joined to the existing clause via AND, if any.
         
        """
        self.append_having(having)

    @_generative
    def distinct(self):
        """return a new select() construct which will apply DISTINCT to its columns
         clause.
         
         """
        self._distinct = True

    @_generative
    def prefix_with(self, clause):
        """return a new select() construct which will apply the given expression to the
         start of its columns clause, not using any commas.

         """
        clause = _literal_as_text(clause)
        self._prefixes = self._prefixes + (clause,)

    @_generative
    def select_from(self, fromclause):
        """return a new select() construct with the given FROM expression applied to its
         list of FROM objects.

         """
        fromclause = _literal_as_text(fromclause)
        self._froms = self._froms.union([fromclause])

    @_generative
    def correlate(self, *fromclauses):
        """return a new select() construct which will correlate the given FROM clauses to
        that of an enclosing select(), if a match is found.
        
        By "match", the given fromclause must be present in this select's list of FROM
        objects and also present in an enclosing select's list of FROM objects.
        
        Calling this method turns off the select's default behavior of
        "auto-correlation". Normally, select() auto-correlates all of its FROM clauses to
        those of an embedded select when compiled.
        
        If the fromclause is None, correlation is disabled for the returned select().

        """
        self._should_correlate = False
        if fromclauses == (None,):
            self._correlate = set()
        else:
            self._correlate = self._correlate.union(fromclauses)

    def append_correlation(self, fromclause):
        """append the given correlation expression to this select() construct."""

        self._should_correlate = False
        self._correlate = self._correlate.union([fromclause])

    def append_column(self, column):
        """append the given column expression to the columns clause of this select()
        construct.
        
        """
        column = _literal_as_column(column)

        if isinstance(column, _ScalarSelect):
            column = column.self_group(against=operators.comma_op)

        self._raw_columns = self._raw_columns + [column]
        self._froms = self._froms.union(_from_objects(column))
        self._reset_exported()

    def append_prefix(self, clause):
        """append the given columns clause prefix expression to this select()
        construct.
        
        """
        clause = _literal_as_text(clause)
        self._prefixes = self._prefixes + (clause,)

    def append_whereclause(self, whereclause):
        """append the given expression to this select() construct's WHERE criterion.

        The expression will be joined to existing WHERE criterion via AND.

        """
        whereclause = _literal_as_text(whereclause)
        self._froms = self._froms.union(_from_objects(whereclause))

        if self._whereclause is not None:
            self._whereclause = and_(self._whereclause, whereclause)
        else:
            self._whereclause = whereclause

    def append_having(self, having):
        """append the given expression to this select() construct's HAVING criterion.

        The expression will be joined to existing HAVING criterion via AND.

        """
        if self._having is not None:
            self._having = and_(self._having, _literal_as_text(having))
        else:
            self._having = _literal_as_text(having)

    def append_from(self, fromclause):
        """append the given FromClause expression to this select() construct's FROM
        clause.

        """
        if _is_literal(fromclause):
            fromclause = _TextClause(fromclause)

        self._froms = self._froms.union([fromclause])

    def __exportable_columns(self):
        for column in self._raw_columns:
            if isinstance(column, Selectable):
                for co in column.columns:
                    yield co
            elif isinstance(column, ColumnElement):
                yield column
            else:
                continue

    def _populate_column_collection(self):
        for c in self.__exportable_columns():
            c._make_proxy(self, name=self.use_labels and c._label or None)

    def self_group(self, against=None):
        """return a 'grouping' construct as per the ClauseElement specification.

        This produces an element that can be embedded in an expression.  Note that
        this method is called automatically as needed when constructing expressions.

        """
        if isinstance(against, CompoundSelect):
            return self
        return _FromGrouping(self)

    def union(self, other, **kwargs):
        """return a SQL UNION of this select() construct against the given selectable."""

        return union(self, other, **kwargs)

    def union_all(self, other, **kwargs):
        """return a SQL UNION ALL of this select() construct against the given
        selectable.
        
        """
        return union_all(self, other, **kwargs)

    def except_(self, other, **kwargs):
        """return a SQL EXCEPT of this select() construct against the given selectable."""

        return except_(self, other, **kwargs)

    def except_all(self, other, **kwargs):
        """return a SQL EXCEPT ALL of this select() construct against the given
        selectable.
        
        """
        return except_all(self, other, **kwargs)

    def intersect(self, other, **kwargs):
        """return a SQL INTERSECT of this select() construct against the given
        selectable.
        
        """
        return intersect(self, other, **kwargs)

    def intersect_all(self, other, **kwargs):
        """return a SQL INTERSECT ALL of this select() construct against the given
        selectable.
        
        """
        return intersect_all(self, other, **kwargs)

    def bind(self):
        if self._bind:
            return self._bind
        if not self._froms:
            for c in self._raw_columns:
                e = c.bind
                if e:
                    self._bind = e
                    return e
        else:
            e = list(self._froms)[0].bind
            if e:
                self._bind = e
                return e

        return None

    def _set_bind(self, bind):
        self._bind = bind
    bind = property(bind, _set_bind)

class _UpdateBase(Executable, ClauseElement):
    """Form the base for ``INSERT``, ``UPDATE``, and ``DELETE`` statements."""

    __visit_name__ = 'update_base'

    _execution_options = Executable._execution_options.union({'autocommit':True})
    kwargs = util.frozendict()
    
    def _process_colparams(self, parameters):
        if isinstance(parameters, (list, tuple)):
            pp = {}
            for i, c in enumerate(self.table.c):
                pp[c.key] = parameters[i]
            return pp
        else:
            return parameters

    def params(self, *arg, **kw):
        raise NotImplementedError(
            "params() is not supported for INSERT/UPDATE/DELETE statements."
            " To set the values for an INSERT or UPDATE statement, use"
            " stmt.values(**parameters).")

    def bind(self):
        return self._bind or self.table.bind

    def _set_bind(self, bind):
        self._bind = bind
    bind = property(bind, _set_bind)

    _returning_re = re.compile(r'(?:firebird|postgres(?:ql)?)_returning')
    def _process_deprecated_kw(self, kwargs):
        for k in list(kwargs):
            m = self._returning_re.match(k)
            if m:
                self._returning = kwargs.pop(k)
                util.warn_deprecated(
                    "The %r argument is deprecated.  Please "
                    "use statement.returning(col1, col2, ...)" % k
                )
        return kwargs
    
    @_generative
    def returning(self, *cols):
        """Add a RETURNING or equivalent clause to this statement.
        
        The given list of columns represent columns within the table
        that is the target of the INSERT, UPDATE, or DELETE.  Each 
        element can be any column expression.  :class:`~sqlalchemy.schema.Table`
        objects will be expanded into their individual columns.
        
        Upon compilation, a RETURNING clause, or database equivalent, 
        will be rendered within the statement.   For INSERT and UPDATE, 
        the values are the newly inserted/updated values.  For DELETE, 
        the values are those of the rows which were deleted.
        
        Upon execution, the values of the columns to be returned
        are made available via the result set and can be iterated
        using ``fetchone()`` and similar.   For DBAPIs which do not
        natively support returning values (i.e. cx_oracle), 
        SQLAlchemy will approximate this behavior at the result level
        so that a reasonable amount of behavioral neutrality is 
        provided.
        
        Note that not all databases/DBAPIs
        support RETURNING.   For those backends with no support,
        an exception is raised upon compilation and/or execution.
        For those who do support it, the functionality across backends
        varies greatly, including restrictions on executemany()
        and other statements which return multiple rows. Please 
        read the documentation notes for the database in use in 
        order to determine the availability of RETURNING.
        
        """
        self._returning = cols
        
class _ValuesBase(_UpdateBase):

    __visit_name__ = 'values_base'

    def __init__(self, table, values):
        self.table = table
        self.parameters = self._process_colparams(values)

    @_generative
    def values(self, *args, **kwargs):
        """specify the VALUES clause for an INSERT statement, or the SET clause for an
        UPDATE.

            \**kwargs
                key=<somevalue> arguments

            \*args
                A single dictionary can be sent as the first positional argument. This
                allows non-string based keys, such as Column objects, to be used.

        """
        if args:
            v = args[0]
        else:
            v = {}

        if self.parameters is None:
            self.parameters = self._process_colparams(v)
            self.parameters.update(kwargs)
        else:
            self.parameters = self.parameters.copy()
            self.parameters.update(self._process_colparams(v))
            self.parameters.update(kwargs)

class Insert(_ValuesBase):
    """Represent an INSERT construct.

    The :class:`Insert` object is created using the :func:`insert()` function.

    """
    __visit_name__ = 'insert'
    
    _prefixes = ()
    
    def __init__(self, 
                table, 
                values=None, 
                inline=False, 
                bind=None, 
                prefixes=None, 
                returning=None,
                **kwargs):
        _ValuesBase.__init__(self, table, values)
        self._bind = bind
        self.select = None
        self.inline = inline
        self._returning = returning
        if prefixes:
            self._prefixes = tuple([_literal_as_text(p) for p in prefixes])
        
        if kwargs:
            self.kwargs = self._process_deprecated_kw(kwargs)

    def get_children(self, **kwargs):
        if self.select is not None:
            return self.select,
        else:
            return ()

    def _copy_internals(self, clone=_clone):
        # TODO: coverage
        self.parameters = self.parameters.copy()

    @_generative
    def prefix_with(self, clause):
        """Add a word or expression between INSERT and INTO. Generative.

        If multiple prefixes are supplied, they will be separated with
        spaces.

        """
        clause = _literal_as_text(clause)
        self._prefixes = self._prefixes + (clause,)

class Update(_ValuesBase):
    """Represent an Update construct.

    The :class:`Update` object is created using the :func:`update()` function.

    """
    __visit_name__ = 'update'

    def __init__(self, 
                table, 
                whereclause, 
                values=None, 
                inline=False, 
                bind=None, 
                returning=None,
                **kwargs):
        _ValuesBase.__init__(self, table, values)
        self._bind = bind
        self._returning = returning
        if whereclause is not None:
            self._whereclause = _literal_as_text(whereclause)
        else:
            self._whereclause = None
        self.inline = inline

        if kwargs:
            self.kwargs = self._process_deprecated_kw(kwargs)

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return self._whereclause,
        else:
            return ()

    def _copy_internals(self, clone=_clone):
        # TODO: coverage
        self._whereclause = clone(self._whereclause)
        self.parameters = self.parameters.copy()

    @_generative
    def where(self, whereclause):
        """return a new update() construct with the given expression added to its WHERE
        clause, joined to the existing clause via AND, if any.
        
        """
        if self._whereclause is not None:
            self._whereclause = and_(self._whereclause, _literal_as_text(whereclause))
        else:
            self._whereclause = _literal_as_text(whereclause)


class Delete(_UpdateBase):
    """Represent a DELETE construct.

    The :class:`Delete` object is created using the :func:`delete()` function.

    """

    __visit_name__ = 'delete'

    def __init__(self, 
            table, 
            whereclause, 
            bind=None, 
            returning =None,
            **kwargs):
        self._bind = bind
        self.table = table
        self._returning = returning
        
        if whereclause is not None:
            self._whereclause = _literal_as_text(whereclause)
        else:
            self._whereclause = None

        if kwargs:
            self.kwargs = self._process_deprecated_kw(kwargs)

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return self._whereclause,
        else:
            return ()

    @_generative
    def where(self, whereclause):
        """Add the given WHERE clause to a newly returned delete construct."""

        if self._whereclause is not None:
            self._whereclause = and_(self._whereclause, _literal_as_text(whereclause))
        else:
            self._whereclause = _literal_as_text(whereclause)

    def _copy_internals(self, clone=_clone):
        # TODO: coverage
        self._whereclause = clone(self._whereclause)

class _IdentifiedClause(Executable, ClauseElement):
    __visit_name__ = 'identified'
    _execution_options = Executable._execution_options.union({'autocommit':False})
    quote = None

    def __init__(self, ident):
        self.ident = ident

class SavepointClause(_IdentifiedClause):
    __visit_name__ = 'savepoint'

class RollbackToSavepointClause(_IdentifiedClause):
    __visit_name__ = 'rollback_to_savepoint'

class ReleaseSavepointClause(_IdentifiedClause):
    __visit_name__ = 'release_savepoint'


