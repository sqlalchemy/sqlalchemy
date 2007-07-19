# sql.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define the base components of SQL expression trees.

All components are derived from a common base class [sqlalchemy.sql#ClauseElement].
Common behaviors are organized based on class hierarchies, in some cases
via mixins.  

All object construction from this package occurs via functions which in some
cases will construct composite ``ClauseElement`` structures together, and
in other cases simply return a single ``ClauseElement`` constructed directly.
The function interface affords a more "DSL-ish" feel to constructing SQL expressions
and also allows future class reorganizations.

Even though classes are not constructed directly from the outside, most 
classes which have additional public methods are considered to be public (i.e. have no leading underscore).
Other classes which are "semi-public" are marked with a single leading
underscore; these classes usually have few or no public methods and
are less guaranteed to stay the same in future releases.

"""

from sqlalchemy import util, exceptions, logging
from sqlalchemy import types as sqltypes
import string, re, random, sets


__all__ = ['AbstractDialect', 'Alias', 'ClauseElement', 'ClauseParameters',
           'ClauseVisitor', 'ColumnCollection', 'ColumnElement',
           'Compiled', 'CompoundSelect', 'Executor', 'FromClause', 'Join',
           'Select', 'Selectable', 'TableClause', 'alias', 'and_', 'asc',
           'between_', 'between', 'bindparam', 'case', 'cast', 'column', 'delete',
           'desc', 'distinct', 'except_', 'except_all', 'exists', 'extract', 'func', 'modifier',
           'insert', 'intersect', 'intersect_all', 'join', 'literal',
           'literal_column', 'not_', 'null', 'or_', 'outerjoin', 'select',
           'subquery', 'table', 'text', 'union', 'union_all', 'update',]

# precedence ordering for common operators.  if an operator is not present in this list,
# it will be parenthesized when grouped against other operators
PRECEDENCE = {
    'FROM':15,
    '*':7,
    '/':7,
	'%':7,
    '+':6,
    '-':6,
    'ILIKE':5,
    'NOT ILIKE':5,
    'LIKE':5,
    'NOT LIKE':5,
    'IN':5,
    'NOT IN':5,
    'IS':5,
    'IS NOT':5,
    '=':5,
    '!=':5,
    '>':5,
    '<':5,
    '>=':5,
    '<=':5,
    'BETWEEN':5,
    'NOT':4,
    'AND':3,
    'OR':2,
    ',':-1,
    'AS':-1,
    'EXISTS':0,
    '_smallest': -1000,
    '_largest': 1000
}

def desc(column):
    """Return a descending ``ORDER BY`` clause element.

    E.g.::

      order_by = [desc(table1.mycol)]
    """
    return _UnaryExpression(column, modifier="DESC")

def asc(column):
    """Return an ascending ``ORDER BY`` clause element.

    E.g.::

      order_by = [asc(table1.mycol)]
    """
    return _UnaryExpression(column, modifier="ASC")

def outerjoin(left, right, onclause=None, **kwargs):
    """Return an ``OUTER JOIN`` clause element.
    
    The returned object is an instance of [sqlalchemy.sql#Join].

    Similar functionality is also available via the ``outerjoin()`` method on any
    [sqlalchemy.sql#FromClause].

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

    The returned object is an instance of [sqlalchemy.sql#Join].

    Similar functionality is also available via the ``join()`` method on any
    [sqlalchemy.sql#FromClause].

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

def select(columns=None, whereclause = None, from_obj = [], **kwargs):
    """Returns a ``SELECT`` clause element.

    Similar functionality is also available via the ``select()`` method on any
    [sqlalchemy.sql#FromClause].
    
    The returned object is an instance of [sqlalchemy.sql#Select].

    All arguments which accept ``ClauseElement`` arguments also
    accept string arguments, which will be converted as appropriate
    into either ``text()`` or ``literal_column()`` constructs.
    
      columns
        A list of ``ClauseElement`` objects, typically ``ColumnElement``
        objects or subclasses, which will form
        the columns clause of the resulting statement.  For all
        members which are instances of ``Selectable``, the individual
        ``ColumnElement`` members of the ``Selectable`` will be 
        added individually to the columns clause.  For example, specifying
        a ``Table`` instance will result in all the contained ``Column``
        objects within to be added to the columns clause. 
    
        This argument is not present on the form of ``select()`` available
        on ``Table``.
      
      whereclause
        A ``ClauseElement`` expression which will be used to form the 
        ``WHERE`` clause.
      
      from_obj
        A list of ``ClauseElement`` objects which will be added to the ``FROM``
        clause of the resulting statement.  Note that "from" objects
        are automatically located within the columns and whereclause
        ClauseElements.  Use this parameter to explicitly specify
        "from" objects which are not automatically locatable.
        This could include ``Table`` objects that aren't otherwise
        present, or ``Join`` objects whose presence will supercede
        that of the ``Table`` objects already located in the other
        clauses.

      \**kwargs
        Additional parameters include:

        order_by
          a scalar or list of ``ClauseElement`` objects
          which will comprise the ``ORDER BY`` clause of the resulting
          select.
       
        group_by
          a list of ``ClauseElement`` objects which will comprise
          the ``GROUP BY`` clause of the resulting select.
        
        having
          a ``ClauseElement`` that will comprise the ``HAVING`` 
          clause of the resulting select when ``GROUP BY`` is used.
        
        use_labels=False
          when ``True``, the statement will be generated using 
          labels for each column in the columns clause, which qualify
          each column with its parent table's (or aliases) name so 
          that name conflicts between columns in different tables don't
          occur.  The format of the label is <tablename>_<column>.  The
          "c" collection of the resulting ``Select`` object will use these
          names as well for targeting column members.
        
        distinct=False
          when ``True``, applies a ``DISTINCT`` qualifier to the 
          columns clause of the resulting statement.
        
        for_update=False
          when ``True``, applies ``FOR UPDATE`` to the end of the
          resulting statement.  Certain database dialects also
          support alternate values for this parameter, for example
          mysql supports "read" which translates to ``LOCK IN SHARE MODE``,
          and oracle supports "nowait" which translates to 
          ``FOR UPDATE NOWAIT``.
        
        bind=None
          an ``Engine`` or ``Connection`` instance to which the resulting ``Select`` 
          object will be bound.  The ``Select`` object will otherwise
          automatically bind to whatever ``Connectable`` instances can be located
          within its contained ``ClauseElement`` members.
        
        engine=None
          deprecated.  a synonym for "bind".
          
        limit=None
          a numerical value which usually compiles to a ``LIMIT`` expression
          in the resulting select.  Databases that don't support ``LIMIT``
          will attempt to provide similar functionality.
        
        offset=None
          a numerical value which usually compiles to an ``OFFSET`` expression
          in the resulting select.  Databases that don't support ``OFFSET``
          will attempt to provide similar functionality.
        
        scalar=False
          when ``True``, indicates that the resulting ``Select`` object
          is to be used in the "columns" clause of another select statement,
          where the evaluated value of the column is the scalar result of 
          this statement.  Normally, placing any ``Selectable`` within the 
          columns clause of a ``select()`` call will expand the member 
          columns of the ``Selectable`` individually.

        correlate=True
          indicates that this ``Select`` object should have its contained
          ``FromClause`` elements "correlated" to an enclosing ``Select``
          object.  This means that any ``ClauseElement`` instance within 
          the "froms" collection of this ``Select`` which is also present
          in the "froms" collection of an enclosing select will not be
          rendered in the ``FROM`` clause of this select statement.
      
    """

    return Select(columns, whereclause = whereclause, from_obj = from_obj, **kwargs)

def subquery(alias, *args, **kwargs):
    """Return an [sqlalchemy.sql#Alias] object derived from a [sqlalchemy.sql#Select].
    
      name
        alias name

      \*args, \**kwargs
        all other arguments are delivered to the [sqlalchemy.sql#select()] function.
    
    """
    
    return Select(*args, **kwargs).alias(alias)

def insert(table, values = None, **kwargs):
    """Return an [sqlalchemy.sql#_Insert] clause element.

    Similar functionality is available via the ``insert()`` 
    method on [sqlalchemy.schema#Table].

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

    return _Insert(table, values, **kwargs)

def update(table, whereclause = None, values = None, **kwargs):
    """Return an [sqlalchemy.sql#_Update] clause element.

    Similar functionality is available via the ``update()`` 
    method on [sqlalchemy.schema#Table].

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

    return _Update(table, whereclause, values, **kwargs)

def delete(table, whereclause = None, **kwargs):
    """Return a [sqlalchemy.sql#_Delete] clause element.

    Similar functionality is available via the ``delete()`` 
    method on [sqlalchemy.schema#Table].

      table
        The table to be updated.

      whereclause
        A ``ClauseElement`` describing the ``WHERE`` condition of the
        ``UPDATE`` statement.

    """

    return _Delete(table, whereclause, **kwargs)

def and_(*clauses):
    """Join a list of clauses together using the ``AND`` operator.

    The ``&`` operator is also overloaded on all [sqlalchemy.sql#_CompareMixin]
    subclasses to produce the same result.
    """
    if len(clauses) == 1:
        return clauses[0]
    return ClauseList(operator='AND', *clauses)

def or_(*clauses):
    """Join a list of clauses together using the ``OR`` operator.

    The ``|`` operator is also overloaded on all [sqlalchemy.sql#_CompareMixin]
    subclasses to produce the same result.
    """

    if len(clauses) == 1:
        return clauses[0]
    return ClauseList(operator='OR', *clauses)

def not_(clause):
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The ``~`` operator is also overloaded on all [sqlalchemy.sql#_CompareMixin]
    subclasses to produce the same result.
    """

    return clause._negate()

def distinct(expr):
    """return a ``DISTINCT`` clause."""
    
    return _UnaryExpression(expr, operator="DISTINCT")

def between(ctest, cleft, cright):
    """Return a ``BETWEEN`` predicate clause.

    Equivalent of SQL ``clausetest BETWEEN clauseleft AND clauseright``.

    The ``between()`` method on all [sqlalchemy.sql#_CompareMixin] subclasses
    provides similar functionality.
    """

    return _BinaryExpression(ctest, ClauseList(_literals_as_binds(cleft, type=ctest.type), _literals_as_binds(cright, type=ctest.type), operator='AND', group=False), 'BETWEEN')

def between_(*args, **kwargs):
    """synonym for [sqlalchemy.sql#between()] (deprecated)."""
    
    return between(*args, **kwargs)

def case(whens, value=None, else_=None):
    """Produce a ``CASE`` statement.

        whens
          A sequence of pairs to be translated into "when / then" clauses.

        value
          Optional for simple case statements.

        else\_
          Optional as well, for case defaults.

    """

    whenlist = [ClauseList('WHEN', c, 'THEN', r, operator=None) for (c,r) in whens]
    if not else_ is None:
        whenlist.append(ClauseList('ELSE', else_, operator=None))
    if len(whenlist):
        type = list(whenlist[-1])[-1].type
    else:
        type = None
    cc = _CalculatedClause(None, 'CASE', value, type=type, operator=None, group_contents=False, *whenlist + ['END'])
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

    expr = _BinaryExpression(text(field), expr, "FROM")
    return func.extract(expr)

def exists(*args, **kwargs):
    """Return an ``EXISTS`` clause as applied to a [sqlalchemy.sql#Select] object.
    
    The resulting [sqlalchemy.sql#_Exists] object can be executed by itself
    or used as a subquery within an enclosing select.
    
        \*args, \**kwargs
          all arguments are sent directly to the [sqlalchemy.sql#select()] function
          to produce a ``SELECT`` statement.
          
    """
    
    return _Exists(*args, **kwargs)

def union(*selects, **kwargs):
    """Return a ``UNION`` of multiple selectables.
    
    The returned object is an instance of [sqlalchemy.sql#CompoundSelect].
    
    A similar ``union()`` method is available on all [sqlalchemy.sql#FromClause]
    subclasses.
    
      \*selects
        a list of [sqlalchemy.sql#Select] instances.

      \**kwargs
         available keyword arguments are the same as those of [sqlalchemy.sql#select()].
    
    """
    
    return _compound_select('UNION', *selects, **kwargs)

def union_all(*selects, **kwargs):
    """Return a ``UNION ALL`` of multiple selectables.
    
    The returned object is an instance of [sqlalchemy.sql#CompoundSelect].
    
    A similar ``union_all()`` method is available on all [sqlalchemy.sql#FromClause]
    subclasses.

        \*selects
          a list of [sqlalchemy.sql#Select] instances.
        
        \**kwargs
          available keyword arguments are the same as those of [sqlalchemy.sql#select()].
          
    """
    return _compound_select('UNION ALL', *selects, **kwargs)

def except_(*selects, **kwargs):
    """Return an ``EXCEPT`` of multiple selectables.
    
    The returned object is an instance of [sqlalchemy.sql#CompoundSelect].

        \*selects
          a list of [sqlalchemy.sql#Select] instances.
        
        \**kwargs
          available keyword arguments are the same as those of [sqlalchemy.sql#select()].
          
    """
    return _compound_select('EXCEPT', *selects, **kwargs)

def except_all(*selects, **kwargs):
    """Return an ``EXCEPT ALL`` of multiple selectables.
    
    The returned object is an instance of [sqlalchemy.sql#CompoundSelect].

        \*selects
          a list of [sqlalchemy.sql#Select] instances.
        
        \**kwargs
          available keyword arguments are the same as those of [sqlalchemy.sql#select()].
          
    """
    return _compound_select('EXCEPT ALL', *selects, **kwargs)

def intersect(*selects, **kwargs):
    """Return an ``INTERSECT`` of multiple selectables.
    
    The returned object is an instance of [sqlalchemy.sql#CompoundSelect].

        \*selects
          a list of [sqlalchemy.sql#Select] instances.
        
        \**kwargs
          available keyword arguments are the same as those of [sqlalchemy.sql#select()].
          
    """
    return _compound_select('INTERSECT', *selects, **kwargs)

def intersect_all(*selects, **kwargs):
    """Return an ``INTERSECT ALL`` of multiple selectables.
    
    The returned object is an instance of [sqlalchemy.sql#CompoundSelect].

        \*selects
          a list of [sqlalchemy.sql#Select] instances.
        
        \**kwargs
          available keyword arguments are the same as those of [sqlalchemy.sql#select()].
          
    """
    return _compound_select('INTERSECT ALL', *selects, **kwargs)

def alias(selectable, alias=None):
    """Return an [sqlalchemy.sql#Alias] object.
    
    An ``Alias`` represents any [sqlalchemy.sql#FromClause] with
    an alternate name assigned within SQL, typically using the ``AS``
    clause when generated, e.g. ``SELECT * FROM table AS aliasname``.
    
    Similar functionality is available via the ``alias()`` method 
    available on all ``FromClause`` subclasses.
    
      selectable
        any ``FromClause`` subclass, such as a table, select statement, etc..
        
      alias
        string name to be assigned as the alias.  If ``None``, a random
        name will be generated.
        
    """
        
    return Alias(selectable, alias=alias)


def literal(value, type=None):
    """Return a literal clause, bound to a bind parameter.

    Literal clauses are created automatically when non-
    ``ClauseElement`` objects (such as strings, ints, dates, etc.) are used in 
    a comparison operation with a [sqlalchemy.sql#_CompareMixin]
    subclass, such as a ``Column`` object.  Use this function
    to force the generation of a literal clause, which will 
    be created as a [sqlalchemy.sql#_BindParamClause] with a bound
    value.
    
      value
        the value to be bound.  can be any Python object supported by
        the underlying DBAPI, or is translatable via the given type
        argument.
    
      type
        an optional [sqlalchemy.types#TypeEngine] which will provide
        bind-parameter translation for this literal.

    """

    return _BindParamClause('literal', value, type=type, unique=True)

def label(name, obj):
    """Return a [sqlalchemy.sql#_Label] object for the given [sqlalchemy.sql#ColumnElement].
    
    A label changes the name of an element in the columns clause 
    of a ``SELECT`` statement, typically via the ``AS`` SQL keyword.
    
    This functionality is more conveniently available via 
    the ``label()`` method on ``ColumnElement``.
    
      name
        label name
        
      obj
        a ``ColumnElement``.
        
    """

    return _Label(name, obj)

def column(text, type=None):
    """Return a textual column clause, as would be in the columns 
    clause of a ``SELECT`` statement.
    
    The object returned is an instance of [sqlalchemy.sql#_ColumnClause],
    which represents the "syntactical" portion of the schema-level
    [sqlalchemy.schema#Column] object.
    
      text
        the name of the column.  Quoting rules will be applied to 
        the clause like any other column name.  For textual column
        constructs that are not to be quoted, use the [sqlalchemy.sql#literal_column()]
        function.
        
      type
        an optional [sqlalchemy.types#TypeEngine] object which will provide
        result-set translation for this column.
        
    """

    return _ColumnClause(text, type=type)

def literal_column(text, type=None):
    """Return a textual column clause, as would be in the columns
    clause of a ``SELECT`` statement.
  
    The object returned is an instance of [sqlalchemy.sql#_ColumnClause],
    which represents the "syntactical" portion of the schema-level
    [sqlalchemy.schema#Column] object.
    
  
      text
        the name of the column.  Quoting rules will not be applied 
        to the column.   For textual column
        constructs that should be quoted like any other column 
        construct, use the [sqlalchemy.sql#column()]
        function.
      
      type
        an optional [sqlalchemy.types#TypeEngine] object which will provide
        result-set translation for this column.
      
    """

    return _ColumnClause(text, type=type, is_literal=True)

def table(name, *columns):
    """Return a [sqlalchemy.sql#Table] object.

    This is a primitive version of the [sqlalchemy.schema#Table] object, which
    is a subclass of this object.
    """

    return TableClause(name, *columns)

def bindparam(key, value=None, type=None, shortname=None, unique=False):
    """Create a bind parameter clause with the given key.

        value
         a default value for this bind parameter.  a bindparam with a value
         is called a ``value-based bindparam``.

        shortname
         an ``alias`` for this bind parameter.  usually used to alias the ``key`` and 
         ``label`` of a column, i.e. ``somecolname`` and ``sometable_somecolname``

        type
         a sqlalchemy.types.TypeEngine object indicating the type of this bind param, will
         invoke type-specific bind parameter processing

        unique
         if True, bind params sharing the same name will have their underlying ``key`` modified
         to a uniquely generated name.  mostly useful with value-based bind params.
       
    """

    if isinstance(key, _ColumnClause):
        return _BindParamClause(key.name, value, type=key.type, shortname=shortname, unique=unique)
    else:
        return _BindParamClause(key, value, type=type, shortname=shortname, unique=unique)

def text(text, bind=None, engine=None, *args, **kwargs):
    """Create literal text to be inserted into a query.

    When constructing a query from a ``select()``, ``update()``,
    ``insert()`` or ``delete()``, using plain strings for argument
    values will usually result in text objects being created
    automatically.  Use this function when creating textual clauses
    outside of other ``ClauseElement`` objects, or optionally wherever
    plain text is to be used.

      text
        The text of the SQL statement to be created.  use ``:<param>``
        to specify bind parameters; they will be compiled to their
        engine-specific format.

      bind
        An optional connection or engine to be used for this text query.
        
      engine
        deprecated.  a synonym for 'bind'.

      bindparams
        A list of ``bindparam()`` instances which can be used to define
        the types and/or initial values for the bind parameters within
        the textual statement; the keynames of the bindparams must match
        those within the text of the statement.  The types will be used
        for pre-processing on bind values.

      typemap
        A dictionary mapping the names of columns represented in the
        ``SELECT`` clause of the textual statement to type objects,
        which will be used to perform post-processing on columns within
        the result set (for textual statements that produce result
        sets).

    """

    return _TextClause(text, engine=engine, bind=bind, *args, **kwargs)

def null():
    """Return a ``_Null`` object, which compiles to ``NULL`` in a sql statement."""

    return _Null()

class _FunctionGenerator(object):
    """Generate ``_Function`` objects based on getattr calls."""

    def __init__(self, **opts):
        self.__names = []
        self.opts = opts

    def __getattr__(self, name):
        if name[-1] == '_':
            name = name[0:-1]
        f = _FunctionGenerator(**self.opts)
        f.__names = list(self.__names) + [name]
        return f

    def __call__(self, *c, **kwargs):
        o = self.opts.copy()
        o.update(kwargs)
        return _Function(self.__names[-1], packagenames=self.__names[0:-1], *c, **o)

func = _FunctionGenerator()

# TODO: use UnaryExpression for this instead ?
modifier = _FunctionGenerator(group=False)

    
def _compound_select(keyword, *selects, **kwargs):
    return CompoundSelect(keyword, *selects, **kwargs)

def _is_literal(element):
    return not isinstance(element, ClauseElement)

def _literals_as_text(element):
    if _is_literal(element):
        return _TextClause(unicode(element))
    else:
        return element

def _literals_as_binds(element, name='literal', type=None):
    if _is_literal(element):
        if element is None:
            return null()
        else:
            return _BindParamClause(name, element, shortname=name, type=type, unique=True)
    else:
        return element
        
def is_column(col):
    return isinstance(col, ColumnElement)

class AbstractDialect(object):
    """Represent the behavior of a particular database.

    Used by ``Compiled`` objects."""
    pass

class ClauseParameters(object):
    """Represent a dictionary/iterator of bind parameter key names/values.

    Tracks the original [sqlalchemy.sql#_BindParamClause] objects as well as the
    keys/position of each parameter, and can return parameters as a
    dictionary or a list.  Will process parameter values according to
    the ``TypeEngine`` objects present in the ``_BindParamClause`` instances.
    """

    def __init__(self, dialect, positional=None):
        super(ClauseParameters, self).__init__()
        self.dialect = dialect
        self.binds = {}
        self.binds_to_names = {}
        self.binds_to_values = {}
        self.positional = positional or []

    def set_parameter(self, bindparam, value, name):
        self.binds[bindparam.key] = bindparam
        self.binds[name] = bindparam
        self.binds_to_names[bindparam] = name
        self.binds_to_values[bindparam] = value
        
    def get_original(self, key):
        """Return the given parameter as it was originally placed in
        this ``ClauseParameters`` object, without any ``Type``
        conversion."""
        return self.binds_to_values[self.binds[key]]

    def get_processed(self, key):
        bind = self.binds[key]
        value = self.binds_to_values[bind]
        return bind.typeprocess(value, self.dialect)
   
    def keys(self):
        return self.binds_to_names.values()
 
    def __getitem__(self, key):
        return self.get_processed(key)
        
    def __contains__(self, key):
        return key in self.binds
    
    def set_value(self, key, value):
        bind = self.binds[key]
        self.binds_to_values[bind] = value
            
    def get_original_dict(self):
        return dict([(self.binds_to_names[b], self.binds_to_values[b]) for b in self.binds_to_names.keys()])

    def get_raw_list(self):
        return [self.get_processed(key) for key in self.positional]

    def get_raw_dict(self):
        d = {}
        for k in self.binds_to_names.values():
            d[k] = self.get_processed(k)
        return d

    def __repr__(self):
        return self.__class__.__name__ + ":" + repr(self.get_original_dict())

class ClauseVisitor(object):
    """A class that knows how to traverse and visit
    ``ClauseElements``.
    
    Each ``ClauseElement``'s accept_visitor() method will call a
    corresponding visit_XXXX() method here. Traversal of a
    hierarchy of ``ClauseElements`` is achieved via the
    ``traverse()`` method, which is passed the lead
    ``ClauseElement``.
    
    By default, ``ClauseVisitor`` traverses all elements
    fully.  Options can be specified at the class level via the 
    ``__traverse_options__`` dictionary which will be passed
    to the ``get_children()`` method of each ``ClauseElement``;
    these options can indicate modifications to the set of 
    elements returned, such as to not return column collections
    (column_collections=False) or to return Schema-level items
    (schema_visitor=True)."""
    __traverse_options__ = {}
    def traverse(self, obj, stop_on=None):
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            if stop_on is None or t not in stop_on:
                traversal.insert(0, t)
                for c in t.get_children(**self.__traverse_options__):
                    stack.append(c)
        for target in traversal:
            v = self
            while v is not None:
                target.accept_visitor(v)
                v = getattr(v, '_next', None)
        return obj
        
    def chain(self, visitor):
        """'chain' an additional ClauseVisitor onto this ClauseVisitor.
        
        the chained visitor will receive all visit events after this one."""
        tail = self
        while getattr(tail, '_next', None) is not None:
            tail = tail._next
        tail._next = visitor
        return self
        
    def visit_column(self, column):
        pass
    def visit_table(self, table):
        pass
    def visit_fromclause(self, fromclause):
        pass
    def visit_bindparam(self, bindparam):
        pass
    def visit_textclause(self, textclause):
        pass
    def visit_compound(self, compound):
        pass
    def visit_compound_select(self, compound):
        pass
    def visit_binary(self, binary):
        pass
    def visit_unary(self, unary):
        pass
    def visit_alias(self, alias):
        pass
    def visit_select(self, select):
        pass
    def visit_join(self, join):
        pass
    def visit_null(self, null):
        pass
    def visit_clauselist(self, list):
        pass
    def visit_calculatedclause(self, calcclause):
        pass
    def visit_grouping(self, gr):
        pass
    def visit_function(self, func):
        pass
    def visit_cast(self, cast):
        pass
    def visit_label(self, label):
        pass
    def visit_typeclause(self, typeclause):
        pass

class LoggingClauseVisitor(ClauseVisitor):
    """extends ClauseVisitor to include debug logging of all traversal.
    
    To install this visitor, set logging.DEBUG for 
    'sqlalchemy.sql.ClauseVisitor' **before** you import the 
    sqlalchemy.sql module.
    """

    def traverse(self, obj, stop_on=None):
        stack = [(obj, "")]
        traversal = []
        while len(stack) > 0:
            (t, indent) = stack.pop()
            if stop_on is None or t not in stop_on:
                traversal.insert(0, (t, indent))
                for c in t.get_children(**self.__traverse_options__):
                    stack.append((c, indent + "    "))
        
        for (target, indent) in traversal:
            self.logger.debug(indent + repr(target))
            v = self
            while v is not None:
                target.accept_visitor(v)
                v = getattr(v, '_next', None)
        return obj

LoggingClauseVisitor.logger = logging.class_logger(ClauseVisitor)

if logging.is_debug_enabled(LoggingClauseVisitor.logger):
    ClauseVisitor=LoggingClauseVisitor

class NoColumnVisitor(ClauseVisitor):
    """a ClauseVisitor that will not traverse the exported Column 
    collections on Table, Alias, Select, and CompoundSelect objects
    (i.e. their 'columns' or 'c' attribute).
    
    this is useful because most traversals don't need those columns, or
    in the case of ANSICompiler it traverses them explicitly; so
    skipping their traversal here greatly cuts down on method call overhead.
    """
    
    __traverse_options__ = {'column_collections':False}
    
class Executor(object):
    """Interface representing a "thing that can produce Compiled objects 
    and execute them"."""

    def execute_compiled(self, compiled, parameters, echo=None, **kwargs):
        """Execute a Compiled object."""

        raise NotImplementedError()

    def compiler(self, statement, parameters, **kwargs):
        """Return a Compiled object for the given statement and parameters."""

        raise NotImplementedError()

class Compiled(ClauseVisitor):
    """Represent a compiled SQL expression.

    The ``__str__`` method of the ``Compiled`` object should produce
    the actual text of the statement.  ``Compiled`` objects are
    specific to their underlying database dialect, and also may
    or may not be specific to the columns referenced within a
    particular set of bind parameters.  In no case should the
    ``Compiled`` object be dependent on the actual values of those
    bind parameters, even though it may reference those values as
    defaults.
    """

    def __init__(self, dialect, statement, parameters, bind=None, engine=None):
        """Construct a new ``Compiled`` object.

        statement
          ``ClauseElement`` to be compiled.

        parameters
          Optional dictionary indicating a set of bind parameters
          specified with this ``Compiled`` object.  These parameters
          are the *default* values corresponding to the
          ``ClauseElement``'s ``_BindParamClauses`` when the
          ``Compiled`` is executed.  In the case of an ``INSERT`` or
          ``UPDATE`` statement, these parameters will also result in
          the creation of new ``_BindParamClause`` objects for each
          key and will also affect the generated column list in an
          ``INSERT`` statement and the ``SET`` clauses of an
          ``UPDATE`` statement.  The keys of the parameter dictionary
          can either be the string names of columns or
          ``_ColumnClause`` objects.

        bind
          optional engine or connection which will be bound to the 
          compiled object.
          
        engine
          deprecated, a synonym for 'bind'
        """
        self.dialect = dialect
        self.statement = statement
        self.parameters = parameters
        self.bind = bind or engine
        self.can_execute = statement.supports_execution()

    def compile(self):
        self.traverse(self.statement)
        self.after_compile()

    def __str__(self):
        """Return the string text of the generated SQL statement."""

        raise NotImplementedError()

    def get_params(self, **params):
        """Deprecated.  use construct_params().  (supports unicode names)
        """

        return self.construct_params(params)

    def construct_params(self, params):
        """Return the bind params for this compiled object.

        Will start with the default parameters specified when this
        ``Compiled`` object was first constructed, and will override
        those values with those sent via `**params`, which are
        key/value pairs.  Each key should match one of the
        ``_BindParamClause`` objects compiled into this object; either
        the `key` or `shortname` property of the ``_BindParamClause``.
        """
        raise NotImplementedError()
        
    def execute(self, *multiparams, **params):
        """Execute this compiled object."""

        e = self.bind
        if e is None:
            raise exceptions.InvalidRequestError("This Compiled object is not bound to any Engine or Connection.")
        return e.execute_compiled(self, *multiparams, **params)

    def scalar(self, *multiparams, **params):
        """Execute this compiled object and return the result's scalar value."""

        return self.execute(*multiparams, **params).scalar()

class ClauseElement(object):
    """Base class for elements of a programmatically constructed SQL
    expression.
    """

    def _get_from_objects(self):
        """Return objects represented in this ``ClauseElement`` that
        should be added to the ``FROM`` list of a query, when this
        ``ClauseElement`` is placed in the column clause of a
        ``Select`` statement.
        """

        raise NotImplementedError(repr(self))

    def _hide_froms(self):
        """Return a list of ``FROM`` clause elements which this
        ``ClauseElement`` replaces.
        """

        return []

    def compare(self, other):
        """Compare this ClauseElement to the given ClauseElement.

        Subclasses should override the default behavior, which is a
        straight identity comparison.
        """

        return self is other

    def accept_visitor(self, visitor):
        """Accept a ``ClauseVisitor`` and call the appropriate
        ``visit_xxx`` method.
        """

        raise NotImplementedError(repr(self))
    
    def get_children(self, **kwargs):
        """return immediate child elements of this ``ClauseElement``.
        
        this is used for visit traversal.
        
        \**kwargs may contain flags that change the collection
        that is returned, for example to return a subset of items
        in order to cut down on larger traversals, or to return 
        child items from a different context (such as schema-level
        collections instead of clause-level)."""
        return []
    
    def self_group(self, against=None):
        return self

    def supports_execution(self):
        """Return True if this clause element represents a complete
        executable statement.
        """

        return False

    def copy_container(self):
        """Return a copy of this ``ClauseElement``, if this
        ``ClauseElement`` contains other ``ClauseElements``.

        If this ``ClauseElement`` is not a container, it should return
        self.  This is used to create copies of expression trees that
        still reference the same *leaf nodes*.  The new structure can
        then be restructured without affecting the original.
        """

        return self

    def _find_engine(self):
        """Default strategy for locating an engine within the clause element.

        Relies upon a local engine property, or looks in the *from*
        objects which ultimately have to contain Tables or
        TableClauses.
        """

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
    
    bind = property(lambda s:s._find_engine(), doc="""Returns the Engine or Connection to which this ClauseElement is bound, or None if none found.""")
    engine = bind

    def execute(self, *multiparams, **params):
        """Compile and execute this ``ClauseElement``."""

        if len(multiparams):
            compile_params = multiparams[0]
        else:
            compile_params = params
        return self.compile(bind=self.bind, parameters=compile_params).execute(*multiparams, **params)

    def scalar(self, *multiparams, **params):
        """Compile and execute this ``ClauseElement``, returning the
        result's scalar representation.
        """

        return self.execute(*multiparams, **params).scalar()

    def compile(self, bind=None, engine=None, parameters=None, compiler=None, dialect=None):
        """Compile this SQL expression.

        Uses the given ``Compiler``, or the given ``AbstractDialect``
        or ``Engine`` to create a ``Compiler``.  If no `compiler`
        arguments are given, tries to use the underlying ``Engine`` this
        ``ClauseElement`` is bound to to create a ``Compiler``, if any.

        Finally, if there is no bound ``Engine``, uses an
        ``ANSIDialect`` to create a default ``Compiler``.

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

        if (isinstance(parameters, list) or isinstance(parameters, tuple)):
            parameters = parameters[0]

        if compiler is None:
            if dialect is not None:
                compiler = dialect.compiler(self, parameters)
            elif bind is not None:
                compiler = bind.compiler(self, parameters)
            elif engine is not None:
                compiler = engine.compiler(self, parameters)
            elif self.bind is not None:
                compiler = self.bind.compiler(self, parameters)

        if compiler is None:
            import sqlalchemy.ansisql as ansisql
            compiler = ansisql.ANSIDialect().compiler(self, parameters=parameters)
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
        return _UnaryExpression(self.self_group(against="NOT"), operator="NOT", negate=None)

class _CompareMixin(object):
    """Defines comparison operations for ``ClauseElement`` instances.
    
    This is a mixin class that adds the capability to produce ``ClauseElement``
    instances based on regular Python operators.  
    These operations are achieved using Python's operator overload methods
    (i.e. ``__eq__()``, ``__ne__()``, etc.
    
    Overridden operators include all comparison operators (i.e. '==', '!=', '<'),
    math operators ('+', '-', '*', etc), the '&' and '|' operators which evaluate
    to ``AND`` and ``OR`` respectively. 

    Other methods exist to create additional SQL clauses such as ``IN``, ``LIKE``, 
    ``DISTINCT``, etc.
    
    """

    def __lt__(self, other):
        return self._compare('<', other)

    def __le__(self, other):
        return self._compare('<=', other)

    def __eq__(self, other):
        return self._compare('=', other)

    def __ne__(self, other):
        return self._compare('!=', other)

    def __gt__(self, other):
        return self._compare('>', other)

    def __ge__(self, other):
        return self._compare('>=', other)

    def like(self, other):
        """produce a ``LIKE`` clause."""
        return self._compare('LIKE', other)

    def in_(self, *other):
        """produce an ``IN`` clause."""
        if len(other) == 0:
            return _Grouping(case([(self.__eq__(None), text('NULL'))], else_=text('0')).__eq__(text('1')))
        elif len(other) == 1:
            o = other[0]
            if _is_literal(o) or isinstance( o, _CompareMixin):
                return self.__eq__( o)    #single item -> ==
            else:
                assert hasattr( o, '_selectable')   #better check?
                return self._compare( 'IN', o, negate='NOT IN')   #single selectable

        args = []
        for o in other:
            if not _is_literal(o):
                if not isinstance( o, _CompareMixin):
                    raise exceptions.InvalidRequestError( "in() function accepts either non-selectable values, or a single selectable: "+repr(o) )
            else:
                o = self._bind_param(o)
            args.append(o)
        return self._compare( 'IN', ClauseList(*args).self_group(against='IN'), negate='NOT IN')

    def startswith(self, other):
        """produce the clause ``LIKE '<other>%'``"""
        perc = isinstance(other,(str,unicode)) and '%' or literal('%',type= sqltypes.String)
        return self._compare('LIKE', other + perc)

    def endswith(self, other):
        """produce the clause ``LIKE '%<other>'``"""
        if isinstance(other,(str,unicode)): po = '%' + other
        else:
            po = literal('%', type= sqltypes.String) + other
            po.type = sqltypes.to_instance( sqltypes.String)     #force!
        return self._compare('LIKE', po)

    def __radd__(self, other):
        return self._bind_param(other)._operate('+', self)
    def __rsub__(self, other):
        return self._bind_param(other)._operate('-', self)
    def __rmul__(self, other):
        return self._bind_param(other)._operate('*', self)
    def __rdiv__(self, other):
        return self._bind_param(other)._operate('/', self)

    def label(self, name):
        """produce a column label, i.e. ``<columnname> AS <name>``"""
        return _Label(name, self, self.type)

    def distinct(self):
        """produce a DISTINCT clause, i.e. ``DISTINCT <columnname>``"""
        return _UnaryExpression(self, operator="DISTINCT")

    def between(self, cleft, cright):
        """produce a BETWEEN clause, i.e. ``<column> BETWEEN <cleft> AND <cright>``"""
        return _BinaryExpression(self, ClauseList(self._check_literal(cleft), self._check_literal(cright), operator='AND', group=False), 'BETWEEN')

    def op(self, operator):
        """produce a generic operator function.
        
        e.g.
        
            somecolumn.op("*")(5)
            
        produces
        
            somecolumn * 5
            
        operator
            a string which will be output as the infix operator 
            between this ``ClauseElement`` and the expression 
            passed to the generated function.
            
        """
        return lambda other: self._operate(operator, other)

    # and here come the math operators:

    def __add__(self, other):
        return self._operate('+', other)

    def __sub__(self, other):
        return self._operate('-', other)

    def __mul__(self, other):
        return self._operate('*', other)

    def __div__(self, other):
        return self._operate('/', other)

    def __mod__(self, other):
        return self._operate('%', other)

    def __truediv__(self, other):
        return self._operate('/', other)

    def _bind_param(self, obj):
        return _BindParamClause('literal', obj, shortname=None, type=self.type, unique=True)

    def _check_literal(self, other):
        if _is_literal(other):
            return self._bind_param(other)
        else:
            return other

    def _compare(self, operator, obj, negate=None):
        if obj is None or isinstance(obj, _Null):
            if operator == '=':
                return _BinaryExpression(self._compare_self(), null(), 'IS', negate='IS NOT')
            elif operator == '!=':
                return _BinaryExpression(self._compare_self(), null(), 'IS NOT', negate='IS')
            else:
                raise exceptions.ArgumentError("Only '='/'!=' operators can be used with NULL")
        else:
            obj = self._check_literal(obj)

        return _BinaryExpression(self._compare_self(), obj, operator, type=self._compare_type(obj), negate=negate)

    def _operate(self, operator, obj):
        if _is_literal(obj):
            obj = self._bind_param(obj)
        return _BinaryExpression(self._compare_self(), obj, operator, type=self._compare_type(obj))

    def _compare_self(self):
        """Allow ``ColumnImpl`` to return its ``Column`` object for
        usage in ``ClauseElements``, all others to just return self.
        """

        return self

    def _compare_type(self, obj):
        """Allow subclasses to override the type used in constructing
        ``_BinaryExpression`` objects.

        Default return value is the type of the given object.
        """

        return obj.type

class Selectable(ClauseElement):
    """Represent a column list-holding object.
    
    this is the common base class of [sqlalchemy.sql#ColumnElement]
    and [sqlalchemy.sql#FromClause].  The reason ``ColumnElement``
    is marked as a "list-holding" object is so that it can be treated
    similarly to ``FromClause`` in column-selection scenarios; it 
    contains a list of columns consisting of itself.
    
    """

    columns = util.NotImplProperty("""a [sqlalchemy.sql#ColumnCollection] containing ``ColumnElement`` instances.""")

    def _selectable(self):
        return self

    def accept_visitor(self, visitor):
        raise NotImplementedError(repr(self))

    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def _group_parenthesized(self):
        """Indicate if this ``Selectable`` requires parenthesis when
        grouped into a compound statement.
        """

        return True

        
class ColumnElement(Selectable, _CompareMixin):
    """Represent an element that is useable within the 
    "column clause" portion of a ``SELECT`` statement. 
    
    This includes columns associated with tables, aliases,
    and subqueries, expressions, function calls, SQL keywords
    such as ``NULL``, literals, etc.  ``ColumnElement`` is the 
    ultimate base class for all such elements.

    ``ColumnElement`` supports the ability to be a *proxy* element,
    which indicates that the ``ColumnElement`` may be associated with
    a ``Selectable`` which was derived from another ``Selectable``. 
    An example of a "derived" ``Selectable`` is an ``Alias`` of 
    a ``Table``.
    
    a ``ColumnElement``, by subclassing the ``_CompareMixin`` mixin 
    class, provides the ability to generate new ``ClauseElement`` 
    objects using Python expressions.  See the ``_CompareMixin`` 
    docstring for more details.
    """

    primary_key = property(lambda self:getattr(self, '_primary_key', False),
                           doc=\
        """Primary key flag.  Indicates if this ``Column`` represents part or 
        whole of a primary key for its parent table.
        """)
    foreign_keys = property(lambda self:getattr(self, '_foreign_keys', []),
                            doc=\
        """Foreign key accessor.  References a list of ``ForeignKey`` objects 
        which each represent a foreign key placed on this column's ultimate
        ancestor.
        """)
    columns = property(lambda self:[self],
                       doc=\
        """Columns accessor which returns ``self``, to provide compatibility 
        with ``Selectable`` objects.
        """)

    def _one_fkey(self):
        if len(self._foreign_keys):
            return list(self._foreign_keys)[0]
        else:
            return None

    foreign_key = property(_one_fkey)

    def _get_orig_set(self):
        try:
            return self.__orig_set
        except AttributeError:
            self.__orig_set = util.Set([self])
            return self.__orig_set

    def _set_orig_set(self, s):
        if len(s) == 0:
            s.add(self)
        self.__orig_set = s
        
    orig_set = property(_get_orig_set, _set_orig_set,
                        doc=\
        """A Set containing TableClause-bound, non-proxied ColumnElements 
        for which this ColumnElement is a proxy.  In all cases except 
        for a column proxied from a Union (i.e. CompoundSelect), this 
        set will be just one element.
        """)
    
    def shares_lineage(self, othercolumn):
        """Return True if the given ``ColumnElement`` has a common
        ancestor to this ``ColumnElement``.
        """

        for c in self.orig_set:
            if c in othercolumn.orig_set:
                return True
        else:
            return False
    
    def _make_proxy(self, selectable, name=None):
        """Create a new ``ColumnElement`` representing this
        ``ColumnElement`` as it appears in the select list of a
        descending selectable.

        The default implementation returns a ``_ColumnClause`` if a
        name is given, else just returns self.
        """

        if name is not None:
            co = _ColumnClause(name, selectable)
            co.orig_set = self.orig_set
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
        
    def add(self, column):
        """Add a column to this collection.

        The key attribute of the column will be used as the hash key
        for this dictionary.
        """

        # Allow an aliased column to replace an unaliased column of the
        # same name.
        if self.has_key(column.name):
            other = self[column.name]
            if other.name == other.key:
                del self[other.name]
        self[column.key] = column
    
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

    def __contains__(self, col):
        return self.contains_column(col)
        
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
            
class FromClause(Selectable):
    """Represent an element that can be used within the ``FROM``
    clause of a ``SELECT`` statement.
    """

    def __init__(self, name=None):
        self.name = name

    def _get_from_objects(self):
        # this could also be [self], at the moment it doesnt matter to the Select object
        return []

    def default_order_by(self):
        return [self.oid_column]

    def accept_visitor(self, visitor):
        visitor.visit_fromclause(self)

    def count(self, whereclause=None, **params):
        if len(self.primary_key):
            col = list(self.primary_key)[0]
        else:
            col = list(self.columns)[0]
        return select([func.count(col).label('tbl_row_count')], whereclause, from_obj=[self], **params)

    def join(self, right, *args, **kwargs):
        return Join(self, right, *args, **kwargs)

    def outerjoin(self, right, *args, **kwargs):
        return Join(self, right, isouter=True, *args, **kwargs)

    def alias(self, name=None):
        return Alias(self, name)

    def named_with_column(self):
        """True if the name of this FromClause may be prepended to a
        column in a generated SQL statement.
        """

        return False

    def _locate_oid_column(self):
        """Subclasses should override this to return an appropriate OID column."""

        return None

    def _get_oid_column(self):
        if not hasattr(self, '_oid_column'):
            self._oid_column = self._locate_oid_column()
        return self._oid_column

    def _get_all_embedded_columns(self):
        ret = []
        class FindCols(ClauseVisitor):
            def visit_column(self, col):
                ret.append(col)
        FindCols().traverse(self)
        return ret

    def corresponding_column(self, column, raiseerr=True, keys_ok=False, require_embedded=False):
        """Given a ``ColumnElement``, return the exported
        ``ColumnElement`` object from this ``Selectable`` which
        corresponds to that original ``Column`` via a common
        anscestor column.
        
        column
          the target ``ColumnElement`` to be matched
            
        raiseerr
          if True, raise an error if the given ``ColumnElement``
          could not be matched. if False, non-matches will
          return None.
            
        keys_ok
          if the ``ColumnElement`` cannot be matched, attempt to
          match based on the string "key" property of the column
          alone. This makes the search much more liberal.
            
        require_embedded
          only return corresponding columns for the given
          ``ColumnElement``, if the given ``ColumnElement`` is
          actually present within a sub-element of this
          ``FromClause``.  Normally the column will match if
          it merely shares a common anscestor with one of
          the exported columns of this ``FromClause``.
        """
        if column in self.c:
            return column
        
        if require_embedded and column not in util.Set(self._get_all_embedded_columns()):
            if not raiseerr:
                return None
            else:
                raise exceptions.InvalidRequestError("Column instance '%s' is not directly present within selectable '%s'" % (str(column), column.table))
        for c in column.orig_set:
            try:
                return self.original_columns[c]
            except KeyError:
                pass
        else:
            if keys_ok:
                try:
                    return self.c[column.name]
                except KeyError:
                    pass
            if not raiseerr:
                return None
            else:
                raise exceptions.InvalidRequestError("Given column '%s', attached to table '%s', failed to locate a corresponding column from table '%s'" % (str(column), str(getattr(column, 'table', None)), self.name))

    def _get_exported_attribute(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            self._export_columns()
            return getattr(self, name)

    columns = property(lambda s:s._get_exported_attribute('_columns'))
    c = property(lambda s:s._get_exported_attribute('_columns'))
    primary_key = property(lambda s:s._get_exported_attribute('_primary_key'))
    foreign_keys = property(lambda s:s._get_exported_attribute('_foreign_keys'))
    original_columns = property(lambda s:s._get_exported_attribute('_orig_cols'), doc=\
        """A dictionary mapping an original Table-bound 
        column to a proxied column in this FromClause.
        """)
    oid_column = property(_get_oid_column)

    def _export_columns(self, columns=None):
        """Initialize column collections.

        The collections include the primary key, foreign keys, list of
        all columns, as well as the *_orig_cols* collection which is a
        dictionary used to match Table-bound columns to proxied
        columns in this ``FromClause``.  The columns in each
        collection are *proxied* from the columns returned by the
        _exportable_columns method, where a *proxied* column maintains
        most or all of the properties of its original column, except
        its parent ``Selectable`` is this ``FromClause``.
        """

        if hasattr(self, '_columns') and columns is None:
            # TODO: put a mutex here ?  this is a key place for threading probs
            return
        self._columns = ColumnCollection()
        self._primary_key = ColumnSet()
        self._foreign_keys = util.Set()
        self._orig_cols = {}
        if columns is None:
            columns = self._adjusted_exportable_columns()
        for co in columns:
            cp = self._proxy_column(co)
            for ci in cp.orig_set:
                cx = self._orig_cols.get(ci)
                # TODO: the '=' thing here relates to the order of columns as they are placed in the
                # "columns" collection of a CompositeSelect, illustrated in test/sql/selectable.SelectableTest.testunion
                # make this relationship less brittle
                if cx is None or cp._distance <= cx._distance:
                    self._orig_cols[ci] = cp
        if self.oid_column is not None:
            for ci in self.oid_column.orig_set:
                self._orig_cols[ci] = self.oid_column
    
    def _adjusted_exportable_columns(self):
        """return the list of ColumnElements represented within this FromClause's _exportable_columns"""
        export = self._exportable_columns()
        for column in export:
            try:
                s = column._selectable()
            except AttributeError:
                continue
            for co in s.columns:
                yield co
        
    def _exportable_columns(self):
        return []

    def _proxy_column(self, column):
        return column._make_proxy(self)

class _BindParamClause(ClauseElement, _CompareMixin):
    """Represent a bind parameter.

    Public constructor is the ``bindparam()`` function.
    """

    def __init__(self, key, value, shortname=None, type=None, unique=False):
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
          Defaults to the key, a *short name* that will also identify
          this bind parameter, similar to an alias.  the bind
          parameter keys sent to a statement compilation or compiled
          execution may match either the key or the shortname of the
          corresponding ``_BindParamClause`` objects.

        type
          A ``TypeEngine`` object that will be used to pre-process the
          value corresponding to this ``_BindParamClause`` at
          execution time.

        unique
          if True, the key name of this BindParamClause will be 
          modified if another ``_BindParamClause`` of the same
          name already has been located within the containing 
          ``ClauseElement``.
        """

        self.key = key
        self.value = value
        self.shortname = shortname or key
        self.unique = unique
        self.type = sqltypes.to_instance(type)

    def accept_visitor(self, visitor):
        visitor.visit_bindparam(self)

    def _get_from_objects(self):
        return []

    def copy_container(self):
        return _BindParamClause(self.key, self.value, self.shortname, self.type, unique=self.unique)

    def typeprocess(self, value, dialect):
        return self.type.dialect_impl(dialect).convert_bind_param(value, dialect)

    def compare(self, other):
        """Compare this ``_BindParamClause`` to the given clause.

        Since ``compare()`` is meant to compare statement syntax, this
        method returns True if the two ``_BindParamClauses`` have just
        the same type.
        """

        return isinstance(other, _BindParamClause) and other.type.__class__ == self.type.__class__

    def __repr__(self):
        return "_BindParamClause(%s, %s, type=%s)" % (repr(self.key), repr(self.value), repr(self.type))

class _TypeClause(ClauseElement):
    """Handle a type keyword in a SQL statement.

    Used by the ``Case`` statement.
    """

    def __init__(self, type):
        self.type = type

    def accept_visitor(self, visitor):
        visitor.visit_typeclause(self)

    def _get_from_objects(self):
        return []

class _TextClause(ClauseElement):
    """Represent a literal SQL text fragment.

    Public constructor is the ``text()`` function.
    """

    def __init__(self, text = "", bind=None, engine=None, bindparams=None, typemap=None):
        self._bind = bind or engine
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
        self.text = re.compile(r'(?<!:):([\w_]+)', re.S).sub(repl, text)
        if bindparams is not None:
            for b in bindparams:
                self.bindparams[b.key] = b

    def _get_type(self):
        if self.typemap is not None and len(self.typemap) == 1:
            return list(self.typemap)[0]
        else:
            return None
    type = property(_get_type)

    columns = property(lambda s:[])

    def get_children(self, **kwargs):
        return self.bindparams.values()

    def accept_visitor(self, visitor):
        visitor.visit_textclause(self)

    def _get_from_objects(self):
        return []

    def supports_execution(self):
        return True

class _Null(ColumnElement):
    """Represent the NULL keyword in a SQL statement.

    Public constructor is the ``null()`` function.
    """

    def __init__(self):
        self.type = sqltypes.NULLTYPE

    def accept_visitor(self, visitor):
        visitor.visit_null(self)

    def _get_from_objects(self):
        return []

class ClauseList(ClauseElement):
    """Describe a list of clauses, separated by an operator.

    By default, is comma-separated, such as a column listing.
    """

    def __init__(self, *clauses, **kwargs):
        self.clauses = []
        self.operator = kwargs.pop('operator', ',')
        self.group = kwargs.pop('group', True)
        self.group_contents = kwargs.pop('group_contents', True)
        for c in clauses:
            if c is None: continue
            self.append(c)

    def __iter__(self):
        return iter(self.clauses)
    def __len__(self):
        return len(self.clauses)
        
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return ClauseList(operator=self.operator, *clauses)

    def append(self, clause):
        # TODO: not sure if i like the 'group_contents' flag.  need to define the difference between
        # a ClauseList of ClauseLists, and a "flattened" ClauseList of ClauseLists.  flatten() method ?
        if self.group_contents:
            self.clauses.append(_literals_as_text(clause).self_group(against=self.operator))
        else:
            self.clauses.append(_literals_as_text(clause))

    def get_children(self, **kwargs):
        return self.clauses

    def accept_visitor(self, visitor):
        visitor.visit_clauselist(self)

    def _get_from_objects(self):
        f = []
        for c in self.clauses:
            f += c._get_from_objects()
        return f

    def self_group(self, against=None):
        if self.group and self.operator != against and PRECEDENCE.get(self.operator, PRECEDENCE['_smallest']) <= PRECEDENCE.get(against, PRECEDENCE['_largest']):
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

    def __init__(self, name, *clauses, **kwargs):
        self.name = name
        self.type = sqltypes.to_instance(kwargs.get('type', None))
        self._bind = kwargs.get('bind', kwargs.get('engine', None))
        self.group = kwargs.pop('group', True)
        self.clauses = ClauseList(operator=kwargs.get('operator', None), group_contents=kwargs.get('group_contents', True), *clauses)
        if self.group:
            self.clause_expr = self.clauses.self_group()
        else:
            self.clause_expr = self.clauses
            
    key = property(lambda self:self.name or "_calc_")

    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return _CalculatedClause(type=self.type, bind=self._bind, *clauses)

    def get_children(self, **kwargs):
        return self.clause_expr,
        
    def accept_visitor(self, visitor):
        visitor.visit_calculatedclause(self)
    def _get_from_objects(self):
        return self.clauses._get_from_objects()

    def _bind_param(self, obj):
        return _BindParamClause(self.name, obj, type=self.type, unique=True)

    def select(self):
        return select([self])

    def scalar(self):
        return select([self]).scalar()

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
        self.type = sqltypes.to_instance(kwargs.get('type', None))
        self.packagenames = kwargs.get('packagenames', None) or []
        kwargs['operator'] = ','
        self._engine = kwargs.get('engine', None)
        _CalculatedClause.__init__(self, name, **kwargs)
        for c in clauses:
            self.append(c)

    key = property(lambda self:self.name)


    def append(self, clause):
        self.clauses.append(_literals_as_binds(clause, self.name))

    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return _Function(self.name, type=self.type, packagenames=self.packagenames, bind=self._bind, *clauses)
        
    def accept_visitor(self, visitor):
        visitor.visit_function(self)

class _Cast(ColumnElement):
    def __init__(self, clause, totype, **kwargs):
        if not hasattr(clause, 'label'):
            clause = literal(clause)
        self.type = sqltypes.to_instance(totype)
        self.clause = clause
        self.typeclause = _TypeClause(self.type)
        self._distance = 0
        
    def get_children(self, **kwargs):
        return self.clause, self.typeclause
    def accept_visitor(self, visitor):
        visitor.visit_cast(self)

    def _get_from_objects(self):
        return self.clause._get_from_objects()

    def _make_proxy(self, selectable, name=None):
        if name is not None:
            co = _ColumnClause(name, selectable, type=self.type)
            co._distance = self._distance + 1
            co.orig_set = self.orig_set
            selectable.columns[name]= co
            return co
        else:
            return self


class _UnaryExpression(ColumnElement):
    def __init__(self, element, operator=None, modifier=None, type=None, negate=None):
        self.operator = operator
        self.modifier = modifier
        
        self.element = _literals_as_text(element).self_group(against=self.operator or self.modifier)
        self.type = sqltypes.to_instance(type)
        self.negate = negate
        
    def copy_container(self):
        return self.__class__(self.element.copy_container(), operator=self.operator, modifier=self.modifier, type=self.type, negate=self.negate)

    def _get_from_objects(self):
        return self.element._get_from_objects()

    def get_children(self, **kwargs):
        return self.element,

    def accept_visitor(self, visitor):
        visitor.visit_unary(self)

    def compare(self, other):
        """Compare this ``_UnaryExpression`` against the given ``ClauseElement``."""

        return (
            isinstance(other, _UnaryExpression) and self.operator == other.operator and
            self.modifier == other.modifier and 
            self.element.compare(other.element)
        )
    def _negate(self):
        if self.negate is not None:
            return _UnaryExpression(self.element, operator=self.negate, negate=self.operator, modifier=self.modifier, type=self.type)
        else:
            return super(_UnaryExpression, self)._negate()
    
    def self_group(self, against):
        if self.operator and PRECEDENCE.get(self.operator, PRECEDENCE['_smallest']) <= PRECEDENCE.get(against, PRECEDENCE['_largest']):
            return _Grouping(self)
        else:
            return self


class _BinaryExpression(ColumnElement):
    """Represent an expression that is ``LEFT <operator> RIGHT``."""
    
    def __init__(self, left, right, operator, type=None, negate=None):
        self.left = _literals_as_text(left).self_group(against=operator)
        self.right = _literals_as_text(right).self_group(against=operator)
        self.operator = operator
        self.type = sqltypes.to_instance(type)
        self.negate = negate

    def copy_container(self):
        return self.__class__(self.left.copy_container(), self.right.copy_container(), self.operator)

    def _get_from_objects(self):
        return self.left._get_from_objects() + self.right._get_from_objects()

    def get_children(self, **kwargs):
        return self.left, self.right

    def accept_visitor(self, visitor):
        visitor.visit_binary(self)

    def compare(self, other):
        """Compare this ``_BinaryExpression`` against the given ``_BinaryExpression``."""

        return (
            isinstance(other, _BinaryExpression) and self.operator == other.operator and
                (
                    self.left.compare(other.left) and self.right.compare(other.right)
                    or (
                        self.operator in ['=', '!=', '+', '*'] and
                        self.left.compare(other.right) and self.right.compare(other.left)
                    )
                )
        )
        
    def self_group(self, against=None):
        # use small/large defaults for comparison so that unknown operators are always parenthesized
        if self.operator != against and (PRECEDENCE.get(self.operator, PRECEDENCE['_smallest']) <= PRECEDENCE.get(against, PRECEDENCE['_largest'])):
            return _Grouping(self)
        else:
            return self
    
    def _negate(self):
        if self.negate is not None:
            return _BinaryExpression(self.left, self.right, self.negate, negate=self.operator, type=self.type)
        else:
            return super(_BinaryExpression, self)._negate()

class _Exists(_UnaryExpression):
    def __init__(self, *args, **kwargs):
        kwargs['correlate'] = True
        s = select(*args, **kwargs).self_group()
        _UnaryExpression.__init__(self, s, operator="EXISTS")

    def _hide_froms(self):
        return self._get_from_objects()

class Join(FromClause):
    """represent a ``JOIN`` construct between two ``FromClause``
    elements.
    
    the public constructor function for ``Join`` is the module-level
    ``join()`` function, as well as the ``join()`` method available
    off all ``FromClause`` subclasses.
    
    """
    def __init__(self, left, right, onclause=None, isouter = False):
        self.left = left._selectable()
        self.right = right._selectable()
        if onclause is None:
            self.onclause = self._match_primaries(self.left, self.right)
        else:
            self.onclause = onclause
        self.isouter = isouter
        self.__folded_equivalents = None
        self._init_primary_key()
        
    name = property(lambda s: "Join object on " + s.left.name + " " + s.right.name)
    encodedname = property(lambda s: s.name.encode('ascii', 'backslashreplace'))

    def _init_primary_key(self):
        pkcol = util.Set([c for c in self._adjusted_exportable_columns() if c.primary_key])
    
        equivs = {}
        def add_equiv(a, b):
            for x, y in ((a, b), (b, a)):
                if x in equivs:
                    equivs[x].add(y)
                else:
                    equivs[x] = util.Set([y])
                    
        class BinaryVisitor(ClauseVisitor):
            def visit_binary(self, binary):
                if binary.operator == '=':
                    add_equiv(binary.left, binary.right)
        BinaryVisitor().traverse(self.onclause)
        
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
            
        self.__primary_key = ColumnSet([c for c in self._adjusted_exportable_columns() if c.primary_key and c not in omit])

    primary_key = property(lambda s:s.__primary_key)
        
    def _locate_oid_column(self):
        return self.left.oid_column

    def _exportable_columns(self):
        return [c for c in self.left.columns] + [c for c in self.right.columns]

    def _proxy_column(self, column):
        self._columns[column._label] = column
        for f in column.foreign_keys:
            self._foreign_keys.add(f)
        return column

    def _match_primaries(self, primary, secondary):
        crit = []
        constraints = util.Set()
        for fk in secondary.foreign_keys:
            if fk.references(primary):
                crit.append(primary.corresponding_column(fk.column) == fk.parent)
                constraints.add(fk.constraint)
                self.foreignkey = fk.parent
        if primary is not secondary:
            for fk in primary.foreign_keys:
                if fk.references(secondary):
                    crit.append(secondary.corresponding_column(fk.column) == fk.parent)
                    constraints.add(fk.constraint)
                    self.foreignkey = fk.parent
        if len(crit) == 0:
            raise exceptions.ArgumentError("Can't find any foreign key relationships "
                                           "between '%s' and '%s'" % (primary.name, secondary.name))
        elif len(constraints) > 1:
            raise exceptions.ArgumentError("Can't determine join between '%s' and '%s'; "
                                           "tables have more than one foreign key "
                                           "constraint relationship between them. "
                                           "Please specify the 'onclause' of this "
                                           "join explicitly." % (primary.name, secondary.name))
        elif len(crit) == 1:
            return (crit[0])
        else:
            return and_(*crit)

    def _group_parenthesized(self):
        return True

    def _get_folded_equivalents(self, equivs=None):
        if self.__folded_equivalents is not None:
            return self.__folded_equivalents
        if equivs is None:
            equivs = util.Set()
        class LocateEquivs(NoColumnVisitor):
            def visit_binary(self, binary):
                if binary.operator == '=' and binary.left.name == binary.right.name:
                    equivs.add(binary.right)
                    equivs.add(binary.left)
        LocateEquivs().traverse(self.onclause)
        collist = []
        if isinstance(self.left, Join):
            left = self.left._get_folded_equivalents(equivs)
        else:
            left = list(self.left.columns)
        if isinstance(self.right, Join):
            right = self.right._get_folded_equivalents(equivs)
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

    folded_equivalents = property(_get_folded_equivalents, doc="Returns the column list of this Join with all equivalently-named, "
                                                            "equated columns folded into one column, where 'equated' means they are "
                                                            "equated to each other in the ON clause of this join.")    
    
    def select(self, whereclause = None, fold_equivalents=False, **kwargs):
        """Create a ``Select`` from this ``Join``.
        
        whereclause
          the WHERE criterion that will be sent to the ``select()`` function
          
        fold_equivalents
          based on the join criterion of this ``Join``, do not include repeat
          column names in the column list of the resulting select, for columns that
          are calculated to be "equivalent" based on the join criterion of this
          ``Join``. this will recursively apply to any joins directly nested by
          this one as well.
          
        \**kwargs
          all other kwargs are sent to the underlying ``select()`` function.
          See the ``select()`` module level function for details.
          
        """
        if fold_equivalents:
            collist = self.folded_equivalents
        else:
            collist = [self.left, self.right]
            
        return select(collist, whereclause, from_obj=[self], **kwargs)

    def get_children(self, **kwargs):
        return self.left, self.right, self.onclause

    def accept_visitor(self, visitor):
        visitor.visit_join(self)

    engine = property(lambda s:s.left.engine or s.right.engine)

    def alias(self, name=None):
        """Create a ``Select`` out of this ``Join`` clause and return an ``Alias`` of it.

        The ``Select`` is not correlating.
        """

        return self.select(use_labels=True, correlate=False).alias(name)

    def _hide_froms(self):
        return self.left._get_from_objects() + self.right._get_from_objects()

    def _get_from_objects(self):
        return [self] + self.onclause._get_from_objects() + self.left._get_from_objects() + self.right._get_from_objects()

class Alias(FromClause):
    """represent an alias, as typically applied to any 
    table or sub-select within a SQL statement using the 
    ``AS`` keyword (or without the keyword on certain databases
    such as Oracle).

    this object is constructed from the ``alias()`` module level function
    as well as the ``alias()`` method available on all ``FromClause``
    subclasses.
    
    """
    def __init__(self, selectable, alias=None):
        baseselectable = selectable
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.selectable
        self.original = baseselectable
        self.selectable = selectable
        if alias is None:
            if self.original.named_with_column():
                alias = getattr(self.original, 'name', None)
            if alias is None:
                alias = 'anon'
            elif len(alias) > 15:
                alias = alias[0:15]
            alias = alias + "_" + hex(random.randint(0, 65535))[2:]
        self.name = alias
        self.encodedname = alias.encode('ascii', 'backslashreplace')
        self.case_sensitive = getattr(baseselectable, "case_sensitive", True)

    def supports_execution(self):
        return self.original.supports_execution()

    def _locate_oid_column(self):
        if self.selectable.oid_column is not None:
            return self.selectable.oid_column._make_proxy(self)
        else:
            return None

    def named_with_column(self):
        return True

    def _exportable_columns(self):
        #return self.selectable._exportable_columns()
        return self.selectable.columns

    def get_children(self, **kwargs):
        for c in self.c:
            yield c
        yield self.selectable
        
    def accept_visitor(self, visitor):
        visitor.visit_alias(self)

    def _get_from_objects(self):
        return [self]

    def _group_parenthesized(self):
        return False

    bind = property(lambda s: s.selectable.bind)
    engine = bind

class _Grouping(ColumnElement):
    def __init__(self, elem):
        self.elem = elem
        self.type = getattr(elem, 'type', None)

    key = property(lambda s: s.elem.key)
    _label = property(lambda s: s.elem._label)
    orig_set = property(lambda s:s.elem.orig_set)
    
    def copy_container(self):
        return _Grouping(self.elem.copy_container())
        
    def accept_visitor(self, visitor):
        visitor.visit_grouping(self)
    def get_children(self, **kwargs):
        return self.elem,
    def _hide_froms(self):
        return self.elem._hide_froms()
    def _get_from_objects(self):
        return self.elem._get_from_objects()
    def __getattr__(self, attr):
        return getattr(self.elem, attr)
        
class _Label(ColumnElement):
    """represent a label, as typically applied to any column-level element
    using the ``AS`` sql keyword.
    
    this object is constructed from the ``label()`` module level function
    as well as the ``label()`` method available on all ``ColumnElement``
    subclasses.
    
    """
    
    def __init__(self, name, obj, type=None):
        self.name = name
        while isinstance(obj, _Label):
            obj = obj.obj
        self.obj = obj.self_group(against='AS')
        self.case_sensitive = getattr(obj, "case_sensitive", True)
        self.type = sqltypes.to_instance(type or getattr(obj, 'type', None))

    key = property(lambda s: s.name)
    _label = property(lambda s: s.name)
    orig_set = property(lambda s:s.obj.orig_set)

    def _compare_self(self):
        return self.obj
    
    def get_children(self, **kwargs):
        return self.obj,

    def accept_visitor(self, visitor):
        visitor.visit_label(self)

    def _get_from_objects(self):
        return self.obj._get_from_objects()

    def _hide_froms(self):
        return self.obj._hide_froms()
        
    def _make_proxy(self, selectable, name = None):
        if isinstance(self.obj, Selectable):
            return self.obj._make_proxy(selectable, name=self.name)
        else:
            return column(self.name)._make_proxy(selectable=selectable)

legal_characters = util.Set(string.ascii_letters + string.digits + '_')

class _ColumnClause(ColumnElement):
    """Represents a generic column expression from any textual string.
    This includes columns associated with tables, aliases and select
    statements, but also any arbitrary text.  May or may not be bound 
    to an underlying ``Selectable``.  ``_ColumnClause`` is usually
    created publically via the ``column()`` function or the 
    ``column_literal()`` function.
    
      text
        the text of the element.
        
      selectable
        parent selectable.
      
      type
        ``TypeEngine`` object which can associate this ``_ColumnClause`` 
        with a type.
      
      case_sensitive
        defines whether identifier quoting rules will be applied to the
        generated text of this ``_ColumnClause`` so that it is identified in
        a case-sensitive manner.
      
      is_literal
        if True, the ``_ColumnClause`` is assumed to be an exact expression
        that will be delivered to the output with no quoting rules applied
        regardless of case sensitive settings.  the ``column_literal()`` function is
        usually used to create such a ``_ColumnClause``.
    
    """

    def __init__(self, text, selectable=None, type=None, _is_oid=False, case_sensitive=True, is_literal=False):
        self.key = self.name = text
        self.encodedname = isinstance(self.name, unicode) and self.name.encode('ascii', 'backslashreplace') or self.name
        self.table = selectable
        self.type = sqltypes.to_instance(type)
        self._is_oid = _is_oid
        self._distance = 0
        self.__label = None
        self.case_sensitive = case_sensitive
        self.is_literal = is_literal

    def _get_label(self):
        """Generate a 'label' for this column.
        
        The label is a product of the parent table name and column
        name, and is treated as a unique identifier of this ``Column``
        across all ``Tables`` and derived selectables for a particular
        metadata collection.
        """
        
        # for a "literal" column, we've no idea what the text is
        # therefore no 'label' can be automatically generated
        if self.is_literal:
            return None
        if self.__label is None:
            if self.table is not None and self.table.named_with_column():
                self.__label = self.table.name + "_" + self.name
                counter = 1
                while self.table.c.has_key(self.__label):
                    self.__label = self.__label + "_%d" % counter
                    counter += 1
            else:
                self.__label = self.name
            self.__label = "".join([x for x in self.__label if x in legal_characters])
        return self.__label

    is_labeled = property(lambda self:self.name != list(self.orig_set)[0].name)

    _label = property(_get_label)

    def label(self, name):
        # if going off the "__label" property and its None, we have
        # no label; return self
        if name is None:
            return self
        else:
            return super(_ColumnClause, self).label(name)
            
    def accept_visitor(self, visitor):
        visitor.visit_column(self)

    def _get_from_objects(self):
        if self.table is not None:
            return [self.table]
        else:
            return []

    def _bind_param(self, obj):
        return _BindParamClause(self._label, obj, shortname = self.name, type=self.type, unique=True)

    def _make_proxy(self, selectable, name = None):
        # propigate the "is_literal" flag only if we are keeping our name,
        # otherwise its considered to be a label
        is_literal = self.is_literal and (name is None or name == self.name)
        c = _ColumnClause(name or self.name, selectable=selectable, _is_oid=self._is_oid, type=self.type, is_literal=is_literal)
        c.orig_set = self.orig_set
        c._distance = self._distance + 1
        if not self._is_oid:
            selectable.columns[c.name] = c
        return c

    def _compare_type(self, obj):
        return self.type

    def _group_parenthesized(self):
        return False

class TableClause(FromClause):
    """represents a "table" construct.
    
    Note that this represents tables only as another 
    syntactical construct within SQL expressions; it 
    does not provide schema-level functionality.
    
    """
    
    def __init__(self, name, *columns):
        super(TableClause, self).__init__(name)
        self.name = self.fullname = name
        self.encodedname = self.name.encode('ascii', 'backslashreplace')
        self._oid_column = _ColumnClause('oid', self, _is_oid=True)
        self._export_columns(columns)

    def named_with_column(self):
        return True

    def append_column(self, c):
        self._columns[c.name] = c
        c.table = self

    def _locate_oid_column(self):
        return self._oid_column

    def _proxy_column(self, c):
        self.append_column(c)
        return c

    def _orig_columns(self):
        try:
            return self._orig_cols
        except AttributeError:
            self._orig_cols= {}
            for c in self.columns:
                for ci in c.orig_set:
                    self._orig_cols[ci] = c
            return self._orig_cols

    original_columns = property(_orig_columns)

    def get_children(self, column_collections=True, **kwargs):
        if column_collections:
            return [c for c in self.c]
        else:
            return []

    def accept_visitor(self, visitor):
        visitor.visit_table(self)

    def _exportable_columns(self):
        raise NotImplementedError()

    def _group_parenthesized(self):
        return False

    def count(self, whereclause=None, **params):
        if len(self.primary_key):
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

    def insert(self, values = None):
        return insert(self, values=values)

    def update(self, whereclause = None, values = None):
        return update(self, whereclause, values)

    def delete(self, whereclause = None):
        return delete(self, whereclause)

    def _get_from_objects(self):
        return [self]

class _SelectBaseMixin(object):
    """Base class for ``Select`` and ``CompoundSelects``."""

    def supports_execution(self):
        return True

    def order_by(self, *clauses):
        if len(clauses) == 1 and clauses[0] is None:
            self.order_by_clause = ClauseList()
        elif getattr(self, 'order_by_clause', None):
            self.order_by_clause = ClauseList(*(list(self.order_by_clause.clauses) + list(clauses)))
        else:
            self.order_by_clause = ClauseList(*clauses)

    def group_by(self, *clauses):
        if len(clauses) == 1 and clauses[0] is None:
            self.group_by_clause = ClauseList()
        elif getattr(self, 'group_by_clause', None):
            self.group_by_clause = ClauseList(*(list(clauses)+list(self.group_by_clause.clauses)))
        else:
            self.group_by_clause = ClauseList(*clauses)

    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def _get_from_objects(self):
        if self.is_where or self.is_scalar:
            return []
        else:
            return [self]

class CompoundSelect(_SelectBaseMixin, FromClause):
    def __init__(self, keyword, *selects, **kwargs):
        _SelectBaseMixin.__init__(self)
        self.keyword = keyword
        self.use_labels = kwargs.pop('use_labels', False)
        self.should_correlate = kwargs.pop('correlate', False)
        self.for_update = kwargs.pop('for_update', False)
        self.nowait = kwargs.pop('nowait', False)
        self.limit = kwargs.pop('limit', None)
        self.offset = kwargs.pop('offset', None)
        self.is_compound = True
        self.is_where = False
        self.is_scalar = False
        self.is_subquery = False

        # unions group from left to right, so don't group first select
        self.selects = [n and select.self_group(self) or select for n,select in enumerate(selects)]

        # some DBs do not like ORDER BY in the inner queries of a UNION, etc.
        for s in selects:
            s.order_by(None)

        self.group_by(*kwargs.pop('group_by', [None]))
        self.order_by(*kwargs.pop('order_by', [None]))
        if len(kwargs):
            raise TypeError("invalid keyword argument(s) for CompoundSelect: %s" % repr(kwargs.keys()))
        self._col_map = {}

    name = property(lambda s:s.keyword + " statement")

    def self_group(self, against=None):
        return _Grouping(self)

    def _locate_oid_column(self):
        return self.selects[0].oid_column

    def _exportable_columns(self):
        for s in self.selects:
            for c in s.c:
                yield c

    def _proxy_column(self, column):
        if self.use_labels:
            col = column._make_proxy(self, name=column._label)
        else:
            col = column._make_proxy(self)
        try:
            colset = self._col_map[col.name]
        except KeyError:
            colset = util.Set()
            self._col_map[col.name] = colset
        [colset.add(c) for c in col.orig_set]
        col.orig_set = colset
        return col

    def get_children(self, column_collections=True, **kwargs):
        return (column_collections and list(self.c) or []) + \
            [self.order_by_clause, self.group_by_clause] + list(self.selects)
    def accept_visitor(self, visitor):
        visitor.visit_compound_select(self)

    def _find_engine(self):
        for s in self.selects:
            e = s._find_engine()
            if e:
                return e
        else:
            return None

class Select(_SelectBaseMixin, FromClause):
    """Represent a ``SELECT`` statement, with appendable clauses, as
    well as the ability to execute itself and return a result set.
    
    """

    def __init__(self, columns=None, whereclause=None, from_obj=[],
                 order_by=None, group_by=None, having=None,
                 use_labels=False, distinct=False, for_update=False,
                 engine=None, bind=None, limit=None, offset=None, scalar=False,
                 correlate=True):
        """construct a Select object.
        
        The public constructor for Select is the [sqlalchemy.sql#select()] function; 
        see that function for argument descriptions.
        """
        _SelectBaseMixin.__init__(self)
        self.__froms = util.OrderedSet()
        self.__hide_froms = util.Set([self])
        self.use_labels = use_labels
        self.whereclause = None
        self.having = None
        self._bind = bind or engine
        self.limit = limit
        self.offset = offset
        self.for_update = for_update
        self.is_compound = False
        
        # indicates that this select statement should not expand its columns
        # into the column clause of an enclosing select, and should instead
        # act like a single scalar column
        self.is_scalar = scalar
        if scalar:
            # allow corresponding_column to return None
            self.orig_set = util.Set()
            
        # indicates if this select statement, as a subquery, should automatically correlate
        # its FROM clause to that of an enclosing select, update, or delete statement.
        # note that the "correlate" method can be used to explicitly add a value to be correlated.
        self.should_correlate = correlate

        # indicates if this select statement is a subquery inside another query
        self.is_subquery = False

        # indicates if this select statement is in the from clause of another query
        self.is_selected_from = False

        # indicates if this select statement is a subquery as a criterion
        # inside of a WHERE clause
        self.is_where = False

        self.distinct = distinct
        self._raw_columns = []
        self.__correlated = {}
        self.__correlator = Select._CorrelatedVisitor(self, False)
        self.__wherecorrelator = Select._CorrelatedVisitor(self, True)
        self.__fromvisitor = Select._FromVisitor(self)

        
        self.order_by_clause = self.group_by_clause = None
        
        if columns is not None:
            for c in columns:
                self.append_column(c)

        if order_by:
            order_by = util.to_list(order_by)
        if group_by:
            group_by = util.to_list(group_by)
        self.order_by(*(order_by or [None]))
        self.group_by(*(group_by or [None]))
        for c in self.order_by_clause:
            self.__correlator.traverse(c)
        for c in self.group_by_clause:
            self.__correlator.traverse(c)

        for f in from_obj:
            self.append_from(f)

        # whereclauses must be appended after the columns/FROM, since it affects
        # the correlation of subqueries.  see test/sql/select.py SelectTest.testwheresubquery
        if whereclause is not None:
            self.append_whereclause(whereclause)
        if having is not None:
            self.append_having(having)


    class _CorrelatedVisitor(NoColumnVisitor):
        """Visit a clause, locate any ``Select`` clauses, and tell
        them that they should correlate their ``FROM`` list to that of
        their parent.
        """

        def __init__(self, select, is_where):
            NoColumnVisitor.__init__(self)
            self.select = select
            self.is_where = is_where

        def visit_compound_select(self, cs):
            self.visit_select(cs)

        def visit_column(self, c):
            pass

        def visit_table(self, c):
            pass

        def visit_select(self, select):
            if select is self.select:
                return
            select.is_where = self.is_where
            select.is_subquery = True
            if not select.should_correlate:
                return
            [select.correlate(x) for x in self.select._Select__froms]

    class _FromVisitor(NoColumnVisitor):
        def __init__(self, select):
            NoColumnVisitor.__init__(self)
            self.select = select
            
        def visit_select(self, select):
            if select is self.select:
                return
            select.is_selected_from = True
            select.is_subquery = True

    def append_column(self, column):
        if _is_literal(column):
            column = literal_column(str(column))

        if isinstance(column, Select) and column.is_scalar:
            column = column.self_group(against=',')

        self._raw_columns.append(column)

        if self.is_scalar and not hasattr(self, 'type'):
            self.type = column.type
        
        # if the column is a Select statement itself,
        # accept visitor
        self.__correlator.traverse(column)

        # visit the FROM objects of the column looking for more Selects
        for f in column._get_from_objects():
            if f is not self:
                self.__correlator.traverse(f)
        self._process_froms(column, False)

    def _make_proxy(self, selectable, name):
        if self.is_scalar:
            return self._raw_columns[0]._make_proxy(selectable, name)
        else:
            raise exceptions.InvalidRequestError("Not a scalar select statement")

    def label(self, name):
        if not self.is_scalar:
            raise exceptions.InvalidRequestError("Not a scalar select statement")
        else:
            return label(name, self)

    def _exportable_columns(self):
        return [c for c in self._raw_columns if isinstance(c, Selectable)]
        
    def _proxy_column(self, column):
        if self.use_labels:
            return column._make_proxy(self, name=column._label)
        else:
            return column._make_proxy(self)

    def _process_froms(self, elem, asfrom):
        for f in elem._get_from_objects():
            self.__fromvisitor.traverse(f)
            self.__froms.add(f)
        if asfrom:
            self.__froms.add(elem)
        for f in elem._hide_froms():
            self.__hide_froms.add(f)

    def self_group(self, against=None):
        if isinstance(against, CompoundSelect):
            return self
        return _Grouping(self)
    
    def append_whereclause(self, whereclause):
        self._append_condition('whereclause', whereclause)

    def append_having(self, having):
        self._append_condition('having', having)

    def _append_condition(self, attribute, condition):
        if isinstance(condition, basestring):
            condition = _TextClause(condition)
        self.__wherecorrelator.traverse(condition)
        self._process_froms(condition, False)
        if getattr(self, attribute) is not None:
            setattr(self, attribute, and_(getattr(self, attribute), condition))
        else:
            setattr(self, attribute, condition)

    def correlate(self, from_obj):
        """Given a ``FROM`` object, correlate this ``SELECT`` statement to it.

        This basically means the given from object will not come out
        in this select statement's ``FROM`` clause when printed.
        """

        self.__correlated[from_obj] = from_obj

    def append_from(self, fromclause):
        if isinstance(fromclause, basestring):
            fromclause = FromClause(fromclause)
        self.__correlator.traverse(fromclause)
        self._process_froms(fromclause, True)

    def _locate_oid_column(self):
        for f in self.__froms:
            if f is self:
                # we might be in our own _froms list if a column with us as the parent is attached,
                # which includes textual columns.
                continue
            oid = f.oid_column
            if oid is not None:
                return oid
        else:
            return None

    def _calc_froms(self):
        f = self.__froms.difference(self.__hide_froms)
        if (len(f) > 1):
            return f.difference(self.__correlated)
        else:
            return f

    froms = property(_calc_froms,
                     doc="""A collection containing all elements
                     of the ``FROM`` clause.""")
    
    def get_children(self, column_collections=True, **kwargs):
        return (column_collections and list(self.columns) or []) + \
            list(self.froms) + \
            [x for x in (self.whereclause, self.having, self.order_by_clause, self.group_by_clause) if x is not None]

    def accept_visitor(self, visitor):
        visitor.visit_select(self)

    def union(self, other, **kwargs):
        return union(self, other, **kwargs)

    def union_all(self, other, **kwargs):
        return union_all(self, other, **kwargs)

    def _find_engine(self):
        """Try to return a Engine, either explicitly set in this
        object, or searched within the from clauses for one.
        """

        if self._bind is not None:
            return self._bind
        for f in self.__froms:
            if f is self:
                continue
            e = f.bind
            if e is not None:
                self._bind = e
                return e
        # look through the columns (largely synomous with looking
        # through the FROMs except in the case of _CalculatedClause/_Function)
        for cc in self._exportable_columns():
            for c in cc.columns:
                if getattr(c, 'table', None) is self:
                    continue
                e = c.bind
                if e is not None:
                    self._bind = e
                    return e
        return None

class _UpdateBase(ClauseElement):
    """Form the base for ``INSERT``, ``UPDATE``, and ``DELETE`` statements."""

    def supports_execution(self):
        return True

    class _SelectCorrelator(NoColumnVisitor):
        def __init__(self, table):
            NoColumnVisitor.__init__(self)
            self.table = table
            
        def visit_select(self, select):
            if select.should_correlate:
                select.correlate(self.table)
    
    def _process_whereclause(self, whereclause):
        if whereclause is not None:
            _UpdateBase._SelectCorrelator(self.table).traverse(whereclause)
        return whereclause
        
    def _process_colparams(self, parameters):
        """Receive the *values* of an ``INSERT`` or ``UPDATE``
        statement and construct appropriate bind parameters.
        """

        if parameters is None:
            return None

        if isinstance(parameters, list) or isinstance(parameters, tuple):
            pp = {}
            i = 0
            for c in self.table.c:
                pp[c.key] = parameters[i]
                i +=1
            parameters = pp

        correlator = _UpdateBase._SelectCorrelator(self.table)
        for key in parameters.keys():
            value = parameters[key]
            if isinstance(value, ClauseElement):
                correlator.traverse(value)
            elif _is_literal(value):
                if _is_literal(key):
                    col = self.table.c[key]
                else:
                    col = key
                try:
                    parameters[key] = bindparam(col, value, unique=True)
                except KeyError:
                    del parameters[key]
        return parameters

    def _find_engine(self):
        return self.table.bind

class _Insert(_UpdateBase):
    def __init__(self, table, values=None):
        self.table = table
        self.select = None
        self.parameters = self._process_colparams(values)

    def get_children(self, **kwargs):
        if self.select is not None:
            return self.select,
        else:
            return ()
    def accept_visitor(self, visitor):
        visitor.visit_insert(self)

class _Update(_UpdateBase):
    def __init__(self, table, whereclause, values=None):
        self.table = table
        self.whereclause = self._process_whereclause(whereclause)
        self.parameters = self._process_colparams(values)

    def get_children(self, **kwargs):
        if self.whereclause is not None:
            return self.whereclause,
        else:
            return ()
    def accept_visitor(self, visitor):
        visitor.visit_update(self)

class _Delete(_UpdateBase):
    def __init__(self, table, whereclause):
        self.table = table
        self.whereclause = self._process_whereclause(whereclause)

    def get_children(self, **kwargs):
        if self.whereclause is not None:
            return self.whereclause,
        else:
            return ()
    def accept_visitor(self, visitor):
        visitor.visit_delete(self)
