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

from sqlalchemy import util, exceptions
from sqlalchemy import types as sqltypes
import re, operator

__all__ = ['Alias', 'ClauseElement', 'ClauseParameters',
           'ClauseVisitor', 'ColumnCollection', 'ColumnElement',
           'CompoundSelect', 'Delete', 'FromClause', 'Insert', 'Join', 
           'Select', 'Selectable', 'TableClause', 'Update', 'alias', 'and_', 'asc',
           'between', 'bindparam', 'case', 'cast', 'column', 'delete',
           'desc', 'distinct', 'except_', 'except_all', 'exists', 'extract', 'func', 'modifier',
           'insert', 'intersect', 'intersect_all', 'join', 'literal',
           'literal_column', 'not_', 'null', 'or_', 'outparam', 'outerjoin', 'select',
           'subquery', 'table', 'text', 'union', 'union_all', 'update',]

BIND_PARAMS = re.compile(r'(?<![:\w\x5c]):(\w+)(?!:)', re.UNICODE)

def desc(column):
    """Return a descending ``ORDER BY`` clause element.

    E.g.::

      order_by = [desc(table1.mycol)]
    """
    return _UnaryExpression(column, modifier=ColumnOperators.desc_op)

def asc(column):
    """Return an ascending ``ORDER BY`` clause element.

    E.g.::

      order_by = [asc(table1.mycol)]
    """
    return _UnaryExpression(column, modifier=ColumnOperators.asc_op)

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

def select(columns=None, whereclause=None, from_obj=[], **kwargs):
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
        
        limit=None
          a numerical value which usually compiles to a ``LIMIT`` expression
          in the resulting select.  Databases that don't support ``LIMIT``
          will attempt to provide similar functionality.
        
        offset=None
          a numerical value which usually compiles to an ``OFFSET`` expression
          in the resulting select.  Databases that don't support ``OFFSET``
          will attempt to provide similar functionality.
        
        scalar=False
          deprecated.  use select(...).as_scalar() to create a "scalar column"
          proxy for an existing Select object.

        correlate=True
          indicates that this ``Select`` object should have its contained
          ``FromClause`` elements "correlated" to an enclosing ``Select``
          object.  This means that any ``ClauseElement`` instance within 
          the "froms" collection of this ``Select`` which is also present
          in the "froms" collection of an enclosing select will not be
          rendered in the ``FROM`` clause of this select statement.
      
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
    """Return an [sqlalchemy.sql#Alias] object derived from a [sqlalchemy.sql#Select].
    
      name
        alias name

      \*args, \**kwargs
        all other arguments are delivered to the [sqlalchemy.sql#select()] function.
    
    """
    
    return Select(*args, **kwargs).alias(alias)

def insert(table, values = None, **kwargs):
    """Return an [sqlalchemy.sql#Insert] clause element.

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

    return Insert(table, values, **kwargs)

def update(table, whereclause = None, values = None, **kwargs):
    """Return an [sqlalchemy.sql#Update] clause element.

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

    return Update(table, whereclause, values, **kwargs)

def delete(table, whereclause = None, **kwargs):
    """Return a [sqlalchemy.sql#Delete] clause element.

    Similar functionality is available via the ``delete()`` 
    method on [sqlalchemy.schema#Table].

      table
        The table to be updated.

      whereclause
        A ``ClauseElement`` describing the ``WHERE`` condition of the
        ``UPDATE`` statement.

    """

    return Delete(table, whereclause, **kwargs)

def and_(*clauses):
    """Join a list of clauses together using the ``AND`` operator.

    The ``&`` operator is also overloaded on all [sqlalchemy.sql#_CompareMixin]
    subclasses to produce the same result.
    """
    if len(clauses) == 1:
        return clauses[0]
    return ClauseList(operator=operator.and_, *clauses)

def or_(*clauses):
    """Join a list of clauses together using the ``OR`` operator.

    The ``|`` operator is also overloaded on all [sqlalchemy.sql#_CompareMixin]
    subclasses to produce the same result.
    """

    if len(clauses) == 1:
        return clauses[0]
    return ClauseList(operator=operator.or_, *clauses)

def not_(clause):
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The ``~`` operator is also overloaded on all [sqlalchemy.sql#_CompareMixin]
    subclasses to produce the same result.
    """

    return operator.inv(clause)

def distinct(expr):
    """return a ``DISTINCT`` clause."""
    
    return _UnaryExpression(expr, operator=ColumnOperators.distinct_op)

def between(ctest, cleft, cright):
    """Return a ``BETWEEN`` predicate clause.

    Equivalent of SQL ``clausetest BETWEEN clauseleft AND clauseright``.

    The ``between()`` method on all [sqlalchemy.sql#_CompareMixin] subclasses
    provides similar functionality.
    """

    ctest = _literal_as_binds(ctest)
    return _BinaryExpression(ctest, ClauseList(_literal_as_binds(cleft, type_=ctest.type), _literal_as_binds(cright, type_=ctest.type), operator=operator.and_, group=False), ColumnOperators.between_op)


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

    expr = _BinaryExpression(text(field), expr, Operators.from_)
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


def literal(value, type_=None):
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
    
      type\_
        an optional [sqlalchemy.types#TypeEngine] which will provide
        bind-parameter translation for this literal.

    """

    return _BindParamClause('literal', value, type_=type_, unique=True)

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

def column(text, type_=None):
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
        
      type\_
        an optional [sqlalchemy.types#TypeEngine] object which will provide
        result-set translation for this column.
        
    """

    return _ColumnClause(text, type_=type_)

def literal_column(text, type_=None):
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

    return _ColumnClause(text, type_=type_, is_literal=True)

def table(name, *columns):
    """Return a [sqlalchemy.sql#Table] object.

    This is a primitive version of the [sqlalchemy.schema#Table] object, which
    is a subclass of this object.
    """

    return TableClause(name, *columns)

def bindparam(key, value=None, type_=None, shortname=None, unique=False):
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
        return _BindParamClause(key.name, value, type_=key.type, shortname=shortname, unique=unique)
    else:
        return _BindParamClause(key, value, type_=type_, shortname=shortname, unique=unique)

def outparam(key, type_=None):
    """create an 'OUT' parameter for usage in functions (stored procedures), for databases 
    whith support them.
    
    The ``outparam`` can be used like a regular function parameter.  The "output" value will
    be available from the [sqlalchemy.engine#ResultProxy] object via its ``out_parameters``
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
        The text of the SQL statement to be created.  use ``:<param>``
        to specify bind parameters; they will be compiled to their
        engine-specific format.

      bind
        An optional connection or engine to be used for this text query.
        
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
        return _Function(self.__names[-1], packagenames=self.__names[0:-1], *c, **o)

func = _FunctionGenerator()

# TODO: use UnaryExpression for this instead ?
modifier = _FunctionGenerator(group=False)

    
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
    
def _literal_as_binds(element, name='literal', type_=None):
    if isinstance(element, Operators):
        return element.expression_element()
    elif _is_literal(element):
        if element is None:
            return null()
        else:
            return _BindParamClause(name, element, shortname=name, type_=type_, unique=True)
    else:
        return element

def _selectable(element):
    if hasattr(element, '__selectable__'):
        return element.__selectable__()
    elif isinstance(element, Selectable):
        return element
    else:
        raise exceptions.ArgumentError("Object '%s' is not a Selectable and does not implement `__selectable__()`" % repr(element))
        
def is_column(col):
    return isinstance(col, ColumnElement)

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
        self.__binds = {}
        self.positional = positional or []

    def get_parameter(self, key):
        return self.__binds[key]

    def set_parameter(self, bindparam, value, name):
        self.__binds[name] = [bindparam, name, value]
        
    def get_original(self, key):
        return self.__binds[key][2]

    def get_type(self, key):
        return self.__binds[key][0].type

    def get_processed(self, key):
        (bind, name, value) = self.__binds[key]
        return bind.typeprocess(value, self.dialect)
   
    def keys(self):
        return self.__binds.keys()

    def __iter__(self):
        return iter(self.keys())
 
    def __getitem__(self, key):
        return self.get_processed(key)
        
    def __contains__(self, key):
        return key in self.__binds
    
    def set_value(self, key, value):
        self.__binds[key][2] = value
            
    def get_original_dict(self):
        return dict([(name, value) for (b, name, value) in self.__binds.values()])

    def get_raw_list(self):
        return [self.get_processed(key) for key in self.positional]

    def get_raw_dict(self, encode_keys=False):
        if encode_keys:
            return dict([(key.encode(self.dialect.encoding), self.get_processed(key)) for key in self.keys()])
        else:
            return dict([(key, self.get_processed(key)) for key in self.keys()])

    def __repr__(self):
        return self.__class__.__name__ + ":" + repr(self.get_original_dict())

class ClauseVisitor(object):
    """A class that knows how to traverse and visit
    ``ClauseElements``.
    
    Calls visit_XXX() methods dynamically generated for each particualr
    ``ClauseElement`` subclass encountered.  Traversal of a
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
    (schema_visitor=True).
    
    ``ClauseVisitor`` also supports a simultaneous copy-and-traverse
    operation, which will produce a copy of a given ``ClauseElement``
    structure while at the same time allowing ``ClauseVisitor`` subclasses
    to modify the new structure in-place.
    
    """
    __traverse_options__ = {}
    
    def traverse_single(self, obj, **kwargs):
        meth = getattr(self, "visit_%s" % obj.__visit_name__, None)
        if meth:
            return meth(obj, **kwargs)

    def iterate(self, obj, stop_on=None):
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            if stop_on is None or t not in stop_on:
                yield t
                traversal.insert(0, t)
                for c in t.get_children(**self.__traverse_options__):
                    stack.append(c)
        
    def traverse(self, obj, stop_on=None, clone=False):
        if clone:
            obj = obj._clone()
            
        stack = [obj]
        traversal = []
        while len(stack) > 0:
            t = stack.pop()
            if stop_on is None or t not in stop_on:
                traversal.insert(0, t)
                if clone:
                    t._copy_internals()
                for c in t.get_children(**self.__traverse_options__):
                    stack.append(c)
        for target in traversal:
            v = self
            while v is not None:
                meth = getattr(v, "visit_%s" % target.__visit_name__, None)
                if meth:
                    meth(target)
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

class NoColumnVisitor(ClauseVisitor):
    """a ClauseVisitor that will not traverse the exported Column 
    collections on Table, Alias, Select, and CompoundSelect objects
    (i.e. their 'columns' or 'c' attribute).
    
    this is useful because most traversals don't need those columns, or
    in the case of ANSICompiler it traverses them explicitly; so
    skipping their traversal here greatly cuts down on method call overhead.
    """
    
    __traverse_options__ = {'column_collections':False}


class _FigureVisitName(type):
    def __init__(cls, clsname, bases, dict):
        if not '__visit_name__' in cls.__dict__:
            m = re.match(r'_?(\w+?)(?:Expression|Clause|Element|$)', clsname)
            x = m.group(1)
            x = re.sub(r'(?!^)[A-Z]', lambda m:'_'+m.group(0).lower(), x)
            cls.__visit_name__ = x.lower()
        super(_FigureVisitName, cls).__init__(clsname, bases, dict)
        
class ClauseElement(object):
    """Base class for elements of a programmatically constructed SQL
    expression.
    """
    __metaclass__ = _FigureVisitName
    
    def _clone(self):
        """create a shallow copy of this ClauseElement.
        
        This method may be used by a generative API.
        Its also used as part of the "deep" copy afforded
        by a traversal that combines the _copy_internals()
        method."""
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        return c

    def _get_from_objects(self, **modifiers):
        """Return objects represented in this ``ClauseElement`` that
        should be added to the ``FROM`` list of a query, when this
        ``ClauseElement`` is placed in the column clause of a
        ``Select`` statement.
        """

        raise NotImplementedError(repr(self))

    def _hide_froms(self, **modifiers):
        """Return a list of ``FROM`` clause elements which this
        ``ClauseElement`` replaces.
        """

        return []

    def unique_params(self, *optionaldict, **kwargs):
        """same functionality as ``params()``, except adds `unique=True` to affected
        bind parameters so that multiple statements can be used.
        """
        
        return self._params(True, optionaldict, kwargs)
    def params(self, *optionaldict, **kwargs):
        """return a copy of this ClauseElement, with ``bindparam()`` elements
        replaced with values taken from the given dictionary.
        
            e.g.::
            
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
            
        class Vis(ClauseVisitor):
            def visit_bindparam(self, bind):
                if bind.key in kwargs:
                    bind.value = kwargs[bind.key]
                if unique:
                    bind.unique=True
        return Vis().traverse(self, clone=True)
        
    def compare(self, other):
        """Compare this ClauseElement to the given ClauseElement.

        Subclasses should override the default behavior, which is a
        straight identity comparison.
        """

        return self is other

    def _copy_internals(self):
        """reassign internal elements to be clones of themselves.
        
        called during a copy-and-traverse operation on newly 
        shallow-copied elements to create a deep copy."""
        
        pass
        
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

    def execute(self, *multiparams, **params):
        """Compile and execute this ``ClauseElement``."""

        if multiparams:
            compile_params = multiparams[0]
        else:
            compile_params = params
        return self.compile(bind=self.bind, parameters=compile_params).execute(*multiparams, **params)

    def scalar(self, *multiparams, **params):
        """Compile and execute this ``ClauseElement``, returning the
        result's scalar representation.
        """

        return self.execute(*multiparams, **params).scalar()

    def compile(self, bind=None, parameters=None, compiler=None, dialect=None):
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

        if isinstance(parameters, (list, tuple)):
            parameters = parameters[0]

        if compiler is None:
            if dialect is not None:
                compiler = dialect.compiler(self, parameters)
            elif bind is not None:
                compiler = bind.compiler(self, parameters)
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
        if hasattr(self, 'negation_clause'):
            return self.negation_clause
        else:
            return _UnaryExpression(self.self_group(against=operator.inv), operator=operator.inv, negate=None)


class Operators(object):
    def from_():
        raise NotImplementedError()
    from_ = staticmethod(from_)
    
    def as_():
        raise NotImplementedError()
    as_ = staticmethod(as_)
    
    def exists():
        raise NotImplementedError()
    exists = staticmethod(exists)

    def is_():
        raise NotImplementedError()
    is_ = staticmethod(is_)
    
    def isnot():
        raise NotImplementedError()
    isnot = staticmethod(isnot)
    
    def __and__(self, other):
        return self.operate(operator.and_, other)

    def __or__(self, other):
        return self.operate(operator.or_, other)

    def __invert__(self):
        return self.operate(operator.inv)

    def clause_element(self):
        raise NotImplementedError()

    def operate(self, op, *other, **kwargs):
        raise NotImplementedError()

    def reverse_operate(self, op, other, **kwargs):
        raise NotImplementedError()

class ColumnOperators(Operators):
    """defines comparison and math operations"""

    def like_op(a, b):
        return a.like(b)
    like_op = staticmethod(like_op)
    
    def notlike_op(a, b):
        raise NotImplementedError()
    notlike_op = staticmethod(notlike_op)

    def ilike_op(a, b):
        return a.ilike(b)
    ilike_op = staticmethod(ilike_op)
    
    def notilike_op(a, b):
        raise NotImplementedError()
    notilike_op = staticmethod(notilike_op)
    
    def between_op(a, b):
        return a.between(b)
    between_op = staticmethod(between_op)
    
    def in_op(a, b):
        return a.in_(*b)
    in_op = staticmethod(in_op)

    def notin_op(a, b):
        raise NotImplementedError()
    notin_op = staticmethod(notin_op)
    
    def distinct_op(a):
        return a.distinct()
    distinct_op = staticmethod(distinct_op)
    
    def startswith_op(a, b):
        return a.startswith(b)
    startswith_op = staticmethod(startswith_op)
    
    def endswith_op(a, b):
        return a.endswith(b)
    endswith_op = staticmethod(endswith_op)

    def comma_op(a, b):
        raise NotImplementedError()
    comma_op = staticmethod(comma_op)

    def concat_op(a, b):
        return a.concat(b)
    concat_op = staticmethod(concat_op)
    
    def desc_op(a):
        return a.desc()
    desc_op = staticmethod(desc_op)

    def asc_op(a):
        return a.asc()
    asc_op = staticmethod(asc_op)
    
    def __lt__(self, other):
        return self.operate(operator.lt, other)

    def __le__(self, other):
        return self.operate(operator.le, other)

    def __eq__(self, other):
        return self.operate(operator.eq, other)

    def __ne__(self, other):
        return self.operate(operator.ne, other)

    def __gt__(self, other):
        return self.operate(operator.gt, other)

    def __ge__(self, other):
        return self.operate(operator.ge, other)

    def concat(self, other):
        return self.operate(ColumnOperators.concat_op, other)
        
    def like(self, other):
        return self.operate(ColumnOperators.like_op, other)
    
    def in_(self, *other):
        return self.operate(ColumnOperators.in_op, other)
    
    def startswith(self, other):
        return self.operate(ColumnOperators.startswith_op, other)

    def endswith(self, other):
        return self.operate(ColumnOperators.endswith_op, other)
    
    def desc(self):
        return self.operate(ColumnOperators.desc_op)
        
    def asc(self):
        return self.operate(ColumnOperators.asc_op)
        
    def __radd__(self, other):
        return self.reverse_operate(operator.add, other)

    def __rsub__(self, other):
        return self.reverse_operate(operator.sub, other)

    def __rmul__(self, other):
        return self.reverse_operate(operator.mul, other)

    def __rdiv__(self, other):
        return self.reverse_operate(operator.div, other)

    def between(self, cleft, cright):
        return self.operate(ColumnOperators.between_op, cleft, cright)

    def distinct(self):
        return self.operate(ColumnOperators.distinct_op)
        
    def __add__(self, other):
        return self.operate(operator.add, other)

    def __sub__(self, other):
        return self.operate(operator.sub, other)

    def __mul__(self, other):
        return self.operate(operator.mul, other)

    def __div__(self, other):
        return self.operate(operator.div, other)

    def __mod__(self, other):
        return self.operate(operator.mod, other)

    def __truediv__(self, other):
        return self.operate(operator.truediv, other)

# precedence ordering for common operators.  if an operator is not present in this list,
# it will be parenthesized when grouped against other operators
_smallest = object()
_largest = object()

PRECEDENCE = {
    Operators.from_:15,
    operator.mul:7,
    operator.div:7,
    operator.mod:7,
    operator.add:6,
    operator.sub:6,
    ColumnOperators.concat_op:6,
    ColumnOperators.ilike_op:5,
    ColumnOperators.notilike_op:5,
    ColumnOperators.like_op:5,
    ColumnOperators.notlike_op:5,
    ColumnOperators.in_op:5,
    ColumnOperators.notin_op:5,
    Operators.is_:5,
    Operators.isnot:5,
    operator.eq:5,
    operator.ne:5,
    operator.gt:5,
    operator.lt:5,
    operator.ge:5,
    operator.le:5,
    ColumnOperators.between_op:5,
    ColumnOperators.distinct_op:5,
    operator.inv:4,
    operator.and_:3,
    operator.or_:2,
    ColumnOperators.comma_op:-1,
    Operators.as_:-1,
    Operators.exists:0,
    _smallest: -1000,
    _largest: 1000
}

class _CompareMixin(ColumnOperators):
    """Defines comparison and math operations for ``ClauseElement`` instances."""

    def __compare(self, op, obj, negate=None):
        if obj is None or isinstance(obj, _Null):
            if op == operator.eq:
                return _BinaryExpression(self.expression_element(), null(), Operators.is_, negate=Operators.isnot)
            elif op == operator.ne:
                return _BinaryExpression(self.expression_element(), null(), Operators.isnot, negate=Operators.is_)
            else:
                raise exceptions.ArgumentError("Only '='/'!=' operators can be used with NULL")
        else:
            obj = self._check_literal(obj)

            
        return _BinaryExpression(self.expression_element(), obj, op, type_=sqltypes.Boolean, negate=negate)

    def __operate(self, op, obj):
        obj = self._check_literal(obj)

        type_ = self._compare_type(obj)
        
        # TODO: generalize operator overloading like this out into the types module
        if op == operator.add and isinstance(type_, (sqltypes.Concatenable)):
            op = ColumnOperators.concat_op
        
        return _BinaryExpression(self.expression_element(), obj, op, type_=type_)

    operators = {
        operator.add : (__operate,),
        operator.mul : (__operate,),
        operator.sub : (__operate,),
        operator.div : (__operate,),
        operator.mod : (__operate,),
        operator.truediv : (__operate,),
        operator.lt : (__compare, operator.ge),
        operator.le : (__compare, operator.gt),
        operator.ne : (__compare, operator.eq),
        operator.gt : (__compare, operator.le),
        operator.ge : (__compare, operator.lt),
        operator.eq : (__compare, operator.ne),
        ColumnOperators.like_op : (__compare, ColumnOperators.notlike_op),
    }

    def operate(self, op, *other):
        o = _CompareMixin.operators[op]
        return o[0](self, op, other[0], *o[1:])
    
    def reverse_operate(self, op, other):
        return self._bind_param(other).operate(op, self)

    def in_(self, *other):
        return self._in_impl(ColumnOperators.in_op, ColumnOperators.notin_op, *other)
        
    def _in_impl(self, op, negate_op, *other):
        if len(other) == 0:
            return _Grouping(case([(self.__eq__(None), text('NULL'))], else_=text('0')).__eq__(text('1')))
        elif len(other) == 1:
            o = other[0]
            if _is_literal(o) or isinstance( o, _CompareMixin):
                return self.__eq__( o)    #single item -> ==
            else:
                assert isinstance(o, Selectable)
                return self.__compare( op, o, negate=negate_op)   #single selectable

        args = []
        for o in other:
            if not _is_literal(o):
                if not isinstance( o, _CompareMixin):
                    raise exceptions.InvalidRequestError( "in() function accepts either non-selectable values, or a single selectable: "+repr(o) )
            else:
                o = self._bind_param(o)
            args.append(o)
        return self.__compare(op, ClauseList(*args).self_group(against=op), negate=negate_op)

    def startswith(self, other):
        """produce the clause ``LIKE '<other>%'``"""

        perc = isinstance(other,(str,unicode)) and '%' or literal('%',type_= sqltypes.String)
        return self.__compare(ColumnOperators.like_op, other + perc)

    def endswith(self, other):
        """produce the clause ``LIKE '%<other>'``"""
        
        if isinstance(other,(str,unicode)): po = '%' + other
        else:
            po = literal('%', type_=sqltypes.String) + other
            po.type = sqltypes.to_instance(sqltypes.String)     #force!
        return self.__compare(ColumnOperators.like_op, po)

    def label(self, name):
        """produce a column label, i.e. ``<columnname> AS <name>``"""
        return _Label(name, self, self.type)
    
    def desc(self):
        """produce a DESC clause, i.e. ``<columnname> DESC``"""
        
        return desc(self)
        
    def asc(self):
        """produce a ASC clause, i.e. ``<columnname> ASC``"""
        
        return asc(self)
        
    def distinct(self):
        """produce a DISTINCT clause, i.e. ``DISTINCT <columnname>``"""
        return _UnaryExpression(self, operator=ColumnOperators.distinct_op)

    def between(self, cleft, cright):
        """produce a BETWEEN clause, i.e. ``<column> BETWEEN <cleft> AND <cright>``"""

        return _BinaryExpression(self, ClauseList(self._check_literal(cleft), self._check_literal(cright), operator=operator.and_, group=False), ColumnOperators.between_op)

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
        return lambda other: self.__operate(operator, other)

    def _bind_param(self, obj):
        return _BindParamClause('literal', obj, shortname=None, type_=self.type, unique=True)

    def _check_literal(self, other):
        if isinstance(other, Operators):
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

class Selectable(ClauseElement):
    """Represent a column list-holding object.
    
    this is the common base class of [sqlalchemy.sql#ColumnElement]
    and [sqlalchemy.sql#FromClause].  The reason ``ColumnElement``
    is marked as a "list-holding" object is so that it can be treated
    similarly to ``FromClause`` in column-selection scenarios; it 
    contains a list of columns consisting of itself.
    
    """

    columns = util.NotImplProperty("""a [sqlalchemy.sql#ColumnCollection] containing ``ColumnElement`` instances.""")

    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

class ColumnElement(ClauseElement, _CompareMixin):
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

    def _one_fkey(self):
        if self._foreign_keys:
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

    def __contains__(self, other):
        if not isinstance(other, basestring):
            raise exceptions.ArgumentError("__contains__ requires a string argument")
        return self.has_key(other)
        
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

    __visit_name__ = 'fromclause'
    
    def __init__(self, name=None):
        self.name = name

    def _get_from_objects(self, **modifiers):
        # this could also be [self], at the moment it doesnt matter to the Select object
        return []

    def default_order_by(self):
        return [self.oid_column]

    def count(self, whereclause=None, **params):
        if self.primary_key:
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

    def is_derived_from(self, fromclause):
        """return True if this FromClause is 'derived' from the given FromClause.
        
        An example would be an Alias of a Table is derived from that Table."""
        
        return False
    
    def replace_selectable(self, old, alias):
      """replace all occurences of FromClause 'old' with the given Alias object, returning a 
      copy of this ``FromClause``."""
      
      from sqlalchemy import sql_util
      return sql_util.ClauseAdapter(alias).traverse(self, clone=True)
      
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
            
        if self.c.contains_column(column):
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

    def _clone_from_clause(self):
        # delete all the "generated" collections of columns for a newly cloned FromClause,
        # so that they will be re-derived from the item.
        # this is because FromClause subclasses, when cloned, need to reestablish new "proxied" 
        # columns that are linked to the new item
        for attr in ('_columns', '_primary_key' '_foreign_keys', '_orig_cols', '_oid_column'):
            if hasattr(self, attr):
                delattr(self, attr)

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
            columns = self._flatten_exportable_columns()
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
    
    def _flatten_exportable_columns(self):
        """return the list of ColumnElements represented within this FromClause's _exportable_columns"""
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

class _BindParamClause(ClauseElement, _CompareMixin):
    """Represent a bind parameter.

    Public constructor is the ``bindparam()`` function.
    """

    __visit_name__ = 'bindparam'
    
    def __init__(self, key, value, shortname=None, type_=None, unique=False, isoutparam=False):
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

        type\_
          A ``TypeEngine`` object that will be used to pre-process the
          value corresponding to this ``_BindParamClause`` at
          execution time.

        unique
          if True, the key name of this BindParamClause will be 
          modified if another ``_BindParamClause`` of the same
          name already has been located within the containing 
          ``ClauseElement``.
          
        isoutparam
          if True, the parameter should be treated like a stored procedure "OUT"
          parameter.
        """

        self.key = key or "{ANON %d param}" % id(self)
        self.value = value
        self.shortname = shortname or key
        self.unique = unique
        self.isoutparam = isoutparam
        type_ = sqltypes.to_instance(type_)
        if isinstance(type_, sqltypes.NullType) and type(value) in _BindParamClause.type_map:
            self.type = sqltypes.to_instance(_BindParamClause.type_map[type(value)])
        else:
            self.type = type_
    
    # TODO: move to types module, obviously
    type_map = {
        str : sqltypes.String,
        unicode : sqltypes.Unicode,
        int : sqltypes.Integer,
        float : sqltypes.Numeric
    }
    
    def _get_from_objects(self, **modifiers):
        return []

    def typeprocess(self, value, dialect):
        return self.type.dialect_impl(dialect).convert_bind_param(value, dialect)

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
        self.text = BIND_PARAMS.sub(repl, text)
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

    def _copy_internals(self):
        self.bindparams = [b._clone() for b in self.bindparams]

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
        self.operator = kwargs.pop('operator', ColumnOperators.comma_op)
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
        # TODO: not sure if i like the 'group_contents' flag.  need to define the difference between
        # a ClauseList of ClauseLists, and a "flattened" ClauseList of ClauseLists.  flatten() method ?
        if self.group_contents:
            self.clauses.append(_literal_as_text(clause).self_group(against=self.operator))
        else:
            self.clauses.append(_literal_as_text(clause))

    def _copy_internals(self):
        self.clauses = [clause._clone() for clause in self.clauses]

    def get_children(self, **kwargs):
        return self.clauses

    def _get_from_objects(self, **modifiers):
        f = []
        for c in self.clauses:
            f += c._get_from_objects(**modifiers)
        return f

    def self_group(self, against=None):
        if self.group and self.operator != against and PRECEDENCE.get(self.operator, PRECEDENCE[_smallest]) <= PRECEDENCE.get(against, PRECEDENCE[_largest]):
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
            
    key = property(lambda self:self.name or "_calc_")

    def _copy_internals(self):
        self.clause_expr = self.clause_expr._clone()
    
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
        kwargs['operator'] = ColumnOperators.comma_op
        _CalculatedClause.__init__(self, name, **kwargs)
        for c in clauses:
            self.append(c)

    key = property(lambda self:self.name)
    columns = property(lambda self:[self])
    
    def _copy_internals(self):
        _CalculatedClause._copy_internals(self)
        self._clone_from_clause()

    def get_children(self, **kwargs):
        return _CalculatedClause.get_children(self, **kwargs)
        
    def append(self, clause):
        self.clauses.append(_literal_as_binds(clause, self.name))

class _Cast(ColumnElement):

    def __init__(self, clause, totype, **kwargs):
        if not hasattr(clause, 'label'):
            clause = literal(clause)
        self.type = sqltypes.to_instance(totype)
        self.clause = clause
        self.typeclause = _TypeClause(self.type)
        self._distance = 0
        
    def _copy_internals(self):
        self.clause = self.clause._clone()
        self.typeclause = self.typeclause._clone()

    def get_children(self, **kwargs):
        return self.clause, self.typeclause

    def _get_from_objects(self, **modifiers):
        return self.clause._get_from_objects(**modifiers)

    def _make_proxy(self, selectable, name=None):
        if name is not None:
            co = _ColumnClause(name, selectable, type_=self.type)
            co._distance = self._distance + 1
            co.orig_set = self.orig_set
            selectable.columns[name]= co
            return co
        else:
            return self


class _UnaryExpression(ColumnElement):
    def __init__(self, element, operator=None, modifier=None, type_=None, negate=None):
        self.operator = operator
        self.modifier = modifier
        
        self.element = _literal_as_text(element).self_group(against=self.operator or self.modifier)
        self.type = sqltypes.to_instance(type_)
        self.negate = negate
        
    def _get_from_objects(self, **modifiers):
        return self.element._get_from_objects(**modifiers)

    def _copy_internals(self):
        self.element = self.element._clone()

    def get_children(self, **kwargs):
        return self.element,

    def compare(self, other):
        """Compare this ``_UnaryExpression`` against the given ``ClauseElement``."""

        return (
            isinstance(other, _UnaryExpression) and self.operator == other.operator and
            self.modifier == other.modifier and 
            self.element.compare(other.element)
        )

    def _negate(self):
        if self.negate is not None:
            return _UnaryExpression(self.element, operator=self.negate, negate=self.operator, modifier=self.modifier, type_=self.type)
        else:
            return super(_UnaryExpression, self)._negate()
    
    def self_group(self, against):
        if self.operator and PRECEDENCE.get(self.operator, PRECEDENCE[_smallest]) <= PRECEDENCE.get(against, PRECEDENCE[_largest]):
            return _Grouping(self)
        else:
            return self


class _BinaryExpression(ColumnElement):
    """Represent an expression that is ``LEFT <operator> RIGHT``."""
    
    def __init__(self, left, right, operator, type_=None, negate=None):
        self.left = _literal_as_text(left).self_group(against=operator)
        self.right = _literal_as_text(right).self_group(against=operator)
        self.operator = operator
        self.type = sqltypes.to_instance(type_)
        self.negate = negate

    def _get_from_objects(self, **modifiers):
        return self.left._get_from_objects(**modifiers) + self.right._get_from_objects(**modifiers)

    def _copy_internals(self):
        self.left = self.left._clone()
        self.right = self.right._clone()

    def get_children(self, **kwargs):
        return self.left, self.right

    def compare(self, other):
        """Compare this ``_BinaryExpression`` against the given ``_BinaryExpression``."""

        return (
            isinstance(other, _BinaryExpression) and self.operator == other.operator and
                (
                    self.left.compare(other.left) and self.right.compare(other.right)
                    or (
                        self.operator in [operator.eq, operator.ne, operator.add, operator.mul] and
                        self.left.compare(other.right) and self.right.compare(other.left)
                    )
                )
        )
        
    def self_group(self, against=None):
        # use small/large defaults for comparison so that unknown operators are always parenthesized
        if self.operator != against and (PRECEDENCE.get(self.operator, PRECEDENCE[_smallest]) <= PRECEDENCE.get(against, PRECEDENCE[_largest])):
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
        _UnaryExpression.__init__(self, s, operator=Operators.exists)

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
      
    def _hide_froms(self, **modifiers):
        return self._get_from_objects(**modifiers)

class Join(FromClause):
    """represent a ``JOIN`` construct between two ``FromClause``
    elements.
    
    the public constructor function for ``Join`` is the module-level
    ``join()`` function, as well as the ``join()`` method available
    off all ``FromClause`` subclasses.
    
    """
    def __init__(self, left, right, onclause=None, isouter = False):
        self.left = _selectable(left)
        self.right = _selectable(right).self_group()
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
        pkcol = util.Set([c for c in self._flatten_exportable_columns() if c.primary_key])
     
        equivs = {}
        def add_equiv(a, b):
            for x, y in ((a, b), (b, a)):
                if x in equivs:
                    equivs[x].add(y)
                else:
                    equivs[x] = util.Set([y])
                    
        class BinaryVisitor(ClauseVisitor):
            def visit_binary(self, binary):
                if binary.operator == operator.eq:
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
            
        self.__primary_key = ColumnSet([c for c in self._flatten_exportable_columns() if c.primary_key and c not in omit])

    primary_key = property(lambda s:s.__primary_key)

    def self_group(self, against=None):
        return _FromGrouping(self)
        
    def _locate_oid_column(self):
        return self.left.oid_column

    def _exportable_columns(self):
        return [c for c in self.left.columns] + [c for c in self.right.columns]

    def _proxy_column(self, column):
        self._columns[column._label] = column
        for f in column.foreign_keys:
            self._foreign_keys.add(f)
        return column

    def _copy_internals(self):
        self._clone_from_clause()
        self.left = self.left._clone()
        self.right = self.right._clone()
        self.onclause = self.onclause._clone()
        self.__folded_equivalents = None
        self._init_primary_key()

    def get_children(self, **kwargs):
        return self.left, self.right, self.onclause

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

    def _get_folded_equivalents(self, equivs=None):
        if self.__folded_equivalents is not None:
            return self.__folded_equivalents
        if equivs is None:
            equivs = util.Set()
        class LocateEquivs(NoColumnVisitor):
            def visit_binary(self, binary):
                if binary.operator == operator.eq and binary.left.name == binary.right.name:
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

    bind = property(lambda s:s.left.bind or s.right.bind)

    def alias(self, name=None):
        """Create a ``Select`` out of this ``Join`` clause and return an ``Alias`` of it.

        The ``Select`` is not correlating.
        """

        return self.select(use_labels=True, correlate=False).alias(name)

    def _hide_froms(self, **modifiers):
        return self.left._get_from_objects(**modifiers) + self.right._get_from_objects(**modifiers)

    def _get_from_objects(self, **modifiers):
        return [self] + self.onclause._get_from_objects(**modifiers) + self.left._get_from_objects(**modifiers) + self.right._get_from_objects(**modifiers)

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
            alias = '{ANON %d %s}' % (id(self), alias or 'anon')
        self.name = alias
        self.encodedname = alias.encode('ascii', 'backslashreplace')
        
    def is_derived_from(self, fromclause):
        x = self.selectable
        while True:
            if x is fromclause:
                return True
            if isinstance(x, Alias):
                x = x.selectable
            else:
                break
        return False

    def supports_execution(self):
        return self.original.supports_execution()

    def _table_iterator(self):
        return self.original._table_iterator()

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

    def _copy_internals(self):
        self._clone_from_clause()
        self.selectable = self.selectable._clone()
        baseselectable = self.selectable
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.selectable
        self.original = baseselectable

    def get_children(self, **kwargs):
        for c in self.c:
            yield c
        yield self.selectable
        
    def _get_from_objects(self):
        return [self]

    bind = property(lambda s: s.selectable.bind)

class _ColumnElementAdapter(ColumnElement):
    """adapts a ClauseElement which may or may not be a
    ColumnElement subclass itself into an object which
    acts like a ColumnElement.
    """
    
    def __init__(self, elem):
        self.elem = elem
        self.type = getattr(elem, 'type', None)
        self.orig_set = getattr(elem, 'orig_set', util.Set())
        
    key = property(lambda s: s.elem.key)
    _label = property(lambda s: s.elem._label)

    def _copy_internals(self):
        self.elem = self.elem._clone()

    def get_children(self, **kwargs):
        return self.elem,

    def _hide_froms(self, **modifiers):
        return self.elem._hide_froms(**modifiers)

    def _get_from_objects(self, **modifiers):
        return self.elem._get_from_objects(**modifiers)

    def __getattr__(self, attr):
        return getattr(self.elem, attr)

class _Grouping(_ColumnElementAdapter):
    """represent a grouping within a column expression"""
    pass

class _FromGrouping(FromClause):
    """represent a grouping of a FROM clause"""
    __visit_name__ = 'grouping'

    def __init__(self, elem):
        self.elem = elem

    columns = c = property(lambda s:s.elem.columns)

    def get_children(self, **kwargs):
        return self.elem,

    def _hide_froms(self, **modifiers):
        return self.elem._hide_froms(**modifiers)

    def _copy_internals(self):
        self.elem = self.elem._clone()

    def _get_from_objects(self, **modifiers):
        return self.elem._get_from_objects(**modifiers)

    def __getattr__(self, attr):
        return getattr(self.elem, attr)
    
class _Label(ColumnElement):
    """represent a label, as typically applied to any column-level element
    using the ``AS`` sql keyword.
    
    this object is constructed from the ``label()`` module level function
    as well as the ``label()`` method available on all ``ColumnElement``
    subclasses.
    
    """
    
    def __init__(self, name, obj, type_=None):
        while isinstance(obj, _Label):
            obj = obj.obj
        self.name = name or "{ANON %d %s}" % (id(self), getattr(obj, 'name', 'anon'))

        self.obj = obj.self_group(against=Operators.as_)
        self.type = sqltypes.to_instance(type_ or getattr(obj, 'type', None))

    key = property(lambda s: s.name)
    _label = property(lambda s: s.name)
    orig_set = property(lambda s:s.obj.orig_set)

    def expression_element(self):
        return self.obj
        
    def _copy_internals(self):
        self.obj = self.obj._clone()

    def get_children(self, **kwargs):
        return self.obj,

    def _get_from_objects(self, **modifiers):
        return self.obj._get_from_objects(**modifiers)

    def _hide_froms(self, **modifiers):
        return self.obj._hide_froms(**modifiers)
        
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
        if True, the ``_ColumnClause`` is assumed to be an exact expression
        that will be delivered to the output with no quoting rules applied
        regardless of case sensitive settings.  the ``literal_column()`` function is
        usually used to create such a ``_ColumnClause``.
    
    """

    def __init__(self, text, selectable=None, type_=None, _is_oid=False, is_literal=False):
        self.key = self.name = text
        self.encodedname = isinstance(self.name, unicode) and self.name.encode('ascii', 'backslashreplace') or self.name
        self.table = selectable
        self.type = sqltypes.to_instance(type_)
        self._is_oid = _is_oid
        self._distance = 0
        self.__label = None
        self.is_literal = is_literal
    
    def _clone(self):
        # ColumnClause is immutable
        return self
        
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
                while self.__label in self.table.c:
                    self.__label = self.__label + "_%d" % counter
                    counter += 1
            else:
                self.__label = self.name
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
            
    def _get_from_objects(self, **modifiers):
        if self.table is not None:
            return [self.table]
        else:
            return []

    def _bind_param(self, obj):
        return _BindParamClause(self._label, obj, shortname=self.name, type_=self.type, unique=True)

    def _make_proxy(self, selectable, name = None):
        # propigate the "is_literal" flag only if we are keeping our name,
        # otherwise its considered to be a label
        is_literal = self.is_literal and (name is None or name == self.name)
        c = _ColumnClause(name or self.name, selectable=selectable, _is_oid=self._is_oid, type_=self.type, is_literal=is_literal)
        c.orig_set = self.orig_set
        c._distance = self._distance + 1
        if not self._is_oid:
            selectable.columns[c.name] = c
        return c

    def _compare_type(self, obj):
        return self.type

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
        
    def _clone(self):
        # TableClause is immutable
        return self

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

    def insert(self, values = None):
        return insert(self, values=values)

    def update(self, whereclause = None, values = None):
        return update(self, whereclause, values)

    def delete(self, whereclause = None):
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
        
        self.append_order_by(*util.to_list(order_by, []))
        self.append_group_by(*util.to_list(group_by, []))
    
    def as_scalar(self):
        return _ScalarSelect(self)
    
    def apply_labels(self):
        s = self._generate()
        s.use_labels = True
        return s
        
    def label(self, name):
        return self.as_scalar().label(name)
        
    def supports_execution(self):
        return True

    def _generate(self):
        s = self._clone()
        s._clone_from_clause()
        return s
    
    def limit(self, limit):
        s = self._generate()
        s._limit = limit
        return s
    
    def offset(self, offset):
        s = self._generate()
        s._offset = offset
        return s
    
    def order_by(self, *clauses):
        s = self._generate()
        s.append_order_by(*clauses)
        return s

    def group_by(self, *clauses):
        s = self._generate()
        s.append_group_by(*clauses)
        return s

    def append_order_by(self, *clauses):
        if clauses == [None]:
            self._order_by_clause = ClauseList()
        else:
            if getattr(self, '_order_by_clause', None):
                clauses = list(self._order_by_clause) + list(clauses)
            self._order_by_clause = ClauseList(*clauses)

    def append_group_by(self, *clauses):
        if clauses == [None]:
            self._group_by_clause = ClauseList()
        else:
            if getattr(self, '_group_by_clause', None):
                clauses = list(self._group_by_clause) + list(clauses)
            self._group_by_clause = ClauseList(*clauses)
            
    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def _get_from_objects(self, is_where=False, **modifiers):
        if is_where:
            return []
        else:
            return [self]

class _ScalarSelect(_Grouping):
    __visit_name__ = 'grouping'

    def __init__(self, elem):
        super(_ScalarSelect, self).__init__(elem)
        self.type = list(elem.inner_columns)[0].type

    def _no_cols(self):
        raise exceptions.InvalidRequestError("Scalar Select expression has no columns; use this object directly within a column-level expression.")
    c = property(_no_cols)
    columns = c
    
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

    name = property(lambda s:s.keyword + " statement")

    def self_group(self, against=None):
        return _FromGrouping(self)

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

    def _copy_internals(self):
        self._clone_from_clause()
        self._col_map = {}
        self.selects = [s._clone() for s in self.selects]
        for attr in ('_order_by_clause', '_group_by_clause'):
            if getattr(self, attr) is not None:
                setattr(self, attr, getattr(self, attr)._clone())

    def get_children(self, column_collections=True, **kwargs):
        return (column_collections and list(self.c) or []) + \
            [self._order_by_clause, self._group_by_clause] + list(self.selects)

    def _table_iterator(self):
        for s in self.selects:
            for t in s._table_iterator():
                yield t
            
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

    def __init__(self, columns, whereclause=None, from_obj=None, distinct=False, having=None, correlate=True, prefixes=None, **kwargs):
        """construct a Select object.
        
        The public constructor for Select is the [sqlalchemy.sql#select()] function; 
        see that function for argument descriptions.
        """
        
        self._should_correlate = correlate
        self._distinct = distinct

        self._raw_columns = []
        self.__correlate = util.Set()
        self._froms = util.OrderedSet()
        self._whereclause = None
        self._having = None
        self._prefixes = []
        
        if columns is not None:
            for c in columns:
                self.append_column(c)

        if from_obj is not None:
            for f in from_obj:
                self.append_from(f)

        if whereclause is not None:
            self.append_whereclause(whereclause)
            
        if having is not None:
            self.append_having(having)

        _SelectBaseMixin.__init__(self, **kwargs)

    def _get_display_froms(self, existing_froms=None):
        """return the full list of 'from' clauses to be displayed.
        
        takes into account a set of existing froms which
        may be rendered in the FROM clause of enclosing selects;
        this Select may want to leave those absent if it is automatically
        correlating.
        """

        froms = util.OrderedSet()
        hide_froms = util.Set()
        
        for col in self._raw_columns:
            for f in col._hide_froms():
                hide_froms.add(f)
            for f in col._get_from_objects():
                froms.add(f)

        if self._whereclause is not None:
            for f in self._whereclause._get_from_objects(is_where=True):
                froms.add(f)
        
        for elem in self._froms:
            froms.add(elem)
            for f in elem._get_from_objects():
                froms.add(f)

        for elem in froms:
            for f in elem._hide_froms():
                hide_froms.add(f)

        froms = froms.difference(hide_froms)
        
        if len(froms) > 1:
            corr = self.__correlate
            if self._should_correlate and existing_froms is not None:
                corr = existing_froms.union(corr)
            f = froms.difference(corr)
            if len(f) == 0:
                raise exceptions.InvalidRequestError("Select statement '%s' is overcorrelated; returned no 'from' clauses" % str(self.__dont_correlate()))
            return f
        else:
            return froms
    
    froms = property(_get_display_froms, doc="""Return a list of all FromClause elements which will be applied to the FROM clause of the resulting statement.""")

    name = property(lambda self:"Select statement")

    def locate_all_froms(self):
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
        return froms

    def _get_inner_columns(self):
        for c in self._raw_columns:
            if isinstance(c, Selectable):
                for co in c.columns:
                    yield co
            else:
                yield c
            
    inner_columns = property(_get_inner_columns)
    
    def _copy_internals(self):
        self._clone_from_clause()
        self._raw_columns = [c._clone() for c in self._raw_columns]
        self._recorrelate_froms([(f, f._clone()) for f in self._froms])
        for attr in ('_whereclause', '_having', '_order_by_clause', '_group_by_clause'):
            if getattr(self, attr) is not None:
                setattr(self, attr, getattr(self, attr)._clone())

    def get_children(self, column_collections=True, **kwargs):
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
        s = self._generate()
        s.append_column(column)
        return s
    
    def where(self, whereclause):
        s = self._generate()
        s.append_whereclause(whereclause)
        return s
    
    def having(self, having):
        s = self._generate()
        s.append_having(having)
        return s
    
    def distinct(self):
        s = self._generate()
        s._distinct = True
        return s

    def prefix_with(self, clause):
        s = self._generate()
        s.append_prefix(clause)
        return s
        
    def select_from(self, fromclause):
        s = self._generate()
        s.append_from(fromclause)
        return s
    
    def __dont_correlate(self):
        s = self._generate()
        s._should_correlate = False
        return s
        
    def correlate(self, fromclause):
        s = self._generate()
        s._should_correlate=False
        if fromclause is None:
            s.__correlate = util.Set()
        else:
            s.append_correlation(fromclause)
        return s
    
    def append_correlation(self, fromclause):
        self.__correlate.add(fromclause)
            
    def append_column(self, column):
        column = _literal_as_column(column)

        if isinstance(column, _ScalarSelect):
            column = column.self_group(against=ColumnOperators.comma_op)

        self._raw_columns.append(column)
    
    def append_prefix(self, clause):
        clause = _literal_as_text(clause)
        self._prefixes.append(clause)
        
    def append_whereclause(self, whereclause):
        if self._whereclause  is not None:
            self._whereclause = and_(self._whereclause, _literal_as_text(whereclause))
        else:
            self._whereclause = _literal_as_text(whereclause)
            
    def append_having(self, having):
        if self._having is not None:
            self._having = and_(self._having, _literal_as_text(having))
        else:
            self._having = _literal_as_text(having)

    def append_from(self, fromclause):
        if _is_literal(fromclause):
            fromclause = FromClause(fromclause)
        self._froms.add(fromclause)

    def _exportable_columns(self):
        return [c for c in self._raw_columns if isinstance(c, (Selectable, ColumnElement))]
        
    def _proxy_column(self, column):
        if self.use_labels:
            return column._make_proxy(self, name=column._label)
        else:
            return column._make_proxy(self)

    def self_group(self, against=None):
        if isinstance(against, CompoundSelect):
            return self
        return _FromGrouping(self)

    def _locate_oid_column(self):
        for f in self.locate_all_froms():
            if f is self:
                # we might be in our own _froms list if a column with us as the parent is attached,
                # which includes textual columns.
                continue
            oid = f.oid_column
            if oid is not None:
                return oid
        else:
            return None

    def union(self, other, **kwargs):
        return union(self, other, **kwargs)

    def union_all(self, other, **kwargs):
        return union_all(self, other, **kwargs)

    def except_(self, other, **kwargs):
        return except_(self, other, **kwargs)

    def except_all(self, other, **kwargs):
        return except_all(self, other, **kwargs)

    def intersect(self, other, **kwargs):
        return intersect(self, other, **kwargs)

    def intersect_all(self, other, **kwargs):
        return intersect_all(self, other, **kwargs)

    def _table_iterator(self):
        for t in NoColumnVisitor().iterate(self):
            if isinstance(t, TableClause):
                yield t
        
    def _find_engine(self):
        """Try to return a Engine, either explicitly set in this
        object, or searched within the from clauses for one.
        """

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

class _UpdateBase(ClauseElement):
    """Form the base for ``INSERT``, ``UPDATE``, and ``DELETE`` statements."""

    def supports_execution(self):
        return True

    def _table_iterator(self):
        return iter([self.table])
        
    def _process_colparams(self, parameters):
        """Receive the *values* of an ``INSERT`` or ``UPDATE``
        statement and construct appropriate bind parameters.
        """

        if parameters is None:
            return None

        if isinstance(parameters, (list, tuple)):
            pp = {}
            i = 0
            for c in self.table.c:
                pp[c.key] = parameters[i]
                i +=1
            parameters = pp

        for key in parameters.keys():
            value = parameters[key]
            if isinstance(value, ClauseElement):
                parameters[key] = value.self_group()
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

class Insert(_UpdateBase):
    def __init__(self, table, values=None):
        self.table = table
        self.select = None
        self.parameters = self._process_colparams(values)

    def get_children(self, **kwargs):
        if self.select is not None:
            return self.select,
        else:
            return ()

    def _copy_internals(self):
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
    def __init__(self, table, whereclause, values=None):
        self.table = table
        self._whereclause = whereclause
        self.parameters = self._process_colparams(values)

    def get_children(self, **kwargs):
        if self._whereclause is not None:
            return self._whereclause,
        else:
            return ()

    def _copy_internals(self):
        self._whereclause = self._whereclause._clone()
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

    def _copy_internals(self):
        self._whereclause = self._whereclause._clone()

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
