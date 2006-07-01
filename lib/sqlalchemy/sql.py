# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines the base components of SQL expression trees."""

from sqlalchemy import util, exceptions
from sqlalchemy import types as sqltypes
import string, re, random, sets
types = __import__('types')

__all__ = ['text', 'table', 'column', 'func', 'select', 'update', 'insert', 'delete', 'join', 'and_', 'or_', 'not_', 'between_', 'case', 'cast', 'union', 'union_all', 'null', 'desc', 'asc', 'outerjoin', 'alias', 'subquery', 'literal', 'bindparam', 'exists']

def desc(column):
    """returns a descending ORDER BY clause element, e.g.:
    
    order_by = [desc(table1.mycol)]
    """    
    return CompoundClause(None, column, "DESC")

def asc(column):
    """returns an ascending ORDER BY clause element, e.g.:
    
    order_by = [asc(table1.mycol)]
    """
    return CompoundClause(None, column, "ASC")

def outerjoin(left, right, onclause=None, **kwargs):
    """returns an OUTER JOIN clause element, given the left and right hand expressions,
    as well as the ON condition's expression.  To chain joins together, use the resulting
    Join object's "join()" or "outerjoin()" methods."""
    return Join(left, right, onclause, isouter = True, **kwargs)

def join(left, right, onclause=None, **kwargs):
    """returns a JOIN clause element (regular inner join), given the left and right 
    hand expressions, as well as the ON condition's expression.  To chain joins 
    together, use the resulting Join object's "join()" or "outerjoin()" methods."""
    return Join(left, right, onclause, **kwargs)

def select(columns=None, whereclause = None, from_obj = [], **kwargs):
    """returns a SELECT clause element.
    
    this can also be called via the table's select() method.
    
    'columns' is a list of columns and/or selectable items to select columns from
    'whereclause' is a text or ClauseElement expression which will form the WHERE clause
    'from_obj' is an list of additional "FROM" objects, such as Join objects, which will 
    extend or override the default "from" objects created from the column list and the 
    whereclause.
    **kwargs - additional parameters for the Select object.
    """
    return Select(columns, whereclause = whereclause, from_obj = from_obj, **kwargs)

def subquery(alias, *args, **kwargs):
    return Select(*args, **kwargs).alias(alias)

    
def insert(table, values = None, **kwargs):
    """returns an INSERT clause element.  
    
    This can also be called from a table directly via the table's insert() method.
    
    'table' is the table to be inserted into.
    
    'values' is a dictionary which specifies the column specifications of the INSERT, 
    and is optional.  If left as None, the column specifications are determined from the 
    bind parameters used during the compile phase of the INSERT statement.  If the 
    bind parameters also are None during the compile phase, then the column
    specifications will be generated from the full list of table columns.

    If both 'values' and compile-time bind parameters are present, the compile-time 
    bind parameters override the information specified within 'values' on a per-key basis.

    The keys within 'values' can be either Column objects or their string identifiers.  
    Each key may reference one of: a literal data value (i.e. string, number, etc.), a Column object,
    or a SELECT statement.  If a SELECT statement is specified which references this INSERT 
    statement's table, the statement will be correlated against the INSERT statement.  
    """
    return Insert(table, values, **kwargs)

def update(table, whereclause = None, values = None, **kwargs):
    """returns an UPDATE clause element.   
    
    This can also be called from a table directly via the table's update() method.
    
    'table' is the table to be updated.
    'whereclause' is a ClauseElement describing the WHERE condition of the UPDATE statement.
    'values' is a dictionary which specifies the SET conditions of the UPDATE, and is
    optional. If left as None, the SET conditions are determined from the bind parameters
    used during the compile phase of the UPDATE statement.  If the bind parameters also are
    None during the compile phase, then the SET conditions will be generated from the full
    list of table columns.

    If both 'values' and compile-time bind parameters are present, the compile-time bind
    parameters override the information specified within 'values' on a per-key basis.

    The keys within 'values' can be either Column objects or their string identifiers. Each
    key may reference one of: a literal data value (i.e. string, number, etc.), a Column
    object, or a SELECT statement.  If a SELECT statement is specified which references this
    UPDATE statement's table, the statement will be correlated against the UPDATE statement.
    """
    return Update(table, whereclause, values, **kwargs)

def delete(table, whereclause = None, **kwargs):
    """returns a DELETE clause element.  
    
    This can also be called from a table directly via the table's delete() method.
    
    'table' is the table to be updated.
    'whereclause' is a ClauseElement describing the WHERE condition of the UPDATE statement.
    """
    return Delete(table, whereclause, **kwargs)

def and_(*clauses):
    """joins a list of clauses together by the AND operator.  the & operator can be used as well."""
    return _compound_clause('AND', *clauses)

def or_(*clauses):
    """joins a list of clauses together by the OR operator.  the | operator can be used as well."""
    return _compound_clause('OR', *clauses)

def not_(clause):
    """returns a negation of the given clause, i.e. NOT(clause).  the ~ operator can be used as well."""
    clause.parens=True
    return BooleanExpression(TextClause("NOT"), clause, None)

def between(ctest, cleft, cright):
    """ returns BETWEEN predicate clause (clausetest BETWEEN clauseleft AND clauseright).
    
    this is better called off a ColumnElement directly, i.e.
    
    column.between(value1, value2). 
    """
    return BooleanExpression(ctest, and_(_check_literal(cleft, ctest.type), _check_literal(cright, ctest.type)), 'BETWEEN')
between_ = between

def case(whens, value=None, else_=None):
    """ SQL CASE statement -- whens are a sequence of pairs to be translated into "when / then" clauses;
        optional [value] for simple case statements, and [else_] for case defaults """
    whenlist = [CompoundClause(None, 'WHEN', c, 'THEN', r) for (c,r) in whens]
    if else_:
        whenlist.append(CompoundClause(None, 'ELSE', else_))
    cc = CalculatedClause(None, 'CASE', value, *whenlist + ['END'])
    for c in cc.clauses:
        c.parens = False
    return cc
   
def cast(clause, totype, **kwargs):
    """ returns CAST function CAST(clause AS totype) 
        Use with a sqlalchemy.types.TypeEngine object, i.e
        cast(table.c.unit_price * table.c.qty, Numeric(10,4))
         or
        cast(table.c.timestamp, DATE)
    """
    return Cast(clause, totype, **kwargs)


def exists(*args, **params):
    params['correlate'] = True
    s = select(*args, **params)
    return BooleanExpression(TextClause("EXISTS"), s, None)

def union(*selects, **params):
    return _compound_select('UNION', *selects, **params)

def union_all(*selects, **params):
    return _compound_select('UNION ALL', *selects, **params)

def alias(*args, **params):
    return Alias(*args, **params)

def _check_literal(value, type):
    if _is_literal(value):
        return literal(value, type)
    else:
        return value

def literal(value, type=None):
    """returns a literal clause, bound to a bind parameter.  
    
    literal clauses are created automatically when used as the right-hand 
    side of a boolean or math operation against a column object.  use this 
    function when a literal is needed on the left-hand side (and optionally on the right as well).
    
    the optional type parameter is a sqlalchemy.types.TypeEngine object which indicates bind-parameter
    and result-set translation for this literal.
    """
    return BindParamClause('literal', value, type=type)

def label(name, obj):
    """returns a Label object for the given selectable, used in the column list for a select statement."""
    return Label(name, obj)
    
def column(text, table=None, type=None):
    """returns a textual column clause, relative to a table.  this is also the primitive version of
    a schema.Column which is a subclass. """
    return ColumnClause(text, table, type)

def table(name, *columns):
    """returns a table clause.  this is a primitive version of the schema.Table object, which is a subclass
    of this object."""
    return TableClause(name, *columns)
    
def bindparam(key, value = None, type=None):
    """creates a bind parameter clause with the given key.  
    
    An optional default value can be specified by the value parameter, and the optional type parameter
    is a sqlalchemy.types.TypeEngine object which indicates bind-parameter and result-set translation for
    this bind parameter."""
    if isinstance(key, ColumnClause):
        return BindParamClause(key.name, value, type=key.type)
    else:
        return BindParamClause(key, value, type=type)

def text(text, engine=None, *args, **kwargs):
    """creates literal text to be inserted into a query.  
    
    When constructing a query from a select(), update(), insert() or delete(), using 
    plain strings for argument values will usually result in text objects being created
    automatically.  Use this function when creating textual clauses outside of other
    ClauseElement objects, or optionally wherever plain text is to be used.
    
    Arguments include:

    text - the text of the SQL statement to be created.  use :<param> to specify
    bind parameters; they will be compiled to their engine-specific format.

    engine - an optional engine to be used for this text query.

    bindparams - a list of bindparam() instances which can be used to define the
    types and/or initial values for the bind parameters within the textual statement;
    the keynames of the bindparams must match those within the text of the statement.
    The types will be used for pre-processing on bind values.

    typemap - a dictionary mapping the names of columns represented in the SELECT
    clause of the textual statement to type objects, which will be used to perform
    post-processing on columns within the result set (for textual statements that 
    produce result sets)."""
    return TextClause(text, engine=engine, *args, **kwargs)

def null():
    """returns a Null object, which compiles to NULL in a sql statement."""
    return Null()

class FunctionGateway(object):
    """returns a callable based on an attribute name, which then returns a Function 
    object with that name."""
    def __getattr__(self, name):
        return getattr(FunctionGenerator(), name)
func = FunctionGateway()

def _compound_clause(keyword, *clauses):
    return CompoundClause(keyword, *clauses)

def _compound_select(keyword, *selects, **kwargs):
    return CompoundSelect(keyword, *selects, **kwargs)

def _is_literal(element):
    return not isinstance(element, ClauseElement)

def is_column(col):
    return isinstance(col, ColumnElement)

class Engine(object):
    """represents a 'thing that can produce Compiled objects and execute them'."""
    def execute_compiled(self, compiled, parameters, echo=None, **kwargs):
        raise NotImplementedError()
    def compiler(self, statement, parameters, **kwargs):
        raise NotImplementedError()

class AbstractDialect(object):
    """represents the behavior of a particular database.  Used by Compiled objects."""
    pass
    
class ClauseParameters(util.OrderedDict):
    """represents a dictionary/iterator of bind parameter key names/values.  Includes parameters compiled with a Compiled object as well as additional arguments passed to the Compiled object's get_params() method.  Parameter values will be converted as per the TypeEngine objects present in the bind parameter objects.  The non-converted value can be retrieved via the get_original method.  For Compiled objects that compile positional parameters, the values() iteration of the object will return the parameter values in the correct order."""
    def __init__(self, dialect):
        super(ClauseParameters, self).__init__(self)
        self.dialect=dialect
        self.binds = {}
    def set_parameter(self, key, value, bindparam):
        self[key] = value
        self.binds[key] = bindparam
    def get_original(self, key):
        """returns the given parameter as it was originally placed in this ClauseParameters object, without any Type conversion"""
        return super(ClauseParameters, self).__getitem__(key)
    def __getitem__(self, key):
        v = super(ClauseParameters, self).__getitem__(key)
        if self.binds.has_key(key):
            v = self.binds[key].typeprocess(v, self.dialect)
        return v
    def values(self):
        return [self[key] for key in self]
    def get_original_dict(self):
        return self.copy()
    def get_raw_dict(self):
        d = {}
        for k in self:
            d[k] = self[k]
        return d
        
class ClauseVisitor(object):
    """Defines the visiting of ClauseElements."""
    def visit_column(self, column):pass
    def visit_table(self, column):pass
    def visit_fromclause(self, fromclause):pass
    def visit_bindparam(self, bindparam):pass
    def visit_textclause(self, textclause):pass
    def visit_compound(self, compound):pass
    def visit_compound_select(self, compound):pass
    def visit_binary(self, binary):pass
    def visit_alias(self, alias):pass
    def visit_select(self, select):pass
    def visit_join(self, join):pass
    def visit_null(self, null):pass
    def visit_clauselist(self, list):pass
    def visit_calculatedclause(self, calcclause):pass
    def visit_function(self, func):pass
    def visit_cast(self, cast):pass
    def visit_label(self, label):pass
    def visit_typeclause(self, typeclause):pass
            
class Compiled(ClauseVisitor):
    """represents a compiled SQL expression.  the __str__ method of the Compiled object
    should produce the actual text of the statement.  Compiled objects are specific to the
    database library that created them, and also may or may not be specific to the columns
    referenced within a particular set of bind parameters.  In no case should the Compiled
    object be dependent on the actual values of those bind parameters, even though it may
    reference those values as defaults."""

    def __init__(self, dialect, statement, parameters, engine=None):
        """constructs a new Compiled object.
        
        statement - ClauseElement to be compiled
        
        parameters - optional dictionary indicating a set of bind parameters
        specified with this Compiled object.  These parameters are the "default"
        values corresponding to the ClauseElement's BindParamClauses when the Compiled 
        is executed.   In the case of an INSERT or UPDATE statement, these parameters 
        will also result in the creation of new BindParamClause objects for each key
        and will also affect the generated column list in an INSERT statement and the SET 
        clauses of an UPDATE statement.  The keys of the parameter dictionary can
        either be the string names of columns or ColumnClause objects.
        
        engine - optional Engine to compile this statement against"""
        self.dialect = dialect
        self.statement = statement
        self.parameters = parameters
        self.engine = engine
        
    def __str__(self):
        """returns the string text of the generated SQL statement."""
        raise NotImplementedError()
    def get_params(self, **params):
        """returns the bind params for this compiled object.
        
        Will start with the default parameters specified when this Compiled object
        was first constructed, and will override those values with those sent via
        **params, which are key/value pairs.  Each key should match one of the 
        BindParamClause objects compiled into this object; either the "key" or 
        "shortname" property of the BindParamClause.
        """
        raise NotImplementedError()

    def compile(self):
        self.statement.accept_visitor(self)
        self.after_compile()

    def execute(self, *multiparams, **params):
        """executes this compiled object using the AbstractEngine it is bound to."""
        e = self.engine
        if e is None:
            raise exceptions.InvalidRequestError("This Compiled object is not bound to any engine.")
        return e.execute_compiled(self, *multiparams, **params)

    def scalar(self, *multiparams, **params):
        """executes this compiled object via the execute() method, then 
        returns the first column of the first row.  Useful for executing functions,
        sequences, rowcounts, etc."""
        # we are still going off the assumption that fetching only the first row
        # in a result set is not performance-wise any different than specifying limit=1
        # else we'd have to construct a copy of the select() object with the limit
        # installed (else if we change the existing select, not threadsafe)
        r = self.execute(*multiparams, **params)
        row = r.fetchone()
        try:
            if row is not None:
                return row[0]
            else:
                return None
        finally:
            r.close()

class Executor(object):
    """context-sensitive executor for the using() function."""
    def __init__(self, clauseelement, abstractengine=None):
        self.engine=abstractengine
        self.clauseelement = clauseelement
    def execute(self, *multiparams, **params):
        return self.clauseelement.execute_using(self.engine)
    def scalar(self, *multiparams, **params):
        return self.clauseelement.scalar_using(self.engine)
            
class ClauseElement(object):
    """base class for elements of a programmatically constructed SQL expression."""
    def _get_from_objects(self):
        """returns objects represented in this ClauseElement that should be added to the
        FROM list of a query, when this ClauseElement is placed in the column clause of a Select
        statement."""
        raise NotImplementedError(repr(self))
    def _process_from_dict(self, data, asfrom):
        """given a dictionary attached to a Select object, places the appropriate
        FROM objects in the dictionary corresponding to this ClauseElement,
        and possibly removes or modifies others."""
        for f in self._get_from_objects():
            data.setdefault(f, f)
        if asfrom:
            data[self] = self
    def compare(self, other):
        """compares this ClauseElement to the given ClauseElement.
        
        Subclasses should override the default behavior, which is a straight
        identity comparison."""
        return self is other
        
    def accept_visitor(self, visitor):
        """accepts a ClauseVisitor and calls the appropriate visit_xxx method."""
        raise NotImplementedError(repr(self))

    def copy_container(self):
        """should return a copy of this ClauseElement, iff this ClauseElement contains other
        ClauseElements.  Otherwise, it should be left alone to return self.  This is used to
        create copies of expression trees that still reference the same "leaf nodes".  The
        new structure can then be restructured without affecting the original."""
        return self

    def is_selectable(self):
        """returns True if this ClauseElement is Selectable, i.e. it contains a list of Column
        objects and can be used as the target of a select statement."""
        return False

    def _find_engine(self):
        """default strategy for locating an engine within the clause element.
        relies upon a local engine property, or looks in the "from" objects which 
        ultimately have to contain Tables or TableClauses. """
        try:
            if self._engine is not None:
                return self._engine
        except AttributeError:
            pass
        for f in self._get_from_objects():
            if f is self:
                continue
            engine = f.engine
            if engine is not None: 
                return engine
        else:
            return None
            
    engine = property(lambda s: s._find_engine(), doc="attempts to locate a Engine within this ClauseElement structure, or returns None if none found.")

    def using(self, abstractengine):
        return Executor(self, abstractengine)

    def execute_using(self, engine, *multiparams, **params):
        compile_params = self._conv_params(*multiparams, **params)
        return self.compile(engine=engine, parameters=compile_params).execute(*multiparams, **params)
    def scalar_using(self, engine, *multiparams, **params):
        compile_params = self._conv_params(*multiparams, **params)
        return self.compile(engine=engine, parameters=compile_params).scalar(*multiparams, **params)
    def _conv_params(self, *multiparams, **params):
        if len(multiparams):
            return multiparams[0]
        else:
            return params
    def compile(self, engine=None, parameters=None, compiler=None, dialect=None):
        """compiles this SQL expression.
        
        Uses the given Compiler, or the given AbstractDialect or Engine to create a Compiler.  If no compiler
        arguments are given, tries to use the underlying Engine this ClauseElement is bound
        to to create a Compiler, if any.  Finally, if there is no bound Engine, uses an ANSIDialect
        to create a default Compiler.
        
        bindparams is a dictionary representing the default bind parameters to be used with 
        the statement.  if the bindparams is a list, it is assumed to be a list of dictionaries
        and the first dictionary in the list is used with which to compile against.
        The bind parameters can in some cases determine the output of the compilation, such as for UPDATE
        and INSERT statements the bind parameters that are present determine the SET and VALUES clause of 
        those statements.
        """

        if (isinstance(parameters, list) or isinstance(parameters, tuple)):
            parameters = parameters[0]
        
        if compiler is None:
            if dialect is not None:
                compiler = dialect.compiler(self, parameters)
            elif engine is not None:
                compiler = engine.compiler(self, parameters)
            elif self.engine is not None:
                compiler = self.engine.compiler(self, parameters)
                
        if compiler is None:
            import sqlalchemy.ansisql as ansisql
            compiler = ansisql.ANSIDialect().compiler(self, parameters=parameters)
        compiler.compile()
        return compiler

    def __str__(self):
        return str(self.compile())
        
    def execute(self, *multiparams, **params):
        return self.execute_using(self.engine, *multiparams, **params)

    def scalar(self, *multiparams, **params):
        return self.scalar_using(self.engine, *multiparams, **params)

    def __and__(self, other):
        return and_(self, other)
    def __or__(self, other):
        return or_(self, other)
    def __invert__(self):
        return not_(self)

class CompareMixin(object):
    """defines comparison operations for ClauseElements."""
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
        return self._compare('LIKE', other)
    def in_(self, *other):
        if len(other) == 0:
            return self.__eq__(None)
        elif len(other) == 1 and not isinstance(other[0], Selectable):
            return self.__eq__(other[0])
        elif _is_literal(other[0]):
            return self._compare('IN', ClauseList(parens=True, *[self._bind_param(o) for o in other]))
        else:
            # assume *other is a list of selects.
            # so put them in a UNION.  if theres only one, you just get one SELECT 
            # statement out of it.
            return self._compare('IN', union(parens=True, *other))
    def startswith(self, other):
        return self._compare('LIKE', str(other) + "%")
    def endswith(self, other):
        return self._compare('LIKE', "%" + str(other))
    def label(self, name):
        return Label(name, self, self.type)
    def distinct(self):
        return CompoundClause(None,"DISTINCT", self)
    def between(self, cleft, cright):
        return BooleanExpression(self, and_(self._check_literal(cleft), self._check_literal(cright)), 'BETWEEN')
    def op(self, operator):
        return lambda other: self._compare(operator, other)
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
        return BindParamClause('literal', obj, shortname=None, type=self.type)
    def _check_literal(self, other):
        if _is_literal(other):
            return self._bind_param(other)
        else:
            return other
    def _compare(self, operator, obj):
        if obj is None or isinstance(obj, Null):
            if operator == '=':
                return BooleanExpression(self._compare_self(), null(), 'IS')
            elif operator == '!=':
                return BooleanExpression(self._compare_self(), null(), 'IS NOT')
            else:
                raise exceptions.ArgumentError("Only '='/'!=' operators can be used with NULL")
        else:
            obj = self._check_literal(obj)

        return BooleanExpression(self._compare_self(), obj, operator, type=self._compare_type(obj))
    def _operate(self, operator, obj):
        if _is_literal(obj):
            obj = self._bind_param(obj)
        return BinaryExpression(self._compare_self(), obj, operator, type=self._compare_type(obj))
    def _compare_self(self):
        """allows ColumnImpl to return its Column object for usage in ClauseElements, all others to
        just return self"""
        return self
    def _compare_type(self, obj):
        """allows subclasses to override the type used in constructing BinaryClause objects.  Default return
        value is the type of the given object."""
        return obj.type
        
class Selectable(ClauseElement):
    """represents a column list-holding object."""

    def accept_visitor(self, visitor):
        raise NotImplementedError(repr(self))
    def is_selectable(self):
        return True
    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)
    def _group_parenthesized(self):
        """indicates if this Selectable requires parenthesis when grouped into a compound
        statement"""
        return True

class ColumnElement(Selectable, CompareMixin):
    """represents a column element within the list of a Selectable's columns.
    A ColumnElement can either be directly associated with a TableClause, or
    a free-standing textual column with no table, or is a "proxy" column, indicating
    it is placed on a Selectable such as an Alias or Select statement and ultimately corresponds 
    to a TableClause-attached column (or in the case of a CompositeSelect, a proxy ColumnElement
    may correspond to several TableClause-attached columns)."""
    
    primary_key = property(lambda self:getattr(self, '_primary_key', False), doc="primary key flag.  indicates if this Column represents part or whole of a primary key.")
    foreign_key = property(lambda self:getattr(self, '_foreign_key', False), doc="foreign key accessor.  points to a ForeignKey object which represents a Foreign Key placed on this column's ultimate ancestor.")
    columns = property(lambda self:[self], doc="Columns accessor which just returns self, to provide compatibility with Selectable objects.")

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
    orig_set = property(_get_orig_set, _set_orig_set,doc="""a Set containing TableClause-bound, non-proxied ColumnElements for which this ColumnElement is a proxy.  In all cases except for a column proxied from a Union (i.e. CompoundSelect), this set will be just one element.""")

    def shares_lineage(self, othercolumn):
        """returns True if the given ColumnElement has a common ancestor to this ColumnElement."""
        for c in self.orig_set:
            if c in othercolumn.orig_set:
                return True
        else:
            return False
    def _make_proxy(self, selectable, name=None):
        """creates a new ColumnElement representing this ColumnElement as it appears in the select list
        of a descending selectable.  The default implementation returns a ColumnClause if a name is given,
        else just returns self."""
        if name is not None:
            co = ColumnClause(name, selectable)
            co.orig_set = self.orig_set
            selectable.columns[name]= co
            return co
        else:
            return self

class FromClause(Selectable):
    """represents an element that can be used within the FROM clause of a SELECT statement."""
    def __init__(self, from_name = None):
        self.from_name = self.name = from_name
    def _display_name(self):
        if self.named_with_column():
            return self.name
        else:
            return None
    displayname = property(_display_name)
    def _get_from_objects(self):
        # this could also be [self], at the moment it doesnt matter to the Select object
        return []
    def default_order_by(self):
        return [self.oid_column]
    def accept_visitor(self, visitor): 
        visitor.visit_fromclause(self)
    def count(self, whereclause=None, **params):
        if len(self.primary_key):
            col = self.primary_key[0]
        else:
            col = list(self.columns)[0]
        return select([func.count(col).label('tbl_row_count')], whereclause, from_obj=[self], **params)
    def join(self, right, *args, **kwargs):
        return Join(self, right, *args, **kwargs)
    def outerjoin(self, right, *args, **kwargs):
        return Join(self, right, isouter = True, *args, **kwargs)
    def alias(self, name=None):
        return Alias(self, name)
    def named_with_column(self):
        """True if the name of this FromClause may be prepended to a column in a generated SQL statement"""
        return False
    def _locate_oid_column(self):
        """subclasses override this to return an appropriate OID column"""
        return None
    def _get_oid_column(self):
        if not hasattr(self, '_oid_column'):
            self._oid_column = self._locate_oid_column()
        return self._oid_column
    def corresponding_column(self, column, raiseerr=True, keys_ok=False):
        """given a ColumnElement, return the ColumnElement object from this 
        Selectable which corresponds to that original Column via a proxy relationship."""
        for c in column.orig_set:
            try:
                return self.original_columns[c]
            except KeyError:
                pass
        else:
            if keys_ok:
                try:
                    return self.c[column.key]
                except KeyError:
                    pass
            if not raiseerr:
                return None
            else:
                raise exceptions.InvalidRequestError("Given column '%s', attached to table '%s', failed to locate a corresponding column from table '%s'" % (str(column), str(column.table), self.name))
                
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
    original_columns = property(lambda s:s._get_exported_attribute('_orig_cols'), doc="a dictionary mapping an original Table-bound column to a proxied column in this FromClause.")
    oid_column = property(_get_oid_column)
    
    def _export_columns(self):
        """this method is called the first time any of the "exported attrbutes" are called. it receives from the Selectable
        a list of all columns to be exported and creates "proxy" columns for each one."""
        if hasattr(self, '_columns'):
            # TODO: put a mutex here ?  this is a key place for threading probs
            return
        self._columns = util.OrderedProperties()
        self._primary_key = []
        self._foreign_keys = []
        self._orig_cols = {}
        export = self._exportable_columns()
        for column in export:
            if column.is_selectable():
                for co in column.columns:
                    cp = self._proxy_column(co)
                    for ci in cp.orig_set:
                        self._orig_cols[ci] = cp
        if self.oid_column is not None:
            for ci in self.oid_column.orig_set:
                self._orig_cols[ci] = self.oid_column
    def _exportable_columns(self):
        return []
    def _proxy_column(self, column):
        return column._make_proxy(self)
    
class BindParamClause(ClauseElement, CompareMixin):
    """represents a bind parameter.  public constructor is the bindparam() function."""
    def __init__(self, key, value, shortname=None, type=None):
        self.key = key
        self.value = value
        self.shortname = shortname
        self.type = sqltypes.to_instance(type)
    def accept_visitor(self, visitor):
        visitor.visit_bindparam(self)
    def _get_from_objects(self):
        return []
    def copy_container(self):
        return BindParamClause(self.key, self.value, self.shortname, self.type)
    def typeprocess(self, value, dialect):
        return self.type.dialect_impl(dialect).convert_bind_param(value, dialect)
    def compare(self, other):
        """compares this BindParamClause to the given clause.
        
        Since compare() is meant to compare statement syntax, this method
        returns True if the two BindParamClauses have just the same type."""
        return isinstance(other, BindParamClause) and other.type.__class__ == self.type.__class__
    def _make_proxy(self, selectable, name = None):
        return self
#        return self.obj._make_proxy(selectable, name=self.name)

class TypeClause(ClauseElement):
    """handles a type keyword in a SQL statement"""
    def __init__(self, type):
        self.type = type
    def accept_visitor(self, visitor):
        visitor.visit_typeclause(self)
    def _get_from_objects(self): 
        return []

class TextClause(ClauseElement):
    """represents literal a SQL text fragment.  public constructor is the 
    text() function.  
    
    TextClauses, since they can be anything, have no comparison operators or
    typing information.
      
    A single literal value within a compiled SQL statement is more useful 
    being specified as a bind parameter via the bindparam() method,
    since it provides more information about what it is, including an optional
    type, as well as providing comparison operations."""
    def __init__(self, text = "", engine=None, bindparams=None, typemap=None):
        self.parens = False
        self._engine = engine
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
    columns = property(lambda s:[])        
    def accept_visitor(self, visitor): 
        for item in self.bindparams.values():
            item.accept_visitor(visitor)
        visitor.visit_textclause(self)
    def _get_from_objects(self):
        return []

class Null(ColumnElement):
    """represents the NULL keyword in a SQL statement. public contstructor is the
    null() function."""
    def __init__(self):
        self.type = sqltypes.NULLTYPE
    def accept_visitor(self, visitor):
        visitor.visit_null(self)
    def _get_from_objects(self):
        return []

class ClauseList(ClauseElement):
    """describes a list of clauses.  by default, is comma-separated, 
    such as a column listing."""
    def __init__(self, *clauses, **kwargs):
        self.clauses = []
        for c in clauses:
            if c is None: continue
            self.append(c)
        self.parens = kwargs.get('parens', False)
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return ClauseList(parens=self.parens, *clauses)
    def append(self, clause):
        if _is_literal(clause):
            clause = TextClause(str(clause))
        self.clauses.append(clause)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_clauselist(self)
    def _get_from_objects(self):
        f = []
        for c in self.clauses:
            f += c._get_from_objects()
        return f
    def compare(self, other):
        """compares this ClauseList to the given ClauseList, including
        a comparison of all the clause items."""
        if isinstance(other, ClauseList) and len(self.clauses) == len(other.clauses):
            for i in range(0, len(self.clauses)):
                if not self.clauses[i].compare(other.clauses[i]):
                    return False
            else:
                return True
        else:
            return False

class CompoundClause(ClauseList):
    """represents a list of clauses joined by an operator, such as AND or OR.  
    extends ClauseList to add the operator as well as a from_objects accessor to 
    help determine FROM objects in a SELECT statement."""
    def __init__(self, operator, *clauses, **kwargs):
        ClauseList.__init__(self, *clauses, **kwargs)
        self.operator = operator
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return CompoundClause(self.operator, *clauses)
    def append(self, clause):
        if isinstance(clause, CompoundClause):
            clause.parens = True
        ClauseList.append(self, clause)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_compound(self)
    def _get_from_objects(self):
        f = []
        for c in self.clauses:
            f += c._get_from_objects()
        return f
    def compare(self, other):
        """compares this CompoundClause to the given item.  
        
        In addition to the regular comparison, has the special case that it 
        returns True if this CompoundClause has only one item, and that 
        item matches the given item."""
        if not isinstance(other, CompoundClause):
            if len(self.clauses) == 1:
                return self.clauses[0].compare(other)
        if ClauseList.compare(self, other):
            return self.operator == other.operator
        else:
            return False

class CalculatedClause(ClauseList, ColumnElement):
    """ describes a calculated SQL expression that has a type, like CASE.  extends ColumnElement to
    provide column-level comparison operators.  """
    def __init__(self, name, *clauses, **kwargs):
        self.name = name
        self.type = sqltypes.to_instance(kwargs.get('type', None))
        self._engine = kwargs.get('engine', None)
        ClauseList.__init__(self, *clauses)
    key = property(lambda self:self.name or "_calc_")
    def _process_from_dict(self, data, asfrom):
        super(CalculatedClause, self)._process_from_dict(data, asfrom)
        # this helps a Select object get the engine from us
        data.setdefault(self, self)
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return CalculatedClause(type=self.type, engine=self._engine, *clauses)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_calculatedclause(self)
    def _bind_param(self, obj):
        return BindParamClause(self.name, obj, type=self.type)
    def select(self):
        return select([self])
    def scalar(self):
        return select([self]).scalar()
    def execute(self):
        return select([self]).execute()
    def _compare_type(self, obj):
        return self.type

                
class Function(CalculatedClause):
    """describes a SQL function. extends CalculatedClause turn the "clauselist" into function
    arguments, also adds a "packagenames" argument"""
    def __init__(self, name, *clauses, **kwargs):
        self.name = name
        self.type = sqltypes.to_instance(kwargs.get('type', None))
        self.packagenames = kwargs.get('packagenames', None) or []
        self._engine = kwargs.get('engine', None)
        ClauseList.__init__(self, parens=True, *clauses)
    key = property(lambda self:self.name)
    def append(self, clause):
        if _is_literal(clause):
            if clause is None:
                clause = null()
            else:
                clause = BindParamClause(self.name, clause, shortname=self.name, type=None)
        self.clauses.append(clause)
    def copy_container(self):
        clauses = [clause.copy_container() for clause in self.clauses]
        return Function(self.name, type=self.type, packagenames=self.packagenames, engine=self._engine, *clauses)
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_function(self)

class Cast(ColumnElement):
    def __init__(self, clause, totype, **kwargs):
        if not hasattr(clause, 'label'):
            clause = literal(clause)
        self.type = sqltypes.to_instance(totype)
        self.clause = clause
        self.typeclause = TypeClause(self.type)
    def accept_visitor(self, visitor):
        self.clause.accept_visitor(visitor)
        self.typeclause.accept_visitor(visitor)
        visitor.visit_cast(self)
    def _get_from_objects(self):
        return self.clause._get_from_objects()
        
class FunctionGenerator(object):
    """generates Function objects based on getattr calls"""
    def __init__(self, engine=None):
        self.__engine = engine
        self.__names = []
    def __getattr__(self, name):
        self.__names.append(name)
        return self
    def __call__(self, *c, **kwargs):
        kwargs.setdefault('engine', self.__engine)
        return Function(self.__names[-1], packagenames=self.__names[0:-1], *c, **kwargs)     
                
class BinaryClause(ClauseElement):
    """represents two clauses with an operator in between"""
    def __init__(self, left, right, operator, type=None):
        self.left = left
        self.right = right
        self.operator = operator
        self.type = sqltypes.to_instance(type)
        self.parens = False
        if isinstance(self.left, BinaryClause) or isinstance(self.left, Selectable):
            self.left.parens = True
        if isinstance(self.right, BinaryClause) or isinstance(self.right, Selectable):
            self.right.parens = True
    def copy_container(self):
        return BinaryClause(self.left.copy_container(), self.right.copy_container(), self.operator)
    def _get_from_objects(self):
        return self.left._get_from_objects() + self.right._get_from_objects()
    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        visitor.visit_binary(self)
    def swap(self):
        c = self.left
        self.left = self.right
        self.right = c
    def compare(self, other):
        """compares this BinaryClause against the given BinaryClause."""
        return (
            isinstance(other, BinaryClause) and self.operator == other.operator and 
            self.left.compare(other.left) and self.right.compare(other.right)
        )

class BooleanExpression(BinaryClause):
    """represents a boolean expression, which is only useable in WHERE criterion."""
    pass
class BinaryExpression(BinaryClause, ColumnElement):
    """represents a binary expression, which can be in a WHERE criterion or in the column list 
    of a SELECT.  By adding "ColumnElement" to its inherited list, it becomes a Selectable
    unit which can be placed in the column list of a SELECT."""
    pass
    
        
class Join(FromClause):
    def __init__(self, left, right, onclause=None, isouter = False):
        self.left = left
        self.right = right
        if onclause is None:
            self.onclause = self._match_primaries(left, right)
        else:
            self.onclause = onclause
        self.isouter = isouter

    name = property(lambda s: "Join object on " + s.left.name + " " + s.right.name)
    def _locate_oid_column(self):
        return self.left.oid_column
    
    def _exportable_columns(self):
        return [c for c in self.left.columns] + [c for c in self.right.columns]
    def _proxy_column(self, column):
        self._columns[column._label] = column
        if column.primary_key:
            self._primary_key.append(column)
        if column.foreign_key:
            self._foreign_keys.append(column.foreign_key)
        return column
    def _match_primaries(self, primary, secondary):
        crit = []
        for fk in secondary.foreign_keys:
            if fk.references(primary):
                crit.append(primary.corresponding_column(fk.column) == fk.parent)
                self.foreignkey = fk.parent
        if primary is not secondary:
            for fk in primary.foreign_keys:
                if fk.references(secondary):
                    crit.append(secondary.corresponding_column(fk.column) == fk.parent)
                    self.foreignkey = fk.parent
        if len(crit) == 0:
            raise exceptions.ArgumentError("Cant find any foreign key relationships between '%s' and '%s'" % (primary.name, secondary.name))
        elif len(crit) == 1:
            return (crit[0])
        else:
            return and_(*crit)
            
    def _group_parenthesized(self):
        return True
    def select(self, whereclauses = None, **params):
        return select([self.left, self.right], whereclauses, from_obj=[self], **params)
    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        self.onclause.accept_visitor(visitor)
        visitor.visit_join(self)

    engine = property(lambda s:s.left.engine or s.right.engine)

    class JoinMarker(FromClause):
        def __init__(self, join):
            FromClause.__init__(self)
            self.join = join
        def _exportable_columns(self):
            return []
    
    def alias(self, name=None):
        """creates a Select out of this Join clause and returns an Alias of it.  The Select is not correlating."""
        return self.select(use_labels=True, correlate=False).alias(name)            
    def _process_from_dict(self, data, asfrom):
        for f in self.onclause._get_from_objects():
            data[f] = f
        for f in self.left._get_from_objects() + self.right._get_from_objects():
            # mark the object as a "blank" "from" that wont be printed
            data[f] = Join.JoinMarker(self)
        # a JOIN always impacts the final FROM list of a select statement
        data[self] = self
        
    def _get_from_objects(self):
        return [self] + self.onclause._get_from_objects() + self.left._get_from_objects() + self.right._get_from_objects()
        
class Alias(FromClause):
    def __init__(self, selectable, alias = None):
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

    def accept_visitor(self, visitor):
        self.selectable.accept_visitor(visitor)
        visitor.visit_alias(self)

    def _get_from_objects(self):
        return [self]

    def _group_parenthesized(self):
        return False
        
    engine = property(lambda s: s.selectable.engine)

    
class Label(ColumnElement):
    def __init__(self, name, obj, type=None):
        self.name = name
        while isinstance(obj, Label):
            obj = obj.obj
        self.obj = obj
        self.type = sqltypes.to_instance(type)
        obj.parens=True
    key = property(lambda s: s.name)
    _label = property(lambda s: s.name)
    orig_set = property(lambda s:s.obj.orig_set)
    def accept_visitor(self, visitor):
        self.obj.accept_visitor(visitor)
        visitor.visit_label(self)
    def _get_from_objects(self):
        return self.obj._get_from_objects()
    def _make_proxy(self, selectable, name = None):
        return self.obj._make_proxy(selectable, name=self.name)
     
class ColumnClause(ColumnElement):
    """represents a textual column clause in a SQL statement.  May or may not
    be bound to an underlying Selectable."""
    def __init__(self, text, selectable=None, type=None, hidden=False):
        self.key = self.name = text
        self.table = selectable
        self.type = sqltypes.to_instance(type)
        self.hidden = hidden
        self.__label = None
    def _get_label(self):
        if self.__label is None:
            if self.table is not None and self.table.named_with_column():
                self.__label =  self.table.name + "_" + self.name
                if self.table.c.has_key(self.__label) or len(self.__label) >= 30:
                    self.__label = self.__label[0:24] + "_" + hex(random.randint(0, 65535))[2:]
            else:
                self.__label = self.name
        return self.__label
    _label = property(_get_label)
    def accept_visitor(self, visitor): 
        visitor.visit_column(self)
    def to_selectable(self, selectable):
        """given a Selectable, returns this column's equivalent in that Selectable, if any.
        
        for example, this could translate the column "name" from a Table object
        to an Alias of a Select off of that Table object."""
        return selectable.corresponding_column(self.original, False)
    def _get_from_objects(self):
        if self.table is not None:
            return [self.table]
        else:
            return []
    def _bind_param(self, obj):
        return BindParamClause(self._label, obj, shortname = self.name, type=self.type)
    def _make_proxy(self, selectable, name = None):
        c = ColumnClause(name or self.name, selectable, hidden=self.hidden)
        c.orig_set = self.orig_set
        if not self.hidden:
            selectable.columns[c.name] = c
        return c
    def _compare_type(self, obj):
        return self.type
    def _group_parenthesized(self):
        return False

class TableClause(FromClause):
    def __init__(self, name, *columns):
        super(TableClause, self).__init__(name)
        self.name = self.fullname = name
        self._columns = util.OrderedProperties()
        self._indexes = util.OrderedProperties()
        self._foreign_keys = []
        self._primary_key = []
        for c in columns:
            self.append_column(c)
        self._oid_column = ColumnClause('oid', self, hidden=True)

    indexes = property(lambda s:s._indexes)

    def named_with_column(self):
        return True
    def append_column(self, c):
        self._columns[c.name] = c
        c.table = self
    def _locate_oid_column(self):
        return self._oid_column
    def _orig_columns(self):
        try:
            return self._orig_cols
        except AttributeError:
            self._orig_cols= {}
            for c in self.columns:
                for ci in c.orig_set:
                    self._orig_cols[ci] = c
            return self._orig_cols
    columns = property(lambda s:s._columns)
    c = property(lambda s:s._columns)
    primary_key = property(lambda s:s._primary_key)
    foreign_keys = property(lambda s:s._foreign_keys)
    original_columns = property(_orig_columns)

    def _clear(self):
        """clears all attributes on this TableClause so that new items can be added again"""
        self.columns.clear()
        self.indexes.clear()
        self.foreign_keys[:] = []
        self.primary_key[:] = []
        try:
            delattr(self, '_orig_cols')
        except AttributeError:
            pass

    def accept_visitor(self, visitor):
        visitor.visit_table(self)
    def _exportable_columns(self):
        raise NotImplementedError()
    def _group_parenthesized(self):
        return False
    def _process_from_dict(self, data, asfrom):
        for f in self._get_from_objects():
            data.setdefault(f, f)
        if asfrom:
            data[self] = self
    def count(self, whereclause=None, **params):
        if len(self.primary_key):
            col = self.primary_key[0]
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

class SelectBaseMixin(object):
    """base class for Select and CompoundSelects"""
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
        if self.is_where or self._scalar:
            return []
        else:
            return [self]
            
class CompoundSelect(SelectBaseMixin, FromClause):
    def __init__(self, keyword, *selects, **kwargs):
        SelectBaseMixin.__init__(self)
        self.keyword = keyword
        self.selects = selects
        self.use_labels = kwargs.pop('use_labels', False)
        self.parens = kwargs.pop('parens', False)
        self.correlate = kwargs.pop('correlate', False)
        self.for_update = kwargs.pop('for_update', False)
        for s in self.selects:
            s.group_by(None)
            s.order_by(None)
        self.group_by(*kwargs.get('group_by', [None]))
        self.order_by(*kwargs.get('order_by', [None]))
        self._col_map = {}

#    name = property(lambda s:s.keyword + " statement")
    def _foo(self):
        raise "this is a temporary assertion while we refactor SQL to not call 'name' on non-table Selectables"    
    name = property(lambda s:s._foo()) #"SELECT statement")
    
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
            col = column._make_proxy(self, name=column.name)
        
        try:
            colset = self._col_map[col.name]
        except KeyError:
            colset = util.Set()
            self._col_map[col.name] = colset
        [colset.add(c) for c in col.orig_set]
        col.orig_set = colset
        return col
    
    def accept_visitor(self, visitor):
        self.order_by_clause.accept_visitor(visitor)
        self.group_by_clause.accept_visitor(visitor)
        for s in self.selects:
            s.accept_visitor(visitor)
        visitor.visit_compound_select(self)
    def _find_engine(self):
        for s in self.selects:
            e = s._find_engine()
            if e:
                return e
        else:
            return None
       
class Select(SelectBaseMixin, FromClause):
    """represents a SELECT statement, with appendable clauses, as well as 
    the ability to execute itself and return a result set."""
    def __init__(self, columns=None, whereclause = None, from_obj = [], order_by = None, group_by=None, having=None, use_labels = False, distinct=False, for_update=False, engine=None, limit=None, offset=None, scalar=False, correlate=True):
        SelectBaseMixin.__init__(self)
        self._froms = util.OrderedDict()
        self.use_labels = use_labels
        self.whereclause = None
        self.having = None
        self._engine = engine
        self.limit = limit
        self.offset = offset
        self.for_update = for_update

        # indicates that this select statement should not expand its columns
        # into the column clause of an enclosing select, and should instead
        # act like a single scalar column
        self._scalar = scalar

        # indicates if this select statement, as a subquery, should correlate
        # its FROM clause to that of an enclosing select statement
        self.correlate = correlate
        
        # indicates if this select statement is a subquery inside another query
        self.issubquery = False
        
        # indicates if this select statement is a subquery as a criterion
        # inside of a WHERE clause
        self.is_where = False
        
        self.distinct = distinct
        self._text = None
        self._raw_columns = []
        self._correlated = None
        self._correlator = Select.CorrelatedVisitor(self, False)
        self._wherecorrelator = Select.CorrelatedVisitor(self, True)

        self.group_by(*(group_by or [None]))
        self.order_by(*(order_by or [None]))
        
        if columns is not None:
            for c in columns:
                self.append_column(c)
            
        if whereclause is not None:
            self.append_whereclause(whereclause)
        if having is not None:
            self.append_having(having)
            
        for f in from_obj:
            self.append_from(f)
    
    def _foo(self):
        raise "this is a temporary assertion while we refactor SQL to not call 'name' on non-table Selectables"    
    name = property(lambda s:s._foo()) #"SELECT statement")
    
    class CorrelatedVisitor(ClauseVisitor):
        """visits a clause, locates any Select clauses, and tells them that they should
        correlate their FROM list to that of their parent."""
        def __init__(self, select, is_where):
            self.select = select
            self.is_where = is_where
        def visit_compound_select(self, cs):
            self.visit_select(cs)
            for s in cs.selects:
                s.parens = False
        def visit_column(self, c):pass
        def visit_table(self, c):pass
        def visit_select(self, select):
            if select is self.select:
                return
            select.is_where = self.is_where
            select.issubquery = True
            select.parens = True
            if not select.correlate:
                return
            if getattr(select, '_correlated', None) is None:
                select._correlated = self.select._froms
                
    def append_column(self, column):
        if _is_literal(column):
            column = ColumnClause(str(column), self)

        self._raw_columns.append(column)

        # if the column is a Select statement itself, 
        # accept visitor
        column.accept_visitor(self._correlator)
        
        # visit the FROM objects of the column looking for more Selects
        for f in column._get_from_objects():
            f.accept_visitor(self._correlator)
        column._process_from_dict(self._froms, False)
    def _exportable_columns(self):
        return self._raw_columns
    def _proxy_column(self, column):
        if self.use_labels:
            return column._make_proxy(self, name=column._label)
        else:
            return column._make_proxy(self, name=column.name)
    def append_whereclause(self, whereclause):
        self._append_condition('whereclause', whereclause)
    def append_having(self, having):
        self._append_condition('having', having)
    def _append_condition(self, attribute, condition):
        if type(condition) == str:
            condition = TextClause(condition)
        condition.accept_visitor(self._wherecorrelator)
        condition._process_from_dict(self._froms, False)
        if getattr(self, attribute) is not None:
            setattr(self, attribute, and_(getattr(self, attribute), condition))
        else:
            setattr(self, attribute, condition)

    def clear_from(self, from_obj):
        self._froms[from_obj] = FromClause(from_name = None)
        
    def append_from(self, fromclause):
        if type(fromclause) == str:
            fromclause = TextClause(fromclause)
        fromclause.accept_visitor(self._correlator)
        fromclause._process_from_dict(self._froms, True)
    def _locate_oid_column(self):
        for f in self._froms.values():
            if f is self:
                # TODO: why would we be in our own _froms list ?
                raise exceptions.AssertionError("Select statement should not be in its own _froms list")
            oid = f.oid_column
            if oid is not None:
                return oid
        else:
            return None
    def _get_froms(self):
        return [f for f in self._froms.values() if f is not self and (self._correlated is None or not self._correlated.has_key(f))]
    froms = property(lambda s: s._get_froms())

    def accept_visitor(self, visitor):
        # TODO: add contextual visit_ methods
        # visit_select_whereclause, visit_select_froms, visit_select_orderby, etc.
        # which will allow the compiler to set contextual flags before traversing 
        # into each thing.  
        for f in self._get_froms():
            f.accept_visitor(visitor)
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        if self.having is not None:
            self.having.accept_visitor(visitor)
        self.order_by_clause.accept_visitor(visitor)
        self.group_by_clause.accept_visitor(visitor)
        visitor.visit_select(self)
    
    def union(self, other, **kwargs):
        return union(self, other, **kwargs)
    def union_all(self, other, **kwargs):
        return union_all(self, other, **kwargs)
    def _find_engine(self):
        """tries to return a Engine, either explicitly set in this object, or searched
        within the from clauses for one"""
        
        if self._engine is not None:
            return self._engine
        for f in self._froms.values():
            if f is self:
                continue
            e = f.engine
            if e is not None: 
                self._engine = e
                return e
        return None

class UpdateBase(ClauseElement):
    """forms the base for INSERT, UPDATE, and DELETE statements."""
    def _process_colparams(self, parameters):
        """receives the "values" of an INSERT or UPDATE statement and constructs
        appropriate ind parameters."""
        if parameters is None:
            return None

        if isinstance(parameters, list) or isinstance(parameters, tuple):
            pp = {}
            i = 0
            for c in self.table.c:
                pp[c.key] = parameters[i]
                i +=1
            parameters = pp
            
        for key in parameters.keys():
            value = parameters[key]
            if isinstance(value, Select):
                value.clear_from(self.table)
            elif _is_literal(value):
                if _is_literal(key):
                    col = self.table.c[key]
                else:
                    col = key
                try:
                    parameters[key] = bindparam(col, value)
                except KeyError:
                    del parameters[key]
        return parameters
    def _find_engine(self):
        return self.table.engine

class Insert(UpdateBase):
    def __init__(self, table, values=None, **params):
        self.table = table
        self.select = None
        self.parameters = self._process_colparams(values)
        
    def accept_visitor(self, visitor):
        if self.select is not None:
            self.select.accept_visitor(visitor)

        visitor.visit_insert(self)

class Update(UpdateBase):
    def __init__(self, table, whereclause, values=None, **params):
        self.table = table
        self.whereclause = whereclause
        self.parameters = self._process_colparams(values)

    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        visitor.visit_update(self)

class Delete(UpdateBase):
    def __init__(self, table, whereclause, **params):
        self.table = table
        self.whereclause = whereclause

    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        visitor.visit_delete(self)

