# sql.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


"""defines the base components of SQL expression trees."""

import sqlalchemy.schema as schema
import sqlalchemy.util as util
import sqlalchemy.types as types
import string, re

__all__ = ['text', 'column', 'func', 'select', 'update', 'insert', 'delete', 'join', 'and_', 'or_', 'not_', 'union', 'union_all', 'desc', 'asc', 'outerjoin', 'alias', 'subquery', 'literal', 'bindparam', 'exists']

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

def outerjoin(left, right, onclause, **kwargs):
    """returns an OUTER JOIN clause element, given the left and right hand expressions,
    as well as the ON condition's expression.  To chain joins together, use the resulting
    Join object's "join()" or "outerjoin()" methods."""
    return Join(left, right, onclause, isouter = True, **kwargs)

def join(left, right, onclause, **kwargs):
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
    return BinaryClause(TextClause("NOT"), clause, None)
            
        
def exists(*args, **params):
    s = select(*args, **params)
    return BinaryClause(TextClause("EXISTS"), s, None)

def union(*selects, **params):
    return _compound_select('UNION', *selects, **params)

def union_all(*selects, **params):
    return _compound_select('UNION ALL', *selects, **params)

def alias(*args, **params):
    return Alias(*args, **params)

def subquery(alias, *args, **params):
    return Alias(Select(*args, **params), alias)

def literal(value, type=None):
    """returns a literal clause, bound to a bind parameter.  
    
    literal clauses are created automatically when used as the right-hand 
    side of a boolean or math operation against a column object.  use this 
    function when a literal is needed on the left-hand side (and optionally on the right as well).
    
    the optional type parameter is a sqlalchemy.types.TypeEngine object which indicates bind-parameter
    and result-set translation for this literal.
    """
    return BindParamClause('literal', value, type=type)

def column(table, text):
    """returns a textual column clause, relative to a table.  this differs from using straight text
    or text() in that the column is treated like a regular column, i.e. gets added to a Selectable's list
    of columns."""
    return ColumnClause(text, table)
    
def bindparam(key, value = None, type=None):
    """creates a bind parameter clause with the given key.  
    
    An optional default value can be specified by the value parameter, and the optional type parameter
    is a sqlalchemy.types.TypeEngine object which indicates bind-parameter and result-set translation for
    this bind parameter."""
    if isinstance(key, schema.Column):
        return BindParamClause(key.name, value, type=key.type)
    else:
        return BindParamClause(key, value, type=type)

def text(text, engine=None):
    """creates literal text to be inserted into a query.  
    
    When constructing a query from a select(), update(), insert() or delete(), using 
    plain strings for argument values will usually result in text objects being created
    automatically.  Use this function when creating textual clauses outside of other
    ClauseElement objects, or optionally wherever plain text is to be used."""
    return TextClause(text, engine=engine)

def null():
    """returns a Null object, which compiles to NULL in a sql statement."""
    return Null()
    
class FunctionGateway(object):
    """returns a callable based on an attribute name, which then returns a Function 
    object with that name."""
    def __getattr__(self, name):
        return lambda *c, **kwargs: Function(name, *c, **kwargs)
func = FunctionGateway()

def _compound_clause(keyword, *clauses):
    return CompoundClause(keyword, *clauses)

def _compound_select(keyword, *selects, **kwargs):
    return CompoundSelect(keyword, *selects, **kwargs)

def _is_literal(element):
    return not isinstance(element, ClauseElement) and not isinstance(element, schema.SchemaItem)

class ClauseVisitor(schema.SchemaVisitor):
    """builds upon SchemaVisitor to define the visiting of SQL statement elements in 
    addition to Schema elements."""
    def visit_columnclause(self, column):pass
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
    def visit_function(self, func):pass
    
class Compiled(ClauseVisitor):
    """represents a compiled SQL expression.  the __str__ method of the Compiled object
    should produce the actual text of the statement.  Compiled objects are specific to the
    database library that created them, and also may or may not be specific to the columns
    referenced within a particular set of bind parameters.  In no case should the Compiled
    object be dependent on the actual values of those bind parameters, even though it may
    reference those values as defaults."""

    def __init__(self, engine, statement, bindparams):
        self.engine = engine
        self.bindparams = bindparams
        self.statement = statement

    def __str__(self):
        """returns the string text of the generated SQL statement."""
        raise NotImplementedError()
    def get_params(self, **params):
        """returns the bind params for this compiled object, with values overridden by 
        those given in the **params dictionary"""
        raise NotImplementedError()

    def execute(self, *multiparams, **params):
        """executes this compiled object using the underlying SQLEngine"""
        if len(multiparams):
            params = [self.get_params(**m) for m in multiparams]
        else:
            params = self.get_params(**params)
        return self.engine.execute(str(self), params, compiled = self, typemap = self.typemap)

    def scalar(self, *multiparams, **params):
        """executes this compiled object via the execute() method, then 
        returns the first column of the first row.  Useful for executing functions,
        sequences, rowcounts, etc."""
        return self.execute(*multiparams, **params).fetchone()[0]
        
class ClauseElement(object):
    """base class for elements of a programmatically constructed SQL expression."""
    def hash_key(self):
        """returns a string that uniquely identifies the concept this ClauseElement
        represents.

        two ClauseElements can have the same value for hash_key() iff they both correspond to
        the exact same generated SQL.  This allows the hash_key() values of a collection of
        ClauseElements to be constructed into a larger identifying string for the purpose of
        caching a SQL expression.

        Note that since ClauseElements may be mutable, the hash_key() value is subject to
        change if the underlying structure of the ClauseElement changes.""" 
        raise NotImplementedError(repr(self))
    def _get_from_objects(self):
        """returns objects represented in this ClauseElement that should be added to the
        FROM list of a query."""
        raise NotImplementedError(repr(self))
    def _process_from_dict(self, data, asfrom):
        """given a dictionary attached to a Select object, places the appropriate
        FROM objects in the dictionary corresponding to this ClauseElement,
        and possibly removes or modifies others."""
        for f in self._get_from_objects():
            data.setdefault(f.id, f)
        if asfrom:
            data[self.id] = self
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
        try:
            if self._engine is not None:
                return self._engine
        except AttributeError:
            pass
        for f in self._get_from_objects():
            engine = f.engine
            if engine is not None: 
                return engine
        else:
            return None
            
    engine = property(lambda s: s._find_engine())

    def _get_columns(self):
        try:
            return self._columns
        except AttributeError:
            return [self]
    columns = property(lambda s: s._get_columns())
    
    def compile(self, engine = None, bindparams = None, typemap=None):
        """compiles this SQL expression using its underlying SQLEngine to produce
        a Compiled object.  If no engine can be found, an ansisql engine is used.
        bindparams is a dictionary representing the default bind parameters to be used with 
        the statement.  """
        if engine is None:
            engine = self.engine

        if engine is None:
            raise "no SQLEngine could be located within this ClauseElement."

        return engine.compile(self, bindparams = bindparams, typemap=typemap)

    def __str__(self):
        e = self.engine
        if e is None:
            import sqlalchemy.ansisql as ansisql
            e = ansisql.engine()
        return str(self.compile(e))
        
    def execute(self, *multiparams, **params):
        """compiles and executes this SQL expression using its underlying SQLEngine. the
        given **params are used as bind parameters when compiling and executing the
        expression. the DBAPI cursor object is returned."""
        e = self.engine
        if len(multiparams):
            bindparams = multiparams[0]
        else:
            bindparams = params
        c = self.compile(e, bindparams = bindparams)
        return c.execute(*multiparams, **params)

    def scalar(self, *multiparams, **params):
        """executes this SQL expression via the execute() method, then 
        returns the first column of the first row.  Useful for executing functions,
        sequences, rowcounts, etc."""
        return self.execute(*multiparams, **params).fetchone()[0]

    def __and__(self, other):
        return and_(self, other)
    def __or__(self, other):
        return or_(self, other)
    def __invert__(self):
        return not_(self)

class CompareMixin(object):
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
            return self._compare('IN', ClauseList(parens=True, *[TextClause(o, isliteral=True) for o in other]))
        else:
            # assume *other is a list of selects.
            # so put them in a UNION.  if theres only one, you just get one SELECT 
            # statement out of it.
            return self._compare('IN', union(*other))

    def startswith(self, other):
        return self._compare('LIKE', str(other) + "%")
    
    def endswith(self, other):
        return self._compare('LIKE', "%" + str(other))

    # and here come the math operators:
    def __add__(self, other):
        return self._compare('+', other)
    def __sub__(self, other):
        return self._compare('-', other)
    def __mul__(self, other):
        return self._compare('*', other)
    def __div__(self, other):
        return self._compare('/', other)
    def __truediv__(self, other):
        return self._compare('/', other)
    def _compare(self, operator, obj):
        if _is_literal(obj):
            if obj is None:
                if operator != '=':
                    raise "Only '=' operator can be used with NULL"
                return BinaryClause(self, null(), 'IS')
            else:
                obj = BindParamClause('literal', obj, shortname=None, type=self.type)

        return BinaryClause(self, obj, operator)

        

class FromClause(ClauseElement):
    """represents a FROM clause element in a SQL statement."""
    
    def __init__(self, from_name = None, from_key = None):
        self.from_name = from_name
        self.id = from_key or from_name
        
    def _get_from_objects(self):
        # this could also be [self], at the moment it doesnt matter to the Select object
        return []
        
    def hash_key(self):
        return "FromClause(%s, %s)" % (repr(self.id), repr(self.from_name))
            
    def accept_visitor(self, visitor): 
        visitor.visit_fromclause(self)
    
class BindParamClause(ClauseElement, CompareMixin):
    def __init__(self, key, value, shortname = None, type = None):
        self.key = key
        self.value = value
        self.shortname = shortname
        self.type = type or types.NULLTYPE

    def accept_visitor(self, visitor):
        visitor.visit_bindparam(self)

    def _get_from_objects(self):
        return []
     
    def hash_key(self):
        return "BindParam(%s, %s, %s)" % (repr(self.key), repr(self.value), repr(self.shortname))

    def typeprocess(self, value):
        return self.type.convert_bind_param(value)
            
class TextClause(ClauseElement):
    """represents literal text, including SQL fragments as well
    as literal (non bind-param) values."""
    
    def __init__(self, text = "", engine=None, isliteral=False):
        self.text = text
        self.parens = False
        self._engine = engine
        self.id = id(self)
        if isliteral:
            if isinstance(text, int) or isinstance(text, long):
                self.text = str(text)
            else:
                text = re.sub(r"'", r"''", text)
                self.text = "'" + text + "'"
    def accept_visitor(self, visitor): 
        visitor.visit_textclause(self)
    def hash_key(self):
        return "TextClause(%s)" % repr(self.text)
    def _get_from_objects(self):
        return []

class Null(ClauseElement):
    def accept_visitor(self, visitor):
        visitor.visit_null(self)
    def _get_from_objects(self):
        return []
    def hash_key(self):
        return "Null"
    
        
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
    def hash_key(self):
        return string.join([c.hash_key() for c in self.clauses], self.operator or " ")

class Function(ClauseList, CompareMixin):
    """describes a SQL function. extends ClauseList to provide comparison operators."""
    def __init__(self, name, *clauses, **kwargs):
        self.name = name
        self.type = kwargs.get('type', None)
        self.label = kwargs.get('label', None)
        ClauseList.__init__(self, parens=True, *clauses)
    key = property(lambda self:self.label or self.name)
    def append(self, clause):
        if _is_literal(clause):
            if clause is None:
                clause = null()
            else:
                clause = BindParamClause(self.name, clause, shortname=self.name, type=None)
        self.clauses.append(clause)
    def copy_container(self):
        return self
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_function(self)
    def _compare(self, operator, obj):
        if _is_literal(obj):
            if obj is None:
                if operator != '=':
                    raise "Only '=' operator can be used with NULL"
                return BinaryClause(self, null(), 'IS')
            else:
                obj = BindParamClause(self.name, obj, shortname=self.name, type=self.type)

        return BinaryClause(self, obj, operator)
    def _make_proxy(self, selectable, name = None):
        return self

        
class BinaryClause(ClauseElement, CompareMixin):
    """represents two clauses with an operator in between"""
    
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator
        self.parens = False

    def copy_container(self):
        return BinaryClause(self.left.copy_container(), self.right.copy_container(), self.operator)
        
    def _get_from_objects(self):
        return self.left._get_from_objects() + self.right._get_from_objects()

    def hash_key(self):
        return self.left.hash_key() + self.operator + self.right.hash_key()
        
    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        visitor.visit_binary(self)

    def swap(self):
        c = self.left
        self.left = self.right
        self.right = c
        
class Selectable(FromClause):
    """represents a column list-holding object, like a table, alias or subquery.  can be used anywhere a Table is used."""
    
    c = property(lambda self: self.columns)

    def accept_visitor(self, visitor):
        raise NotImplementedError()
    
    def is_selectable(self):
        return True
        
    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def _get_col_by_original(self, column):
        """given a column which is a schema.Column object attached to a schema.Table object
        (i.e. an "original" column), return the Column object from this 
        Selectable which corresponds to that original Column, or None if this Selectable
        does not contain the column."""
        raise NotImplementedError()

    def join(self, right, *args, **kwargs):
        return Join(self, right, *args, **kwargs)

    def outerjoin(self, right, *args, **kwargs):
        return Join(self, right, isouter = True, *args, **kwargs)

    def alias(self, name):
        return Alias(self, name)
    def _group_parenthesized(self):
        """indicates if this Selectable requires parenthesis when grouped into a compound
        statement"""
        return True
        
class Join(Selectable):
    # TODO: put "using" + "natural" concepts in here and make "onclause" optional
    def __init__(self, left, right, onclause, isouter = False, allcols = True):
        self.left = left
        self.right = right
        self.id = self.left.id + "_" + self.right.id
        self.allcols = allcols
        if allcols:
            self._columns = [c for c in self.left.columns] + [c for c in self.right.columns]
        else:
            self._columns = self.right.columns

        # TODO: if no onclause, do NATURAL JOIN
        self.onclause = onclause
        self.isouter = isouter
        self.rowid_column = self.left.rowid_column
        
    primary_key = property (lambda self: [c for c in self.left.columns if c.primary_key] + [c for c in self.right.columns if c.primary_key])

    def _group_parenthesized(self):
        """indicates if this Selectable requires parenthesis when grouped into a compound
        statement"""
        return True

    def _get_col_by_original(self, column):
        for c in self.columns:
            if c.original is column:
                return c
        else:
            return None

    def hash_key(self):
        return "Join(%s, %s, %s, %s)" % (repr(self.left.hash_key()), repr(self.right.hash_key()), repr(self.onclause.hash_key()), repr(self.isouter))

    def select(self, whereclauses = None, **params):
        return select([self.left, self.right], whereclauses, from_obj=[self], **params)

    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        self.onclause.accept_visitor(visitor)
        visitor.visit_join(self)

    engine = property(lambda s:s.left.engine or s.right.engine)

    class JoinMarker(FromClause):
        def __init__(self, id, join):
            FromClause.__init__(self, from_key=id)
            self.join = join
            
    def _process_from_dict(self, data, asfrom):
        for f in self.onclause._get_from_objects():
            data[f.id] = f
        for f in self.left._get_from_objects() + self.right._get_from_objects():
            # mark the object as a "blank" "from" that wont be printed
            data[f.id] = Join.JoinMarker(f.id, self)
        # a JOIN always impacts the final FROM list of a select statement
        data[self.id] = self
        
    def _get_from_objects(self):
        return [self] + self.onclause._get_from_objects() + self.left._get_from_objects() + self.right._get_from_objects()
        
class Alias(Selectable):
    def __init__(self, selectable, alias = None):
        self.selectable = selectable
        self._columns = util.OrderedProperties()
        self.foreign_keys = []
        if alias is None:
            alias = id(self)
        self.name = alias
        self.id = self.name
        self.count = 0
        self.rowid_column = self.selectable.rowid_column._make_proxy(self)
        for co in selectable.columns:
            co._make_proxy(self)

    primary_key = property (lambda self: [c for c in self.columns if c.primary_key])
    
    def hash_key(self):
        return "Alias(%s, %s)" % (repr(self.selectable.hash_key()), repr(self.name))

    def _get_col_by_original(self, column):
        return self.columns.get(column.key, None)

    def accept_visitor(self, visitor):
        self.selectable.accept_visitor(visitor)
        visitor.visit_alias(self)

    def _get_from_objects(self):
        return [self]

    def _group_parenthesized(self):
        return False
        
    engine = property(lambda s: s.selectable.engine)

class ColumnClause(Selectable, CompareMixin):
    """represents a textual column clause in a SQL statement. allows the creation
    of an additional ad-hoc column that is compiled against a particular table."""

    def __init__(self, text, selectable=None):
        self.text = text
        self.table = selectable
        self._impl = ColumnImpl(self)
        self.type = types.NullTypeEngine()

    name = property(lambda self:self.text)
    key = property(lambda self:self.text)
    label = property(lambda self:self.text)

    def accept_visitor(self, visitor): 
        visitor.visit_columnclause(self)

    def hash_key(self):
        if self.table is not None:
            return "ColumnClause(%s, %s)" % (self.text, self.table.hash_key())
        else:
            return "ColumnClause(%s)" % self.text

    def _get_from_objects(self):
        return []

    def _compare(self, operator, obj):
        if _is_literal(obj):
            if obj is None:
                if operator != '=':
                    raise "Only '=' operator can be used with NULL"
                return BinaryClause(self, null(), 'IS')
            elif self.table.name is None:
                obj = BindParamClause(self.text, obj, shortname=self.text, type=self.type)
            else:
                obj = BindParamClause(self.table.name + "_" + self.text, obj, shortname = self.text, type=self.type)

        return BinaryClause(self, obj, operator)

    def _make_proxy(self, selectable, name = None):
        c = ColumnClause(self.text or name, selectable)
        selectable.columns[c.key] = c
        c._impl = ColumnImpl(c)
        return c

class ColumnImpl(Selectable, CompareMixin):
    """Selectable implementation that gets attached to a schema.Column object."""
    
    def __init__(self, column):
        self.column = column
        self.name = column.name
        self._columns = [self.column]
        
        if column.table.name:
            self.label = column.table.name + "_" + self.column.name
        else:
            self.label = self.column.name

    engine = property(lambda s: s.column.engine)
    
    def copy_container(self):
        return self.column

    def _get_col_by_original(self, column):
        if self.column.original is column:
            return self.column
        else:
            return None
            
    def _group_parenthesized(self):
        return False
        
    def _get_from_objects(self):
        return [self.column.table]
    
    def _compare(self, operator, obj):
        if _is_literal(obj):
            if obj is None:
                if operator != '=':
                    raise "Only '=' operator can be used with NULL"
                return BinaryClause(self.column, null(), 'IS')
            elif self.column.table.name is None:
                obj = BindParamClause(self.name, obj, shortname = self.name, type = self.column.type)
            else:
                obj = BindParamClause(self.column.table.name + "_" + self.name, obj, shortname = self.name, type = self.column.type)

        return BinaryClause(self.column, obj, operator)

class TableImpl(Selectable):
    """attached to a schema.Table to provide it with a Selectable interface
    as well as other functions
    """

    def __init__(self, table):
        self.table = table
        self.id = self.table.name
        self._rowid_column = schema.Column(self.table.engine.rowid_column_name(), types.Integer, hidden=True)
        self._rowid_column._set_parent(table)
    
    rowid_column = property(lambda s: s._rowid_column)
    
    engine = property(lambda s: s.table.engine)

    def _get_col_by_original(self, column):
        try:
          col = self.columns[column.key]
        except KeyError:
          return None
        if col.original is column:
          return col
        else:
          return None

    def _group_parenthesized(self):
        return False

    def _process_from_dict(self, data, asfrom):
        for f in self._get_from_objects():
            data.setdefault(f.id, f)
        if asfrom:
            data[self.id] = self.table
    
    def join(self, right, *args, **kwargs):
        return Join(self.table, right, *args, **kwargs)
    
    def outerjoin(self, right, *args, **kwargs):
        return Join(self.table, right, isouter = True, *args, **kwargs)

    def alias(self, name):
        return Alias(self.table, name)
            
    def select(self, whereclause = None, **params):
        return select([self.table], whereclause, **params)

    def insert(self, values = None):
        return insert(self.table, values=values)

    def update(self, whereclause = None, values = None):
        return update(self.table, whereclause, values)

    def delete(self, whereclause = None):
        return delete(self.table, whereclause)
        
    columns = property(lambda self: self.table.columns)

    def _get_from_objects(self):
        return [self.table]

    def create(self, **params):
        self.table.engine.create(self.table)

    def drop(self, **params):
        self.table.engine.drop(self.table)

class TailClauseMixin(object):
    def order_by(self, *clauses):
        self._append_clause('order_by_clause', "ORDER BY", *clauses)
    def group_by(self, *clauses):
        self._append_clause('group_by_clause', "GROUP BY", *clauses)
    def _append_clause(self, attribute, prefix, *clauses):
        if not hasattr(self, attribute):
            l = ClauseList(*clauses)
            setattr(self, attribute, l)
            self.append_clause(prefix, l)
        else:
            getattr(self, attribute).clauses  += clauses
    def append_clause(self, keyword, clause):
        if type(clause) == str:
            clause = TextClause(clause)
        self.clauses.append((keyword, clause))
            
class CompoundSelect(Selectable, TailClauseMixin):
    def __init__(self, keyword, *selects, **kwargs):
        self.keyword = keyword
        self.selects = selects
        self.clauses = []
        order_by = kwargs.get('order_by', None)
        if order_by:
            self.order_by(*order_by)
        group_by = kwargs.get('group_by', None)
        if group_by:
            self.group_by(*group_by)

    columns = property(lambda s:s.selects[0].columns)
    def accept_visitor(self, visitor):
        for tup in self.clauses:
            tup[1].accept_visitor(visitor)
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
        
class Select(Selectable, TailClauseMixin):
    """finally, represents a SELECT statement, with appendable clauses, as well as 
    the ability to execute itself and return a result set."""
    def __init__(self, columns=None, whereclause = None, from_obj = [], order_by = None, group_by=None, having=None, use_labels = False, distinct=False, engine = None, limit=None, offset=None):
        self._columns = util.OrderedProperties()
        self._froms = util.OrderedDict()
        self.use_labels = use_labels
        self.id = "Select(%d)" % id(self)
        self.name = None
        self.whereclause = None
        self.having = None
        self._engine = engine
        self.rowid_column = None
        self.limit = limit
        self.offset = offset
        
        # indicates if this select statement is a subquery inside another query
        self.issubquery = False
        # indicates if this select statement is a subquery as a criterion
        # inside of a WHERE clause
        self.is_where = False
        self.clauses = []

        self.distinct = distinct
        self._text = None
        self._raw_columns = []
        self._correlated = None
        self._correlator = Select.CorrelatedVisitor(self, False)
        self._wherecorrelator = Select.CorrelatedVisitor(self, True)
        
        if columns is not None:
            for c in columns:
                self.append_column(c)
            
        if whereclause is not None:
            self.append_whereclause(whereclause)
        if having is not None:
            self.append_having(having)
            
        for f in from_obj:
            self.append_from(f)

        if order_by:
            self.order_by(*order_by)
        if group_by:
            self.group_by(*group_by)
            
    class CorrelatedVisitor(ClauseVisitor):
        """visits a clause, locates any Select clauses, and tells them that they should
        correlate their FROM list to that of their parent."""
        def __init__(self, select, is_where):
            self.select = select
            self.is_where = is_where
        def visit_select(self, select):
            if select is self.select:
                return
            select.is_where = self.is_where
            select.issubquery = True
            if select._correlated is None:
                select._correlated = self.select._froms

    def append_column(self, column):
        if _is_literal(column):
            column = ColumnClause(str(column), self)

        self._raw_columns.append(column)

        for f in column._get_from_objects():
            f.accept_visitor(self._correlator)
            if self.rowid_column is None and hasattr(f, 'rowid_column'):
                self.rowid_column = f.rowid_column._make_proxy(self)
        column._process_from_dict(self._froms, False)

        if column.is_selectable():
            for co in column.columns:
                if self.use_labels:
                    co._make_proxy(self, name = co.label)
                else:
                    co._make_proxy(self)

    def _get_col_by_original(self, column):
        if self.use_labels:
            return self.columns.get(column.label,None)
        else:
            return self.columns.get(column.key,None)

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
    
    def clear_from(self, id):
        self.append_from(FromClause(from_name = None, from_key = id))
        
    def append_from(self, fromclause):
        if type(fromclause) == str:
            fromclause = FromClause(from_name = fromclause)

        fromclause.accept_visitor(self._correlator)
        fromclause._process_from_dict(self._froms, True)

    def _get_froms(self):
        return [f for f in self._froms.values() if self._correlated is None or not self._correlated.has_key(f.id)]
    froms = property(lambda s: s._get_froms())

    def accept_visitor(self, visitor):
        for f in self.froms:
            f.accept_visitor(visitor)
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        if self.having is not None:
            self.having.accept_visitor(visitor)
        for tup in self.clauses:
            tup[1].accept_visitor(visitor)
            
        visitor.visit_select(self)
    
    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)
    def union(self, other, **kwargs):
        return union(self, other, **kwargs)
    def union_all(self, other, **kwargs):
        return union_all(self, other, **kwargs)

    def _find_engine(self):
        """tries to return a SQLEngine, either explicitly set in this object, or searched
        within the from clauses for one"""
        
        if self._engine is not None:
            return self._engine
        
        for f in self._froms.values():
            e = f.engine
            if e is not None: 
                self._engine = e
                return e
            
        return None

#    engine = property(lambda s: s._find_engine())
    
    def _get_from_objects(self):
        if self.is_where:
            return []
        else:
            return [self]


class UpdateBase(ClauseElement):
    """forms the base for INSERT, UPDATE, and DELETE statements.  
    Deals with the special needs of INSERT and UPDATE parameter lists -  
    these statements have two separate lists of parameters, those
    defined when the statement is constructed, and those specified at compile time."""
    
    def _process_colparams(self, parameters):
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
                value.clear_from(self.table.id)
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
        
    def get_colparams(self, parameters):
        # case one: no parameters in the statement, no parameters in the 
        # compiled params - just return binds for all the table columns
        if parameters is None and self.parameters is None:
            return [(c, bindparam(c.name, type=c.type)) for c in self.table.columns]

        # if we have statement parameters - set defaults in the 
        # compiled params
        if parameters is None:
            parameters = {}
        else:
            parameters = parameters.copy()
            
        if self.parameters is not None:
            for k, v in self.parameters.iteritems():
                parameters.setdefault(k, v)

        # now go thru compiled params, get the Column object for each key
        d = {}
        for key, value in parameters.iteritems():
            if isinstance(key, schema.Column):
                d[key] = value
            else:
                try:
                    d[self.table.columns[str(key)]] = value
                except KeyError:
                    pass

        # create a list of column assignment clauses as tuples
        values = []
        for c in self.table.columns:
            if d.has_key(c):
                value = d[c]
                if _is_literal(value):
                    value = bindparam(c.name, value, type=c.type)
                values.append((c, value))
        return values


class Insert(UpdateBase):
    def __init__(self, table, values=None, **params):
        self.table = table
        self.select = None
        self.parameters = self._process_colparams(values)
        self._engine = self.table.engine
        
    def accept_visitor(self, visitor):
        if self.select is not None:
            self.select.accept_visitor(visitor)

        visitor.visit_insert(self)

class Update(UpdateBase):
    def __init__(self, table, whereclause, values=None, **params):
        self.table = table
        self.whereclause = whereclause
        self.parameters = self._process_colparams(values)
        self._engine = self.table.engine

    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        visitor.visit_update(self)

class Delete(UpdateBase):
    def __init__(self, table, whereclause, **params):
        self.table = table
        self.whereclause = whereclause
        self._engine = self.table.engine

    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        visitor.visit_delete(self)

        
