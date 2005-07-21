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


"""base sql module used by all sql implementations.  defines abstract units which construct
expression trees that generate into text strings + bind parameters.
"""
import sqlalchemy.schema as schema
import sqlalchemy.util as util
import string

__ALL__ = ['textclause', 'select', 'join', 'and_', 'or_', 'union', 'desc', 'asc', 'outerjoin', 'alias', 'subquery', 'bindparam']

def desc(column):
    return CompoundClause(None, column, "DESC")

def asc(column):
    return CompoundClause(None, column, "ASC")

def outerjoin(left, right, onclause, **params):
    return Join(left, right, onclause, isouter = True, **params)
    
def join(left, right, onclause, **params):
    return Join(left, right, onclause, **params)

def select(columns, whereclause = None, from_obj = [], **params):
    return Select(columns, whereclause = whereclause, from_obj = from_obj, **params)

def insert(table, values = None, **params):
    return Insert(table, values, **params)

def update(table, whereclause = None, values = None, **params):
    return Update(table, whereclause, values, **params)

def delete(table, whereclause = None, **params):
    return Delete(table, whereclause, **params)

def and_(*clauses):
    return _compound_clause('AND', *clauses)

def or_(*clauses):
    clause = _compound_clause('OR', *clauses)
    return clause
    
def union(*selects, **params):
    return _compound_select('UNION', *selects, **params)

def alias(*args, **params):
    return Alias(*args, **params)

def subquery(alias, *args, **params):
    return Alias(Select(*args, **params), alias)

def bindparam(key, value = None):
    return BindParamClause(key, value)

def textclause(text, params = None):
    return TextClause(text, params)

def _compound_clause(keyword, *clauses):
    return CompoundClause(keyword, *clauses)

def _compound_select(keyword, *selects, **params):
    if len(selects) == 0: return None
    
    s = selects[0]
    for n in selects[1:]:
        s.append_clause(keyword, n)
        
    if params.get('order_by', None) is not None:
        s.order_by(*params['order_by'])

    return s

class ClauseVisitor(schema.SchemaVisitor):
    def visit_columnclause(self, column):pass
    def visit_fromclause(self, fromclause):pass
    def visit_bindparam(self, bindparam):pass
    def visit_textclause(self, textclause):pass
    def visit_compound(self, compound):pass
    def visit_binary(self, binary):pass
    def visit_alias(self, alias):pass
    def visit_select(self, select):pass
    def visit_join(self, join):pass
    
class Compiled(ClauseVisitor):
    pass
    
class ClauseElement(object):
    """base class for elements of a generated SQL statement.
    
    includes a parameter hash to store bind parameter key/value pairs,
    as well as a list of 'from objects' which collects items to be placed
    in the FROM clause of a SQL statement.
    
    when many ClauseElements are attached together, the from objects and bind
    parameters are scooped up into the enclosing-most ClauseElement.
    """

    def _get_from_objects(self):raise NotImplementedError(repr(self))

    def accept_visitor(self, visitor): raise NotImplementedError(repr(self))

    def compile(self, engine, bindparams = None):
        return engine.compile(self, bindparams = bindparams)
    
    def _engine(self):
        raise NotImplementedError("Object %s has no built-in SQLEngine." % repr(self))
        
    def execute(self, **params):
        e = self._engine()
        c = self.compile(e, bindparams = params)
        return e.execute(str(c), c.get_params(**params))

    def result(self, **params):
        e = self._engine()
        c = self.compile(e, bindparams = params)
        return e.result(str(c), c.get_params(**params))
        
class ColumnClause(ClauseElement):
    """represents a column clause element in a SQL statement."""
    
    def __init__(self, text, selectable):
        self.text = text
        self.table = selectable
        self._impl = ColumnSelectable(self)

    columns = property(lambda self: [self])
    name = property(lambda self:self.text)
    key = property(lambda self:self.text)
    label = property(lambda self:self.text)
    fullname = property(lambda self:self.text)

    def accept_visitor(self, visitor): visitor.visit_columnclause(self)

    def _get_from_objects(self):
        return []

    def _make_proxy(self, selectable, name = None):
        c = ColumnClause(self.text or name, selectable)
        selectable.columns[c.key] = c
        c._impl = ColumnSelectable(c)
        return c

class FromClause(ClauseElement):
    """represents a FROM clause element in a SQL statement."""
    
    def __init__(self, params = None, from_name = None, from_key = None):
        self.from_name = from_name
        self.id = from_key or from_name
        
    def _get_from_objects(self):
        # this could also be [self], at the moment it doesnt matter to the Select object
        return []

    def _engine(self):
        return None
        
    def accept_visitor(self, visitor): visitor.visit_fromclause(self)
    
class BindParamClause(ClauseElement):
    def __init__(self, key, value, shortname = None):
        self.key = key
        self.value = value
        self.shortname = shortname
        self.fromobj = []

    def accept_visitor(self, visitor):
        visitor.visit_bindparam(self)

    def _get_from_objects(self):
        return []
      
class TextClause(ClauseElement):
    """represents any plain text WHERE clause or full SQL statement"""
    
    def __init__(self, text = "", params = None):
        self.text = text
        self.parens = False
        self.params = params or {}

    def accept_visitor(self, visitor): visitor.visit_textclause(self)

    def _get_from_objects(self):
        return []
        
class CompoundClause(ClauseElement):
    """represents a list of clauses joined by an operator"""
    def __init__(self, operator, *clauses):
        self.operator = operator
        self.fromobj = []
        self.clauses = []
        self.parens = False
        for c in clauses:
            if c is None: continue
            self.append(c)
    
    def append(self, clause):
        if type(clause) == str:
            clause = TextClause(clause)
        elif isinstance(clause, CompoundClause):
            clause.parens = True
            
        self.clauses.append(clause)
        self.fromobj += clause._get_from_objects()
        
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_compound(self)

    def _get_from_objects(self):
        return self.fromobj
        
class ClauseList(ClauseElement):
    def __init__(self, *clauses):
        self.clauses = clauses
        
    def accept_visitor(self, visitor):
        for c in self.clauses:
            c.accept_visitor(visitor)
        visitor.visit_clauselist(self)
        
        
class BinaryClause(ClauseElement):
    """represents two clauses with an operator in between"""
    
    def __init__(self, left, right, operator, fromobj = None):
        self.left = left
        self.right = right
        self.operator = operator
        self.fromobj = fromobj or []
        self.parens = False
        self.fromobj += left._get_from_objects()
        self.fromobj += right._get_from_objects()
        
    def _get_from_objects(self):
        return self.fromobj

    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        visitor.visit_binary(self)

        
class Selectable(FromClause):
    """represents a column list-holding object, like a table or subquery.  can be used anywhere
    a Table is used."""
    
    c = property(lambda self: self.columns)

    def accept_visitor(self, visitor):
        raise NotImplementedError()
    
    def select(self, whereclauses = None, **params):
        raise NotImplementedError()

class Join(Selectable):
    def __init__(self, left, right, onclause, isouter = False, allcols = True):
        self.left = left
        self.right = right
        self.id = self.left.id + "_" + self.right.id
        if allcols:
            self.columns = [c for c in self.left.columns] + [c for c in self.right.columns]
        else:
            self.columns = [c for c in self.right.columns]

        # TODO: if no onclause, do NATURAL JOIN
        self.onclause = onclause
        self.isouter = isouter
    
    def add_join(self, join):
        pass
        
    def select(self, whereclauses = None, **params):
        return select([self.left, self.right], and_(self.onclause, whereclauses), **params)
    
    def accept_visitor(self, visitor):
        self.left.accept_visitor(visitor)
        self.right.accept_visitor(visitor)
        self.onclause.accept_visitor(visitor)
        visitor.visit_join(self)
            
    def _engine(self):
        return self.left._engine() or self.right._engine()
        
    def _get_from_objects(self):
        m = {}
        for x in self.onclause._get_from_objects():
            m[x.id] = x
        result = [self] + [FromClause(from_key = c.id) for c in self.left._get_from_objects() + self.right._get_from_objects()] 
        for x in result:
            m[x.id] = x
        result = m.values()
        return result
        
class Alias(Selectable):
    def __init__(self, selectable, alias):
        self.selectable = selectable
        self.columns = util.OrderedProperties()
        self.name = alias
        self.id = self.name
        self.count = 0
        for co in selectable.columns:
            co._make_proxy(self)

    primary_keys = property (lambda self: [c for c in self.columns if c.primary_key])
    
    def accept_visitor(self, visitor):
        self.selectable.accept_visitor(visitor)
        visitor.visit_alias(self)

    def _get_from_objects(self):
        return [self]

    def _engine(self):
        return self.selectable._engine()

    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)


class ColumnSelectable(Selectable):
    """Selectable implementation that gets attached to a schema.Column object."""
    
    def __init__(self, column):
        self.column = column
        self.name = column.name
        self.columns = [self.column]

        if column.table.name:
            self.label = column.table.name + "_" + self.column.name
            self.fullname = column.table.name + "." + self.column.name
        else:
            self.label = self.column.name
            self.fullname = self.column.name
    
    def _get_from_objects(self):
        return [self.column.table]
    
    def _compare(self, operator, obj):
        if not isinstance(obj, BindParamClause) and not isinstance(obj, schema.Column):
            if self.column.table.name is None:
                obj = BindParamClause(self.name, obj, shortname = self.name)
            else:
                obj = BindParamClause(self.column.table.name + "_" + self.name, obj, shortname = self.name)
        
        return BinaryClause(self.column, obj, operator, [self.column.table])

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
    
    def startswith(self, other):
        return self._compare('LIKE', str(other) + "%")
    
    def endswith(self, other):
        return self._compare('LIKE', "%" + str(other))
        
class TableImpl(Selectable):
    """attached to a schema.Table to provide it with a Selectable interface
    as well as other functions
    """

    def _engine(self):
        return self.table.engine

    def select(self, whereclauses = None, **params):
        return select([self.table], whereclauses, **params)

    def insert(self, select = None):
        return insert(self.table, select = select)

    def update(self, whereclause = None, parameters = None):
        return update(self.table, whereclause, parameters)

    def delete(self, whereclause = None):
        return delete(self.table, whereclause)
        
    columns = property(lambda self: self.table.columns)

    def _get_from_objects(self):
        return [self.table]

    def build(self, sqlproxy = None, **params):
        if sqlproxy is None:
            sqlproxy = self.table.engine.proxy()
        
        self.table.accept_visitor(self.table.engine.schemagenerator(sqlproxy, **params))

    def drop(self, sqlproxy = None, **params):
        if sqlproxy is None:
            sqlproxy = self.table.engine.proxy()
        
        self.table.accept_visitor(self.table.engine.schemadropper(sqlproxy, **params))
        
    
class Select(Selectable):
    """finally, represents a SELECT statement, with appendable clauses, as well as 
    the ability to execute itself and return a result set."""
    def __init__(self, columns, whereclause = None, from_obj = [], group_by = None, order_by = None, use_labels = False, engine = None):
        self.columns = util.OrderedProperties()
        self.froms = util.OrderedDict()
        self.use_labels = use_labels
        self.id = id(self)
        self.name = None
        self.whereclause = whereclause
        self.engine = engine
        
        self._text = None
        self._raw_columns = []
        self._clauses = []
        
        for c in columns:
            self.append_column(c)
            
        if whereclause is not None:
            self.set_whereclause(whereclause)
    
        for f in from_obj:
            self.append_from(f)

        if group_by:
            self.append_clause("GROUP_BY", group_by)

        if order_by:
            self.order_by(*order_by)

    def append_column(self, column):
        if type(column) == str:
            column = ColumnClause(column, self)

        self._raw_columns.append(column)

        for f in column._get_from_objects():
            self.froms.setdefault(f.id, f)
                
        for co in column.columns:
            if self.use_labels:
                co._make_proxy(self, name = co.label)
            else:
                co._make_proxy(self)

    def set_whereclause(self, whereclause):
        if type(whereclause) == str:
            self.whereclause = TextClause(whereclause)
            
        for f in self.whereclause._get_from_objects():
            self.froms.setdefault(f.id, f)

    def append_from(self, fromclause):
        if type(fromclause) == str:
            fromclause = FromClause(from_name = fromclause)

        self.froms[fromclause.id] = fromclause

        for r in fromclause._get_from_objects():
            self.froms[r.id] = r
        

    def append_clause(self, keyword, clause):
        if type(clause) == str:
            clause = TextClause(clause)
        
        self._clauses.append((keyword, clause))
        
    def compile(self, engine = None, bindparams = None):
        if engine is None:
            if self.engine is None:
                for f in self.froms.values():
                    self.engine = f._engine()
                    if self.engine is not None: break
                    
            engine = self.engine
            
        if engine is None:
            raise "no engine supplied, and no engine could be located within the clauses!"

        return engine.compile(self, bindparams)

    def accept_visitor(self, visitor):
#        for c in self._raw_columns:
#            c.accept_visitor(visitor)
        for f in self.froms.values():
            f.accept_visitor(visitor)
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)
        for tup in self._clauses:
            tup[1].accept_visitor(visitor)
            
        visitor.visit_select(self)
    
    def order_by(self, *clauses):
        self.append_clause("ORDER BY", ClauseList(*clauses))
        
    def select(self, whereclauses = None, **params):
        return select([self], whereclauses, **params)

    def _engine(self):
        """tries to return a SQLEngine, either explicitly set in this object, or searched
        within the from clauses for one"""
        
        if self.engine:
            return self.engine
            
        for f in self.froms.values():
            e = f._engine()
            if e:
                return e
            
        return None

    def _get_from_objects(self):
        return [self]


class UpdateBase(ClauseElement):
    def get_colparams(self, parameters):
        values = []

        if parameters is None:
            parameters = self.parameters
            
        if parameters is None:
            for c in self.table.columns:
                values.append((c, bindparam(c.name)))                
        else:
            d = {}
            for key, value in parameters.iteritems():
                if isinstance(key, schema.Column):
                    d[key] = value
                else:
                    d[self.table.columns[str(key)]] = value
                
            for c in self.table.columns:
                if d.has_key(c):
                    value = d[c]
                    if not isinstance(value, BindParamClause):
                        value = bindparam(c.name, value)
                    values.append((c, value))
        return values

    def _engine(self):
        return self.engine

    def compile(self, engine = None, bindparams = None):
        if engine is None:
            engine = self.engine
            
        if engine is None:
            raise "no engine supplied, and no engine could be located within the clauses!"

        return engine.compile(self, bindparams)

class Insert(UpdateBase):
    def __init__(self, table, parameters = None, **params):
        self.table = table
        self.select = None
        self.parameters = parameters
        self.engine = self.table._engine()
        
    def accept_visitor(self, visitor):
        if self.select is not None:
            self.select.accept_visitor(visitor)

        visitor.visit_insert(self)

    def _engine(self):
        return self.engine
        
    def compile(self, engine = None, bindparams = None):
        if engine is None:
            engine = self.engine
            
        if engine is None:
            raise "no engine supplied, and no engine could be located within the clauses!"

        return engine.compile(self, bindparams)

class Update(UpdateBase):
    def __init__(self, table, whereclause, parameters = None, **params):
        self.table = table
        self.whereclause = whereclause

        self.parameters = parameters
        self.engine = self.table._engine()

    
    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)

        visitor.visit_update(self)

class Delete(UpdateBase):
    def __init__(self, table, whereclause, **params):
        self.table = table
        self.whereclause = whereclause

        self.engine = self.table._engine()

    
    def accept_visitor(self, visitor):
        if self.whereclause is not None:
            self.whereclause.accept_visitor(visitor)

        visitor.visit_delete(self)
        
