# schema.py
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

from sqlalchemy.util import *
from sqlalchemy.types import *
import copy, re


__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'SchemaEngine', 'SchemaVisitor']


class SchemaItem(object):
    """base class for items that define a database schema."""
    def _init_items(self, *args):
        for item in args:
            if item is not None:
                item._set_parent(self)

    def accept_visitor(self, visitor):
        raise NotImplementedError()

    def _set_parent(self, parent):
        """a child item attaches itself to its parent via this method."""
        raise NotImplementedError()

    def hash_key(self):
        """returns a string that identifies this SchemaItem uniquely"""
        return repr(self)

    def __getattr__(self, key):
        """proxies method calls to an underlying implementation object for methods not found
        locally"""
        if not hasattr(self, '_impl'):
            raise AttributeError(key)
        return getattr(self._impl, key)

def _get_table_key(engine, name, schema):
    if schema is not None and schema == engine.get_default_schema_name():
        schema = None
    if schema is None:
        return name
    else:
        return schema + "." + name
        
class TableSingleton(type):
    def __call__(self, name, engine, *args, **kwargs):
        try:
            schema = kwargs.get('schema', None)
            autoload = kwargs.pop('autoload', False)
            redefine = kwargs.pop('redefine', False)
            mustexist = kwargs.pop('mustexist', False)
            useexisting = kwargs.pop('useexisting', False)
            key = _get_table_key(engine, name, schema)
            table = engine.tables[key]
            if len(args):
                if redefine:
                    table.reload_values(*args)
                elif not useexisting:
                    raise "Table '%s.%s' is already defined. specify 'redefine=True' to remap columns, or 'useexisting=True' to use the existing table" % (schema, name)
            return table
        except KeyError:
            if mustexist:
                raise "Table '%s.%s' not defined" % (schema, name)
            table = type.__call__(self, name, engine, *args, **kwargs)
            engine.tables[key] = table
            # load column definitions from the database if 'autoload' is defined
            # we do it after the table is in the singleton dictionary to support
            # circular foreign keys
            if autoload:
                engine.reflecttable(table)

            return table

        
class Table(SchemaItem):
    """represents a relational database table.  
    
    Be sure to look at sqlalchemy.sql.TableImpl for additional methods defined on a Table."""
    __metaclass__ = TableSingleton
    
    def __init__(self, name, engine, *args, **kwargs):
        self.name = name
        self.columns = OrderedProperties()
        self.c = self.columns
        self.foreign_keys = []
        self.primary_key = []
        self.engine = engine
        self._impl = self.engine.tableimpl(self)
        self._init_items(*args)
        self.schema = kwargs.pop('schema', None)
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name
        if len(kwargs):
            raise "Unknown arguments passed to Table: " + repr(kwargs.keys())
    
    def reload_values(self, *args):
        self.columns = OrderedProperties()
        self.c = self.columns
        self.foreign_keys = []
        self.primary_key = []
        self._impl = self.engine.tableimpl(self)
        self._init_items(*args)

    def append_item(self, item):
        self._init_items(item)
        
    def _set_parent(self, schema):
        schema.tables[self.name] = self
        self.schema = schema

    def accept_visitor(self, visitor): 
        for c in self.columns:
            c.accept_visitor(visitor)
        return visitor.visit_table(self)
    
    def deregister(self):
        """removes this table from it's engines table registry.  this does not
        issue a SQL DROP statement."""
        key = _get_table_key(self.engine, self.name, self.schema)
        del self.engine.tables[key]
        
    def toengine(self, engine, schema=None):
        """returns a singleton instance of this Table with a different engine"""
        try:
            if schema is None:
                schema = self.schema
            key = _get_table_key(engine, self.name, schema)
            return engine.tables[key]
        except:
            args = []
            for c in self.columns:
                args.append(c.copy())
            return Table(self.name, engine, schema=schema, *args)

class Column(SchemaItem):
    """represents a column in a database table."""
    def __init__(self, name, type, *args, **kwargs):
        self.name = name
        self.type = type
        self.args = args
        self.key = kwargs.pop('key', name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.hidden = kwargs.pop('hidden', False)
        self.foreign_key = None
        self.sequence = None
        self._orig = None
        if len(kwargs):
            raise "Unknown arguments passed to Column: " + repr(kwargs.keys())
        
    original = property(lambda s: s._orig or s)
    engine = property(lambda s: s.table.engine)
    
    def _set_primary_key(self):
        if self.primary_key:
            return
        self.primary_key = True
        self.nullable = False
        self.table.primary_key.append(self)
            
    def _set_parent(self, table):
        if not self.hidden:
            table.columns[self.key] = self
            if self.primary_key:
                table.primary_key.append(self)
        self.table = table
        if self.table.engine is not None:
            self.type = self.table.engine.type_descriptor(self.type)
            
        self._impl = self.table.engine.columnimpl(self)

        self._init_items(*self.args)
        self.args = None

    def copy(self):
        """creates a copy of this Column, unitialized"""
        if self.foreign_key is None:
            fk = None
        else:
            fk = self.foreign_key.copy()
        return Column(self.name, self.type, fk, self.sequence, key = self.key, primary_key = self.primary_key)
        
    def _make_proxy(self, selectable, name = None):
        """creates a copy of this Column, initialized the way this Column is"""
        if self.foreign_key is None:
            fk = None
        else:
            fk = self.foreign_key.copy()
        c = Column(name or self.name, self.type, fk, self.sequence, key = name or self.key, primary_key = self.primary_key, hidden=self.hidden)
        c.table = selectable
        c._orig = self.original
        if not c.hidden:
            selectable.columns[c.key] = c
        c._impl = self.engine.columnimpl(c)
        return c

    def accept_visitor(self, visitor):
        if self.sequence is not None:
            self.sequence.accept_visitor(visitor)
        if self.foreign_key is not None:
            self.foreign_key.accept_visitor(visitor)
        visitor.visit_column(self)

    def __lt__(self, other): return self._impl.__lt__(other)
    def __le__(self, other): return self._impl.__le__(other)
    def __eq__(self, other): return self._impl.__eq__(other)
    def __ne__(self, other): return self._impl.__ne__(other)
    def __gt__(self, other): return self._impl.__gt__(other)
    def __ge__(self, other): return self._impl.__ge__(other)
    def __add__(self, other): return self._impl.__add__(other)
    def __sub__(self, other): return self._impl.__sub__(other)
    def __mul__(self, other): return self._impl.__mul__(other)
    def __and__(self, other): return self._impl.__and__(other)
    def __or__(self, other): return self._impl.__or__(other)
    def __div__(self, other): return self._impl.__div__(other)
    def __truediv__(self, other): return self._impl.__truediv__(other)
    def __invert__(self, other): return self._impl.__invert__(other)
    def __str__(self): return self._impl.__str__()

class ForeignKey(SchemaItem):
    def __init__(self, column):
        self._colspec = column
        self._column = None

    def copy(self):
        if isinstance(self._colspec, str):
            return ForeignKey(self._colspec)
        else:
            return ForeignKey("%s.%s" % (self._colspec.table.name, self._colspec.column.key))
    
    def references(self, table):
        """returns True if the given table is referenced by this ForeignKey."""
        return (
            # simple test
            self.column.table is table      
            or
            # test for an indirect relation via a Selectable
            table._get_col_by_original(self.column) is not None
        )
        
    def _init_column(self):
        # ForeignKey inits its remote column as late as possible, so tables can
        # be defined without dependencies
        if self._column is None:
            if isinstance(self._colspec, str):
                m = re.match(r"([\w_-]+)(?:\.([\w_-]+))?", self._colspec)
                if m is None:
                    raise "Invalid foreign key column specification: " + self._colspec
                (tname, colname) = m.group(1, 2)
                table = Table(tname, self.parent.engine, mustexist = True)
                if colname is None:
                    key = self.parent
                    self._column = table.c[self.parent.key]
                else:
                    self._column = table.c[colname]
            else:
                self._column = self._colspec

        return self._column
            
    column = property(lambda s: s._init_column())

    def accept_visitor(self, visitor):
        visitor.visit_foreign_key(self)
        
    def _set_parent(self, column):
        if not isinstance(column, Column):
          raise "hi" + repr(type(column))
        self.parent = column
        self.parent.foreign_key = self
        self.parent.table.foreign_keys.append(self)
        
class Sequence(SchemaItem):
    """represents a sequence, which applies to Oracle and Postgres databases."""
    def __init__(self, name, start = None, increment = None, optional=False):
        self.name = name
        self.start = start
        self.increment = increment
        self.optional=optional
    def _set_parent(self, column):
        self.column = column
        self.column.sequence = self
    def accept_visitor(self, visitor):
        return visitor.visit_sequence(self)

class SchemaEngine(object):
    """a factory object used to create implementations for schema objects"""
    def tableimpl(self, table):
        raise NotImplementedError()
    def columnimpl(self, column):
        raise NotImplementedError()
    def reflecttable(self, table):
        raise NotImplementedError()
        
class SchemaVisitor(object):
    """base class for an object that traverses across Schema objects"""

    def visit_schema(self, schema):pass
    def visit_table(self, table):pass
    def visit_column(self, column):pass
    def visit_foreign_key(self, join):pass
    def visit_index(self, index):pass
    def visit_sequence(self, sequence):pass

            
            