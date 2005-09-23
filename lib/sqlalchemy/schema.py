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


__ALL__ = ['Table', 'Column', 'Sequence', 'ForeignKey']


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
        """proxies method calls to an underlying implementation object for methods not found locally"""
        if not hasattr(self, '_impl'):
            raise AttributeError(key)
        return getattr(self._impl, key)


class TableSingleton(type):
    def __call__(self, name, engine, *args, **kwargs):
        try:
            return engine.tables[name]
        except:
            if kwargs.get('mustexist', False):
                raise "Table '%s' not defined" % name
            table = type.__call__(self, name, engine, *args, **kwargs)
            engine.tables[name] = table
            # load column definitions from the database if 'autoload' is defined
            # we do it after the table is in the singleton dictionary to support
            # circular foreign keys
            if kwargs.get('autoload', False):
                engine.reflecttable(table)

            return table

        
        
class Table(SchemaItem):
    """represents a relational database table."""
    __metaclass__ = TableSingleton
    
    def __init__(self, name, engine, *args, **kwargs):
        self.name = name
        self.columns = OrderedProperties()
        self.c = self.columns
        self.foreign_keys = OrderedProperties()
        self.primary_keys = []
        self.engine = engine
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

class Column(SchemaItem):
    """represents a column in a database table."""
    def __init__(self, name, type, key = None, primary_key = False, foreign_key = None, sequence = None, nullable = True):
        self.name = name
        self.type = type
        self.sequence = sequence
        self.foreign_key = foreign_key
        self.key = key or name
        self.primary_key = primary_key
        if primary_key:
            nullable = False
        self.nullable = nullable
        self._orig = None
        
    original = property(lambda s: s._orig or s)
    
    def _set_parent(self, table):
        table.columns[self.key] = self
        if self.primary_key:
            table.primary_keys.append(self)
        self.table = table
        self.engine = table.engine
        self.type = self.engine.type_descriptor(self.type)
        self._impl = self.engine.columnimpl(self)
        
        if self.foreign_key is not None:
            self._init_items(self.foreign_key)
#            table.foreign_keys[self.foreign_key.column.key] = self.foreign_key

    def set_foreign_key(self, fk):
        self.foreign_key = fk
        self._init_items(self.foreign_key)
#        self.table.foreign_keys[self.foreign_key.column.key] = self.foreign_key
    
    def _make_proxy(self, selectable, name = None):
        """creates a copy of this Column for use in a new selectable unit"""
        # using copy.copy(c) seems to add a full second to the select.py unittest package
        #c = copy.copy(self)
        #if name is not None:
         #   c.name = name
         #   c.key = name
        # TODO: do we want the same foreign_key object here ?  
        c = Column(name or self.name, self.type, key = name or self.key, primary_key = self.primary_key, foreign_key = self.foreign_key, sequence = self.sequence)
        c.table = selectable
        c.engine = self.engine
        c._orig = self.original
        selectable.columns[c.key] = c
        c._impl = self.engine.columnimpl(c)
        return c

    def accept_visitor(self, visitor): 
        return visitor.visit_column(self)

    def __lt__(self, other): return self._impl.__lt__(other)
    def __le__(self, other): return self._impl.__le__(other)
    def __eq__(self, other): return self._impl.__eq__(other)
    def __ne__(self, other): return self._impl.__ne__(other)
    def __gt__(self, other): return self._impl.__gt__(other)
    def __ge__(self, other): return self._impl.__ge__(other)
    def __str__(self): return self._impl.__str__()

class ForeignKey(SchemaItem):
    def __init__(self, column):
        self._colspec = column
        self._column = None

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

            self.parent.table.foreign_keys[self._column.key] = self
        return self._column
            
    column = property(lambda s: s._init_column())
    
    def _set_parent(self, column):
        self.parent = column
        
class Sequence(SchemaItem):
    """represents a sequence, which applies to Oracle and Postgres databases."""
    def _set_parent(self, column, key):
        self.column = column
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

        
