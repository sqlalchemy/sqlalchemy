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

engine = None


__ALL__ = ['Table', 'Column', 'Relation', 'Sequence', 
            'INT', 'CHAR', 'VARCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 
            'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB', 'BOOLEAN'
            ]


class INT: pass

class CHAR:
    def __init__(self, length):
        self.length = length
        
class VARCHAR:
    def __init__(self, length):
        self.length = length
        
class TEXT: pass
class FLOAT:
    def __init__(self, precision, length):
        self.precision = precision
        self.length = length
        
class DECIMAL: pass
class TIMESTAMP: pass
class DATETIME: pass
class CLOB: pass
class BLOB: pass
class BOOLEAN: pass


class SchemaItem(object):
    """base class for items that define a database schema."""
    def _init_items(self, *args):
        for item in args:
            item._set_parent(self)
            
    def accept_visitor(self, visitor): raise NotImplementedError()
    def _set_parent(self, parent): raise NotImplementedError()

    def __getattr__(self, key):
        return getattr(self._impl, key)

class Table(SchemaItem):
    """represents a relational database table."""
    
    def __init__(self, name, engine, *args, **params):
        self.name = name
        self.columns = OrderedProperties()
        self.c = self.columns
        self.relations = []
        self.engine = engine
        self._impl = self.engine.tableimpl(self)
        self._init_items(*args)
        
    def _set_parent(self, schema):
        schema.tables[self.name] = self
        self.schema = schema

    primary_keys = property (lambda self: [c for c in self.columns if c.primary_key])
        
    def accept_visitor(self, visitor): 
        for c in self.columns:
            c.accept_visitor(visitor)
        return visitor.visit_table(self)

class Column(SchemaItem):
    """represents a column in a database table."""
    def __init__(self, name, type, reference = None, key = None, primary_key = False, *args, **params):
        self.name = name
        self.type = type
        self.sequences = OrderedProperties()
        self.reference = reference
        self.key = key or name
        self.primary_key = primary_key
        self._items = args

    def _set_parent(self, table):
        table.columns[self.key] = self
        self.table = table
        self.engine = table.engine

        self._impl = self.engine.columnimpl(self)
                        
        self._init_items(*self._items)
 
        if self.reference is not None:
            Relation(self.table, self.reference.table, self == self.reference)

    def _make_proxy(self, selectable, name = None):
        # wow! using copy.copy(c) adds a full second to the select.py unittest package
        c = Column(name or self.name, self.type, key = name or self.key, primary_key = self.primary_key)
        c.table = selectable
        c.engine = self.engine
        c._items = self._items
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


class Relation(SchemaItem):
    def __init__(self, parent, child, relationship, association = None, lazy = True):
        self.parent = parent
        self.child = child
        self.relationship = relationship
        self.lazy = lazy
        self.association = association

        self._set_parent(parent)

    def _set_parent(self, table):
        table.relations.append(self)
        self.table = table

    def accept_visitor(self, visitor):
        visitor.visit_relation(self)
            
class Sequence(SchemaItem):
    """represents a sequence."""
    def set_parent(self, column, key):
        column.sequences[key] = self
        self.column = column
        
    def accept_visitor(self, visitor): 
        return visitor.visit_sequence(self)
        
class SchemaEngine(object):
    def tableimpl(self, table):
        raise NotImplementedError()
        
    def columnimpl(self, column):
        raise NotImplementedError()

class SchemaVisitor(object):
        """base class for an object that traverses a Schema object structure,
        or sub-objects within one, and acts upon each node."""

        def visit_schema(self, schema):pass
        def visit_table(self, table):pass
        def visit_column(self, column):pass
        def visit_relation(self, join):pass
        def visit_index(self, index):pass
        def visit_sequence(self, sequence):pass

        
