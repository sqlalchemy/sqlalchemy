# schema.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""the schema module provides the building blocks for database metadata.  This means
all the entities within a SQL database that we might want to look at, modify, or create
and delete are described by these objects, in a database-agnostic way.   

A structure of SchemaItems also provides a "visitor" interface which is the primary 
method by which other methods operate upon the schema.  The SQL package extends this
structure with its own clause-specific objects as well as the visitor interface, so that
the schema package "plugs in" to the SQL package.

"""

from sqlalchemy.util import *
from sqlalchemy.types import *
import copy, re, string

__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'SchemaEngine', 'SchemaVisitor']


class SchemaItem(object):
    """base class for items that define a database schema."""
    def _init_items(self, *args):
        for item in args:
            if item is not None:
                item._set_parent(self)

    def accept_visitor(self, visitor):
        """all schema items implement an accept_visitor method that should call the appropriate
        visit_XXXX method upon the given visitor object."""
        raise NotImplementedError()

    def _set_parent(self, parent):
        """a child item attaches itself to its parent via this method."""
        raise NotImplementedError()

    def hash_key(self):
        """returns a string that identifies this SchemaItem uniquely"""
        return "%s(%d)" % (self.__class__.__name__, id(self))

    def __repr__(self):
        return "%s()" % self.__class__.__name__

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
    """a metaclass used by the Table object to provide singleton behavior."""
    def __call__(self, name, engine, *args, **kwargs):
        try:
            name = str(name)    # in case of incoming unicode
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
        """Table objects can be constructed directly.  The init method is actually called via 
        the TableSingleton metaclass.  Arguments are:
        
        name : the name of this table, exactly as it appears, or will appear, in the database.
        This property, along with the "schema", indicates the "singleton identity" of this table.
        Further tables constructed with the same name/schema combination will return the same 
        Table instance.
        
        engine : a SchemaEngine instance to provide services to this table.  Usually a subclass of
        sql.SQLEngine.
        
        *args : should contain a listing of the Column objects for this table.
        
        **kwargs : options include:
        
        schema=None : the "schema name" for this table, which is required if the table resides in a 
        schema other than the default selected schema for the engine's database connection.
        
        autoload=False : the Columns for this table should be reflected from the database.  Usually
        there will be no Column objects in the constructor if this property is set.
        
        redefine=False : if this Table has already been defined in the application, clear out its columns
        and redefine with new arguments.
        
        mustexist=False : indicates that this Table must already have been defined elsewhere in the application,
        else an exception is raised.
        
        useexisting=False : indicates that if this Table was already defined elsewhere in the application, disregard
        the rest of the constructor arguments.  If this flag and the "redefine" flag are not set, constructing 
        the same table twice will result in an exception.
        
        """
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

    def __repr__(self):
       return "Table(%s)" % string.join(
        [repr(self.name)] + [repr(self.engine)] +
        [repr(x) for x in self.columns] +
        ["%s=%s" % (k, repr(getattr(self, k))) for k in ['schema']]
       , ',\n')
    
    def reload_values(self, *args):
        """clears out the columns and other properties of this Table, and reloads them from the 
        given argument list.  This is used with the "redefine" keyword argument sent to the
        metaclass constructor."""
        self.columns = OrderedProperties()
        self.c = self.columns
        self.foreign_keys = []
        self.primary_key = []
        self._impl = self.engine.tableimpl(self)
        self._init_items(*args)

    def append_item(self, item):
        """appends a Column item or other schema item to this Table."""
        self._init_items(item)
        
    def _set_parent(self, schema):
        schema.tables[self.name] = self
        self.schema = schema

    def accept_visitor(self, visitor): 
        """traverses the given visitor across the Column objects inside this Table,
        then calls the visit_table method on the visitor."""
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
        """constructs a new Column object.  Arguments are:
        
        name : the name of this column.  this should be the identical name as it appears,
        or will appear, in the database.
        
        type : this is the type of column. This can be any subclass of types.TypeEngine,
        including the database-agnostic types defined in the types module, database-specific types
        defined within specific database modules, or user-defined types.
        
        *args : ForeignKey and Sequence objects should be added as list values.
        
        **kwargs : keyword arguments include:
        
        key=None : an optional "alias name" for this column.  The column will then be identified everywhere
        in an application, including the column list on its Table, by this key, and not the given name.  
        Generated SQL, however, will still reference the column by its actual name.
        
        primary_key=False : True if this column is a primary key column.  Multiple columns can have this flag
        set to specify composite primary keys.
        
        nullable=True : True if this column should allow nulls. Defaults to True unless this column is a primary
        key column.
        
        default=None : a scalar, python callable, or ClauseElement representing the "default value" for this column,
        which will be invoked upon insert if this column is not present in the insert list or is given a value
        of None.
        
        hidden=False : indicates this column should not be listed in the table's list of columns.  Used for the "oid" 
        column, which generally isnt in column lists.
        """
        self.name = str(name) # in case of incoming unicode
        self.type = type
        self.args = args
        self.key = kwargs.pop('key', name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.hidden = kwargs.pop('hidden', False)
        self.default = kwargs.pop('default', None)
        self.foreign_key = None
        self._orig = None
        if len(kwargs):
            raise "Unknown arguments passed to Column: " + repr(kwargs.keys())
        
    original = property(lambda s: s._orig or s)
    engine = property(lambda s: s.table.engine)
     
    def __repr__(self):
       return "Column(%s)" % string.join(
        [repr(self.name)] + [repr(self.type)] +
        [repr(x) for x in [self.foreign_key] if x is not None] +
        ["%s=%s" % (k, repr(getattr(self, k))) for k in ['key', 'primary_key', 'nullable', 'hidden', 'default']]
       , ',')
        
    def append_item(self, item):
        self._init_items(item)
        
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

        if self.default is not None:
            self.default = ColumnDefault(self.default)
            self._init_items(self.default)
        self._init_items(*self.args)
        self.args = None

    def copy(self):
        """creates a copy of this Column, unitialized"""
        if self.foreign_key is None:
            fk = None
        else:
            fk = self.foreign_key.copy()
        return Column(self.name, self.type, fk, self.default, key = self.key, primary_key = self.primary_key)
        
    def _make_proxy(self, selectable, name = None):
        """creates a copy of this Column, initialized the way this Column is"""
        if self.foreign_key is None:
            fk = None
        else:
            fk = self.foreign_key.copy()
        c = Column(name or self.name, self.type, fk, self.default, key = name or self.key, primary_key = self.primary_key, hidden=self.hidden)
        c.table = selectable
        c._orig = self.original
        if not c.hidden:
            selectable.columns[c.key] = c
            if self.primary_key:
                selectable.primary_key.append(c)
        c._impl = self.engine.columnimpl(c)
        if fk is not None:
            c._init_items(fk)
        return c

    def accept_visitor(self, visitor):
        """traverses the given visitor to this Column's default and foreign key object,
        then calls visit_column on the visitor."""
        if self.default is not None:
            self.default.accept_visitor(visitor)
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
    """defines a ForeignKey constraint between two columns.  ForeignKey is 
    specified as an argument to a Column object."""
    def __init__(self, column):
        """Constructs a new ForeignKey object.  "column" can be a schema.Column
        object representing the relationship, or just its string name given as 
        "tablename.columnname".  schema can be specified as 
        "schemaname.tablename.columnname" """
        self._colspec = column
        self._column = None

    def __repr__(self):
        return "ForeignKey(%s)" % repr(self._get_colspec())
        
    def copy(self):
        """produces a copy of this ForeignKey object."""
        return ForeignKey(self._get_colspec())
    
    def _get_colspec(self):
        if isinstance(self._colspec, str):
            return self._colspec
        elif self._colspec.table.schema is not None:
            return "%s.%s.%s" % (self._colspec.table.schema, self._colspec.table.name, self._colspec.column.key)
        else:
            return "%s.%s" % (self._colspec.table.name, self._colspec.column.key)
        
    def references(self, table):
        """returns True if the given table is referenced by this ForeignKey."""
        try:
            return table._get_col_by_original(self.column) is not None
        except:
            x = self._init_column()
        
    def _init_column(self):
        # ForeignKey inits its remote column as late as possible, so tables can
        # be defined without dependencies
        if self._column is None:
            if isinstance(self._colspec, str):
                m = re.match(r"^([\w_-]+)(?:\.([\w_-]+))?(?:\.([\w_-]+))?$", self._colspec)
                if m is None:
                    raise ValueError("Invalid foreign key column specification: " + self._colspec)
                if m.group(3) is None:
                    (tname, colname) = m.group(1, 2)
                    # use default schema
                    schema = None
                else:
                    (schema,tname,colname) = m.group(1,2,3)
                table = Table(tname, self.parent.engine, mustexist=True, schema=schema)
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
        """calls the visit_foreign_key method on the given visitor."""
        visitor.visit_foreign_key(self)
        
    def _set_parent(self, column):
        self.parent = column
        # if a foreign key was already set up for this, replace it with 
        # this one, including removing from the parent
        if self.parent.foreign_key is not None:
            self.parent.table.foreign_keys.remove(self.parent.foreign_key)
        self.parent.foreign_key = self
        self.parent.table.foreign_keys.append(self)

class DefaultGenerator(SchemaItem):
    """Base class for column "default" values, which can be a plain default
    or a Sequence."""
    def _set_parent(self, column):
        self.column = column
        self.column.default = self
    def __repr__(self):
        return "DefaultGenerator()"
        
class ColumnDefault(DefaultGenerator):
    """A plain default value on a column.  this could correspond to a constant, 
    a callable function, or a SQL clause."""
    def __init__(self, arg):
        self.arg = arg
    def accept_visitor(self, visitor):
        """calls the visit_column_default method on the given visitor."""
        return visitor.visit_column_default(self)
    def __repr__(self):
        return "ColumnDefault(%s)" % repr(self.arg)
        
class Sequence(DefaultGenerator):
    """represents a sequence, which applies to Oracle and Postgres databases."""
    def __init__(self, name, start = None, increment = None, optional=False):
        self.name = name
        self.start = start
        self.increment = increment
        self.optional=optional
    def __repr__(self):
        return "Sequence(%s)" % string.join(
             [repr(self.name)] +
             ["%s=%s" % (k, repr(getattr(self, k))) for k in ['start', 'increment', 'optional']]
            , ',')
    
    def accept_visitor(self, visitor):
        """calls the visit_seauence method on the given visitor."""
        return visitor.visit_sequence(self)

class SchemaEngine(object):
    """a factory object used to create implementations for schema objects.  This object
    is the ultimate base class for the engine.SQLEngine class."""
    def tableimpl(self, table):
        """returns a new implementation object for a Table (usually sql.TableImpl)"""
        raise NotImplementedError()
    def columnimpl(self, column):
        """returns a new implementation object for a Column (usually sql.ColumnImpl)"""
        raise NotImplementedError()
    def reflecttable(self, table):
        """given a table, will query the database and populate its Column and ForeignKey 
        objects."""
        raise NotImplementedError()
        
class SchemaVisitor(object):
    """base class for an object that traverses across Schema structures."""
    def visit_schema(self, schema):
        """visit a generic SchemaItem"""
        pass
    def visit_table(self, table):
        """visit a Table."""
        pass
    def visit_column(self, column):
        """visit a Column."""
        pass
    def visit_foreign_key(self, join):
        """visit a ForeignKey."""
        pass
    def visit_index(self, index):
        """visit an Index (not implemented yet)."""
        pass
    def visit_column_default(self, default):
        """visit a ColumnDefault."""
        pass
    def visit_sequence(self, sequence):
        """visit a Sequence."""
        pass

            
            