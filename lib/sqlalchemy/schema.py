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
import sql
from util import *
from types import *
from exceptions import *
import copy, re, string

__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'Index',
           'SchemaEngine', 'SchemaVisitor', 'PassiveDefault', 'ColumnDefault']

class SchemaItem(object):
    """base class for items that define a database schema."""
    def _init_items(self, *args):
        for item in args:
            if item is not None:
                item._set_parent(self)
    def _set_parent(self, parent):
        """a child item attaches itself to its parent via this method."""
        raise NotImplementedError()
    def __repr__(self):
        return "%s()" % self.__class__.__name__

def _get_table_key(engine, name, schema):
    if schema is not None and schema == engine.get_default_schema_name():
        schema = None
    if schema is None:
        return name
    else:
        return schema + "." + name
        
class TableSingleton(type):
    """a metaclass used by the Table object to provide singleton behavior."""
    def __call__(self, name, engine=None, *args, **kwargs):
        try:
            if not isinstance(engine, SchemaEngine):
                args = [engine] + list(args)
                engine = None
            if engine is None:
                engine = default_engine
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
                    raise ArgumentError("Table '%s.%s' is already defined. specify 'redefine=True' to remap columns, or 'useexisting=True' to use the existing table" % (schema, name))
            return table
        except KeyError:
            if mustexist:
                raise ArgumentError("Table '%s.%s' not defined" % (schema, name))
            table = type.__call__(self, name, engine, **kwargs)
            engine.tables[key] = table
            # load column definitions from the database if 'autoload' is defined
            # we do it after the table is in the singleton dictionary to support
            # circular foreign keys
            if autoload:
                engine.reflecttable(table)
            # initialize all the column, etc. objects.  done after
            # reflection to allow user-overrides
            table._init_items(*args)
            return table

        
class Table(sql.TableClause, SchemaItem):
    """represents a relational database table.  This subclasses sql.TableClause to provide
    a table that is "wired" to an engine.  Whereas TableClause represents a table as its 
    used in a SQL expression, Table represents a table as its created in the database.  
    
    Be sure to look at sqlalchemy.sql.TableImpl for additional methods defined on a Table."""
    __metaclass__ = TableSingleton
    
    def __init__(self, name, engine, **kwargs):
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
        super(Table, self).__init__(name)
        self._engine = engine
        self.schema = kwargs.pop('schema', None)
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name
        self.kwargs = kwargs
        
    def __repr__(self):
        return "Table(%s)" % string.join(
        [repr(self.name)] + [repr(self.engine)] +
        [repr(x) for x in self.columns] +
        ["%s=%s" % (k, repr(getattr(self, k))) for k in ['schema']]
       , ',\n')
    
    def __str__(self):
        if self.schema is None:
            return self.name
        else:
            return self.schema + "." + self.name
        
    def reload_values(self, *args):
        """clears out the columns and other properties of this Table, and reloads them from the 
        given argument list.  This is used with the "redefine" keyword argument sent to the
        metaclass constructor."""
        self._clear()
        
        self._init_items(*args)

    def append_item(self, item):
        """appends a Column item or other schema item to this Table."""
        self._init_items(item)
    
    def append_column(self, column):
        if not column.hidden:
            self._columns[column.key] = column
        if column.primary_key:
            self.primary_key.append(column)
        column.table = self
        column.type = self.engine.type_descriptor(column.type)

    def append_index(self, index):
        self.indexes[index.name] = index
        
    def _set_parent(self, schema):
        schema.tables[self.name] = self
        self.schema = schema
    def accept_schema_visitor(self, visitor): 
        """traverses the given visitor across the Column objects inside this Table,
        then calls the visit_table method on the visitor."""
        for c in self.columns:
            c.accept_schema_visitor(visitor)
        return visitor.visit_table(self)

    def append_index_column(self, column, index=None, unique=None):
        """Add an index or a column to an existing index of the same name.
        """
        if index is not None and unique is not None:
            raise ValueError("index and unique may not both be specified")
        if index:
            if index is True:
                name = 'ix_%s' % column.name
            else:
                name = index
        elif unique:
            if unique is True:
                name = 'ux_%s' % column.name
            else:
                name = unique
        # find this index in self.indexes
        # add this column to it if found
        # otherwise create new
        try:
            index = self.indexes[name]
            index.append_column(column)
        except KeyError:
            index = Index(name, column, unique=unique)
        return index
    
    def deregister(self):
        """removes this table from it's engines table registry.  this does not
        issue a SQL DROP statement."""
        key = _get_table_key(self.engine, self.name, self.schema)
        del self.engine.tables[key]
    def create(self, **params):
        self.engine.create(self)
        return self
    def drop(self, **params):
        self.engine.drop(self)
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

class Column(sql.ColumnClause, SchemaItem):
    """represents a column in a database table.  this is a subclass of sql.ColumnClause and
    represents an actual existing table in the database, in a similar fashion as TableClause/Table."""
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
        
        hidden=False : indicates this column should not be listed in the
        table's list of columns.  Used for the "oid" column, which generally
        isnt in column lists.

        index=None : True or index name. Indicates that this column is
        indexed. Pass true to autogenerate the index name. Pass a string to
        specify the index name. Multiple columns that specify the same index
        name will all be included in the index, in the order of their
        creation.

        unique=None : True or index name. Indicates that this column is
        indexed in a unique index . Pass true to autogenerate the index
        name. Pass a string to specify the index name. Multiple columns that
        specify the same index name will all be included in the index, in the
        order of their creation.  """
        
        name = str(name) # in case of incoming unicode
        super(Column, self).__init__(name, None, type)
        self.args = args
        self.key = kwargs.pop('key', name)
        self._primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.hidden = kwargs.pop('hidden', False)
        self.default = kwargs.pop('default', None)
        self.index = kwargs.pop('index', None)
        self.unique = kwargs.pop('unique', None)
        self.onupdate = kwargs.pop('onupdate', None)
        if self.index is not None and self.unique is not None:
            raise ArgumentError("Column may not define both index and unique")
        self._foreign_key = None
        self._orig = None
        self._parent = None
        if len(kwargs):
            raise ArgumentError("Unknown arguments passed to Column: " + repr(kwargs.keys()))

    primary_key = AttrProp('_primary_key')
    foreign_key = AttrProp('_foreign_key')
    original = property(lambda s: s._orig or s)
    parent = property(lambda s:s._parent or s)
    engine = property(lambda s: s.table.engine)
    columns = property(lambda self:[self])

    def __repr__(self):
       return "Column(%s)" % string.join(
        [repr(self.name)] + [repr(self.type)] +
        [repr(x) for x in [self.foreign_key] if x is not None] +
        ["%s=%s" % (k, repr(getattr(self, k))) for k in ['key', 'primary_key', 'nullable', 'hidden', 'default', 'onupdate']]
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
        if getattr(self, 'table', None) is not None:
            raise ArgumentError("this Column already has a table!")
        table.append_column(self)
        if self.index or self.unique:
            table.append_index_column(self, index=self.index,
                                      unique=self.unique)
        
        if self.default is not None:
            self.default = ColumnDefault(self.default)
            self._init_items(self.default)
        if self.onupdate is not None:
            self.onupdate = ColumnDefault(self.onupdate, for_update=True)
            self._init_items(self.onupdate)
        self._init_items(*self.args)
        self.args = None

    def copy(self):
        """creates a copy of this Column, unitialized"""
        if self.foreign_key is None:
            fk = None
        else:
            fk = self.foreign_key.copy()
        return Column(self.name, self.type, fk, self.default, key = self.key, primary_key = self.primary_key, nullable = self.nullable, hidden = self.hidden)
        
    def _make_proxy(self, selectable, name = None):
        """creates a copy of this Column, initialized the way this Column is"""
        if self.foreign_key is None:
            fk = None
        else:
            fk = self.foreign_key.copy()
        c = Column(name or self.name, self.type, fk, self.default, key = name or self.key, primary_key = self.primary_key, nullable = self.nullable, hidden = self.hidden)
        c.table = selectable
        c._orig = self.original
        c._parent = self
        if not c.hidden:
            selectable.columns[c.key] = c
            if self.primary_key:
                selectable.primary_key.append(c)
        if fk is not None:
            c._init_items(fk)
        return c

    def accept_schema_visitor(self, visitor):
        """traverses the given visitor to this Column's default and foreign key object,
        then calls visit_column on the visitor."""
        if self.default is not None:
            self.default.accept_schema_visitor(visitor)
        if self.onupdate is not None:
            self.onupdate.accept_schema_visitor(visitor)
        if self.foreign_key is not None:
            self.foreign_key.accept_schema_visitor(visitor)
        visitor.visit_column(self)


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
            return "%s.%s.%s" % (self._colspec.table.schema, self._colspec.table.name, self._colspec.key)
        else:
            return "%s.%s" % (self._colspec.table.name, self._colspec.key)
        
    def references(self, table):
        """returns True if the given table is referenced by this ForeignKey."""
        return table._get_col_by_original(self.column, False) is not None
        
    def _init_column(self):
        # ForeignKey inits its remote column as late as possible, so tables can
        # be defined without dependencies
        if self._column is None:
            if isinstance(self._colspec, str):
                m = re.match(r"^([\w_-]+)(?:\.([\w_-]+))?(?:\.([\w_-]+))?$", self._colspec)
                if m is None:
                    raise ArgumentError("Invalid foreign key column specification: " + self._colspec)
                if m.group(3) is None:
                    (tname, colname) = m.group(1, 2)
                    schema = self.parent.original.table.schema
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

    def accept_schema_visitor(self, visitor):
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
    """Base class for column "default" values."""
    def __init__(self, for_update=False, engine=None):
        self.for_update = for_update
        self.engine = engine
    def _set_parent(self, column):
        self.column = column
        if self.engine is None:
            self.engine = column.table.engine
        if self.for_update:
            self.column.onupdate = self
        else:
            self.column.default = self
    def execute(self):
        return self.accept_schema_visitor(self.engine.defaultrunner(self.engine.proxy))
    def __repr__(self):
        return "DefaultGenerator()"

class PassiveDefault(DefaultGenerator):
    """a default that takes effect on the database side"""
    def __init__(self, arg, **kwargs):
        super(PassiveDefault, self).__init__(**kwargs)
        self.arg = arg
    def accept_schema_visitor(self, visitor):
        return visitor.visit_passive_default(self)
    def __repr__(self):
        return "PassiveDefault(%s)" % repr(self.arg)
        
class ColumnDefault(DefaultGenerator):
    """A plain default value on a column.  this could correspond to a constant, 
    a callable function, or a SQL clause."""
    def __init__(self, arg, **kwargs):
        super(ColumnDefault, self).__init__(**kwargs)
        self.arg = arg
    def accept_schema_visitor(self, visitor):
        """calls the visit_column_default method on the given visitor."""
        if self.for_update:
            return visitor.visit_column_onupdate(self)
        else:
            return visitor.visit_column_default(self)
    def __repr__(self):
        return "ColumnDefault(%s)" % repr(self.arg)
        
class Sequence(DefaultGenerator):
    """represents a sequence, which applies to Oracle and Postgres databases."""
    def __init__(self, name, start = None, increment = None, optional=False, **kwargs):
        super(Sequence, self).__init__(**kwargs)
        self.name = name
        self.start = start
        self.increment = increment
        self.optional=optional
    def __repr__(self):
        return "Sequence(%s)" % string.join(
             [repr(self.name)] +
             ["%s=%s" % (k, repr(getattr(self, k))) for k in ['start', 'increment', 'optional']]
            , ',')
    def _set_parent(self, column):
        super(Sequence, self)._set_parent(column)
        column.sequence = self
    def create(self):
       self.engine.create(self)
       return self
    def drop(self):
       self.engine.drop(self)
    def accept_schema_visitor(self, visitor):
        """calls the visit_seauence method on the given visitor."""
        return visitor.visit_sequence(self)

class Index(SchemaItem):
    """Represents an index of columns from a database table
    """
    def __init__(self, name, *columns, **kw):
        """Constructs an index object. Arguments are:

        name : the name of the index

        *columns : columns to include in the index. All columns must belong to
        the same table, and no column may appear more than once.

        **kw : keyword arguments include:

        unique=True : create a unique index
        """
        self.name = name
        self.columns = []
        self.table = None
        self.unique = kw.pop('unique', False)
        self._init_items(*columns)

    engine = property(lambda s:s.table.engine)
    def _init_items(self, *args):
        for column in args:
            self.append_column(column)
            
    def append_column(self, column):
        # make sure all columns are from the same table
        # and no column is repeated
        if self.table is None:
            self.table = column.table
            self.table.append_index(self)
        elif column.table != self.table:
            # all columns muse be from same table
            raise ArgumentError("All index columns must be from same table. "
                                "%s is from %s not %s" % (column,
                                                          column.table,
                                                          self.table))
        elif column.name in [ c.name for c in self.columns ]:
            raise ArgumentError("A column may not appear twice in the "
                                "same index (%s already has column %s)"
                                % (self.name, column))
        self.columns.append(column)
        
    def create(self):
       self.engine.create(self)
       return self
    def drop(self):
       self.engine.drop(self)
    def execute(self):
       self.create()
    def accept_schema_visitor(self, visitor):
        visitor.visit_index(self)
    def __str__(self):
        return repr(self)
    def __repr__(self):
        return 'Index("%s", %s%s)' % (self.name,
                                      ', '.join([repr(c)
                                                 for c in self.columns]),
                                      (self.unique and ', unique=True') or '')
        
class SchemaEngine(object):
    """a factory object used to create implementations for schema objects.  This object
    is the ultimate base class for the engine.SQLEngine class."""

    def __init__(self):
        # a dictionary that stores Table objects keyed off their name (and possibly schema name)
        self.tables = {}
        
    def reflecttable(self, table):
        """given a table, will query the database and populate its Column and ForeignKey 
        objects."""
        raise NotImplementedError()
        
class SchemaVisitor(sql.ClauseVisitor):
    """defines the visiting for SchemaItem objects"""
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
        """visit an Index."""
        pass
    def visit_passive_default(self, default):
        """visit a passive default"""
        pass
    def visit_column_default(self, default):
        """visit a ColumnDefault."""
        pass
    def visit_column_onupdate(self, onupdate):
        """visit a ColumnDefault with the "for_update" flag set."""
        pass
    def visit_sequence(self, sequence):
        """visit a Sequence."""
        pass

            
            
