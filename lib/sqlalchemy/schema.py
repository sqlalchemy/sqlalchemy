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
from sqlalchemy import sql, types, exceptions,util
import sqlalchemy
import copy, re, string

__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'Index',
           'MetaData', 'BoundMetaData', 'DynamicMetaData', 'SchemaVisitor', 'PassiveDefault', 'ColumnDefault']

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
    def _derived_metadata(self):
        """subclasses override this method to return a the MetaData
        to which this item is bound"""
        return None
    def _get_engine(self):
        return self._derived_metadata().engine
    engine = property(lambda s:s._get_engine())
    metadata = property(lambda s:s._derived_metadata())
    
def _get_table_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name
        
class TableSingleton(type):
    """a metaclass used by the Table object to provide singleton behavior."""
    def __call__(self, name, metadata, *args, **kwargs):
        try:
            if isinstance(metadata, sql.Engine):
                # backwards compatibility - get a BoundSchema associated with the engine
                engine = metadata
                if not hasattr(engine, '_legacy_metadata'):
                    engine._legacy_metadata = BoundMetaData(engine)
                metadata = engine._legacy_metadata
            elif metadata is not None and not isinstance(metadata, MetaData):
                # they left MetaData out, so assume its another SchemaItem, add it to *args
                args = list(args)
                args.insert(0, metadata)
                metadata = None
                
            if metadata is None:
                metadata = default_metadata
                
            name = str(name)    # in case of incoming unicode
            schema = kwargs.get('schema', None)
            autoload = kwargs.pop('autoload', False)
            autoload_with = kwargs.pop('autoload_with', False)
            redefine = kwargs.pop('redefine', False)
            mustexist = kwargs.pop('mustexist', False)
            useexisting = kwargs.pop('useexisting', False)
            key = _get_table_key(name, schema)
            table = metadata.tables[key]
            if len(args):
                if redefine:
                    table.reload_values(*args)
                elif not useexisting:
                    raise exceptions.ArgumentError("Table '%s.%s' is already defined. specify 'redefine=True' to remap columns, or 'useexisting=True' to use the existing table" % (schema, name))
            return table
        except KeyError:
            if mustexist:
                raise exceptions.ArgumentError("Table '%s.%s' not defined" % (schema, name))
            table = type.__call__(self, name, metadata, **kwargs)
            table._set_parent(metadata)
            # load column definitions from the database if 'autoload' is defined
            # we do it after the table is in the singleton dictionary to support
            # circular foreign keys
            if autoload:
                if autoload_with:
                    autoload_with.reflecttable(table)
                else:
                    metadata.engine.reflecttable(table)
            # initialize all the column, etc. objects.  done after
            # reflection to allow user-overrides
            table._init_items(*args)
            return table

        
class Table(SchemaItem, sql.TableClause):
    """represents a relational database table.  This subclasses sql.TableClause to provide
    a table that is "wired" to an engine.  Whereas TableClause represents a table as its 
    used in a SQL expression, Table represents a table as its created in the database.  
    
    Be sure to look at sqlalchemy.sql.TableImpl for additional methods defined on a Table."""
    __metaclass__ = TableSingleton
    
    def __init__(self, name, metadata, **kwargs):
        """Table objects can be constructed directly.  The init method is actually called via 
        the TableSingleton metaclass.  Arguments are:
        
        name : the name of this table, exactly as it appears, or will appear, in the database.
        This property, along with the "schema", indicates the "singleton identity" of this table.
        Further tables constructed with the same name/schema combination will return the same 
        Table instance.
        
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
        self._metadata = metadata
        self.schema = kwargs.pop('schema', None)
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name
        self.kwargs = kwargs
    def _derived_metadata(self):
        return self._metadata
    def __repr__(self):
        return "Table(%s)" % string.join(
        [repr(self.name)] + [repr(self.metadata)] +
        [repr(x) for x in self.columns] +
        ["%s=%s" % (k, repr(getattr(self, k))) for k in ['schema']]
       , ',\n')
    
    def __str__(self):
        return _get_table_key(self.name, self.schema)
        
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

    def append_index(self, index):
        self.indexes[index.name] = index
        
    def _set_parent(self, metadata):
        metadata.tables[_get_table_key(self.name, self.schema)] = self
        self._metadata = metadata
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
                name = 'ix_%s' % column._label
            else:
                name = index
        elif unique:
            if unique is True:
                name = 'ux_%s' % column._label
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
        """removes this table from it's metadata.  this does not
        issue a SQL DROP statement."""
        key = _get_table_key(self.name, self.schema)
        del self.metadata.tables[key]
    def create(self, connectable=None):
        if connectable is not None:
            connectable.create(self)
        else:
            self.engine.create(self)
        return self
    def drop(self, connectable=None):
        if connectable is not None:
            connectable.drop(self)
        else:
            self.engine.drop(self)
    def tometadata(self, metadata, schema=None):
        """returns a singleton instance of this Table with a different Schema"""
        try:
            if schema is None:
                schema = self.schema
            key = _get_table_key(self.name, schema)
            return metadata.tables[key]
        except KeyError:
            args = []
            for c in self.columns:
                args.append(c.copy())
            return Table(self.name, metadata, schema=schema, *args)

class Column(SchemaItem, sql.ColumnClause):
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
        order of their creation.

        """
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
            raise exceptions.ArgumentError("Column may not define both index and unique")
        self._foreign_key = None
        if len(kwargs):
            raise exceptions.ArgumentError("Unknown arguments passed to Column: " + repr(kwargs.keys()))

    primary_key = util.SimpleProperty('_primary_key')
    foreign_key = util.SimpleProperty('_foreign_key')
    columns = property(lambda self:[self])

    def __str__(self):
        if self.table is not None:
            tname = self.table.displayname
            if tname is not None:
                return tname + "." + self.name
            else:
                return self.name
        else:
            return self.name
    
    def _derived_metadata(self):
        return self.table.metadata
    def _get_engine(self):
        return self.table.engine
        
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
            raise exceptions.ArgumentError("this Column already has a table!")
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
        c.orig_set = self.orig_set
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
        "schema.tablename.columnname" """
        if isinstance(column, unicode):
            column = str(column)
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
        return table.corresponding_column(self.column, False) is not None
        
    def _init_column(self):
        # ForeignKey inits its remote column as late as possible, so tables can
        # be defined without dependencies
        if self._column is None:
            if isinstance(self._colspec, str):
                # locate the parent table this foreign key is attached to.  
                # we use the "original" column which our parent column represents
                # (its a list of columns/other ColumnElements if the parent table is a UNION)
                for c in self.parent.orig_set:
                    if isinstance(c, Column):
                        parenttable = c.table
                        break
                else:
                    raise exceptions.ArgumentError("Parent column '%s' does not descend from a table-attached Column" % str(self.parent))
                m = re.match(r"^([\w_-]+)(?:\.([\w_-]+))?(?:\.([\w_-]+))?$", self._colspec)
                if m is None:
                    raise exceptions.ArgumentError("Invalid foreign key column specification: " + self._colspec)
                if m.group(3) is None:
                    (tname, colname) = m.group(1, 2)
                    schema = parenttable.schema
                else:
                    (schema,tname,colname) = m.group(1,2,3)
                table = Table(tname, parenttable.metadata, mustexist=True, schema=schema)
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
    def __init__(self, for_update=False, metadata=None):
        self.for_update = for_update
        self._metadata = metadata
    def _derived_metadata(self):
        try:
            return self.column.table.metadata
        except AttributeError:
            return self._metadata
    def _set_parent(self, column):
        self.column = column
        self._metadata = self.column.table.metadata
        if self.for_update:
            self.column.onupdate = self
        else:
            self.column.default = self
    def execute(self, **kwargs):
        return self.engine.execute_default(self, **kwargs)
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

    def _derived_metadata(self):
        return self.table.metadata
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
            raise exceptions.ArgumentError("All index columns must be from same table. "
                                "%s is from %s not %s" % (column,
                                                          column.table,
                                                          self.table))
        elif column.name in [ c.name for c in self.columns ]:
            raise exceptions.ArgumentError("A column may not appear twice in the "
                                "same index (%s already has column %s)"
                                % (self.name, column))
        self.columns.append(column)
        
    def create(self, engine=None):
        if engine is not None:
            engine.create(self)
        else:
            self.engine.create(self)
        return self
    def drop(self, engine=None):
        if engine is not None:
            engine.drop(self)
        else:
            self.engine.drop(self)
    def accept_schema_visitor(self, visitor):
        visitor.visit_index(self)
    def __str__(self):
        return repr(self)
    def __repr__(self):
        return 'Index("%s", %s%s)' % (self.name,
                                      ', '.join([repr(c)
                                                 for c in self.columns]),
                                      (self.unique and ', unique=True') or '')
        
class MetaData(SchemaItem):
    """represents a collection of Tables and their associated schema constructs."""
    def __init__(self, name=None):
        # a dictionary that stores Table objects keyed off their name (and possibly schema name)
        self.tables = {}
        self.name = name
    def is_bound(self):
        return False
    def clear(self):
        self.tables.clear()
    def table_iterator(self, reverse=True):
        return self._sort_tables(self.tables.values(), reverse=reverse)
        
    def create_all(self, engine=None, tables=None):
        if not tables:
            tables = self.tables.values()

        if engine is None and self.is_bound():
            engine = self.engine

        def do(conn):
            e = conn.engine
            ts = self._sort_tables( tables )
            for table in ts:
                if e.dialect.has_table(conn, table.name):
                    continue
                conn.create(table)
        engine.run_callable(do)
        
    def drop_all(self, engine=None, tables=None):
        if not tables:
            tables = self.tables.values()

        if engine is None and self.is_bound():
            engine = self.engine
        
        def do(conn):
            e = conn.engine
            ts = self._sort_tables( tables, reverse=True )
            for table in ts:
                if e.dialect.has_table(conn, table.name):
                    conn.drop(table)
        engine.run_callable(do)
                
    def _sort_tables(self, tables, reverse=False):
        import sqlalchemy.sql_util
        sorter = sqlalchemy.sql_util.TableCollection()
        for t in self.tables.values():
            sorter.add(t)
        return sorter.sort(reverse=reverse)
        
    def _derived_metadata(self):
        return self
    def _get_engine(self):
        if not self.is_bound():
            return None
        return self._engine
                
class BoundMetaData(MetaData):
    """builds upon MetaData to provide the capability to bind to an Engine implementation."""
    def __init__(self, engine_or_url, name=None, **kwargs):
        super(BoundMetaData, self).__init__(name)
        if isinstance(engine_or_url, str):
            self._engine = sqlalchemy.create_engine(engine_or_url, **kwargs)
        else:
            self._engine = engine_or_url
    def is_bound(self):
        return True

class DynamicMetaData(MetaData):
    """builds upon MetaData to provide the capability to bind to multiple Engine implementations
    on a dynamically alterable, thread-local basis."""
    def __init__(self, name=None, threadlocal=True):
        super(DynamicMetaData, self).__init__(name)
        if threadlocal:
            self.context = util.ThreadLocal()
        else:
            self.context = self
        self.__engines = {}
    def connect(self, engine_or_url, **kwargs):
        if isinstance(engine_or_url, str):
            try:
                self.context._engine = self.__engines[engine_or_url]
            except KeyError:
                e = sqlalchemy.create_engine(engine_or_url, **kwargs)
                self.__engines[engine_or_url] = e
                self.context._engine = e
        else:
            if not self.__engines.has_key(engine_or_url):
                self.__engines[engine_or_url] = engine_or_url
            self.context._engine = engine_or_url
    def is_bound(self):
        return self.context._engine is not None
    def dispose(self):
        """disposes all Engines to which this DynamicMetaData has been connected."""
        for e in self.__engines.values():
            e.dispose()
    engine=property(lambda s:s.context._engine)
            
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

default_metadata = DynamicMetaData('default')

            
