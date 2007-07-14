# schema.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The schema module provides the building blocks for database metadata.

This means all the entities within a SQL database that we might want
to look at, modify, or create and delete are described by these
objects, in a database-agnostic way.

A structure of SchemaItems also provides a *visitor* interface which is
the primary method by which other methods operate upon the schema.
The SQL package extends this structure with its own clause-specific
objects as well as the visitor interface, so that the schema package
*plugs in* to the SQL package.
"""

from sqlalchemy import sql, types, exceptions, util, databases
import sqlalchemy
import copy, re, string

__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'Index', 'ForeignKeyConstraint',
            'PrimaryKeyConstraint', 'CheckConstraint', 'UniqueConstraint', 'DefaultGenerator', 'Constraint',
           'MetaData', 'ThreadLocalMetaData', 'BoundMetaData', 'DynamicMetaData', 'SchemaVisitor', 'PassiveDefault', 'ColumnDefault']

class SchemaItem(object):
    """Base class for items that define a database schema."""

    def _init_items(self, *args):
        """Initialize the list of child items for this SchemaItem."""

        for item in args:
            if item is not None:
                item._set_parent(self)

    def _get_parent(self):
        raise NotImplementedError()

    def _set_parent(self, parent):
        """Associate with this SchemaItem's parent object."""

        raise NotImplementedError()
    
    def get_children(self, **kwargs):
        """used to allow SchemaVisitor access"""
        return []
        
    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def _derived_metadata(self):
        """Return the the MetaData to which this item is bound."""

        return None

    def _get_engine(self, raiseerr=False):
        """Return the engine or None if no engine."""

        if raiseerr:
            m = self._derived_metadata()
            e = m and m.bind or None
            if e is None:
                raise exceptions.InvalidRequestError("This SchemaItem is not connected to any Engine or Connection.")
            else:
                return e
        else:
            m = self._derived_metadata()
            return m and m.bind or None

    def get_engine(self):
        """Return the engine or raise an error if no engine.
        
        Deprecated.  use the "bind" attribute.
        """
        
        return self._get_engine(raiseerr=True)

    def _set_casing_strategy(self, name, kwargs, keyname='case_sensitive'):
        """Set the "case_sensitive" argument sent via keywords to the item's constructor.

        For the purposes of Table's 'schema' property, the name of the
        variable is optionally configurable.
        """
        setattr(self, '_%s_setting' % keyname, kwargs.pop(keyname, None))

    def _determine_case_sensitive(self, name, keyname='case_sensitive'):
        """Determine the `case_sensitive` value for this item.

        For the purposes of Table's `schema` property, the name of the
        variable is optionally configurable.

        A local non-None value overrides all others.  After that, the
        parent item (i.e. ``Column`` for a ``Sequence``, ``Table`` for
        a ``Column``, ``MetaData`` for a ``Table``) is searched for a
        non-None setting, traversing each parent until none are found.
        finally, case_sensitive is set to True as a default.
        """

        local = getattr(self, '_%s_setting' % keyname, None)
        if local is not None:
            return local
        parent = self
        while parent is not None:
            parent = parent._get_parent()
            if parent is not None:
                parentval = getattr(parent, '_case_sensitive_setting', None)
                if parentval is not None:
                    return parentval
        return True

    def _get_case_sensitive(self):
        try:
            return self.__case_sensitive
        except AttributeError:
            self.__case_sensitive = self._determine_case_sensitive(self.name)
            return self.__case_sensitive
    case_sensitive = property(_get_case_sensitive)

    engine = property(lambda s:s._get_engine())
    metadata = property(lambda s:s._derived_metadata())
    bind = property(lambda s:s.engine)
    
def _get_table_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name

class _TableSingleton(type):
    """A metaclass used by the ``Table`` object to provide singleton behavior."""

    def __call__(self, name, metadata, *args, **kwargs):
        if isinstance(metadata, sql.Executor):
            # backwards compatibility - get a BoundSchema associated with the engine
            engine = metadata
            if not hasattr(engine, '_legacy_metadata'):
                engine._legacy_metadata = MetaData(engine)
            metadata = engine._legacy_metadata
        elif metadata is not None and not isinstance(metadata, MetaData):
            # they left MetaData out, so assume its another SchemaItem, add it to *args
            args = list(args)
            args.insert(0, metadata)
            metadata = None

        if metadata is None:
            metadata = default_metadata

        schema = kwargs.get('schema', None)
        autoload = kwargs.pop('autoload', False)
        autoload_with = kwargs.pop('autoload_with', False)
        mustexist = kwargs.pop('mustexist', False)
        useexisting = kwargs.pop('useexisting', False)
        key = _get_table_key(name, schema)
        try:
            table = metadata.tables[key]
            if len(args):
                if not useexisting:
                    raise exceptions.ArgumentError("Table '%s' is already defined for this MetaData instance." % key)
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
                try:
                    if autoload_with:
                        autoload_with.reflecttable(table)
                    else:
                        metadata._get_engine(raiseerr=True).reflecttable(table)
                except exceptions.NoSuchTableError:
                    del metadata.tables[key]
                    raise
            # initialize all the column, etc. objects.  done after
            # reflection to allow user-overrides
            table._init_items(*args)
            return table


class Table(SchemaItem, sql.TableClause):
    """Represent a relational database table.

    This subclasses ``sql.TableClause`` to provide a table that is
    associated with an instance of ``MetaData``, which in turn
    may be associated with an instance of ``SQLEngine``.  

    Whereas ``TableClause`` represents a table as its used in an SQL
    expression, ``Table`` represents a table as it exists in a
    database schema.
    
    If this ``Table`` is ultimately associated with an engine,
    the ``Table`` gains the ability to access the database directly
    without the need for dealing with an explicit ``Connection`` object;
    this is known as "implicit execution".
    
    Implicit operation allows the ``Table`` to access the database to
    reflect its own properties (via the autoload=True flag), it allows
    the create() and drop() methods to be called without passing
    a connectable, and it also propigates the underlying engine to
    constructed SQL objects so that they too can be executed via their
    execute() method without the need for a ``Connection``.
    """

    __metaclass__ = _TableSingleton

    def __init__(self, name, metadata, **kwargs):
        """Construct a Table.

        Table objects can be constructed directly.  The init method is
        actually called via the TableSingleton metaclass.  Arguments
        are:

        name
          The name of this table, exactly as it appears, or will
          appear, in the database.

          This property, along with the *schema*, indicates the
          *singleton identity* of this table.

          Further tables constructed with the same name/schema
          combination will return the same Table instance.

        \*args
          Should contain a listing of the Column objects for this table.

        \**kwargs
          options include:

          schema
            Defaults to None: the *schema name* for this table, which is
            required if the table resides in a schema other than the
            default selected schema for the engine's database
            connection.

          autoload
            Defaults to False: the Columns for this table should be
            reflected from the database.  Usually there will be no
            Column objects in the constructor if this property is set.

          mustexist
            Defaults to False: indicates that this Table must already
            have been defined elsewhere in the application, else an
            exception is raised.

          useexisting
            Defaults to False: indicates that if this Table was
            already defined elsewhere in the application, disregard
            the rest of the constructor arguments.

          owner
            Defaults to None: optional owning user of this table.
            useful for databases such as Oracle to aid in table
            reflection.

          quote
            Defaults to False: indicates that the Table identifier
            must be properly escaped and quoted before being sent to
            the database. This flag overrides all other quoting
            behavior.

          quote_schema
            Defaults to False: indicates that the Namespace identifier
            must be properly escaped and quoted before being sent to
            the database. This flag overrides all other quoting
            behavior.

          case_sensitive
            Defaults to True: indicates quoting should be used if the
            identifier contains mixed case.

          case_sensitive_schema
            Defaults to True: indicates quoting should be used if the
            identifier contains mixed case.
        """
        super(Table, self).__init__(name)
        self._metadata = metadata
        self.schema = kwargs.pop('schema', None)
        self.indexes = util.Set()
        self.constraints = util.Set()
        self._columns = sql.ColumnCollection()
        self.primary_key = PrimaryKeyConstraint()
        self._foreign_keys = util.OrderedSet()
        self.quote = kwargs.pop('quote', False)
        self.quote_schema = kwargs.pop('quote_schema', False)
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name
        self.owner = kwargs.pop('owner', None)

        self._set_casing_strategy(name, kwargs)
        self._set_casing_strategy(self.schema or '', kwargs, keyname='case_sensitive_schema')

        if len([k for k in kwargs if not re.match(r'^(?:%s)_' % '|'.join(databases.__all__), k)]):
            raise TypeError("Invalid argument(s) for Table: %s" % repr(kwargs.keys()))

        # store extra kwargs, which should only contain db-specific options
        self.kwargs = kwargs

    def _export_columns(self, columns=None):
        # override FromClause's collection initialization logic; TableClause and Table
        # implement it differently
        pass

    def _get_case_sensitive_schema(self):
        try:
            return getattr(self, '_case_sensitive_schema')
        except AttributeError:
            setattr(self, '_case_sensitive_schema', self._determine_case_sensitive(self.schema or '', keyname='case_sensitive_schema'))
            return getattr(self, '_case_sensitive_schema')
    case_sensitive_schema = property(_get_case_sensitive_schema)

    def _set_primary_key(self, pk):
        if getattr(self, '_primary_key', None) in self.constraints:
            self.constraints.remove(self._primary_key)
        self._primary_key = pk
        self.constraints.add(pk)
    primary_key = property(lambda s:s._primary_key, _set_primary_key)

    def _derived_metadata(self):
        return self._metadata

    def __repr__(self):
        return "Table(%s)" % string.join(
            [repr(self.name)] + [repr(self.metadata)] +
            [repr(x) for x in self.columns] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in ['schema']]
            , ',')

    def __str__(self):
        return _get_table_key(self.encodedname, self.schema)

    def append_column(self, column):
        """Append a ``Column`` to this ``Table``."""

        column._set_parent(self)

    def append_constraint(self, constraint):
        """Append a ``Constraint`` to this ``Table``."""

        constraint._set_parent(self)

    def _get_parent(self):
        return self._metadata

    def _set_parent(self, metadata):
        metadata.tables[_get_table_key(self.name, self.schema)] = self
        self._metadata = metadata

    def get_children(self, column_collections=True, schema_visitor=False, **kwargs):
        if not schema_visitor:
            return sql.TableClause.get_children(self, column_collections=column_collections, **kwargs)
        else:
            if column_collections:
                return [c for c in self.columns]
            else:
                return []

    def exists(self, bind=None, connectable=None):
        """Return True if this table exists."""

        if connectable is not None:
            bind = connectable
            
        if bind is None:
            bind = self._get_engine(raiseerr=True)

        def do(conn):
            e = conn.engine
            return e.dialect.has_table(conn, self.name, schema=self.schema)
        return bind.run_callable(do)

    def create(self, bind=None, checkfirst=False, connectable=None):
        """Issue a ``CREATE`` statement for this table.

        See also ``metadata.create_all()``."""

        if connectable is not None:
            bind = connectable
        self.metadata.create_all(bind=bind, checkfirst=checkfirst, tables=[self])

    def drop(self, bind=None, checkfirst=False, connectable=None):
        """Issue a ``DROP`` statement for this table.

        See also ``metadata.drop_all()``."""

        if connectable is not None:
            bind = connectable
        self.metadata.drop_all(bind=bind, checkfirst=checkfirst, tables=[self])

    def tometadata(self, metadata, schema=None):
        """Return a copy of this ``Table`` associated with a different ``MetaData``."""

        try:
            if schema is None:
                schema = self.schema
            key = _get_table_key(self.name, schema)
            return metadata.tables[key]
        except KeyError:
            args = []
            for c in self.columns:
                args.append(c.copy())
            for c in self.constraints:
                args.append(c.copy())
            return Table(self.name, metadata, schema=schema, *args)

class Column(SchemaItem, sql._ColumnClause):
    """Represent a column in a database table.

    This is a subclass of ``sql.ColumnClause`` and represents an
    actual existing table in the database, in a similar fashion as
    ``TableClause``/``Table``.
    """

    def __init__(self, name, type, *args, **kwargs):
        """Construct a new ``Column`` object.

        Arguments are:

        name
          The name of this column.  This should be the identical name
          as it appears, or will appear, in the database.

        type
          The ``TypeEngine`` for this column.  This can be any
          subclass of ``types.AbstractType``, including the
          database-agnostic types defined in the types module,
          database-specific types defined within specific database
          modules, or user-defined types. If the column contains a
          ForeignKey, the type can also be None, in which case the
          type assigned will be that of the referenced column.

        \*args
          Constraint, ForeignKey, ColumnDefault and Sequence objects
          should be added as list values.

        \**kwargs
          Keyword arguments include:

          key
            Defaults to None: an optional *alias name* for this column.
            The column will then be identified everywhere in an
            application, including the column list on its Table, by
            this key, and not the given name.  Generated SQL, however,
            will still reference the column by its actual name.

          primary_key
            Defaults to False: True if this column is a primary key
            column.  Multiple columns can have this flag set to
            specify composite primary keys.  As an alternative, the
            primary key of a Table can be specified via an explicit
            ``PrimaryKeyConstraint`` instance appended to the Table's
            list of objects.

          nullable
            Defaults to True : True if this column should allow
            nulls. True is the default unless this column is a primary
            key column.

          default
            Defaults to None: a scalar, Python callable, or ``ClauseElement``
            representing the *default value* for this column, which will
            be invoked upon insert if this column is not present in
            the insert list or is given a value of None.  The default
            expression will be converted into a ``ColumnDefault`` object
            upon initialization.

          _is_oid
            Defaults to False: used internally to indicate that this
            column is used as the quasi-hidden "oid" column

          index
            Defaults to False: indicates that this column is
            indexed. The name of the index is autogenerated.  to
            specify indexes with explicit names or indexes that
            contain multiple columns, use the ``Index`` construct instead.

          unique
            Defaults to False: indicates that this column contains a
            unique constraint, or if `index` is True as well,
            indicates that the Index should be created with the unique
            flag.  To specify multiple columns in the constraint/index
            or to specify an explicit name, use the
            ``UniqueConstraint`` or ``Index`` constructs instead.

          autoincrement
            Defaults to True: indicates that integer-based primary key
            columns should have autoincrementing behavior, if
            supported by the underlying database.  This will affect
            ``CREATE TABLE`` statements such that they will use the
            databases *auto-incrementing* keyword (such as ``SERIAL``
            for Postgres, ``AUTO_INCREMENT`` for Mysql) and will also
            affect the behavior of some dialects during ``INSERT``
            statement execution such that they will assume primary key
            values are created in this manner.  If a ``Column`` has an
            explicit ``ColumnDefault`` object (such as via the `default`
            keyword, or a ``Sequence`` or ``PassiveDefault``), then
            the value of `autoincrement` is ignored and is assumed to be
            False.  `autoincrement` value is only significant for a
            column with a type or subtype of Integer.

          quote
            Defaults to False: indicates that the Column identifier
            must be properly escaped and quoted before being sent to
            the database.  This flag should normally not be required
            as dialects can auto-detect conditions where quoting is
            required.

          case_sensitive
            Defaults to True: indicates quoting should be used if the
            identifier contains mixed case.
        """

        super(Column, self).__init__(name, None, type)
        self.args = args
        self.key = kwargs.pop('key', name)
        self._primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self._is_oid = kwargs.pop('_is_oid', False)
        self.default = kwargs.pop('default', None)
        self.index = kwargs.pop('index', None)
        self.unique = kwargs.pop('unique', None)
        self.quote = kwargs.pop('quote', False)
        self._set_casing_strategy(name, kwargs)
        self.onupdate = kwargs.pop('onupdate', None)
        self.autoincrement = kwargs.pop('autoincrement', True)
        self.constraints = util.Set()
        self.__originating_column = self
        self._foreign_keys = util.OrderedSet()
        if len(kwargs):
            raise exceptions.ArgumentError("Unknown arguments passed to Column: " + repr(kwargs.keys()))

    primary_key = util.SimpleProperty('_primary_key')
    foreign_keys = util.SimpleProperty('_foreign_keys')
    columns = property(lambda self:[self])

    def __str__(self):
        if self.table is not None:
            if self.table.named_with_column():
                return (self.table.encodedname + "." + self.encodedname)
            else:
                return self.encodedname
        else:
            return self.encodedname

    def _derived_metadata(self):
        return self.table.metadata

    def _get_engine(self):
        return self.table.bind

    def references(self, column):
        """return true if this column references the given column via foreign key"""
        for fk in self.foreign_keys:
            if fk.column is column:
                return True
        else:
            return False
            
    def append_foreign_key(self, fk):
        fk._set_parent(self)

    def __repr__(self):
        kwarg = []
        if self.key != self.name:
            kwarg.append('key')
        if self._primary_key:
            kwarg.append('primary_key')
        if not self.nullable:
            kwarg.append('nullable')
        if self.onupdate:
            kwarg.append('onupdate')
        if self.default:
            kwarg.append('default')
        return "Column(%s)" % string.join(
            [repr(self.name)] + [repr(self.type)] +
            [repr(x) for x in self.foreign_keys if x is not None] +
            [repr(x) for x in self.constraints] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
            , ',')

    def _get_parent(self):
        return self.table

    def _set_parent(self, table):
        if getattr(self, 'table', None) is not None:
            raise exceptions.ArgumentError("this Column already has a table!")
        if not self._is_oid:
            table._columns.add(self)
        if self.primary_key:
            table.primary_key.add(self)
        elif self.key in table.primary_key:
            raise exceptions.ArgumentError("Trying to redefine primary-key column '%s' as a non-primary-key column on table '%s'" % (self.key, table.fullname))
            # if we think this should not raise an error, we'd instead do this:
            #table.primary_key.remove(self)
        self.table = table

        if self.index:
            if isinstance(self.index, basestring):
                raise exceptions.ArgumentError("The 'index' keyword argument on Column is boolean only.  To create indexes with a specific name, create an explicit Index object external to the Table.")
            Index('ix_%s' % self._label, self, unique=self.unique)
        elif self.unique:
            if isinstance(self.unique, basestring):
                raise exceptions.ArgumentError("The 'unique' keyword argument on Column is boolean only.  To create unique constraints or indexes with a specific name, append an explicit UniqueConstraint to the Table's list of elements, or create an explicit Index object external to the Table.")
            table.append_constraint(UniqueConstraint(self.key))

        toinit = list(self.args)
        if self.default is not None:
            toinit.append(ColumnDefault(self.default))
        if self.onupdate is not None:
            toinit.append(ColumnDefault(self.onupdate, for_update=True))
        self._init_items(*toinit)
        self.args = None

    def copy(self):
        """Create a copy of this ``Column``, unitialized.

        This is used in ``Table.tometadata``.
        """

        return Column(self.name, self.type, self.default, key = self.key, primary_key = self.primary_key, nullable = self.nullable, _is_oid = self._is_oid, case_sensitive=self._case_sensitive_setting, quote=self.quote, index=self.index, *[c.copy() for c in self.constraints])

    def _make_proxy(self, selectable, name = None):
        """Create a *proxy* for this column.

        This is a copy of this ``Column`` referenced by a different parent
        (such as an alias or select statement).
        """
        fk = [ForeignKey(f._colspec) for f in self.foreign_keys]
        c = Column(name or self.name, self.type, self.default, key = name or self.key, primary_key = self.primary_key, nullable = self.nullable, _is_oid = self._is_oid, quote=self.quote, *fk)
        c.table = selectable
        c.orig_set = self.orig_set
        c._distance = self._distance + 1
        c.__originating_column = self.__originating_column
        if not c._is_oid:
            selectable.columns.add(c)
            if self.primary_key:
                selectable.primary_key.add(c)
        [c._init_items(f) for f in fk]
        return c


    def _case_sens(self):
        """Redirect the `case_sensitive` accessor to use the ultimate
        parent column which created this one."""

        return self.__originating_column._get_case_sensitive()
    case_sensitive = property(_case_sens, lambda s,v:None)

    def get_children(self, schema_visitor=False, **kwargs):
        if schema_visitor:
            return [x for x in (self.default, self.onupdate) if x is not None] + \
                list(self.foreign_keys) + list(self.constraints)
        else:
            return sql._ColumnClause.get_children(self, **kwargs)


class ForeignKey(SchemaItem):
    """Defines a column-level ``ForeignKey`` constraint between two columns.

    ``ForeignKey`` is specified as an argument to a Column object.

    One or more ``ForeignKey`` objects are used within a
    ``ForeignKeyConstraint`` object which represents the table-level
    constraint definition.
    """

    def __init__(self, column, constraint=None, use_alter=False, name=None, onupdate=None, ondelete=None):
        """Construct a new ``ForeignKey`` object.

        column
          Can be a ``schema.Column`` object representing the relationship,
          or just its string name given as ``tablename.columnname``.
          schema can be specified as ``schema.tablename.columnname``.

        constraint
          Is the owning ``ForeignKeyConstraint`` object, if any.  if not
          given, then a ``ForeignKeyConstraint`` will be automatically
          created and added to the parent table.
        """

        self._colspec = column
        self._column = None
        self.constraint = constraint
        self.use_alter = use_alter
        self.name = name
        self.onupdate = onupdate
        self.ondelete = ondelete

    def __repr__(self):
        return "ForeignKey(%s)" % repr(self._get_colspec())

    def copy(self):
        """Produce a copy of this ForeignKey object."""

        return ForeignKey(self._get_colspec())

    def _get_colspec(self):
        if isinstance(self._colspec, basestring):
            return self._colspec
        elif self._colspec.table.schema is not None:
            return "%s.%s.%s" % (self._colspec.table.schema, self._colspec.table.name, self._colspec.key)
        else:
            return "%s.%s" % (self._colspec.table.name, self._colspec.key)

    def references(self, table):
        """Return True if the given table is referenced by this ``ForeignKey``."""

        return table.corresponding_column(self.column, False) is not None
    
    def _init_column(self):
        # ForeignKey inits its remote column as late as possible, so tables can
        # be defined without dependencies
        if self._column is None:
            if isinstance(self._colspec, basestring):
                # locate the parent table this foreign key is attached to.
                # we use the "original" column which our parent column represents
                # (its a list of columns/other ColumnElements if the parent table is a UNION)
                for c in self.parent.orig_set:
                    if isinstance(c, Column):
                        parenttable = c.table
                        break
                else:
                    raise exceptions.ArgumentError("Parent column '%s' does not descend from a table-attached Column" % str(self.parent))
                m = re.match(r"^([\w_-]+)(?:\.([\w_-]+))?(?:\.([\w_-]+))?$", self._colspec, re.UNICODE)
                if m is None:
                    raise exceptions.ArgumentError("Invalid foreign key column specification: " + self._colspec)
                if m.group(3) is None:
                    (tname, colname) = m.group(1, 2)
                    schema = None
                else:
                    (schema,tname,colname) = m.group(1,2,3)
                table = Table(tname, parenttable.metadata, mustexist=True, schema=schema)
                try:
                    if colname is None:
                        # colname is None in the case that ForeignKey argument was specified
                        # as table name only, in which case we match the column name to the same
                        # column on the parent.
                        key = self.parent
                        self._column = table.c[self.parent.key]
                    else:
                        self._column = table.c[colname]
                except KeyError, e:
                    raise exceptions.ArgumentError("Could not create ForeignKey '%s' on table '%s': table '%s' has no column named '%s'" % (self._colspec, parenttable.name, table.name, str(e)))
            else:
                self._column = self._colspec
        # propigate TypeEngine to parent if it didnt have one
        if self.parent.type is types.NULLTYPE:
            self.parent.type = self._column.type
        return self._column

    column = property(lambda s: s._init_column())

    def accept_visitor(self, visitor):
        """Call the `visit_foreign_key` method on the given visitor."""

        visitor.visit_foreign_key(self)

    def _get_parent(self):
        return self.parent

    def _set_parent(self, column):
        self.parent = column

        if self.constraint is None and isinstance(self.parent.table, Table):
            self.constraint = ForeignKeyConstraint([],[], use_alter=self.use_alter, name=self.name, onupdate=self.onupdate, ondelete=self.ondelete)
            self.parent.table.append_constraint(self.constraint)
            self.constraint._append_fk(self)

        self.parent.foreign_keys.add(self)
        self.parent.table.foreign_keys.add(self)

class DefaultGenerator(SchemaItem):
    """Base class for column *default* values."""

    def __init__(self, for_update=False, metadata=None):
        self.for_update = for_update
        self._metadata = util.assert_arg_type(metadata, (MetaData, type(None)), 'metadata')

    def _derived_metadata(self):
        try:
            return self.column.table.metadata
        except AttributeError:
            return self._metadata

    def _get_parent(self):
        return getattr(self, 'column', None)

    def _set_parent(self, column):
        self.column = column
        self._metadata = self.column.table.metadata
        if self.for_update:
            self.column.onupdate = self
        else:
            self.column.default = self

    def execute(self, bind=None, **kwargs):
        if bind is None:
            bind = self._get_engine(raiseerr=True)
        return bind.execute_default(self, **kwargs)

    def __repr__(self):
        return "DefaultGenerator()"

class PassiveDefault(DefaultGenerator):
    """A default that takes effect on the database side."""

    def __init__(self, arg, **kwargs):
        super(PassiveDefault, self).__init__(**kwargs)
        self.arg = arg

    def accept_visitor(self, visitor):
        return visitor.visit_passive_default(self)

    def __repr__(self):
        return "PassiveDefault(%s)" % repr(self.arg)

class ColumnDefault(DefaultGenerator):
    """A plain default value on a column.

    This could correspond to a constant, a callable function, or a SQL
    clause.
    """

    def __init__(self, arg, **kwargs):
        super(ColumnDefault, self).__init__(**kwargs)
        self.arg = arg

    def accept_visitor(self, visitor):
        """Call the visit_column_default method on the given visitor."""

        if self.for_update:
            return visitor.visit_column_onupdate(self)
        else:
            return visitor.visit_column_default(self)

    def __repr__(self):
        return "ColumnDefault(%s)" % repr(self.arg)

class Sequence(DefaultGenerator):
    """Represent a sequence, which applies to Oracle and Postgres databases."""

    def __init__(self, name, start = None, increment = None, optional=False, quote=False, **kwargs):
        super(Sequence, self).__init__(**kwargs)
        self.name = name
        self.start = start
        self.increment = increment
        self.optional=optional
        self.quote = quote
        self._set_casing_strategy(name, kwargs)

    def __repr__(self):
        return "Sequence(%s)" % string.join(
            [repr(self.name)] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in ['start', 'increment', 'optional']]
            , ',')

    def _set_parent(self, column):
        super(Sequence, self)._set_parent(column)
        column.sequence = self

    def create(self, bind=None):
        if bind is None:
            bind = self._get_engine(raiseerr=True)
        bind.create(self)

    def drop(self, bind=None):
        if bind is None:
            bind = self._get_engine(raiseerr=True)
        bind.drop(self)

    def accept_visitor(self, visitor):
        """Call the visit_seauence method on the given visitor."""

        return visitor.visit_sequence(self)

class Constraint(SchemaItem):
    """Represent a table-level ``Constraint`` such as a composite primary key, foreign key, or unique constraint.

    Implements a hybrid of dict/setlike behavior with regards to the
    list of underying columns.
    """

    def __init__(self, name=None):
        self.name = name
        self.columns = sql.ColumnCollection()

    def __contains__(self, x):
        return x in self.columns

    def keys(self):
        return self.columns.keys()

    def __add__(self, other):
        return self.columns + other

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)

    def copy(self):
        raise NotImplementedError()

    def _get_parent(self):
        return getattr(self, 'table', None)

class CheckConstraint(Constraint):
    def __init__(self, sqltext, name=None):
        super(CheckConstraint, self).__init__(name)
        self.sqltext = sqltext

    def accept_visitor(self, visitor):
        if isinstance(self.parent, Table):
            visitor.visit_check_constraint(self)
        else:
            visitor.visit_column_check_constraint(self)

    def _set_parent(self, parent):
        self.parent = parent
        parent.constraints.add(self)

    def copy(self):
        return CheckConstraint(self.sqltext, name=self.name)

class ForeignKeyConstraint(Constraint):
    """Table-level foreign key constraint, represents a collection of ``ForeignKey`` objects."""

    def __init__(self, columns, refcolumns, name=None, onupdate=None, ondelete=None, use_alter=False):
        super(ForeignKeyConstraint, self).__init__(name)
        self.__colnames = columns
        self.__refcolnames = refcolumns
        self.elements = util.OrderedSet()
        self.onupdate = onupdate
        self.ondelete = ondelete
        if self.name is None and use_alter:
            raise exceptions.ArgumentError("Alterable ForeignKey/ForeignKeyConstraint requires a name")
        self.use_alter = use_alter

    def _set_parent(self, table):
        self.table = table
        table.constraints.add(self)
        for (c, r) in zip(self.__colnames, self.__refcolnames):
            self.append_element(c,r)

    def accept_visitor(self, visitor):
        visitor.visit_foreign_key_constraint(self)

    def append_element(self, col, refcol):
        fk = ForeignKey(refcol, constraint=self, name=self.name, onupdate=self.onupdate, ondelete=self.ondelete, use_alter=self.use_alter)
        fk._set_parent(self.table.c[col])
        self._append_fk(fk)

    def _append_fk(self, fk):
        self.columns.add(self.table.c[fk.parent.key])
        self.elements.add(fk)

    def copy(self):
        return ForeignKeyConstraint([x.parent.name for x in self.elements], [x._get_colspec() for x in self.elements], name=self.name, onupdate=self.onupdate, ondelete=self.ondelete, use_alter=self.use_alter)

class PrimaryKeyConstraint(Constraint):
    def __init__(self, *columns, **kwargs):
        super(PrimaryKeyConstraint, self).__init__(name=kwargs.pop('name', None))
        self.__colnames = list(columns)

    def _set_parent(self, table):
        self.table = table
        table.primary_key = self
        for c in self.__colnames:
            self.append_column(table.c[c])

    def accept_visitor(self, visitor):
        visitor.visit_primary_key_constraint(self)

    def add(self, col):
        self.append_column(col)

    def remove(self, col):
        col.primary_key=False
        del self.columns[col.key]

    def append_column(self, col):
        self.columns.add(col)
        col.primary_key=True

    def copy(self):
        return PrimaryKeyConstraint(name=self.name, *[c.key for c in self])

    def __eq__(self, other):
        return self.columns == other

class UniqueConstraint(Constraint):
    def __init__(self, *columns, **kwargs):
        super(UniqueConstraint, self).__init__(name=kwargs.pop('name', None))
        self.__colnames = list(columns)

    def _set_parent(self, table):
        self.table = table
        table.constraints.add(self)
        for c in self.__colnames:
            self.append_column(table.c[c])

    def append_column(self, col):
        self.columns.add(col)

    def accept_visitor(self, visitor):
        visitor.visit_unique_constraint(self)

    def copy(self):
        return UniqueConstraint(name=self.name, *self.__colnames)

class Index(SchemaItem):
    """Represent an index of columns from a database table."""

    def __init__(self, name, *columns, **kwargs):
        """Construct an index object.

        Arguments are:

        name
          The name of the index

        \*columns
          Columns to include in the index. All columns must belong to
          the same table, and no column may appear more than once.

        \**kwargs
          Keyword arguments include:

          unique
            Defaults to True: create a unique index.
            
        """

        self.name = name
        self.columns = []
        self.table = None
        self.unique = kwargs.pop('unique', False)
        self._init_items(*columns)

    def _derived_metadata(self):
        return self.table.metadata

    def _init_items(self, *args):
        for column in args:
            self.append_column(column)

    def _get_parent(self):
        return self.table

    def _set_parent(self, table):
        self.table = table
        table.indexes.add(self)

    def append_column(self, column):
        # make sure all columns are from the same table
        # and no column is repeated
        if self.table is None:
            self._set_parent(column.table)
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

    def create(self, connectable=None):
        if connectable is not None:
            connectable.create(self)
        else:
            self._get_engine(raiseerr=True).create(self)
        return self

    def drop(self, connectable=None):
        if connectable is not None:
            connectable.drop(self)
        else:
            self._get_engine(raiseerr=True).drop(self)

    def accept_visitor(self, visitor):
        visitor.visit_index(self)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'Index("%s", %s%s)' % (self.name,
                                      ', '.join([repr(c)
                                                 for c in self.columns]),
                                      (self.unique and ', unique=True') or '')

class MetaData(SchemaItem):
    """Represent a collection of Tables and their associated schema constructs."""

    def __init__(self, engine_or_url=None, url=None, bind=None, engine=None, **kwargs):
        """create a new MetaData object.
            
            bind
                an Engine, or a string or URL instance which will be passed
                to create_engine(), along with \**kwargs - this MetaData will
                be bound to the resulting engine.

            engine_or_url
                deprecated; a synonym for 'bind'
                
            url
                deprecated.  a string or URL instance which will be passed to
                create_engine(), along with \**kwargs - this MetaData will be
                bound to the resulting engine.
            
            engine
                deprecated. an Engine instance to which this MetaData will
                be bound.
                
            case_sensitive
                popped from \**kwargs, indicates default case sensitive
                setting for all contained objects.  defaults to True.
            
            name
                deprecated, optional name for this MetaData instance.
            
        """

        # transition from <= 0.3.8 signature:
        #   MetaData(name=None, url=None, engine=None)
        # to 0.4 signature:
        #   MetaData(engine_or_url=None)
        name = kwargs.get('name', None)
        if engine_or_url is None:
            engine_or_url = url or bind or engine
        elif 'name' in kwargs:
            engine_or_url = engine_or_url or bind or engine or url
        else:
            import sqlalchemy.engine as engine
            import sqlalchemy.engine.url as url
            if (not isinstance(engine_or_url, url.URL) and
                not isinstance(engine_or_url, engine.Connectable)):
                try:
                    url.make_url(engine_or_url)
                except exceptions.ArgumentError:
                    # nope, must have been a name as 1st positional
                    name, engine_or_url = engine_or_url, (url or engine or bind)
        kwargs.pop('name', None)

        self.tables = {}
        self.name = name
        self._bind = None
        self._set_casing_strategy(name, kwargs)
        if engine_or_url:
            self.connect(engine_or_url, **kwargs)

    def __getstate__(self):
        return {'tables':self.tables, 'name':self.name, 'casesensitive':self._case_sensitive_setting}
    
    def __setstate__(self, state):
        self.tables = state['tables']
        self.name = state['name']
        self._case_sensitive_setting = state['casesensitive']
        self._bind = None
        
    def is_bound(self):
        """return True if this MetaData is bound to an Engine."""
        return self._bind is not None

    def connect(self, bind=None, **kwargs):
        """bind this MetaData to an Engine.
            
            DEPRECATED.  use metadata.bind = <engine> or metadata.bind = <url>.
        
            bind
                a string, URL or Engine instance.  If a string or URL,
                will be passed to create_engine() along with \**kwargs to
                produce the engine which to connect to.  otherwise connects
                directly to the given Engine.

            engine_or_url
                deprecated.  synonymous with "bind"
                
        """
        
        if bind is None:
            bind = kwargs.pop('engine_or_url', None)
        if bind is None:
            raise exceptions.ArguemntError("'bind' argument is required for connect()")
        from sqlalchemy.engine.url import URL
        if isinstance(bind, (basestring, URL)):
            self._bind = sqlalchemy.create_engine(bind, **kwargs)
        else:
            self._bind = bind

    bind = property(lambda self:self._bind, connect, doc="""an Engine or Connection to which this MetaData is bound.  this is a settable property as well.""")
    
    def clear(self):
        self.tables.clear()

    def table_iterator(self, reverse=True, tables=None):
        import sqlalchemy.sql_util
        if tables is None:
            tables = self.tables.values()
        else:
            tables = util.Set(tables).intersection(self.tables.values())
        sorter = sqlalchemy.sql_util.TableCollection(list(tables))
        return iter(sorter.sort(reverse=reverse))

    def _get_parent(self):
        return None

    def create_all(self, bind=None, tables=None, checkfirst=True, connectable=None):
        """Create all tables stored in this metadata.

        This will conditionally create tables depending on if they do
        not yet exist in the database.

        bind
          A ``Connectable`` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.

        connectable
          deprecated.  synonymous with "bind"

        tables
          Optional list of tables, which is a subset of the total
          tables in the ``MetaData`` (others are ignored).
        """

        if connectable is not None:
            bind = connectable
        if bind is None:
            bind = self._get_engine(raiseerr=True)
        bind.create(self, checkfirst=checkfirst, tables=tables)

    def drop_all(self, bind=None, tables=None, checkfirst=True, connectable=None):
        """Drop all tables stored in this metadata.

        This will conditionally drop tables depending on if they
        currently exist in the database.

        bind
          A ``Connectable`` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.
          
        connectable
          deprecated.  synonymous with "bind"

        tables
          Optional list of tables, which is a subset of the total
          tables in the ``MetaData`` (others are ignored).
        """

        if connectable is not None:
            bind = connectable
        if bind is None:
            bind = self._get_engine(raiseerr=True)
        bind.drop(self, checkfirst=checkfirst, tables=tables)

    def accept_visitor(self, visitor):
        visitor.visit_metadata(self)

    def _derived_metadata(self):
        return self

    def _get_engine(self, raiseerr=False):
        if not self.is_bound():
            if raiseerr:
                raise exceptions.InvalidRequestError("This SchemaItem is not connected to any Engine or Connection.")
            else:
                return None
        return self._bind


class BoundMetaData(MetaData):
    """Deprecated.  Use ``MetaData``."""

    def __init__(self, engine_or_url, name=None, **kwargs):
        super(BoundMetaData, self).__init__(engine_or_url=engine_or_url,
                                            name=name, **kwargs)


class ThreadLocalMetaData(MetaData):
    """A ``MetaData`` that binds to multiple ``Engine`` implementations on a thread-local basis."""

    def __init__(self, name=None, **kwargs):
        self.context = util.ThreadLocal()
        self.__engines = {}
        super(ThreadLocalMetaData, self).__init__(engine_or_url=None,
                                                  name=name, **kwargs)

    def connect(self, engine_or_url, **kwargs):
        from sqlalchemy.engine.url import URL
        if isinstance(engine_or_url, basestring) or isinstance(engine_or_url, URL):
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
        return hasattr(self.context, '_engine') and self.context._engine is not None

    def dispose(self):
        """Dispose all ``Engines`` to which this ``ThreadLocalMetaData`` has been connected."""

        for e in self.__engines.values():
            e.dispose()

    def _get_engine(self, raiseerr=False):
        if hasattr(self.context, '_engine'):
            return self.context._engine
        else:
            if raiseerr:
                raise exceptions.InvalidRequestError("This SchemaItem is not connected to any Engine or Connection.")
            else: 
                return None
    engine=property(_get_engine)
    bind = property(_get_engine, connect)

def DynamicMetaData(name=None, threadlocal=True, **kw): 
    """Deprecated.  Use ``MetaData`` or ``ThreadLocalMetaData``.""" 

    if threadlocal: 
        return ThreadLocalMetaData(name=name, **kw) 
    else: 
        return MetaData(name=name, **kw) 


class SchemaVisitor(sql.ClauseVisitor):
    """Define the visiting for ``SchemaItem`` objects."""

    __traverse_options__ = {'schema_visitor':True}

    def visit_schema(self, schema):
        """Visit a generic ``SchemaItem``."""
        pass

    def visit_table(self, table):
        """Visit a ``Table``."""
        pass

    def visit_column(self, column):
        """Visit a ``Column``."""
        pass

    def visit_foreign_key(self, join):
        """Visit a ``ForeignKey``."""
        pass

    def visit_index(self, index):
        """Visit an ``Index``."""
        pass

    def visit_passive_default(self, default):
        """Visit a passive default."""
        pass

    def visit_column_default(self, default):
        """Visit a ``ColumnDefault``."""
        pass

    def visit_column_onupdate(self, onupdate):
        """Visit a ``ColumnDefault`` with the `for_update` flag set."""
        pass

    def visit_sequence(self, sequence):
        """Visit a ``Sequence``."""
        pass

    def visit_primary_key_constraint(self, constraint):
        """Visit a ``PrimaryKeyConstraint``."""
        pass

    def visit_foreign_key_constraint(self, constraint):
        """Visit a ``ForeignKeyConstraint``."""
        pass

    def visit_unique_constraint(self, constraint):
        """Visit a ``UniqueConstraint``."""
        pass

    def visit_check_constraint(self, constraint):
        """Visit a ``CheckConstraint``."""
        pass

    def visit_column_check_constraint(self, constraint):
        """Visit a ``CheckConstraint`` on a ``Column``."""
        pass

default_metadata = ThreadLocalMetaData(name='default')
