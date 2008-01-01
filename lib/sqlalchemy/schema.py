# schema.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The schema module provides the building blocks for database metadata.

Each element within this module describes a database entity 
which can be created and dropped, or is otherwise part of such an entity.
Examples include tables, columns, sequences, and indexes.

All entities are subclasses of [sqlalchemy.schema#SchemaItem], and as 
defined in this module they are intended to be agnostic of any 
vendor-specific constructs.

A collection of entities are grouped into a unit called [sqlalchemy.schema#MetaData].
MetaData serves as a logical grouping of schema elements, and can also
be associated with an actual database connection such that operations 
involving the contained elements can contact the database as needed.

Two of the elements here also build upon their "syntactic" counterparts,
which are defined in [sqlalchemy.sql.expression#], specifically [sqlalchemy.schema#Table]
and [sqlalchemy.schema#Column].  Since these objects are part of the
SQL expression language, they are usable as components in SQL expressions.
"""

import re, inspect
from sqlalchemy import types, exceptions, util, databases
from sqlalchemy.sql import expression, visitors

URL = None

__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'Index',
           'ForeignKeyConstraint', 'PrimaryKeyConstraint', 'CheckConstraint',
           'UniqueConstraint', 'DefaultGenerator', 'Constraint', 'MetaData',
           'ThreadLocalMetaData', 'SchemaVisitor', 'PassiveDefault',
           'ColumnDefault']

class SchemaItem(object):
    """Base class for items that define a database schema."""

    __metaclass__ = expression._FigureVisitName

    def _init_items(self, *args):
        """Initialize the list of child items for this SchemaItem."""

        for item in args:
            if item is not None:
                item._set_parent(self)

    def _set_parent(self, parent):
        """Associate with this SchemaItem's parent object."""

        raise NotImplementedError()
    
    def get_children(self, **kwargs):
        """used to allow SchemaVisitor access"""
        return []
        
    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def bind(self):
        """Return the connectable associated with this SchemaItem."""

        m = self.metadata
        return m and m.bind or None
    bind = property(bind)
    
    def info(self):
        try:
            return self._info
        except AttributeError:
            self._info = {}
            return self._info
    info = property(info)
    

def _get_table_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name

class _TableSingleton(expression._FigureVisitName):
    """A metaclass used by the ``Table`` object to provide singleton behavior."""

    def __call__(self, name, metadata, *args, **kwargs):
        schema = kwargs.get('schema', None)
        useexisting = kwargs.pop('useexisting', False)
        mustexist = kwargs.pop('mustexist', False)
        key = _get_table_key(name, schema)
        try:
            table = metadata.tables[key]
            if args:
                if not useexisting:
                    raise exceptions.ArgumentError("Table '%s' is already defined for this MetaData instance." % key)
            return table
        except KeyError:
            if mustexist:
                raise exceptions.ArgumentError("Table '%s' not defined" % (key))
            try:
                return type.__call__(self, name, metadata, *args, **kwargs)
            except:
                if key in metadata.tables:
                    del metadata.tables[key]
                raise


class Table(SchemaItem, expression.TableClause):
    """Represent a relational database table."""

    __metaclass__ = _TableSingleton

    def __init__(self, name, metadata, *args, **kwargs):
        """Construct a Table.

        Table objects can be constructed directly.  Arguments
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
          kwargs include:
          
          schema
            The *schema name* for this table, which is
            required if the table resides in a schema other than the
            default selected schema for the engine's database
            connection.  Defaults to ``None``.

          autoload
            Defaults to False: the Columns for this table should be
            reflected from the database.  Usually there will be no
            Column objects in the constructor if this property is set.

          autoload_with
            if autoload==True, this is an optional Engine or Connection
            instance to be used for the table reflection.  If ``None``,
            the underlying MetaData's bound connectable will be used.
        
          include_columns
            A list of strings indicating a subset of columns to be 
            loaded via the ``autoload`` operation; table columns who
            aren't present in this list will not be represented on the resulting
            ``Table`` object.  Defaults to ``None`` which indicates all 
            columns should be reflected.
        
          info
            Defaults to {}: A space to store application specific data;
            this must be a dictionary.

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

        """
        super(Table, self).__init__(name)
        self.metadata = metadata
        self.schema = kwargs.pop('schema', None)
        self.indexes = util.Set()
        self.constraints = util.Set()
        self._columns = expression.ColumnCollection()
        self.primary_key = PrimaryKeyConstraint()
        self._foreign_keys = util.OrderedSet()
        self.quote = kwargs.pop('quote', False)
        self.quote_schema = kwargs.pop('quote_schema', False)
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name
        self.owner = kwargs.pop('owner', None)
        if kwargs.get('info'):
            self._info = kwargs.pop('info')
        
        autoload = kwargs.pop('autoload', False)
        autoload_with = kwargs.pop('autoload_with', None)
        include_columns = kwargs.pop('include_columns', None)

        # validate remaining kwargs that they all specify DB prefixes
        if len([k for k in kwargs if not re.match(r'^(?:%s)_' % '|'.join(databases.__all__), k)]):
            raise TypeError("Invalid argument(s) for Table: %s" % repr(kwargs.keys()))

        self.kwargs = kwargs
        
        self._set_parent(metadata)
        # load column definitions from the database if 'autoload' is defined
        # we do it after the table is in the singleton dictionary to support
        # circular foreign keys
        if autoload:
            if autoload_with:
                autoload_with.reflecttable(self, include_columns=include_columns)
            else:
                _bind_or_error(metadata).reflecttable(self, include_columns=include_columns)
                
        # initialize all the column, etc. objects.  done after
        # reflection to allow user-overrides
        self._init_items(*args)
    
    def key(self):
        return _get_table_key(self.name, self.schema)
    key = property(key)
    
    def _export_columns(self, columns=None):
        # override FromClause's collection initialization logic; Table implements it differently
        pass

    def _set_primary_key(self, pk):
        if getattr(self, '_primary_key', None) in self.constraints:
            self.constraints.remove(self._primary_key)
        self._primary_key = pk
        self.constraints.add(pk)

    def primary_key(self):
        return self._primary_key
    primary_key = property(primary_key, _set_primary_key)

    def __repr__(self):
        return "Table(%s)" % ', '.join(
            [repr(self.name)] + [repr(self.metadata)] +
            [repr(x) for x in self.columns] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in ['schema']])

    def __str__(self):
        return _get_table_key(self.description, self.schema)

    def append_column(self, column):
        """Append a ``Column`` to this ``Table``."""

        column._set_parent(self)

    def append_constraint(self, constraint):
        """Append a ``Constraint`` to this ``Table``."""

        constraint._set_parent(self)

    def _set_parent(self, metadata):
        metadata.tables[_get_table_key(self.name, self.schema)] = self
        self.metadata = metadata

    def get_children(self, column_collections=True, schema_visitor=False, **kwargs):
        if not schema_visitor:
            return expression.TableClause.get_children(self, column_collections=column_collections, **kwargs)
        else:
            if column_collections:
                return [c for c in self.columns]
            else:
                return []

    def exists(self, bind=None):
        """Return True if this table exists."""

        if bind is None:
            bind = _bind_or_error(self)

        def do(conn):
            return conn.dialect.has_table(conn, self.name, schema=self.schema)
        return bind.run_callable(do)

    def create(self, bind=None, checkfirst=False):
        """Issue a ``CREATE`` statement for this table.

        See also ``metadata.create_all()``."""

        self.metadata.create_all(bind=bind, checkfirst=checkfirst, tables=[self])

    def drop(self, bind=None, checkfirst=False):
        """Issue a ``DROP`` statement for this table.

        See also ``metadata.drop_all()``."""

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

class Column(SchemaItem, expression._ColumnClause):
    """Represent a column in a database table.

    This is a subclass of ``expression.ColumnClause`` and represents an
    actual existing table in the database, in a similar fashion as
    ``TableClause``/``Table``.
    """

    def __init__(self, name, type_, *args, **kwargs):
        """Construct a new ``Column`` object.

        Arguments are:

        name
          The name of this column.  This should be the identical name
          as it appears, or will appear, in the database.

        type\_
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

          info
            Defaults to {}: A space to store application specific data;
            this must be a dictionary.

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

        """

        super(Column, self).__init__(name, None, type_)
        self.args = args
        self.key = kwargs.pop('key', name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self._is_oid = kwargs.pop('_is_oid', False)
        self.default = kwargs.pop('default', None)
        self.index = kwargs.pop('index', None)
        self.unique = kwargs.pop('unique', None)
        self.quote = kwargs.pop('quote', False)
        self.onupdate = kwargs.pop('onupdate', None)
        self.autoincrement = kwargs.pop('autoincrement', True)
        self.constraints = util.Set()
        self.foreign_keys = util.OrderedSet()
        if kwargs.get('info'):
            self._info = kwargs.pop('info')
        if kwargs:
            raise exceptions.ArgumentError("Unknown arguments passed to Column: " + repr(kwargs.keys()))
    
    def __str__(self):
        if self.table is not None:
            if self.table.named_with_column:
                return (self.table.description + "." + self.description)
            else:
                return self.description
        else:
            return self.description

    def bind(self):
        return self.table.bind
    bind = property(bind)
    
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
        if self.primary_key:
            kwarg.append('primary_key')
        if not self.nullable:
            kwarg.append('nullable')
        if self.onupdate:
            kwarg.append('onupdate')
        if self.default:
            kwarg.append('default')
        return "Column(%s)" % ', '.join(
            [repr(self.name)] + [repr(self.type)] +
            [repr(x) for x in self.foreign_keys if x is not None] +
            [repr(x) for x in self.constraints] +
            [(self.table and "table=<%s>" % self.table.description or "")] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg])

    def _set_parent(self, table):
        self.metadata = table.metadata
        if getattr(self, 'table', None) is not None:
            raise exceptions.ArgumentError("this Column already has a table!")
        if not self._is_oid:
            self._pre_existing_column = table._columns.get(self.key)

            table._columns.replace(self)
        else:
            self._pre_existing_column = None
            
        if self.primary_key:
            table.primary_key.replace(self)
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

        return Column(self.name, self.type, self.default, key = self.key, primary_key = self.primary_key, nullable = self.nullable, _is_oid = self._is_oid, quote=self.quote, index=self.index, *[c.copy() for c in self.constraints])

    def _make_proxy(self, selectable, name = None):
        """Create a *proxy* for this column.

        This is a copy of this ``Column`` referenced by a different parent
        (such as an alias or select statement).
        """

        fk = [ForeignKey(f._colspec) for f in self.foreign_keys]
        c = Column(name or self.name, self.type, self.default, key = name or self.key, primary_key = self.primary_key, nullable = self.nullable, _is_oid = self._is_oid, quote=self.quote, *fk)
        c.table = selectable
        c.proxies = [self]
        c._pre_existing_column = self._pre_existing_column
        if not c._is_oid:
            selectable.columns.add(c)
            if self.primary_key:
                selectable.primary_key.add(c)
        [c._init_items(f) for f in fk]
        return c


    def get_children(self, schema_visitor=False, **kwargs):
        if schema_visitor:
            return [x for x in (self.default, self.onupdate) if x is not None] + \
                list(self.foreign_keys) + list(self.constraints)
        else:
            return expression._ColumnClause.get_children(self, **kwargs)


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

        return table.corresponding_column(self.column) is not None
    
    def get_referent(self, table):
        """return the column in the given table referenced by this ``ForeignKey``, or
        None if this ``ForeignKey`` does not reference the given table.
        """
        return table.corresponding_column(self.column)
        
    def column(self):
        # ForeignKey inits its remote column as late as possible, so tables can
        # be defined without dependencies
        if self._column is None:
            if isinstance(self._colspec, basestring):
                # locate the parent table this foreign key is attached to.
                # we use the "original" column which our parent column represents
                # (its a list of columns/other ColumnElements if the parent table is a UNION)
                for c in self.parent.base_columns:
                    if isinstance(c, Column):
                        parenttable = c.table
                        break
                else:
                    raise exceptions.ArgumentError("Parent column '%s' does not descend from a table-attached Column" % str(self.parent))
                m = re.match(r"^(.+?)(?:\.(.+?))?(?:\.(.+?))?$", self._colspec, re.UNICODE)
                if m is None:
                    raise exceptions.ArgumentError("Invalid foreign key column specification: " + self._colspec)
                if m.group(3) is None:
                    (tname, colname) = m.group(1, 2)
                    schema = None
                else:
                    (schema,tname,colname) = m.group(1,2,3)
                if _get_table_key(tname, schema) not in parenttable.metadata:
                    raise exceptions.InvalidRequestError("Could not find table '%s' with which to generate a foreign key" % tname)
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
        if isinstance(self.parent.type, types.NullType):
            self.parent.type = self._column.type
        return self._column

    column = property(column)

    def _set_parent(self, column):
        self.parent = column
        
        if self.parent._pre_existing_column is not None:
            # remove existing FK which matches us
            for fk in self.parent._pre_existing_column.foreign_keys:
                if fk._colspec == self._colspec:
                    self.parent.table.foreign_keys.remove(fk)
                    self.parent.table.constraints.remove(fk.constraint)
            
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
        self.metadata = util.assert_arg_type(metadata, (MetaData, type(None)), 'metadata')

    def _set_parent(self, column):
        self.column = column
        self.metadata = self.column.table.metadata
        if self.for_update:
            self.column.onupdate = self
        else:
            self.column.default = self

    def execute(self, bind=None, **kwargs):
        if bind is None:
            bind = _bind_or_error(self)
        return bind._execute_default(self, **kwargs)

    def __repr__(self):
        return "DefaultGenerator()"

class PassiveDefault(DefaultGenerator):
    """A default that takes effect on the database side."""

    def __init__(self, arg, **kwargs):
        super(PassiveDefault, self).__init__(**kwargs)
        self.arg = arg

    def __repr__(self):
        return "PassiveDefault(%s)" % repr(self.arg)

class ColumnDefault(DefaultGenerator):
    """A plain default value on a column.

    This could correspond to a constant, a callable function, or a SQL
    clause.
    """

    def __init__(self, arg, **kwargs):
        super(ColumnDefault, self).__init__(**kwargs)
        if callable(arg):
            if not inspect.isfunction(arg):
                self.arg = lambda ctx: arg()
            else:
                argspec = inspect.getargspec(arg)
                if len(argspec[0]) == 0:
                    self.arg = lambda ctx: arg()
                else:
                    defaulted = argspec[3] is not None and len(argspec[3]) or 0
                    if len(argspec[0]) - defaulted > 1:
                        raise exceptions.ArgumentError(
                            "ColumnDefault Python function takes zero or one "
                            "positional arguments")
                    else:
                        self.arg = arg
        else:
            self.arg = arg

    def _visit_name(self):
        if self.for_update:
            return "column_onupdate"
        else:
            return "column_default"
    __visit_name__ = property(_visit_name)

    def __repr__(self):
        return "ColumnDefault(%s)" % repr(self.arg)

class Sequence(DefaultGenerator):
    """Represents a named sequence."""

    def __init__(self, name, start=None, increment=None, schema=None,
                 optional=False, quote=False, **kwargs):
        super(Sequence, self).__init__(**kwargs)
        self.name = name
        self.start = start
        self.increment = increment
        self.optional=optional
        self.quote = quote
        self.schema = schema
        self.kwargs = kwargs

    def __repr__(self):
        return "Sequence(%s)" % ', '.join(
            [repr(self.name)] +
            ["%s=%s" % (k, repr(getattr(self, k)))
             for k in ['start', 'increment', 'optional']])

    def _set_parent(self, column):
        super(Sequence, self)._set_parent(column)
        column.sequence = self

    def create(self, bind=None, checkfirst=True):
        """Creates this sequence in the database."""
        
        if bind is None:
            bind = _bind_or_error(self)
        bind.create(self, checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=True):
        """Drops this sequence from the database."""

        if bind is None:
            bind = _bind_or_error(self)
        bind.drop(self, checkfirst=checkfirst)


class Constraint(SchemaItem):
    """Represent a table-level ``Constraint`` such as a composite primary key, foreign key, or unique constraint.

    Implements a hybrid of dict/setlike behavior with regards to the
    list of underying columns.
    """

    def __init__(self, name=None):
        self.name = name
        self.columns = expression.ColumnCollection()

    def __contains__(self, x):
        return self.columns.contains_column(x)

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

class CheckConstraint(Constraint):
    def __init__(self, sqltext, name=None):
        super(CheckConstraint, self).__init__(name)
        self.sqltext = sqltext

    def __visit_name__(self):
        if isinstance(self.parent, Table):
            return "check_constraint"
        else:
            return "column_check_constraint"
    __visit_name__ = property(__visit_name__)

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
            self.add(table.c[c])
    
    def add(self, col):
        self.columns.add(col)
        col.primary_key=True
    append_column = add
    
    def replace(self, col):
        self.columns.replace(col)

    def remove(self, col):
        col.primary_key=False
        del self.columns[col.key]

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
            Defaults to False: create a unique index.

          postgres_where
            Defaults to None: create a partial index when using PostgreSQL
        """

        self.name = name
        self.columns = []
        self.table = None
        self.unique = kwargs.pop('unique', False)

        self.kwargs = kwargs

        self._init_items(*columns)

    def _init_items(self, *args):
        for column in args:
            self.append_column(column)

    def _set_parent(self, table):
        self.table = table
        self.metadata = table.metadata
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

    def create(self, bind=None):
        if bind is None:
            bind = _bind_or_error(self)
        bind.create(self)
        return self

    def drop(self, bind=None):
        if bind is None:
            bind = _bind_or_error(self)
        bind.drop(self)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return 'Index("%s", %s%s)' % (self.name,
                                      ', '.join([repr(c)
                                                 for c in self.columns]),
                                      (self.unique and ', unique=True') or '')

class MetaData(SchemaItem):
    """A collection of Tables and their associated schema constructs.
    
    Holds a collection of Tables and an optional binding to an
    ``Engine`` or ``Connection``.  If bound, the
    [sqlalchemy.schema#Table] objects in the collection and their
    columns may participate in implicit SQL execution.

    The ``bind`` property may be assigned to dynamically.  A common
    pattern is to start unbound and then bind later when an engine is
    available::

      metadata = MetaData()
      # define tables
      Table('mytable', metadata, ...)
      # connect to an engine later, perhaps after loading a URL from a
      # configuration file
      metadata.bind = an_engine

    MetaData is a thread-safe object after tables have been explicitly
    defined or loaded via reflection.
    """

    __visit_name__ = 'metadata'
    
    def __init__(self, bind=None, reflect=False):
        """Create a new MetaData object.
            
        bind
          An Engine or Connection to bind to.  May also be a string or
          URL instance, these are passed to create_engine() and this
          MetaData will be bound to the resulting engine.

        reflect
          Optional, automatically load all tables from the bound database.
          Defaults to False. ``bind`` is required when this option is
          set.  For finer control over loaded tables, use the ``reflect``
          method of ``MetaData``.

        """        

        self.tables = {}
        self.bind = bind
        self.metadata = self
        if reflect:
            if not bind:
                raise exceptions.ArgumentError(
                    "A bind must be supplied in conjunction with reflect=True")
            self.reflect()
        
    def __repr__(self):
        return 'MetaData(%r)' % self.bind

    def __contains__(self, key):
        return key in self.tables
        
    def __getstate__(self):
        return {'tables': self.tables}

    def __setstate__(self, state):
        self.tables = state['tables']
        self._bind = None
        
    def is_bound(self):
        """True if this MetaData is bound to an Engine or Connection."""

        return self._bind is not None
        
    # @deprecated
    def connect(self, bind, **kwargs):
        """Bind this MetaData to an Engine.
            
        Use ``metadata.bind = <engine>`` or ``metadata.bind = <url>``.

        bind
          A string, ``URL``, ``Engine`` or ``Connection`` instance.  If
          a string or ``URL``, will be passed to ``create_engine()`` along
          with ``\**kwargs`` to produce the engine which to connect to.
          Otherwise connects directly to the given ``Engine``.
        """
        
        global URL
        if URL is None:
            from sqlalchemy.engine.url import URL
        if isinstance(bind, (basestring, URL)):
            self._bind = sqlalchemy.create_engine(bind, **kwargs)
        else:
            self._bind = bind
    connect = util.deprecated(connect)

    def bind(self):
        """An Engine or Connection to which this MetaData is bound.

        This property may be assigned an ``Engine`` or
        ``Connection``, or assigned a string or URL to
        automatically create a basic ``Engine`` for this bind
        with ``create_engine()``.
        """
        
        return self._bind
        
    def _bind_to(self, bind):
        """Bind this MetaData to an Engine, Connection, string or URL."""

        global URL
        if URL is None:
            from sqlalchemy.engine.url import URL

        if isinstance(bind, (basestring, URL)):
            from sqlalchemy import create_engine
            self._bind = create_engine(bind)
        else:
            self._bind = bind
    bind = property(bind, _bind_to)
    
    def clear(self):
        self.tables.clear()

    def remove(self, table):
        # TODO: scan all other tables and remove FK _column 
        del self.tables[table.key]
        
    def table_iterator(self, reverse=True, tables=None):
        from sqlalchemy.sql.util import sort_tables
        if tables is None:
            tables = self.tables.values()
        else:
            tables = util.Set(tables).intersection(self.tables.values())
        return iter(sort_tables(tables, reverse=reverse))

    def reflect(self, bind=None, schema=None, only=None):
        """Load all available table definitions from the database.

        Automatically creates ``Table`` entries in this ``MetaData`` for any
        table available in the database but not yet present in the ``MetaData``.
        May be called multiple times to pick up tables recently added to the
        database, however no special action is taken if a table in this
        ``MetaData`` no longer exists in the database.

        bind
          A ``Connectable`` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.

        schema
          Optional, query and reflect tables from an alterate schema.

        only
          Optional.  Load only a sub-set of available named tables.  May
          be specified as a sequence of names or a callable.
        
          If a sequence of names is provided, only those tables will be
          reflected.  An error is raised if a table is requested but not
          available.  Named tables already present in this ``MetaData`` are
          ignored.

          If a callable is provided, it will be used as a boolean predicate
          to filter the list of potential table names.  The callable is
          called with a table name and this ``MetaData`` instance as positional
          arguments and should return a true value for any table to reflect.
        """

        reflect_opts = {'autoload': True}
        if bind is None:
            bind = _bind_or_error(self)
            conn = None
        else:
            reflect_opts['autoload_with'] = bind
            conn = bind.contextual_connect()

        if schema is not None:
            reflect_opts['schema'] = schema

        available = util.OrderedSet(bind.engine.table_names(schema,
                                                            connection=conn))
        current = util.Set(self.tables.keys())

        if only is None:
            load = [name for name in available if name not in current]
        elif callable(only):
            load = [name for name in available
                    if name not in current and only(name, self)]
        else:
            missing = [name for name in only if name not in available]
            if missing:
                s = schema and (" schema '%s'" % schema) or ''
                raise exceptions.InvalidRequestError(
                    'Could not reflect: requested table(s) not available '
                    'in %s%s: (%s)' % (bind.engine.url, s, ', '.join(missing)))
            load = [name for name in only if name not in current]

        for name in load:
            Table(name, self, **reflect_opts)

    def create_all(self, bind=None, tables=None, checkfirst=True):
        """Create all tables stored in this metadata.

        This will conditionally create tables depending on if they do
        not yet exist in the database.

        bind
          A ``Connectable`` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.

        tables
          Optional list of ``Table`` objects, which is a subset of the
          total tables in the ``MetaData`` (others are ignored).
        """

        if bind is None:
            bind = _bind_or_error(self)
        bind.create(self, checkfirst=checkfirst, tables=tables)

    def drop_all(self, bind=None, tables=None, checkfirst=True):
        """Drop all tables stored in this metadata.

        This will conditionally drop tables depending on if they
        currently exist in the database.

        bind
          A ``Connectable`` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.
          
        tables
          Optional list of ``Table`` objects, which is a subset of the
          total tables in the ``MetaData`` (others are ignored).
        """

        if bind is None:
            bind = _bind_or_error(self)
        bind.drop(self, checkfirst=checkfirst, tables=tables)
    
class ThreadLocalMetaData(MetaData):
    """A MetaData variant that presents a different ``bind`` in every thread.

    Makes the ``bind`` property of the MetaData a thread-local value,
    allowing this collection of tables to be bound to different ``Engine``
    implementations or connections in each thread.

    The ThreadLocalMetaData starts off bound to None in each thread.
    Binds must be made explicitly by assigning to the ``bind`` property or
    using ``connect()``.  You can also re-bind dynamically multiple times per
    thread, just like a regular ``MetaData``.

    Use this type of MetaData when your tables are present in more than
    one database and you need to address them simultanesouly.
    """

    __visit_name__ = 'metadata'

    def __init__(self):
        """Construct a ThreadLocalMetaData."""
    
        self.context = util.ThreadLocal()
        self.__engines = {}
        super(ThreadLocalMetaData, self).__init__()

    # @deprecated
    def connect(self, bind, **kwargs): 
        """Bind to an Engine in the caller's thread.
            
        Use ``metadata.bind=<engine>`` or ``metadata.bind=<url>``.
        
        bind
          A string, ``URL``, ``Engine`` or ``Connection`` instance.  If
          a string or ``URL``, will be passed to ``create_engine()`` along
          with ``\**kwargs`` to produce the engine which to connect to.
          Otherwise connects directly to the given ``Engine``.
        """
       
        global URL
        if URL is None:
            from sqlalchemy.engine.url import URL

        if isinstance(bind, (basestring, URL)):
            try:
                engine = self.__engines[bind]
            except KeyError:
                engine = sqlalchemy.create_engine(bind, **kwargs)
            bind = engine
        self._bind_to(bind)
    connect = util.deprecated(connect)

    def bind(self):
        """The bound Engine or Connection for this thread.

        This property may be assigned an Engine or Connection,
        or assigned a string or URL to automatically create a
        basic Engine for this bind with ``create_engine()``."""
        
        return getattr(self.context, '_engine', None)

    def _bind_to(self, bind):
        """Bind to a Connectable in the caller's thread."""

        global URL
        if URL is None:
            from sqlalchemy.engine.url import URL

        if isinstance(bind, (basestring, URL)):
            try:
                self.context._engine = self.__engines[bind]
            except KeyError:
                e = sqlalchemy.create_engine(bind)
                self.__engines[bind] = e
                self.context._engine = e
        else:
            # TODO: this is squirrely.  we shouldnt have to hold onto engines
            # in a case like this
            if bind not in self.__engines:
                self.__engines[bind] = bind
            self.context._engine = bind

    bind = property(bind, _bind_to)

    def is_bound(self):
        """True if there is a bind for this thread."""
        return (hasattr(self.context, '_engine') and
                self.context._engine is not None)

    def dispose(self):
        """Dispose any and all ``Engines`` to which this ``ThreadLocalMetaData`` has been connected."""

        for e in self.__engines.values():
            if hasattr(e, 'dispose'):
                e.dispose()

class SchemaVisitor(visitors.ClauseVisitor):
    """Define the visiting for ``SchemaItem`` objects."""

    __traverse_options__ = {'schema_visitor':True}

def _bind_or_error(schemaitem):
    bind = schemaitem.bind
    if not bind:
        raise exceptions.InvalidRequestError("This SchemaItem is not connected to any Engine or Connection.")
    return bind