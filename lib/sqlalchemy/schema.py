# schema.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The schema module provides the building blocks for database metadata.

Each element within this module describes a database entity which can be
created and dropped, or is otherwise part of such an entity.  Examples include
tables, columns, sequences, and indexes.

All entities are subclasses of :class:`~sqlalchemy.schema.SchemaItem`, and as defined
in this module they are intended to be agnostic of any vendor-specific
constructs.

A collection of entities are grouped into a unit called
:class:`~sqlalchemy.schema.MetaData`.  MetaData serves as a logical grouping of schema
elements, and can also be associated with an actual database connection such
that operations involving the contained elements can contact the database as
needed.

Two of the elements here also build upon their "syntactic" counterparts, which
are defined in :class:`~sqlalchemy.sql.expression.`, specifically
:class:`~sqlalchemy.schema.Table` and :class:`~sqlalchemy.schema.Column`.  Since these objects
are part of the SQL expression language, they are usable as components in SQL
expressions.

"""
import re, inspect
from sqlalchemy import exc, util, dialects
from sqlalchemy.sql import expression, visitors

URL = None

__all__ = ['SchemaItem', 'Table', 'Column', 'ForeignKey', 'Sequence', 'Index',
           'ForeignKeyConstraint', 'PrimaryKeyConstraint', 'CheckConstraint',
           'UniqueConstraint', 'DefaultGenerator', 'Constraint', 'MetaData',
           'ThreadLocalMetaData', 'SchemaVisitor', 'PassiveDefault',
           'DefaultClause', 'FetchedValue', 'ColumnDefault', 'DDL',
           'CreateTable', 'DropTable', 'CreateSequence', 'DropSequence',
           'AddConstraint', 'DropConstraint',
           ]
__all__.sort()

RETAIN_SCHEMA = util.symbol('retain_schema')

class SchemaItem(visitors.Visitable):
    """Base class for items that define a database schema."""

    __visit_name__ = 'schema_item'
    quote = None

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

    @util.memoized_property
    def info(self):
        return {}

def _get_table_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name

class Table(SchemaItem, expression.TableClause):
    """Represent a table in a database.
    
    e.g.::
    
        mytable = Table("mytable", metadata, 
                        Column('mytable_id', Integer, primary_key=True),
                        Column('value', String(50))
                   )

    The Table object constructs a unique instance of itself based on its
    name within the given MetaData object.   Constructor
    arguments are as follows:
    
    :param name: The name of this table as represented in the database. 

        This property, along with the *schema*, indicates the *singleton
        identity* of this table in relation to its parent :class:`MetaData`.
        Additional calls to :class:`Table` with the same name, metadata,
        and schema name will return the same :class:`Table` object.

        Names which contain no upper case characters
        will be treated as case insensitive names, and will not be quoted
        unless they are a reserved word.  Names with any number of upper
        case characters will be quoted and sent exactly.  Note that this
        behavior applies even for databases which standardize upper 
        case names as case insensitive such as Oracle.

    :param metadata: a :class:`MetaData` object which will contain this 
        table.  The metadata is used as a point of association of this table
        with other tables which are referenced via foreign key.  It also
        may be used to associate this table with a particular 
        :class:`~sqlalchemy.engine.base.Connectable`.

    :param \*args: Additional positional arguments are used primarily
        to add the list of :class:`Column` objects contained within this table.
        Similar to the style of a CREATE TABLE statement, other :class:`SchemaItem`
        constructs may be added here, including :class:`PrimaryKeyConstraint`,
        and :class:`ForeignKeyConstraint`.
        
    :param autoload: Defaults to False: the Columns for this table should be reflected
        from the database.  Usually there will be no Column objects in the
        constructor if this property is set.

    :param autoload_with: If autoload==True, this is an optional Engine or Connection
        instance to be used for the table reflection.  If ``None``, the
        underlying MetaData's bound connectable will be used.

    :param implicit_returning: True by default - indicates that 
        RETURNING can be used by default to fetch newly inserted primary key 
        values, for backends which support this.  Note that 
        create_engine() also provides an implicit_returning flag.

    :param include_columns: A list of strings indicating a subset of columns to be loaded via
        the ``autoload`` operation; table columns who aren't present in
        this list will not be represented on the resulting ``Table``
        object.  Defaults to ``None`` which indicates all columns should
        be reflected.

    :param info: A dictionary which defaults to ``{}``.  A space to store application 
        specific data. This must be a dictionary.

    :param mustexist: When ``True``, indicates that this Table must already 
        be present in the given :class:`MetaData`` collection.

    :param prefixes:
        A list of strings to insert after CREATE in the CREATE TABLE
        statement.  They will be separated by spaces.

    :param quote: Force quoting of this table's name on or off, corresponding
        to ``True`` or ``False``.  When left at its default of ``None``,
        the column identifier will be quoted according to whether the name is
        case sensitive (identifiers with at least one upper case character are 
        treated as case sensitive), or if it's a reserved word.  This flag 
        is only needed to force quoting of a reserved word which is not known
        by the SQLAlchemy dialect.

    :param quote_schema: same as 'quote' but applies to the schema identifier.

    :param schema: The *schema name* for this table, which is required if the table
        resides in a schema other than the default selected schema for the
        engine's database connection.  Defaults to ``None``.

    :param useexisting: When ``True``, indicates that if this Table is already
        present in the given :class:`MetaData`, apply further arguments within
        the constructor to the existing :class:`Table`.  If this flag is not 
        set, an error is raised when the parameters of an existing :class:`Table`
        are overwritten.

    """
    
    __visit_name__ = 'table'

    ddl_events = ('before-create', 'after-create', 'before-drop', 'after-drop')

    def __new__(cls, *args, **kw):
        if not args:
            # python3k pickle seems to call this
            return object.__new__(cls)
            
        try:
            name, metadata, args = args[0], args[1], args[2:]
        except IndexError:
            raise TypeError("Table() takes at least two arguments")
        
        schema = kw.get('schema', None)
        useexisting = kw.pop('useexisting', False)
        mustexist = kw.pop('mustexist', False)
        key = _get_table_key(name, schema)
        if key in metadata.tables:
            if not useexisting and bool(args):
                raise exc.InvalidRequestError(
                    "Table '%s' is already defined for this MetaData instance.  "
                    "Specify 'useexisting=True' to redefine options and "
                    "columns on an existing Table object." % key)
            table = metadata.tables[key]
            table._init_existing(*args, **kw)
            return table
        else:
            if mustexist:
                raise exc.InvalidRequestError(
                    "Table '%s' not defined" % (key))
            metadata.tables[key] = table = object.__new__(cls)
            try:
                table._init(name, metadata, *args, **kw)
                return table
            except:
                metadata.tables.pop(key)
                raise
                
    def __init__(self, *args, **kw):
        # __init__ is overridden to prevent __new__ from 
        # calling the superclass constructor.
        pass
        
    def _init(self, name, metadata, *args, **kwargs):
        super(Table, self).__init__(name)
        self.metadata = metadata
        self.schema = kwargs.pop('schema', None)
        self.indexes = set()
        self.constraints = set()
        self._columns = expression.ColumnCollection()
        self._set_primary_key(PrimaryKeyConstraint())
        self._foreign_keys = util.OrderedSet()
        self._extra_dependencies = set()
        self.ddl_listeners = util.defaultdict(list)
        self.kwargs = {}
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name

        autoload = kwargs.pop('autoload', False)
        autoload_with = kwargs.pop('autoload_with', None)
        include_columns = kwargs.pop('include_columns', None)

        self.implicit_returning = kwargs.pop('implicit_returning', True)
        self.quote = kwargs.pop('quote', None)
        self.quote_schema = kwargs.pop('quote_schema', None)
        if 'info' in kwargs:
            self.info = kwargs.pop('info')

        self._prefixes = kwargs.pop('prefixes', [])

        self._extra_kwargs(**kwargs)

        # load column definitions from the database if 'autoload' is defined
        # we do it after the table is in the singleton dictionary to support
        # circular foreign keys
        if autoload:
            if autoload_with:
                autoload_with.reflecttable(self, include_columns=include_columns)
            else:
                _bind_or_error(metadata, msg="No engine is bound to this Table's MetaData. "
                                        "Pass an engine to the Table via "
                                        "autoload_with=<someengine>, "
                                        "or associate the MetaData with an engine via "
                                        "metadata.bind=<someengine>").\
                                        reflecttable(self, include_columns=include_columns)

        # initialize all the column, etc. objects.  done after reflection to
        # allow user-overrides
        self._init_items(*args)

    def _init_existing(self, *args, **kwargs):
        autoload = kwargs.pop('autoload', False)
        autoload_with = kwargs.pop('autoload_with', None)
        schema = kwargs.pop('schema', None)
        if schema and schema != self.schema:
            raise exc.ArgumentError(
                "Can't change schema of existing table from '%s' to '%s'",
                (self.schema, schema))

        include_columns = kwargs.pop('include_columns', None)
        if include_columns:
            for c in self.c:
                if c.name not in include_columns:
                    self.c.remove(c)

        for key in ('quote', 'quote_schema'):
            if key in kwargs:
                setattr(self, key, kwargs.pop(key))

        if 'info' in kwargs:
            self.info = kwargs.pop('info')

        self._extra_kwargs(**kwargs)
        self._init_items(*args)

    def _extra_kwargs(self, **kwargs):
        # validate remaining kwargs that they all specify DB prefixes
        if len([k for k in kwargs
                if not re.match(r'^(?:%s)_' % '|'.join(dialects.__all__), k)]):
            raise TypeError(
                "Invalid argument(s) for Table: %r" % kwargs.keys())
        self.kwargs.update(kwargs)

    def _set_primary_key(self, pk):
        if getattr(self, '_primary_key', None) in self.constraints:
            self.constraints.remove(self._primary_key)
        self._primary_key = pk
        self.constraints.add(pk)

        for c in pk.columns:
            c.primary_key = True

    @util.memoized_property
    def _autoincrement_column(self):
        for col in self.primary_key:
            if col.autoincrement and \
                isinstance(col.type, types.Integer) and \
                not col.foreign_keys and \
                isinstance(col.default, (type(None), Sequence)):

                return col

    @property
    def key(self):
        return _get_table_key(self.name, self.schema)

    @property
    def primary_key(self):
        return self._primary_key

    def __repr__(self):
        return "Table(%s)" % ', '.join(
            [repr(self.name)] + [repr(self.metadata)] +
            [repr(x) for x in self.columns] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in ['schema']])

    def __str__(self):
        return _get_table_key(self.description, self.schema)

    @property
    def bind(self):
        """Return the connectable associated with this Table."""

        return self.metadata and self.metadata.bind or None

    def add_is_dependent_on(self, table):
        """Add a 'dependency' for this Table.
        
        This is another Table object which must be created
        first before this one can, or dropped after this one.
        
        Usually, dependencies between tables are determined via 
        ForeignKey objects.   However, for other situations that 
        create dependencies outside of foreign keys (rules, inheriting),
        this method can manually establish such a link.
        
        """
        self._extra_dependencies.add(table)
        
    def append_column(self, column):
        """Append a ``Column`` to this ``Table``."""

        column._set_parent(self)

    def append_constraint(self, constraint):
        """Append a ``Constraint`` to this ``Table``."""

        constraint._set_parent(self)

    def append_ddl_listener(self, event, listener):
        """Append a DDL event listener to this ``Table``.

        The ``listener`` callable will be triggered when this ``Table`` is
        created or dropped, either directly before or after the DDL is issued
        to the database.  The listener may modify the Table, but may not abort
        the event itself.

        Arguments are:

        event
          One of ``Table.ddl_events``; e.g. 'before-create', 'after-create',
          'before-drop' or 'after-drop'.

        listener
          A callable, invoked with three positional arguments:

          event
            The event currently being handled
          target
            The ``Table`` object being created or dropped
          bind
            The ``Connection`` bueing used for DDL execution.

        Listeners are added to the Table's ``ddl_listeners`` attribute.
        """

        if event not in self.ddl_events:
            raise LookupError(event)
        self.ddl_listeners[event].append(listener)

    def _set_parent(self, metadata):
        metadata.tables[_get_table_key(self.name, self.schema)] = self
        self.metadata = metadata

    def get_children(self, column_collections=True, schema_visitor=False, **kwargs):
        if not schema_visitor:
            return expression.TableClause.get_children(
                self, column_collections=column_collections, **kwargs)
        else:
            if column_collections:
                return list(self.columns)
            else:
                return []

    def exists(self, bind=None):
        """Return True if this table exists."""

        if bind is None:
            bind = _bind_or_error(self)

        return bind.run_callable(bind.dialect.has_table, self.name, schema=self.schema)

    def create(self, bind=None, checkfirst=False):
        """Issue a ``CREATE`` statement for this table.

        See also ``metadata.create_all()``.

        """

        if bind is None:
            bind = _bind_or_error(self)
        bind.create(self, checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=False):
        """Issue a ``DROP`` statement for this table.

        See also ``metadata.drop_all()``.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind.drop(self, checkfirst=checkfirst)
        

    def tometadata(self, metadata, schema=RETAIN_SCHEMA):
        """Return a copy of this ``Table`` associated with a different ``MetaData``."""

        try:
            if schema is RETAIN_SCHEMA:
                schema = self.schema
            key = _get_table_key(self.name, schema)
            return metadata.tables[key]
        except KeyError:
            args = []
            for c in self.columns:
                args.append(c.copy(schema=schema))
            for c in self.constraints:
                args.append(c.copy(schema=schema))
            return Table(self.name, metadata, schema=schema, *args)

class Column(SchemaItem, expression.ColumnClause):
    """Represents a column in a database table."""

    __visit_name__ = 'column'
    
    def __init__(self, *args, **kwargs):
        """
        Construct a new ``Column`` object.
        
        :param name: The name of this column as represented in the database. 
          This argument may be the first positional argument, or specified
          via keyword.
          
          Names which contain no upper case characters
          will be treated as case insensitive names, and will not be quoted
          unless they are a reserved word.  Names with any number of upper
          case characters will be quoted and sent exactly.  Note that this
          behavior applies even for databases which standardize upper 
          case names as case insensitive such as Oracle.
          
          The name field may be omitted at construction time and applied
          later, at any time before the Column is associated with a 
          :class:`Table`.  This is to support convenient
          usage within the :mod:`~sqlalchemy.ext.declarative` extension.
          
        :param type\_: The column's type, indicated using an instance which 
          subclasses :class:`~sqlalchemy.types.AbstractType`.  If no arguments
          are required for the type, the class of the type can be sent
          as well, e.g.::
          
            # use a type with arguments
            Column('data', String(50))
            
            # use no arguments
            Column('level', Integer)
            
          The ``type`` argument may be the second positional argument
          or specified by keyword.

          There is partial support for automatic detection of the 
          type based on that of a :class:`ForeignKey` associated 
          with this column, if the type is specified as ``None``. 
          However, this feature is not fully implemented and 
          may not function in all cases.

        :param \*args: Additional positional arguments include various 
          :class:`SchemaItem` derived constructs which will be applied 
          as options to the column.  These include instances of 
          :class:`Constraint`, :class:`ForeignKey`, :class:`ColumnDefault`, 
          and :class:`Sequence`.  In some cases an equivalent keyword 
          argument is available such as ``server_default``, ``default``
          and ``unique``.

        :param autoincrement: This flag may be set to ``False`` to 
          indicate an integer primary key column that should not be
          considered to be the "autoincrement" column, that is
          the integer primary key column which generates values 
          implicitly upon INSERT and whose value is usually returned
          via the DBAPI cursor.lastrowid attribute.   It defaults
          to ``True`` to satisfy the common use case of a table
          with a single integer primary key column.  If the table
          has a composite primary key consisting of more than one
          integer column, set this flag to True only on the 
          column that should be considered "autoincrement".
          
          The setting *only* has an effect for columns which are:
          
          * Integer derived (i.e. INT, SMALLINT, BIGINT)
          
          * Part of the primary key
          
          * Are not referenced by any foreign keys
          
          * have no server side or client side defaults (with the exception
            of Postgresql SERIAL).
            
          The setting has these two effects on columns that meet the
          above criteria:
          
          * DDL issued for the column will include database-specific
            keywords intended to signify this column as an
            "autoincrement" column, such as AUTO INCREMENT on MySQL,
            SERIAL on Postgresql, and IDENTITY on MS-SQL.  It does 
            *not* issue AUTOINCREMENT for SQLite since this is a
            special SQLite flag that is not required for autoincrementing
            behavior.  See the SQLite dialect documentation for
            information on SQLite's AUTOINCREMENT.
            
          * The column will be considered to be available as 
            cursor.lastrowid or equivalent, for those dialects which
            "post fetch" newly inserted identifiers after a row has
            been inserted (SQLite, MySQL, MS-SQL).  It does not have 
            any effect in this regard for databases that use sequences 
            to generate primary key identifiers (i.e. Firebird, Postgresql, 
            Oracle).

        :param default: A scalar, Python callable, or
            :class:`~sqlalchemy.sql.expression.ClauseElement` representing the
            *default value* for this column, which will be invoked upon insert
            if this column is otherwise not specified in the VALUES clause of
            the insert. This is a shortcut to using :class:`ColumnDefault` as
            a positional argument.
          
            Contrast this argument to ``server_default`` which creates a 
            default generator on the database side.
        
        :param doc: optional String that can be used by the ORM or similar
            to document attributes.   This attribute does not render SQL
            comments (a future attribute 'comment' will achieve that).
            
        :param key: An optional string identifier which will identify this
            ``Column`` object on the :class:`Table`. When a key is provided,
            this is the only identifier referencing the ``Column`` within the
            application, including ORM attribute mapping; the ``name`` field
            is used only when rendering SQL.

        :param index: When ``True``, indicates that the column is indexed.
            This is a shortcut for using a :class:`Index` construct on the
            table. To specify indexes with explicit names or indexes that
            contain multiple columns, use the :class:`Index` construct
            instead.

        :param info: A dictionary which defaults to ``{}``. A space to store
            application specific data. This must be a dictionary.

        :param nullable: If set to the default of ``True``, indicates the 
            column will be rendered as allowing NULL, else it's rendered as
            NOT NULL. This parameter is only used when issuing CREATE TABLE
            statements.

        :param onupdate: A scalar, Python callable, or
            :class:`~sqlalchemy.sql.expression.ClauseElement` representing a
            default value to be applied to the column within UPDATE
            statements, which wil be invoked upon update if this column is not
            present in the SET clause of the update. This is a shortcut to
            using :class:`ColumnDefault` as a positional argument with
            ``for_update=True``.
            
        :param primary_key: If ``True``, marks this column as a primary key
            column. Multiple columns can have this flag set to specify
            composite primary keys. As an alternative, the primary key of a
            :class:`Table` can be specified via an explicit
            :class:`PrimaryKeyConstraint` object.

        :param server_default: A :class:`FetchedValue` instance, str, Unicode
            or :func:`~sqlalchemy.sql.expression.text` construct representing
            the DDL DEFAULT value for the column.

            String types will be emitted as-is, surrounded by single quotes::

                Column('x', Text, server_default="val")

                x TEXT DEFAULT 'val'

            A :func:`~sqlalchemy.sql.expression.text` expression will be
            rendered as-is, without quotes::

                Column('y', DateTime, server_default=text('NOW()'))0

                y DATETIME DEFAULT NOW()

            Strings and text() will be converted into a :class:`DefaultClause`
            object upon initialization.
          
            Use :class:`FetchedValue` to indicate that an already-existing
            column will generate a default value on the database side which
            will be available to SQLAlchemy for post-fetch after inserts. This
            construct does not specify any DDL and the implementation is left
            to the database, such as via a trigger.

        :param server_onupdate:   A :class:`FetchedValue` instance
             representing a database-side default generation function. This
             indicates to SQLAlchemy that a newly generated value will be
             available after updates. This construct does not specify any DDL
             and the implementation is left to the database, such as via a
             trigger.

        :param quote: Force quoting of this column's name on or off,
             corresponding to ``True`` or ``False``. When left at its default
             of ``None``, the column identifier will be quoted according to
             whether the name is case sensitive (identifiers with at least one
             upper case character are treated as case sensitive), or if it's a
             reserved word. This flag is only needed to force quoting of a
             reserved word which is not known by the SQLAlchemy dialect.

        :param unique: When ``True``, indicates that this column contains a
             unique constraint, or if ``index`` is ``True`` as well, indicates
             that the :class:`Index` should be created with the unique flag.
             To specify multiple columns in the constraint/index or to specify
             an explicit name, use the :class:`UniqueConstraint` or
             :class:`Index` constructs explicitly.

        """

        name = kwargs.pop('name', None)
        type_ = kwargs.pop('type_', None)
        args = list(args)
        if args:
            if isinstance(args[0], basestring):
                if name is not None:
                    raise exc.ArgumentError(
                        "May not pass name positionally and as a keyword.")
                name = args.pop(0)
        if args:
            coltype = args[0]
            
            if (isinstance(coltype, types.AbstractType) or
                (isinstance(coltype, type) and
                 issubclass(coltype, types.AbstractType))):
                if type_ is not None:
                    raise exc.ArgumentError(
                        "May not pass type_ positionally and as a keyword.")
                type_ = args.pop(0)
        
        no_type = type_ is None
        
        super(Column, self).__init__(name, None, type_)
        self.key = kwargs.pop('key', name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.default = kwargs.pop('default', None)
        self.server_default = kwargs.pop('server_default', None)
        self.server_onupdate = kwargs.pop('server_onupdate', None)
        self.index = kwargs.pop('index', None)
        self.unique = kwargs.pop('unique', None)
        self.quote = kwargs.pop('quote', None)
        self.doc = kwargs.pop('doc', None)
        self.onupdate = kwargs.pop('onupdate', None)
        self.autoincrement = kwargs.pop('autoincrement', True)
        self.constraints = set()
        self.foreign_keys = util.OrderedSet()
        self._table_events = set()

        # check if this Column is proxying another column
        if '_proxies' in kwargs:
            self.proxies = kwargs.pop('_proxies')
        # otherwise, add DDL-related events
        elif isinstance(self.type, types.SchemaType):
            self.type._set_parent(self)
            
        if self.default is not None:
            if isinstance(self.default, (ColumnDefault, Sequence)):
                args.append(self.default)
            else:
                args.append(ColumnDefault(self.default))

        if self.server_default is not None:
            if isinstance(self.server_default, FetchedValue):
                args.append(self.server_default)
            else:
                args.append(DefaultClause(self.server_default))
                
        if self.onupdate is not None:
            if isinstance(self.onupdate, (ColumnDefault, Sequence)):
                args.append(self.onupdate)
            else:
                args.append(ColumnDefault(self.onupdate, for_update=True))
            
        if self.server_onupdate is not None:
            if isinstance(self.server_onupdate, FetchedValue):
                args.append(self.server_default)
            else:
                args.append(DefaultClause(self.server_onupdate,
                                            for_update=True))
        self._init_items(*args)

        if not self.foreign_keys and no_type:
            raise exc.ArgumentError("'type' is required on Column objects "
                                        "which have no foreign keys.")
        util.set_creation_order(self)

        if 'info' in kwargs:
            self.info = kwargs.pop('info')
            
        if kwargs:
            raise exc.ArgumentError(
                "Unknown arguments passed to Column: " + repr(kwargs.keys()))

    def __str__(self):
        if self.name is None:
            return "(no name)"
        elif self.table is not None:
            if self.table.named_with_column:
                return (self.table.description + "." + self.description)
            else:
                return self.description
        else:
            return self.description

    def references(self, column):
        """Return True if this Column references the given column via foreign key."""
        for fk in self.foreign_keys:
            if fk.references(column.table):
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
        if self.server_default:
            kwarg.append('server_default')
        return "Column(%s)" % ', '.join(
            [repr(self.name)] + [repr(self.type)] +
            [repr(x) for x in self.foreign_keys if x is not None] +
            [repr(x) for x in self.constraints] +
            [(self.table is not None and "table=<%s>" % self.table.description or "")] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg])

    def _set_parent(self, table):
        if self.name is None:
            raise exc.ArgumentError(
                "Column must be constructed with a name or assign .name "
                "before adding to a Table.")
        if self.key is None:
            self.key = self.name

        if getattr(self, 'table', None) is not None:
            raise exc.ArgumentError("this Column already has a table!")

        if self.key in table._columns:
            col = table._columns.get(self.key)
            for fk in col.foreign_keys:
                col.foreign_keys.remove(fk)
                table.foreign_keys.remove(fk)
                table.constraints.remove(fk.constraint)
            
        table._columns.replace(self)

        if self.primary_key:
            table.primary_key._replace(self)
        elif self.key in table.primary_key:
            raise exc.ArgumentError(
                "Trying to redefine primary-key column '%s' as a "
                "non-primary-key column on table '%s'" % (
                self.key, table.fullname))
        self.table = table

        if self.index:
            if isinstance(self.index, basestring):
                raise exc.ArgumentError(
                    "The 'index' keyword argument on Column is boolean only. "
                    "To create indexes with a specific name, create an "
                    "explicit Index object external to the Table.")
            Index('ix_%s' % self._label, self, unique=self.unique)
        elif self.unique:
            if isinstance(self.unique, basestring):
                raise exc.ArgumentError(
                    "The 'unique' keyword argument on Column is boolean only. "
                    "To create unique constraints or indexes with a specific "
                    "name, append an explicit UniqueConstraint to the Table's "
                    "list of elements, or create an explicit Index object "
                    "external to the Table.")
            table.append_constraint(UniqueConstraint(self.key))

        for fn in self._table_events:
            fn(table, self)
        del self._table_events
    
    def _on_table_attach(self, fn):
        if self.table is not None:
            fn(self.table, self)
        else:
            self._table_events.add(fn)
            
    def copy(self, **kw):
        """Create a copy of this ``Column``, unitialized.

        This is used in ``Table.tometadata``.

        """
        
        # Constraint objects plus non-constraint-bound ForeignKey objects
        args = \
            [c.copy(**kw) for c in self.constraints] + \
            [c.copy(**kw) for c in self.foreign_keys if not c.constraint]
            
        c = Column(
                name=self.name, 
                type_=self.type, 
                key = self.key, 
                primary_key = self.primary_key, 
                nullable = self.nullable, 
                unique = self.unique, 
                quote=self.quote, 
                index=self.index, 
                autoincrement=self.autoincrement, 
                default=self.default,
                server_default=self.server_default,
                onupdate=self.onupdate,
                server_onupdate=self.server_onupdate,
                *args
                )
        if hasattr(self, '_table_events'):
            c._table_events = list(self._table_events)
        return c
        
    def _make_proxy(self, selectable, name=None):
        """Create a *proxy* for this column.

        This is a copy of this ``Column`` referenced by a different parent
        (such as an alias or select statement).  The column should
        be used only in select scenarios, as its full DDL/default
        information is not transferred.
        
        """
        fk = [ForeignKey(f.column) for f in self.foreign_keys]
        c = self._constructor(
            name or self.name, 
            self.type, 
            key = name or self.key, 
            primary_key = self.primary_key, 
            nullable = self.nullable, 
            quote=self.quote, _proxies=[self], *fk)
        c.table = selectable
        selectable.columns.add(c)
        if self.primary_key:
            selectable.primary_key.add(c)
        for fn in c._table_events:
            fn(selectable, c)
        del c._table_events
        return c

    def get_children(self, schema_visitor=False, **kwargs):
        if schema_visitor:
            return [x for x in (self.default, self.onupdate) if x is not None] + \
                list(self.foreign_keys) + list(self.constraints)
        else:
            return expression.ColumnClause.get_children(self, **kwargs)


class ForeignKey(SchemaItem):
    """Defines a dependency between two columns.

    ``ForeignKey`` is specified as an argument to a :class:`Column` object,
    e.g.::
    
        t = Table("remote_table", metadata, 
            Column("remote_id", ForeignKey("main_table.id"))
        )
    
    Note that ``ForeignKey`` is only a marker object that defines
    a dependency between two columns.   The actual constraint
    is in all cases represented by the :class:`ForeignKeyConstraint`
    object.   This object will be generated automatically when
    a ``ForeignKey`` is associated with a :class:`Column` which 
    in turn is associated with a :class:`Table`.   Conversely,
    when :class:`ForeignKeyConstraint` is applied to a :class:`Table`,
    ``ForeignKey`` markers are automatically generated to be
    present on each associated :class:`Column`, which are also
    associated with the constraint object.
    
    Note that you cannot define a "composite" foreign key constraint,
    that is a constraint between a grouping of multiple parent/child
    columns, using ``ForeignKey`` objects.   To define this grouping,
    the :class:`ForeignKeyConstraint` object must be used, and applied
    to the :class:`Table`.   The associated ``ForeignKey`` objects
    are created automatically.
    
    The ``ForeignKey`` objects associated with an individual 
    :class:`Column` object are available in the `foreign_keys` collection
    of that column.
    
    Further examples of foreign key configuration are in
    :ref:`metadata_foreignkeys`.

    """

    __visit_name__ = 'foreign_key'

    def __init__(self, column, _constraint=None, use_alter=False, name=None,
                    onupdate=None, ondelete=None, deferrable=None,
                    initially=None, link_to_name=False):
        """
        Construct a column-level FOREIGN KEY.  
        
        The :class:`ForeignKey` object when constructed generates a
        :class:`ForeignKeyConstraint` which is associated with the parent
        :class:`Table` object's collection of constraints.

        :param column: A single target column for the key relationship. A
            :class:`Column` object or a column name as a string:
            ``tablename.columnkey`` or ``schema.tablename.columnkey``.
            ``columnkey`` is the ``key`` which has been assigned to the column
            (defaults to the column name itself), unless ``link_to_name`` is
            ``True`` in which case the rendered name of the column is used.

        :param name: Optional string. An in-database name for the key if
            `constraint` is not provided.

        :param onupdate: Optional string. If set, emit ON UPDATE <value> when
            issuing DDL for this constraint. Typical values include CASCADE,
            DELETE and RESTRICT.

        :param ondelete: Optional string. If set, emit ON DELETE <value> when
            issuing DDL for this constraint. Typical values include CASCADE,
            DELETE and RESTRICT.

        :param deferrable: Optional bool. If set, emit DEFERRABLE or NOT
            DEFERRABLE when issuing DDL for this constraint.

        :param initially: Optional string. If set, emit INITIALLY <value> when
            issuing DDL for this constraint.
        
        :param link_to_name: if True, the string name given in ``column`` is
            the rendered name of the referenced column, not its locally
            assigned ``key``.
          
        :param use_alter: passed to the underlying
            :class:`ForeignKeyConstraint` to indicate the constraint should be
            generated/dropped externally from the CREATE TABLE/ DROP TABLE
            statement. See that classes' constructor for details.
        
        """

        self._colspec = column
        
        # the linked ForeignKeyConstraint.
        # ForeignKey will create this when parent Column
        # is attached to a Table, *or* ForeignKeyConstraint
        # object passes itself in when creating ForeignKey 
        # markers.
        self.constraint = _constraint
        
        
        self.use_alter = use_alter
        self.name = name
        self.onupdate = onupdate
        self.ondelete = ondelete
        self.deferrable = deferrable
        self.initially = initially
        self.link_to_name = link_to_name

    def __repr__(self):
        return "ForeignKey(%r)" % self._get_colspec()

    def copy(self, schema=None):
        """Produce a copy of this ForeignKey object."""
        
        return ForeignKey(
                self._get_colspec(schema=schema),
                use_alter=self.use_alter,
                name=self.name,
                onupdate=self.onupdate,
                ondelete=self.ondelete,
                deferrable=self.deferrable,
                initially=self.initially,
                link_to_name=self.link_to_name
                )

    def _get_colspec(self, schema=None):
        if schema:
            return schema + "." + self.column.table.name + "." + self.column.key
        elif isinstance(self._colspec, basestring):
            return self._colspec
        elif hasattr(self._colspec, '__clause_element__'):
            _column = self._colspec.__clause_element__()
        else:
            _column = self._colspec
            
        return "%s.%s" % (_column.table.fullname, _column.key)

    target_fullname = property(_get_colspec)

    def references(self, table):
        """Return True if the given table is referenced by this ForeignKey."""
        return table.corresponding_column(self.column) is not None

    def get_referent(self, table):
        """Return the column in the given table referenced by this ForeignKey.

        Returns None if this ``ForeignKey`` does not reference the given table.

        """

        return table.corresponding_column(self.column)

    @util.memoized_property
    def column(self):
        # ForeignKey inits its remote column as late as possible, so tables
        # can be defined without dependencies
        if isinstance(self._colspec, basestring):
            # locate the parent table this foreign key is attached to.  we
            # use the "original" column which our parent column represents
            # (its a list of columns/other ColumnElements if the parent
            # table is a UNION)
            for c in self.parent.base_columns:
                if isinstance(c, Column):
                    parenttable = c.table
                    break
            else:
                raise exc.ArgumentError(
                    "Parent column '%s' does not descend from a "
                    "table-attached Column" % str(self.parent))

            m = self._colspec.split('.')

            if m is None:
                raise exc.ArgumentError(
                    "Invalid foreign key column specification: %s" %
                    self._colspec)

            # A FK between column 'bar' and table 'foo' can be
            # specified as 'foo', 'foo.bar', 'dbo.foo.bar',
            # 'otherdb.dbo.foo.bar'. Once we have the column name and
            # the table name, treat everything else as the schema
            # name. Some databases (e.g. Sybase) support
            # inter-database foreign keys. See tickets#1341 and --
            # indirectly related -- Ticket #594. This assumes that '.'
            # will never appear *within* any component of the FK.

            (schema, tname, colname) = (None, None, None)
            if (len(m) == 1):
                tname   = m.pop()
            else:
                colname = m.pop()
                tname   = m.pop()

            if (len(m) > 0):
                schema = '.'.join(m)

            if _get_table_key(tname, schema) not in parenttable.metadata:
                raise exc.NoReferencedTableError(
                    "Could not find table '%s' with which to generate a "
                    "foreign key" % tname)
            table = Table(tname, parenttable.metadata,
                          mustexist=True, schema=schema)
                          
            _column = None
            if colname is None:
                # colname is None in the case that ForeignKey argument
                # was specified as table name only, in which case we
                # match the column name to the same column on the
                # parent.
                key = self.parent
                _column = table.c.get(self.parent.key, None)
            elif self.link_to_name:
                key = colname
                for c in table.c:
                    if c.name == colname:
                        _column = c
            else:
                key = colname
                _column = table.c.get(colname, None)

            if _column is None:
                raise exc.NoReferencedColumnError(
                    "Could not create ForeignKey '%s' on table '%s': "
                    "table '%s' has no column named '%s'" % (
                    self._colspec, parenttable.name, table.name, key))

        elif hasattr(self._colspec, '__clause_element__'):
            _column = self._colspec.__clause_element__()
        else:
            _column = self._colspec

        # propagate TypeEngine to parent if it didn't have one
        if isinstance(self.parent.type, types.NullType):
            self.parent.type = _column.type
        return _column

    def _set_parent(self, column):
        if hasattr(self, 'parent'):
            if self.parent is column:
                return
            raise exc.InvalidRequestError("This ForeignKey already has a parent !")
        self.parent = column
        self.parent.foreign_keys.add(self)
        self.parent._on_table_attach(self._set_table)
    
    def _set_table(self, table, column):
        # standalone ForeignKey - create ForeignKeyConstraint
        # on the hosting Table when attached to the Table.
        if self.constraint is None and isinstance(table, Table):
            self.constraint = ForeignKeyConstraint(
                [], [], use_alter=self.use_alter, name=self.name,
                onupdate=self.onupdate, ondelete=self.ondelete,
                deferrable=self.deferrable, initially=self.initially,
                )
            self.constraint._elements[self.parent] = self
            self.constraint._set_parent(table)
        table.foreign_keys.add(self)
        
class DefaultGenerator(SchemaItem):
    """Base class for column *default* values."""

    __visit_name__ = 'default_generator'

    is_sequence = False
    
    def __init__(self, for_update=False):
        self.for_update = for_update

    def _set_parent(self, column):
        self.column = column
        if self.for_update:
            self.column.onupdate = self
        else:
            self.column.default = self

    def execute(self, bind=None, **kwargs):
        if bind is None:
            bind = _bind_or_error(self)
        return bind._execute_default(self, **kwargs)

    @property
    def bind(self):
        """Return the connectable associated with this default."""
        if getattr(self, 'column', None) is not None:
            return self.column.table.bind
        else:
            return None

    def __repr__(self):
        return "DefaultGenerator()"


class ColumnDefault(DefaultGenerator):
    """A plain default value on a column.

    This could correspond to a constant, a callable function, or a SQL clause.
    """

    def __init__(self, arg, **kwargs):
        super(ColumnDefault, self).__init__(**kwargs)
        if isinstance(arg, FetchedValue):
            raise exc.ArgumentError(
                "ColumnDefault may not be a server-side default type.")
        if util.callable(arg):
            arg = self._maybe_wrap_callable(arg)
        self.arg = arg
    
    @util.memoized_property
    def is_callable(self):
        return util.callable(self.arg)
        
    @util.memoized_property
    def is_clause_element(self):
        return isinstance(self.arg, expression.ClauseElement)
    
    @util.memoized_property
    def is_scalar(self):
        return not self.is_callable and not self.is_clause_element and not self.is_sequence
        
    def _maybe_wrap_callable(self, fn):
        """Backward compat: Wrap callables that don't accept a context."""

        if inspect.isfunction(fn):
            inspectable = fn
        elif inspect.isclass(fn):
            inspectable = fn.__init__
        elif hasattr(fn, '__call__'):
            inspectable = fn.__call__
        else:
            # probably not inspectable, try anyways.
            inspectable = fn
        try:
            argspec = inspect.getargspec(inspectable)
        except TypeError:
            return lambda ctx: fn()

        positionals = len(argspec[0])
        
        # Py3K compat - no unbound methods
        if inspect.ismethod(inspectable) or inspect.isclass(fn):
            positionals -= 1

        if positionals == 0:
            return lambda ctx: fn()

        defaulted = argspec[3] is not None and len(argspec[3]) or 0
        if positionals - defaulted > 1:
            raise exc.ArgumentError(
                "ColumnDefault Python function takes zero or one "
                "positional arguments")
        return fn

    def _visit_name(self):
        if self.for_update:
            return "column_onupdate"
        else:
            return "column_default"
    __visit_name__ = property(_visit_name)

    def __repr__(self):
        return "ColumnDefault(%r)" % self.arg

class Sequence(DefaultGenerator):
    """Represents a named database sequence."""

    __visit_name__ = 'sequence'

    is_sequence = True
    
    def __init__(self, name, start=None, increment=None, schema=None,
                 optional=False, quote=None, metadata=None, for_update=False):
        super(Sequence, self).__init__(for_update=for_update)
        self.name = name
        self.start = start
        self.increment = increment
        self.optional = optional
        self.quote = quote
        self.schema = schema
        self.metadata = metadata

    @util.memoized_property
    def is_callable(self):
        return False

    @util.memoized_property
    def is_clause_element(self):
        return False

    def __repr__(self):
        return "Sequence(%s)" % ', '.join(
            [repr(self.name)] +
            ["%s=%s" % (k, repr(getattr(self, k)))
             for k in ['start', 'increment', 'optional']])

    def _set_parent(self, column):
        super(Sequence, self)._set_parent(column)
        column._on_table_attach(self._set_table)
    
    def _set_table(self, table, column):
        self.metadata = table.metadata
        
    @property
    def bind(self):
        if self.metadata:
            return self.metadata.bind
        else:
            return None
        
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


class FetchedValue(object):
    """A default that takes effect on the database side."""

    def __init__(self, for_update=False):
        self.for_update = for_update

    def _set_parent(self, column):
        self.column = column
        if self.for_update:
            self.column.server_onupdate = self
        else:
            self.column.server_default = self

    def __repr__(self):
        return 'FetchedValue(for_update=%r)' % self.for_update


class DefaultClause(FetchedValue):
    """A DDL-specified DEFAULT column value."""

    def __init__(self, arg, for_update=False):
        util.assert_arg_type(arg, (basestring,
                                   expression.ClauseElement,
                                   expression._TextClause), 'arg')
        super(DefaultClause, self).__init__(for_update)
        self.arg = arg

    def __repr__(self):
        return "DefaultClause(%r, for_update=%r)" % (self.arg, self.for_update)

class PassiveDefault(DefaultClause):
    def __init__(self, *arg, **kw):
        util.warn_deprecated("PassiveDefault is deprecated.  Use DefaultClause.")
        DefaultClause.__init__(self, *arg, **kw)

class Constraint(SchemaItem):
    """A table-level SQL constraint."""

    __visit_name__ = 'constraint'

    def __init__(self, name=None, deferrable=None, initially=None, 
                            _create_rule=None):
        """Create a SQL constraint.

        name
          Optional, the in-database name of this ``Constraint``.

        deferrable
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        initially
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.
          
        _create_rule
          a callable which is passed the DDLCompiler object during
          compilation. Returns True or False to signal inline generation of
          this Constraint.

          The AddConstraint and DropConstraint DDL constructs provide
          DDLElement's more comprehensive "conditional DDL" approach that is
          passed a database connection when DDL is being issued. _create_rule
          is instead called during any CREATE TABLE compilation, where there
          may not be any transaction/connection in progress. However, it
          allows conditional compilation of the constraint even for backends
          which do not support addition of constraints through ALTER TABLE,
          which currently includes SQLite.

          _create_rule is used by some types to create constraints.
          Currently, its call signature is subject to change at any time.
          
        """

        self.name = name
        self.deferrable = deferrable
        self.initially = initially
        self._create_rule = _create_rule

    @property
    def table(self):
        try:
            if isinstance(self.parent, Table):
                return self.parent
        except AttributeError:
            pass
        raise exc.InvalidRequestError("This constraint is not bound to a table.  Did you mean to call table.add_constraint(constraint) ?")

    def _set_parent(self, parent):
        self.parent = parent
        parent.constraints.add(self)

    def copy(self, **kw):
        raise NotImplementedError()

class ColumnCollectionConstraint(Constraint):
    """A constraint that proxies a ColumnCollection."""
    
    def __init__(self, *columns, **kw):
        """
        \*columns
          A sequence of column names or Column objects.

        name
          Optional, the in-database name of this constraint.

        deferrable
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        initially
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.
        
        """
        super(ColumnCollectionConstraint, self).__init__(**kw)
        self.columns = expression.ColumnCollection()
        self._pending_colargs = [_to_schema_column_or_string(c) for c in columns]
        if self._pending_colargs and \
                isinstance(self._pending_colargs[0], Column) and \
                self._pending_colargs[0].table is not None:
            self._set_parent(self._pending_colargs[0].table)
        
    def _set_parent(self, table):
        super(ColumnCollectionConstraint, self)._set_parent(table)
        for col in self._pending_colargs:
            if isinstance(col, basestring):
                col = table.c[col]
            self.columns.add(col)

    def __contains__(self, x):
        return x in self.columns

    def copy(self, **kw):
        return self.__class__(name=self.name, deferrable=self.deferrable,
                              initially=self.initially, *self.columns.keys())

    def contains_column(self, col):
        return self.columns.contains_column(col)

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)


class CheckConstraint(Constraint):
    """A table- or column-level CHECK constraint.

    Can be included in the definition of a Table or Column.
    """

    def __init__(self, sqltext, name=None, deferrable=None, 
                    initially=None, table=None, _create_rule=None):
        """Construct a CHECK constraint.

        sqltext
          A string containing the constraint definition, which will be used
          verbatim, or a SQL expression construct.
          
        name
          Optional, the in-database name of the constraint.

        deferrable
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        initially
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.
          
        """

        super(CheckConstraint, self).__init__(name, deferrable, initially, _create_rule)
        self.sqltext = expression._literal_as_text(sqltext)
        if table is not None:
            self._set_parent(table)
            
    def __visit_name__(self):
        if isinstance(self.parent, Table):
            return "check_constraint"
        else:
            return "column_check_constraint"
    __visit_name__ = property(__visit_name__)

    def copy(self, **kw):
        return CheckConstraint(self.sqltext, name=self.name)

class ForeignKeyConstraint(Constraint):
    """A table-level FOREIGN KEY constraint.

    Defines a single column or composite FOREIGN KEY ... REFERENCES
    constraint. For a no-frills, single column foreign key, adding a
    :class:`ForeignKey` to the definition of a :class:`Column` is a shorthand
    equivalent for an unnamed, single column :class:`ForeignKeyConstraint`.
    
    Examples of foreign key configuration are in :ref:`metadata_foreignkeys`.
    
    """
    __visit_name__ = 'foreign_key_constraint'

    def __init__(self, columns, refcolumns, name=None, onupdate=None,
            ondelete=None, deferrable=None, initially=None, use_alter=False,
            link_to_name=False, table=None):
        """Construct a composite-capable FOREIGN KEY.

        :param columns: A sequence of local column names. The named columns
          must be defined and present in the parent Table. The names should
          match the ``key`` given to each column (defaults to the name) unless
          ``link_to_name`` is True.

        :param refcolumns: A sequence of foreign column names or Column
          objects. The columns must all be located within the same Table.

        :param name: Optional, the in-database name of the key.

        :param onupdate: Optional string. If set, emit ON UPDATE <value> when
          issuing DDL for this constraint. Typical values include CASCADE,
          DELETE and RESTRICT.

        :param ondelete: Optional string. If set, emit ON DELETE <value> when
          issuing DDL for this constraint. Typical values include CASCADE,
          DELETE and RESTRICT.

        :param deferrable: Optional bool. If set, emit DEFERRABLE or NOT
          DEFERRABLE when issuing DDL for this constraint.

        :param initially: Optional string. If set, emit INITIALLY <value> when
          issuing DDL for this constraint.

        :param link_to_name: if True, the string name given in ``column`` is
          the rendered name of the referenced column, not its locally assigned
          ``key``.

        :param use_alter: If True, do not emit the DDL for this constraint as
          part of the CREATE TABLE definition. Instead, generate it via an
          ALTER TABLE statement issued after the full collection of tables
          have been created, and drop it via an ALTER TABLE statement before
          the full collection of tables are dropped. This is shorthand for the
          usage of :class:`AddConstraint` and :class:`DropConstraint` applied
          as "after-create" and "before-drop" events on the MetaData object.
          This is normally used to generate/drop constraints on objects that
          are mutually dependent on each other.
          
        """
        super(ForeignKeyConstraint, self).__init__(name, deferrable, initially)

        self.onupdate = onupdate
        self.ondelete = ondelete
        self.link_to_name = link_to_name
        if self.name is None and use_alter:
            raise exc.ArgumentError("Alterable Constraint requires a name")
        self.use_alter = use_alter

        self._elements = util.OrderedDict()
        
        # standalone ForeignKeyConstraint - create
        # associated ForeignKey objects which will be applied to hosted
        # Column objects (in col.foreign_keys), either now or when attached 
        # to the Table for string-specified names
        for col, refcol in zip(columns, refcolumns):
            self._elements[col] = ForeignKey(
                    refcol, 
                    _constraint=self, 
                    name=self.name, 
                    onupdate=self.onupdate, 
                    ondelete=self.ondelete, 
                    use_alter=self.use_alter, 
                    link_to_name=self.link_to_name
                )

        if table is not None:
            self._set_parent(table)
    
    @property
    def columns(self):
        return self._elements.keys()
        
    @property
    def elements(self):
        return self._elements.values()
        
    def _set_parent(self, table):
        super(ForeignKeyConstraint, self)._set_parent(table)
        for col, fk in self._elements.iteritems():
            # string-specified column names now get
            # resolved to Column objects
            if isinstance(col, basestring):
                col = table.c[col]
            fk._set_parent(col)
            
        if self.use_alter:
            def supports_alter(ddl, event, schema_item, bind, **kw):
                return table in set(kw['tables']) and bind.dialect.supports_alter
            AddConstraint(self, on=supports_alter).execute_at('after-create', table.metadata)
            DropConstraint(self, on=supports_alter).execute_at('before-drop', table.metadata)
            
    def copy(self, **kw):
        return ForeignKeyConstraint(
                    [x.parent.name for x in self._elements.values()], 
                    [x._get_colspec(**kw) for x in self._elements.values()], 
                    name=self.name, 
                    onupdate=self.onupdate, 
                    ondelete=self.ondelete, 
                    use_alter=self.use_alter,
                    deferrable=self.deferrable,
                    initially=self.initially,
                    link_to_name=self.link_to_name
                )

class PrimaryKeyConstraint(ColumnCollectionConstraint):
    """A table-level PRIMARY KEY constraint.

    Defines a single column or composite PRIMARY KEY constraint. For a
    no-frills primary key, adding ``primary_key=True`` to one or more
    ``Column`` definitions is a shorthand equivalent for an unnamed single- or
    multiple-column PrimaryKeyConstraint.
    """

    __visit_name__ = 'primary_key_constraint'

    def _set_parent(self, table):
        super(PrimaryKeyConstraint, self)._set_parent(table)
        table._set_primary_key(self)

    def _replace(self, col):
        self.columns.replace(col)

class UniqueConstraint(ColumnCollectionConstraint):
    """A table-level UNIQUE constraint.

    Defines a single column or composite UNIQUE constraint. For a no-frills,
    single column constraint, adding ``unique=True`` to the ``Column``
    definition is a shorthand equivalent for an unnamed, single column
    UniqueConstraint.
    """

    __visit_name__ = 'unique_constraint'

class Index(SchemaItem):
    """A table-level INDEX.

    Defines a composite (one or more column) INDEX. For a no-frills, single
    column index, adding ``index=True`` to the ``Column`` definition is
    a shorthand equivalent for an unnamed, single column Index.
    """

    __visit_name__ = 'index'

    def __init__(self, name, *columns, **kwargs):
        """Construct an index object.

        Arguments are:

        name
          The name of the index

        \*columns
          Columns to include in the index. All columns must belong to the same
          table.

        \**kwargs
          Keyword arguments include:

          unique
            Defaults to False: create a unique index.

          postgresql_where
            Defaults to None: create a partial index when using PostgreSQL
        """

        self.name = name
        self.columns = expression.ColumnCollection()
        self.table = None
        self.unique = kwargs.pop('unique', False)
        self.kwargs = kwargs

        for column in columns:
            column = _to_schema_column(column)
            if self.table is None:
                self._set_parent(column.table)
            elif column.table != self.table:
                # all columns muse be from same table
                raise exc.ArgumentError(
                    "All index columns must be from same table. "
                    "%s is from %s not %s" % (column, column.table, self.table))
            self.columns.add(column)

    def _set_parent(self, table):
        self.table = table
        table.indexes.add(self)

    @property
    def bind(self):
        """Return the connectable associated with this Index."""
        
        return self.table.bind

    def create(self, bind=None):
        if bind is None:
            bind = _bind_or_error(self)
        bind.create(self)
        return self

    def drop(self, bind=None):
        if bind is None:
            bind = _bind_or_error(self)
        bind.drop(self)

    def __repr__(self):
        return 'Index("%s", %s%s)' % (self.name,
                                      ', '.join(repr(c) for c in self.columns),
                                      (self.unique and ', unique=True') or '')

class MetaData(SchemaItem):
    """A collection of Tables and their associated schema constructs.

    Holds a collection of Tables and an optional binding to an ``Engine`` or
    ``Connection``.  If bound, the :class:`~sqlalchemy.schema.Table` objects
    in the collection and their columns may participate in implicit SQL
    execution.

    The `Table` objects themselves are stored in the `metadata.tables`
    dictionary.

    The ``bind`` property may be assigned to dynamically.  A common pattern is
    to start unbound and then bind later when an engine is available::

      metadata = MetaData()
      # define tables
      Table('mytable', metadata, ...)
      # connect to an engine later, perhaps after loading a URL from a
      # configuration file
      metadata.bind = an_engine

    MetaData is a thread-safe object after tables have been explicitly defined
    or loaded via reflection.

    .. index::
      single: thread safety; MetaData

    """

    __visit_name__ = 'metadata'

    ddl_events = ('before-create', 'after-create', 'before-drop', 'after-drop')

    def __init__(self, bind=None, reflect=False):
        """Create a new MetaData object.

        bind
          An Engine or Connection to bind to.  May also be a string or URL
          instance, these are passed to create_engine() and this MetaData will
          be bound to the resulting engine.

        reflect
          Optional, automatically load all tables from the bound database.
          Defaults to False. ``bind`` is required when this option is set.
          For finer control over loaded tables, use the ``reflect`` method of
          ``MetaData``.

        """
        self.tables = {}
        self.bind = bind
        self.metadata = self
        self.ddl_listeners = util.defaultdict(list)
        if reflect:
            if not bind:
                raise exc.ArgumentError(
                    "A bind must be supplied in conjunction with reflect=True")
            self.reflect()

    def __repr__(self):
        return 'MetaData(%r)' % self.bind

    def __contains__(self, table_or_key):
        if not isinstance(table_or_key, basestring):
            table_or_key = table_or_key.key
        return table_or_key in self.tables

    def __getstate__(self):
        return {'tables': self.tables}

    def __setstate__(self, state):
        self.tables = state['tables']
        self._bind = None

    def is_bound(self):
        """True if this MetaData is bound to an Engine or Connection."""

        return self._bind is not None

    def bind(self):
        """An Engine or Connection to which this MetaData is bound.

        This property may be assigned an ``Engine`` or ``Connection``, or
        assigned a string or URL to automatically create a basic ``Engine``
        for this bind with ``create_engine()``.
        
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
        """Clear all Table objects from this MetaData."""
        # TODO: why have clear()/remove() but not all
        # other accesors/mutators for the tables dict ?
        self.tables.clear()

    def remove(self, table):
        """Remove the given Table object from this MetaData."""
        
        # TODO: scan all other tables and remove FK _column
        del self.tables[table.key]

    @property
    def sorted_tables(self):
        """Returns a list of ``Table`` objects sorted in order of
        dependency.
        """
        from sqlalchemy.sql.util import sort_tables
        return sort_tables(self.tables.itervalues())
        
    def reflect(self, bind=None, schema=None, only=None):
        """Load all available table definitions from the database.

        Automatically creates ``Table`` entries in this ``MetaData`` for any
        table available in the database but not yet present in the
        ``MetaData``.  May be called multiple times to pick up tables recently
        added to the database, however no special action is taken if a table
        in this ``MetaData`` no longer exists in the database.

        bind
          A :class:`~sqlalchemy.engine.base.Connectable` used to access the database; if None, uses the
          existing bind on this ``MetaData``, if any.

        schema
          Optional, query and reflect tables from an alterate schema.

        only
          Optional.  Load only a sub-set of available named tables.  May be
          specified as a sequence of names or a callable.

          If a sequence of names is provided, only those tables will be
          reflected.  An error is raised if a table is requested but not
          available.  Named tables already present in this ``MetaData`` are
          ignored.

          If a callable is provided, it will be used as a boolean predicate to
          filter the list of potential table names.  The callable is called
          with a table name and this ``MetaData`` instance as positional
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
        current = set(self.tables.iterkeys())

        if only is None:
            load = [name for name in available if name not in current]
        elif util.callable(only):
            load = [name for name in available
                    if name not in current and only(name, self)]
        else:
            missing = [name for name in only if name not in available]
            if missing:
                s = schema and (" schema '%s'" % schema) or ''
                raise exc.InvalidRequestError(
                    'Could not reflect: requested table(s) not available '
                    'in %s%s: (%s)' % (bind.engine.url, s, ', '.join(missing)))
            load = [name for name in only if name not in current]

        for name in load:
            Table(name, self, **reflect_opts)

    def append_ddl_listener(self, event, listener):
        """Append a DDL event listener to this ``MetaData``.

        The ``listener`` callable will be triggered when this ``MetaData`` is
        involved in DDL creates or drops, and will be invoked either before
        all Table-related actions or after.

        Arguments are:

        event
          One of ``MetaData.ddl_events``; 'before-create', 'after-create',
          'before-drop' or 'after-drop'.
        listener
          A callable, invoked with three positional arguments:

          event
            The event currently being handled
          target
            The ``MetaData`` object being operated upon
          bind
            The ``Connection`` bueing used for DDL execution.

        Listeners are added to the MetaData's ``ddl_listeners`` attribute.

        Note: MetaData listeners are invoked even when ``Tables`` are created
        in isolation.  This may change in a future release. I.e.::

          # triggers all MetaData and Table listeners:
          metadata.create_all()

          # triggers MetaData listeners too:
          some.table.create()

        """
        if event not in self.ddl_events:
            raise LookupError(event)
        self.ddl_listeners[event].append(listener)

    def create_all(self, bind=None, tables=None, checkfirst=True):
        """Create all tables stored in this metadata.

        Conditional by default, will not attempt to recreate tables already
        present in the target database.

        bind
          A :class:`~sqlalchemy.engine.base.Connectable` used to access the database; if None, uses the
          existing bind on this ``MetaData``, if any.

        tables
          Optional list of ``Table`` objects, which is a subset of the total
          tables in the ``MetaData`` (others are ignored).

        checkfirst
          Defaults to True, don't issue CREATEs for tables already present
          in the target database.
          
        """
        if bind is None:
            bind = _bind_or_error(self)
        bind.create(self, checkfirst=checkfirst, tables=tables)

    def drop_all(self, bind=None, tables=None, checkfirst=True):
        """Drop all tables stored in this metadata.

        Conditional by default, will not attempt to drop tables not present in
        the target database.

        bind
          A :class:`~sqlalchemy.engine.base.Connectable` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.

        tables
          Optional list of ``Table`` objects, which is a subset of the
          total tables in the ``MetaData`` (others are ignored).

        checkfirst
          Defaults to True, only issue DROPs for tables confirmed to be present
          in the target database.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind.drop(self, checkfirst=checkfirst, tables=tables)

class ThreadLocalMetaData(MetaData):
    """A MetaData variant that presents a different ``bind`` in every thread.

    Makes the ``bind`` property of the MetaData a thread-local value, allowing
    this collection of tables to be bound to different ``Engine``
    implementations or connections in each thread.

    The ThreadLocalMetaData starts off bound to None in each thread.  Binds
    must be made explicitly by assigning to the ``bind`` property or using
    ``connect()``.  You can also re-bind dynamically multiple times per
    thread, just like a regular ``MetaData``.

    """

    __visit_name__ = 'metadata'

    def __init__(self):
        """Construct a ThreadLocalMetaData."""

        self.context = util.threading.local()
        self.__engines = {}
        super(ThreadLocalMetaData, self).__init__()

    def bind(self):
        """The bound Engine or Connection for this thread.

        This property may be assigned an Engine or Connection, or assigned a
        string or URL to automatically create a basic Engine for this bind
        with ``create_engine()``."""

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
                from sqlalchemy import create_engine
                e = create_engine(bind)
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
        """Dispose all bound engines, in all thread contexts."""

        for e in self.__engines.itervalues():
            if hasattr(e, 'dispose'):
                e.dispose()

class SchemaVisitor(visitors.ClauseVisitor):
    """Define the visiting for ``SchemaItem`` objects."""

    __traverse_options__ = {'schema_visitor':True}


class DDLElement(expression.Executable, expression.ClauseElement):
    """Base class for DDL expression constructs."""
    
    _execution_options = expression.Executable.\
                            _execution_options.union({'autocommit':True})

    target = None
    on = None
    
    def execute(self, bind=None, target=None):
        """Execute this DDL immediately.

        Executes the DDL statement in isolation using the supplied
        :class:`~sqlalchemy.engine.base.Connectable` or :class:`~sqlalchemy.engine.base.Connectable` assigned to the ``.bind`` property,
        if not supplied.  If the DDL has a conditional ``on`` criteria, it
        will be invoked with None as the event.

        bind
          Optional, an ``Engine`` or ``Connection``.  If not supplied, a
          valid :class:`~sqlalchemy.engine.base.Connectable` must be present in the ``.bind`` property.

        target
          Optional, defaults to None.  The target SchemaItem for the 
          execute call.  Will be passed to the ``on`` callable if any, 
          and may also provide string expansion data for the
          statement. See ``execute_at`` for more information.
        """

        if bind is None:
            bind = _bind_or_error(self)

        if self._should_execute(None, target, bind):
            return bind.execute(self.against(target))
        else:
            bind.engine.logger.info("DDL execution skipped, criteria not met.")

    def execute_at(self, event, target):
        """Link execution of this DDL to the DDL lifecycle of a SchemaItem.

        Links this ``DDLElement`` to a ``Table`` or ``MetaData`` instance, executing
        it when that schema item is created or dropped.  The DDL statement
        will be executed using the same Connection and transactional context
        as the Table create/drop itself.  The ``.bind`` property of this
        statement is ignored.
        
        event
          One of the events defined in the schema item's ``.ddl_events``;
          e.g. 'before-create', 'after-create', 'before-drop' or 'after-drop'

        target
          The Table or MetaData instance for which this DDLElement will
          be associated with.

        A DDLElement instance can be linked to any number of schema items. 

        ``execute_at`` builds on the ``append_ddl_listener`` interface of
        MetaDta and Table objects.

        Caveat: Creating or dropping a Table in isolation will also trigger
        any DDL set to ``execute_at`` that Table's MetaData.  This may change
        in a future release.
        """

        if not hasattr(target, 'ddl_listeners'):
            raise exc.ArgumentError(
                "%s does not support DDL events" % type(target).__name__)
        if event not in target.ddl_events:
            raise exc.ArgumentError(
                "Unknown event, expected one of (%s), got '%r'" %
                (', '.join(target.ddl_events), event))
        target.ddl_listeners[event].append(self)
        return self

    @expression._generative
    def against(self, target):
        """Return a copy of this DDL against a specific schema item."""

        self.target = target

    def __call__(self, event, target, bind, **kw):
        """Execute the DDL as a ddl_listener."""

        if self._should_execute(event, target, bind, **kw):
            return bind.execute(self.against(target))

    def _check_ddl_on(self, on):
        if (on is not None and
            (not isinstance(on, (basestring, tuple, list, set)) and not util.callable(on))):
            raise exc.ArgumentError(
                "Expected the name of a database dialect, a tuple of names, or a callable for "
                "'on' criteria, got type '%s'." % type(on).__name__)

    def _should_execute(self, event, target, bind, **kw):
        if self.on is None:
            return True
        elif isinstance(self.on, basestring):
            return self.on == bind.engine.name
        elif isinstance(self.on, (tuple, list, set)):
            return bind.engine.name in self.on
        else:
            return self.on(self, event, target, bind, **kw)

    def bind(self):
        if self._bind:
            return self._bind
    def _set_bind(self, bind):
        self._bind = bind
    bind = property(bind, _set_bind)

    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s
    
    def _compiler(self, dialect, **kw):
        """Return a compiler appropriate for this ClauseElement, given a Dialect."""
        
        return dialect.ddl_compiler(dialect, self, **kw)

class DDL(DDLElement):
    """A literal DDL statement.

    Specifies literal SQL DDL to be executed by the database.  DDL objects can
    be attached to ``Tables`` or ``MetaData`` instances, conditionally
    executing SQL as part of the DDL lifecycle of those schema items.  Basic
    templating support allows a single DDL instance to handle repetitive tasks
    for multiple tables.

    Examples::

      tbl = Table('users', metadata, Column('uid', Integer)) # ...
      DDL('DROP TRIGGER users_trigger').execute_at('before-create', tbl)

      spow = DDL('ALTER TABLE %(table)s SET secretpowers TRUE', on='somedb')
      spow.execute_at('after-create', tbl)

      drop_spow = DDL('ALTER TABLE users SET secretpowers FALSE')
      connection.execute(drop_spow)

    When operating on Table events, the following ``statement``
    string substitions are available::

      %(table)s  - the Table name, with any required quoting applied
      %(schema)s - the schema name, with any required quoting applied
      %(fullname)s - the Table name including schema, quoted if needed

    The DDL's ``context``, if any, will be combined with the standard
    substutions noted above.  Keys present in the context will override
    the standard substitutions.

    """

    __visit_name__ = "ddl"
    
    def __init__(self, statement, on=None, context=None, bind=None):
        """Create a DDL statement.

        statement
          A string or unicode string to be executed.  Statements will be
          processed with Python's string formatting operator.  See the
          ``context`` argument and the ``execute_at`` method.

          A literal '%' in a statement must be escaped as '%%'.

          SQL bind parameters are not available in DDL statements.

        on
          Optional filtering criteria.  May be a string, tuple or a callable
          predicate.  If a string, it will be compared to the name of the
          executing database dialect::

            DDL('something', on='postgresql')

          If a tuple, specifies multiple dialect names::

            DDL('something', on=('postgresql', 'mysql'))

          If a callable, it will be invoked with four positional arguments
          as well as optional keyword arguments:
            
            ddl
              This DDL element.
              
            event
              The name of the event that has triggered this DDL, such as
              'after-create' Will be None if the DDL is executed explicitly.

            target
              The ``Table`` or ``MetaData`` object which is the target of 
              this event. May be None if the DDL is executed explicitly.

            connection
              The ``Connection`` being used for DDL execution

            \**kw
              Keyword arguments which may be sent include:
                tables - a list of Table objects which are to be created/
                dropped within a MetaData.create_all() or drop_all() method
                call.
              
          If the callable returns a true value, the DDL statement will be
          executed.

        context
          Optional dictionary, defaults to None.  These values will be
          available for use in string substitutions on the DDL statement.

        bind
          Optional. A :class:`~sqlalchemy.engine.base.Connectable`, used by default when ``execute()``
          is invoked without a bind argument.
          
        """

        if not isinstance(statement, basestring):
            raise exc.ArgumentError(
                "Expected a string or unicode SQL statement, got '%r'" %
                statement)

        self.statement = statement
        self.context = context or {}

        self._check_ddl_on(on)
        self.on = on
        self._bind = bind


    def __repr__(self):
        return '<%s@%s; %s>' % (
            type(self).__name__, id(self),
            ', '.join([repr(self.statement)] +
                      ['%s=%r' % (key, getattr(self, key))
                       for key in ('on', 'context')
                       if getattr(self, key)]))

def _to_schema_column(element):
   if hasattr(element, '__clause_element__'):
       element = element.__clause_element__()
   if not isinstance(element, Column):
       raise exc.ArgumentError("schema.Column object expected")
   return element

def _to_schema_column_or_string(element):
  if hasattr(element, '__clause_element__'):
      element = element.__clause_element__()
  return element

class _CreateDropBase(DDLElement):
    """Base class for DDL constucts that represent CREATE and DROP or equivalents.

    The common theme of _CreateDropBase is a single
    ``element`` attribute which refers to the element
    to be created or dropped.
    
    """
    
    def __init__(self, element, on=None, bind=None):
        self.element = element
        self._check_ddl_on(on)
        self.on = on
        self.bind = bind

    def _create_rule_disable(self, compiler):
        """Allow disable of _create_rule using a callable.
        
        Pass to _create_rule using 
        util.portable_instancemethod(self._create_rule_disable)
        to retain serializability.
        
        """
        return False

class CreateTable(_CreateDropBase):
    """Represent a CREATE TABLE statement."""
    
    __visit_name__ = "create_table"
    
class DropTable(_CreateDropBase):
    """Represent a DROP TABLE statement."""

    __visit_name__ = "drop_table"

class CreateSequence(_CreateDropBase):
    """Represent a CREATE SEQUENCE statement."""
    
    __visit_name__ = "create_sequence"

class DropSequence(_CreateDropBase):
    """Represent a DROP SEQUENCE statement."""

    __visit_name__ = "drop_sequence"
    
class CreateIndex(_CreateDropBase):
    """Represent a CREATE INDEX statement."""
    
    __visit_name__ = "create_index"

class DropIndex(_CreateDropBase):
    """Represent a DROP INDEX statement."""

    __visit_name__ = "drop_index"

class AddConstraint(_CreateDropBase):
    """Represent an ALTER TABLE ADD CONSTRAINT statement."""
    
    __visit_name__ = "add_constraint"

    def __init__(self, element, *args, **kw):
        super(AddConstraint, self).__init__(element, *args, **kw)
        element._create_rule = util.portable_instancemethod(self._create_rule_disable)
        
class DropConstraint(_CreateDropBase):
    """Represent an ALTER TABLE DROP CONSTRAINT statement."""

    __visit_name__ = "drop_constraint"
    
    def __init__(self, element, cascade=False, **kw):
        self.cascade = cascade
        super(DropConstraint, self).__init__(element, **kw)
        element._create_rule = util.portable_instancemethod(self._create_rule_disable)

def _bind_or_error(schemaitem, msg=None):
    bind = schemaitem.bind
    if not bind:
        name = schemaitem.__class__.__name__
        label = getattr(schemaitem, 'fullname',
                        getattr(schemaitem, 'name', None))
        if label:
            item = '%s %r' % (name, label)
        else:
            item = name
        if isinstance(schemaitem, (MetaData, DDL)):
            bindable = "the %s's .bind" % name
        else:
            bindable = "this %s's .metadata.bind" % name
        
        if msg is None:
            msg = ('The %s is not bound to an Engine or Connection.  '
                   'Execution can not proceed without a database to execute '
                   'against.  Either execute with an explicit connection or '
                   'assign %s to enable implicit execution.') % (item, bindable)
        raise exc.UnboundExecutionError(msg)
    return bind

