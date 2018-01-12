# sql/schema.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The schema module provides the building blocks for database metadata.

Each element within this module describes a database entity which can be
created and dropped, or is otherwise part of such an entity.  Examples include
tables, columns, sequences, and indexes.

All entities are subclasses of :class:`~sqlalchemy.schema.SchemaItem`, and as
defined in this module they are intended to be agnostic of any vendor-specific
constructs.

A collection of entities are grouped into a unit called
:class:`~sqlalchemy.schema.MetaData`. MetaData serves as a logical grouping of
schema elements, and can also be associated with an actual database connection
such that operations involving the contained elements can contact the database
as needed.

Two of the elements here also build upon their "syntactic" counterparts, which
are defined in :class:`~sqlalchemy.sql.expression.`, specifically
:class:`~sqlalchemy.schema.Table` and :class:`~sqlalchemy.schema.Column`.
Since these objects are part of the SQL expression language, they are usable
as components in SQL expressions.

"""
from __future__ import absolute_import

from .. import exc, util, event, inspection
from .base import SchemaEventTarget, DialectKWArgs
import operator
from . import visitors
from . import type_api
from .base import _bind_or_error, ColumnCollection
from .elements import ClauseElement, ColumnClause, \
    _as_truncated, TextClause, _literal_as_text,\
    ColumnElement, quoted_name
from .selectable import TableClause
import collections
import sqlalchemy
from . import ddl

RETAIN_SCHEMA = util.symbol('retain_schema')

BLANK_SCHEMA = util.symbol(
    'blank_schema',
    """Symbol indicating that a :class:`.Table` or :class:`.Sequence`
    should have 'None' for its schema, even if the parent
    :class:`.MetaData` has specified a schema.

    .. versionadded:: 1.0.14

    """
)


def _get_table_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name


@inspection._self_inspects
class SchemaItem(SchemaEventTarget, visitors.Visitable):
    """Base class for items that define a database schema."""

    __visit_name__ = 'schema_item'

    def _init_items(self, *args):
        """Initialize the list of child items for this SchemaItem."""

        for item in args:
            if item is not None:
                item._set_parent_with_dispatch(self)

    def get_children(self, **kwargs):
        """used to allow SchemaVisitor access"""
        return []

    def __repr__(self):
        return util.generic_repr(self, omit_kwarg=['info'])

    @property
    @util.deprecated('0.9', 'Use ``<obj>.name.quote``')
    def quote(self):
        """Return the value of the ``quote`` flag passed
        to this schema object, for those schema items which
        have a ``name`` field.

        """

        return self.name.quote

    @util.memoized_property
    def info(self):
        """Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.SchemaItem`.

        The dictionary is automatically generated when first accessed.
        It can also be specified in the constructor of some objects,
        such as :class:`.Table` and :class:`.Column`.

        """
        return {}

    def _schema_item_copy(self, schema_item):
        if 'info' in self.__dict__:
            schema_item.info = self.info.copy()
        schema_item.dispatch._update(self.dispatch)
        return schema_item

    def _translate_schema(self, effective_schema, map_):
        return map_.get(effective_schema, effective_schema)


class Table(DialectKWArgs, SchemaItem, TableClause):
    r"""Represent a table in a database.

    e.g.::

        mytable = Table("mytable", metadata,
                        Column('mytable_id', Integer, primary_key=True),
                        Column('value', String(50))
                   )

    The :class:`.Table` object constructs a unique instance of itself based
    on its name and optional schema name within the given
    :class:`.MetaData` object. Calling the :class:`.Table`
    constructor with the same name and same :class:`.MetaData` argument
    a second time will return the *same* :class:`.Table` object - in this way
    the :class:`.Table` constructor acts as a registry function.

    .. seealso::

        :ref:`metadata_describing` - Introduction to database metadata

    Constructor arguments are as follows:

    :param name: The name of this table as represented in the database.

        The table name, along with the value of the ``schema`` parameter,
        forms a key which uniquely identifies this :class:`.Table` within
        the owning :class:`.MetaData` collection.
        Additional calls to :class:`.Table` with the same name, metadata,
        and schema name will return the same :class:`.Table` object.

        Names which contain no upper case characters
        will be treated as case insensitive names, and will not be quoted
        unless they are a reserved word or contain special characters.
        A name with any number of upper case characters is considered
        to be case sensitive, and will be sent as quoted.

        To enable unconditional quoting for the table name, specify the flag
        ``quote=True`` to the constructor, or use the :class:`.quoted_name`
        construct to specify the name.

    :param metadata: a :class:`.MetaData` object which will contain this
        table.  The metadata is used as a point of association of this table
        with other tables which are referenced via foreign key.  It also
        may be used to associate this table with a particular
        :class:`.Connectable`.

    :param \*args: Additional positional arguments are used primarily
        to add the list of :class:`.Column` objects contained within this
        table. Similar to the style of a CREATE TABLE statement, other
        :class:`.SchemaItem` constructs may be added here, including
        :class:`.PrimaryKeyConstraint`, and :class:`.ForeignKeyConstraint`.

    :param autoload: Defaults to False, unless :paramref:`.Table.autoload_with`
        is set in which case it defaults to True; :class:`.Column` objects
        for this table should be reflected from the database, possibly
        augmenting or replacing existing :class:`.Column` objects that were
        explicitly specified.

        .. versionchanged:: 1.0.0 setting the :paramref:`.Table.autoload_with`
           parameter implies that :paramref:`.Table.autoload` will default
           to True.

        .. seealso::

            :ref:`metadata_reflection_toplevel`

    :param autoload_replace: Defaults to ``True``; when using
        :paramref:`.Table.autoload`
        in conjunction with :paramref:`.Table.extend_existing`, indicates
        that :class:`.Column` objects present in the already-existing
        :class:`.Table` object should be replaced with columns of the same
        name retrieved from the autoload process.   When ``False``, columns
        already present under existing names will be omitted from the
        reflection process.

        Note that this setting does not impact :class:`.Column` objects
        specified programmatically within the call to :class:`.Table` that
        also is autoloading; those :class:`.Column` objects will always
        replace existing columns of the same name when
        :paramref:`.Table.extend_existing` is ``True``.

        .. versionadded:: 0.7.5

        .. seealso::

            :paramref:`.Table.autoload`

            :paramref:`.Table.extend_existing`

    :param autoload_with: An :class:`.Engine` or :class:`.Connection` object
        with which this :class:`.Table` object will be reflected; when
        set to a non-None value, it implies that :paramref:`.Table.autoload`
        is ``True``.   If left unset, but :paramref:`.Table.autoload` is
        explicitly set to ``True``, an autoload operation will attempt to
        proceed by locating an :class:`.Engine` or :class:`.Connection` bound
        to the underlying :class:`.MetaData` object.

        .. seealso::

            :paramref:`.Table.autoload`

    :param extend_existing: When ``True``, indicates that if this
        :class:`.Table` is already present in the given :class:`.MetaData`,
        apply further arguments within the constructor to the existing
        :class:`.Table`.

        If :paramref:`.Table.extend_existing` or
        :paramref:`.Table.keep_existing` are not set, and the given name
        of the new :class:`.Table` refers to a :class:`.Table` that is
        already present in the target :class:`.MetaData` collection, and
        this :class:`.Table` specifies additional columns or other constructs
        or flags that modify the table's state, an
        error is raised.  The purpose of these two mutually-exclusive flags
        is to specify what action should be taken when a :class:`.Table`
        is specified that matches an existing :class:`.Table`, yet specifies
        additional constructs.

        :paramref:`.Table.extend_existing` will also work in conjunction
        with :paramref:`.Table.autoload` to run a new reflection
        operation against the database, even if a :class:`.Table`
        of the same name is already present in the target
        :class:`.MetaData`; newly reflected :class:`.Column` objects
        and other options will be added into the state of the
        :class:`.Table`, potentially overwriting existing columns
        and options of the same name.

        .. versionchanged:: 0.7.4 :paramref:`.Table.extend_existing` will
           invoke a new reflection operation when combined with
           :paramref:`.Table.autoload` set to True.

        As is always the case with :paramref:`.Table.autoload`,
        :class:`.Column` objects can be specified in the same :class:`.Table`
        constructor, which will take precedence.  Below, the existing
        table ``mytable`` will be augmented with :class:`.Column` objects
        both reflected from the database, as well as the given :class:`.Column`
        named "y"::

            Table("mytable", metadata,
                        Column('y', Integer),
                        extend_existing=True,
                        autoload=True,
                        autoload_with=engine
                    )

        .. seealso::

            :paramref:`.Table.autoload`

            :paramref:`.Table.autoload_replace`

            :paramref:`.Table.keep_existing`


    :param implicit_returning: True by default - indicates that
        RETURNING can be used by default to fetch newly inserted primary key
        values, for backends which support this.  Note that
        create_engine() also provides an implicit_returning flag.

    :param include_columns: A list of strings indicating a subset of
        columns to be loaded via the ``autoload`` operation; table columns who
        aren't present in this list will not be represented on the resulting
        ``Table`` object. Defaults to ``None`` which indicates all columns
        should be reflected.

    :param info: Optional data dictionary which will be populated into the
        :attr:`.SchemaItem.info` attribute of this object.

    :param keep_existing: When ``True``, indicates that if this Table
        is already present in the given :class:`.MetaData`, ignore
        further arguments within the constructor to the existing
        :class:`.Table`, and return the :class:`.Table` object as
        originally created. This is to allow a function that wishes
        to define a new :class:`.Table` on first call, but on
        subsequent calls will return the same :class:`.Table`,
        without any of the declarations (particularly constraints)
        being applied a second time.

        If :paramref:`.Table.extend_existing` or
        :paramref:`.Table.keep_existing` are not set, and the given name
        of the new :class:`.Table` refers to a :class:`.Table` that is
        already present in the target :class:`.MetaData` collection, and
        this :class:`.Table` specifies additional columns or other constructs
        or flags that modify the table's state, an
        error is raised.  The purpose of these two mutually-exclusive flags
        is to specify what action should be taken when a :class:`.Table`
        is specified that matches an existing :class:`.Table`, yet specifies
        additional constructs.

        .. seealso::

            :paramref:`.Table.extend_existing`

    :param listeners: A list of tuples of the form ``(<eventname>, <fn>)``
        which will be passed to :func:`.event.listen` upon construction.
        This alternate hook to :func:`.event.listen` allows the establishment
        of a listener function specific to this :class:`.Table` before
        the "autoload" process begins.  Particularly useful for
        the :meth:`.DDLEvents.column_reflect` event::

            def listen_for_reflect(table, column_info):
                "handle the column reflection event"
                # ...

            t = Table(
                'sometable',
                autoload=True,
                listeners=[
                    ('column_reflect', listen_for_reflect)
                ])

    :param mustexist: When ``True``, indicates that this Table must already
        be present in the given :class:`.MetaData` collection, else
        an exception is raised.

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

    :param schema: The schema name for this table, which is required if
        the table resides in a schema other than the default selected schema
        for the engine's database connection.  Defaults to ``None``.

        If the owning :class:`.MetaData` of this :class:`.Table` specifies
        its own :paramref:`.MetaData.schema` parameter, then that schema
        name will be applied to this :class:`.Table` if the schema parameter
        here is set to ``None``.  To set a blank schema name on a :class:`.Table`
        that would otherwise use the schema set on the owning :class:`.MetaData`,
        specify the special symbol :attr:`.BLANK_SCHEMA`.

        .. versionadded:: 1.0.14  Added the :attr:`.BLANK_SCHEMA` symbol to
           allow a :class:`.Table` to have a blank schema name even when the
           parent :class:`.MetaData` specifies :paramref:`.MetaData.schema`.

        The quoting rules for the schema name are the same as those for the
        ``name`` parameter, in that quoting is applied for reserved words or
        case-sensitive names; to enable unconditional quoting for the
        schema name, specify the flag
        ``quote_schema=True`` to the constructor, or use the
        :class:`.quoted_name` construct to specify the name.

    :param useexisting: Deprecated.  Use :paramref:`.Table.extend_existing`.

    :param \**kw: Additional keyword arguments not mentioned above are
        dialect specific, and passed in the form ``<dialectname>_<argname>``.
        See the documentation regarding an individual dialect at
        :ref:`dialect_toplevel` for detail on documented arguments.

    """

    __visit_name__ = 'table'

    def __new__(cls, *args, **kw):
        if not args:
            # python3k pickle seems to call this
            return object.__new__(cls)

        try:
            name, metadata, args = args[0], args[1], args[2:]
        except IndexError:
            raise TypeError("Table() takes at least two arguments")

        schema = kw.get('schema', None)
        if schema is None:
            schema = metadata.schema
        elif schema is BLANK_SCHEMA:
            schema = None
        keep_existing = kw.pop('keep_existing', False)
        extend_existing = kw.pop('extend_existing', False)
        if 'useexisting' in kw:
            msg = "useexisting is deprecated.  Use extend_existing."
            util.warn_deprecated(msg)
            if extend_existing:
                msg = "useexisting is synonymous with extend_existing."
                raise exc.ArgumentError(msg)
            extend_existing = kw.pop('useexisting', False)

        if keep_existing and extend_existing:
            msg = "keep_existing and extend_existing are mutually exclusive."
            raise exc.ArgumentError(msg)

        mustexist = kw.pop('mustexist', False)
        key = _get_table_key(name, schema)
        if key in metadata.tables:
            if not keep_existing and not extend_existing and bool(args):
                raise exc.InvalidRequestError(
                    "Table '%s' is already defined for this MetaData "
                    "instance.  Specify 'extend_existing=True' "
                    "to redefine "
                    "options and columns on an "
                    "existing Table object." % key)
            table = metadata.tables[key]
            if extend_existing:
                table._init_existing(*args, **kw)
            return table
        else:
            if mustexist:
                raise exc.InvalidRequestError(
                    "Table '%s' not defined" % (key))
            table = object.__new__(cls)
            table.dispatch.before_parent_attach(table, metadata)
            metadata._add_table(name, schema, table)
            try:
                table._init(name, metadata, *args, **kw)
                table.dispatch.after_parent_attach(table, metadata)
                return table
            except:
                with util.safe_reraise():
                    metadata._remove_table(name, schema)

    @property
    @util.deprecated('0.9', 'Use ``table.schema.quote``')
    def quote_schema(self):
        """Return the value of the ``quote_schema`` flag passed
        to this :class:`.Table`.
        """

        return self.schema.quote

    def __init__(self, *args, **kw):
        """Constructor for :class:`~.schema.Table`.

        This method is a no-op.   See the top-level
        documentation for :class:`~.schema.Table`
        for constructor arguments.

        """
        # __init__ is overridden to prevent __new__ from
        # calling the superclass constructor.

    def _init(self, name, metadata, *args, **kwargs):
        super(Table, self).__init__(
            quoted_name(name, kwargs.pop('quote', None)))
        self.metadata = metadata

        self.schema = kwargs.pop('schema', None)
        if self.schema is None:
            self.schema = metadata.schema
        elif self.schema is BLANK_SCHEMA:
            self.schema = None
        else:
            quote_schema = kwargs.pop('quote_schema', None)
            self.schema = quoted_name(self.schema, quote_schema)

        self.indexes = set()
        self.constraints = set()
        self._columns = ColumnCollection()
        PrimaryKeyConstraint(_implicit_generated=True).\
            _set_parent_with_dispatch(self)
        self.foreign_keys = set()
        self._extra_dependencies = set()
        if self.schema is not None:
            self.fullname = "%s.%s" % (self.schema, self.name)
        else:
            self.fullname = self.name

        autoload_with = kwargs.pop('autoload_with', None)
        autoload = kwargs.pop('autoload', autoload_with is not None)
        # this argument is only used with _init_existing()
        kwargs.pop('autoload_replace', True)
        _extend_on = kwargs.pop("_extend_on", None)

        include_columns = kwargs.pop('include_columns', None)

        self.implicit_returning = kwargs.pop('implicit_returning', True)

        if 'info' in kwargs:
            self.info = kwargs.pop('info')
        if 'listeners' in kwargs:
            listeners = kwargs.pop('listeners')
            for evt, fn in listeners:
                event.listen(self, evt, fn)

        self._prefixes = kwargs.pop('prefixes', [])

        self._extra_kwargs(**kwargs)

        # load column definitions from the database if 'autoload' is defined
        # we do it after the table is in the singleton dictionary to support
        # circular foreign keys
        if autoload:
            self._autoload(
                metadata, autoload_with,
                include_columns, _extend_on=_extend_on)

        # initialize all the column, etc. objects.  done after reflection to
        # allow user-overrides
        self._init_items(*args)

    def _autoload(self, metadata, autoload_with, include_columns,
                  exclude_columns=(), _extend_on=None):

        if autoload_with:
            autoload_with.run_callable(
                autoload_with.dialect.reflecttable,
                self, include_columns, exclude_columns,
                _extend_on=_extend_on
            )
        else:
            bind = _bind_or_error(
                metadata,
                msg="No engine is bound to this Table's MetaData. "
                "Pass an engine to the Table via "
                "autoload_with=<someengine>, "
                "or associate the MetaData with an engine via "
                "metadata.bind=<someengine>")
            bind.run_callable(
                bind.dialect.reflecttable,
                self, include_columns, exclude_columns,
                _extend_on=_extend_on
            )

    @property
    def _sorted_constraints(self):
        """Return the set of constraints as a list, sorted by creation
        order.

        """
        return sorted(self.constraints, key=lambda c: c._creation_order)

    @property
    def foreign_key_constraints(self):
        """:class:`.ForeignKeyConstraint` objects referred to by this
        :class:`.Table`.

        This list is produced from the collection of :class:`.ForeignKey`
        objects currently associated.

        .. versionadded:: 1.0.0

        """
        return set(fkc.constraint for fkc in self.foreign_keys)

    def _init_existing(self, *args, **kwargs):
        autoload_with = kwargs.pop('autoload_with', None)
        autoload = kwargs.pop('autoload', autoload_with is not None)
        autoload_replace = kwargs.pop('autoload_replace', True)
        schema = kwargs.pop('schema', None)
        _extend_on = kwargs.pop('_extend_on', None)

        if schema and schema != self.schema:
            raise exc.ArgumentError(
                "Can't change schema of existing table from '%s' to '%s'",
                (self.schema, schema))

        include_columns = kwargs.pop('include_columns', None)

        if include_columns is not None:
            for c in self.c:
                if c.name not in include_columns:
                    self._columns.remove(c)

        for key in ('quote', 'quote_schema'):
            if key in kwargs:
                raise exc.ArgumentError(
                    "Can't redefine 'quote' or 'quote_schema' arguments")

        if 'info' in kwargs:
            self.info = kwargs.pop('info')

        if autoload:
            if not autoload_replace:
                # don't replace columns already present.
                # we'd like to do this for constraints also however we don't
                # have simple de-duping for unnamed constraints.
                exclude_columns = [c.name for c in self.c]
            else:
                exclude_columns = ()
            self._autoload(
                self.metadata, autoload_with,
                include_columns, exclude_columns, _extend_on=_extend_on)

        self._extra_kwargs(**kwargs)
        self._init_items(*args)

    def _extra_kwargs(self, **kwargs):
        self._validate_dialect_kwargs(kwargs)

    def _init_collections(self):
        pass

    def _reset_exported(self):
        pass

    @property
    def _autoincrement_column(self):
        return self.primary_key._autoincrement_column

    @property
    def key(self):
        """Return the 'key' for this :class:`.Table`.

        This value is used as the dictionary key within the
        :attr:`.MetaData.tables` collection.   It is typically the same
        as that of :attr:`.Table.name` for a table with no
        :attr:`.Table.schema` set; otherwise it is typically of the form
        ``schemaname.tablename``.

        """
        return _get_table_key(self.name, self.schema)

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
        """Append a :class:`~.schema.Column` to this :class:`~.schema.Table`.

        The "key" of the newly added :class:`~.schema.Column`, i.e. the
        value of its ``.key`` attribute, will then be available
        in the ``.c`` collection of this :class:`~.schema.Table`, and the
        column definition will be included in any CREATE TABLE, SELECT,
        UPDATE, etc. statements generated from this :class:`~.schema.Table`
        construct.

        Note that this does **not** change the definition of the table
        as it exists within any underlying database, assuming that
        table has already been created in the database.   Relational
        databases support the addition of columns to existing tables
        using the SQL ALTER command, which would need to be
        emitted for an already-existing table that doesn't contain
        the newly added column.

        """

        column._set_parent_with_dispatch(self)

    def append_constraint(self, constraint):
        """Append a :class:`~.schema.Constraint` to this
        :class:`~.schema.Table`.

        This has the effect of the constraint being included in any
        future CREATE TABLE statement, assuming specific DDL creation
        events have not been associated with the given
        :class:`~.schema.Constraint` object.

        Note that this does **not** produce the constraint within the
        relational database automatically, for a table that already exists
        in the database.   To add a constraint to an
        existing relational database table, the SQL ALTER command must
        be used.  SQLAlchemy also provides the
        :class:`.AddConstraint` construct which can produce this SQL when
        invoked as an executable clause.

        """

        constraint._set_parent_with_dispatch(self)

    def append_ddl_listener(self, event_name, listener):
        """Append a DDL event listener to this ``Table``.

        .. deprecated:: 0.7
            See :class:`.DDLEvents`.

        """

        def adapt_listener(target, connection, **kw):
            listener(event_name, target, connection)

        event.listen(self, "" + event_name.replace('-', '_'), adapt_listener)

    def _set_parent(self, metadata):
        metadata._add_table(self.name, self.schema, self)
        self.metadata = metadata

    def get_children(self, column_collections=True,
                     schema_visitor=False, **kw):
        if not schema_visitor:
            return TableClause.get_children(
                self, column_collections=column_collections, **kw)
        else:
            if column_collections:
                return list(self.columns)
            else:
                return []

    def exists(self, bind=None):
        """Return True if this table exists."""

        if bind is None:
            bind = _bind_or_error(self)

        return bind.run_callable(bind.dialect.has_table,
                                 self.name, schema=self.schema)

    def create(self, bind=None, checkfirst=False):
        """Issue a ``CREATE`` statement for this
        :class:`.Table`, using the given :class:`.Connectable`
        for connectivity.

        .. seealso::

            :meth:`.MetaData.create_all`.

        """

        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaGenerator,
                          self,
                          checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=False):
        """Issue a ``DROP`` statement for this
        :class:`.Table`, using the given :class:`.Connectable`
        for connectivity.

        .. seealso::

            :meth:`.MetaData.drop_all`.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper,
                          self,
                          checkfirst=checkfirst)

    def tometadata(self, metadata, schema=RETAIN_SCHEMA,
                   referred_schema_fn=None, name=None):
        """Return a copy of this :class:`.Table` associated with a different
        :class:`.MetaData`.

        E.g.::

            m1 = MetaData()

            user = Table('user', m1, Column('id', Integer, priamry_key=True))

            m2 = MetaData()
            user_copy = user.tometadata(m2)

        :param metadata: Target :class:`.MetaData` object, into which the
         new :class:`.Table` object will be created.

        :param schema: optional string name indicating the target schema.
         Defaults to the special symbol :attr:`.RETAIN_SCHEMA` which indicates
         that no change to the schema name should be made in the new
         :class:`.Table`.  If set to a string name, the new :class:`.Table`
         will have this new name as the ``.schema``.  If set to ``None``, the
         schema will be set to that of the schema set on the target
         :class:`.MetaData`, which is typically ``None`` as well, unless
         set explicitly::

            m2 = MetaData(schema='newschema')

            # user_copy_one will have "newschema" as the schema name
            user_copy_one = user.tometadata(m2, schema=None)

            m3 = MetaData()  # schema defaults to None

            # user_copy_two will have None as the schema name
            user_copy_two = user.tometadata(m3, schema=None)

        :param referred_schema_fn: optional callable which can be supplied
         in order to provide for the schema name that should be assigned
         to the referenced table of a :class:`.ForeignKeyConstraint`.
         The callable accepts this parent :class:`.Table`, the
         target schema that we are changing to, the
         :class:`.ForeignKeyConstraint` object, and the existing
         "target schema" of that constraint.  The function should return the
         string schema name that should be applied.
         E.g.::

                def referred_schema_fn(table, to_schema,
                                                constraint, referred_schema):
                    if referred_schema == 'base_tables':
                        return referred_schema
                    else:
                        return to_schema

                new_table = table.tometadata(m2, schema="alt_schema",
                                        referred_schema_fn=referred_schema_fn)

         .. versionadded:: 0.9.2

        :param name: optional string name indicating the target table name.
         If not specified or None, the table name is retained.  This allows
         a :class:`.Table` to be copied to the same :class:`.MetaData` target
         with a new name.

         .. versionadded:: 1.0.0

        """
        if name is None:
            name = self.name
        if schema is RETAIN_SCHEMA:
            schema = self.schema
        elif schema is None:
            schema = metadata.schema
        key = _get_table_key(name, schema)
        if key in metadata.tables:
            util.warn("Table '%s' already exists within the given "
                      "MetaData - not copying." % self.description)
            return metadata.tables[key]

        args = []
        for c in self.columns:
            args.append(c.copy(schema=schema))
        table = Table(
            name, metadata, schema=schema,
            *args, **self.kwargs
        )
        for c in self.constraints:
            if isinstance(c, ForeignKeyConstraint):
                referred_schema = c._referred_schema
                if referred_schema_fn:
                    fk_constraint_schema = referred_schema_fn(
                        self, schema, c, referred_schema)
                else:
                    fk_constraint_schema = (
                        schema if referred_schema == self.schema else None)
                table.append_constraint(
                    c.copy(schema=fk_constraint_schema, target_table=table))
            elif not c._type_bound:
                # skip unique constraints that would be generated
                # by the 'unique' flag on Column
                if isinstance(c, UniqueConstraint) and \
                    len(c.columns) == 1 and \
                        list(c.columns)[0].unique:
                    continue

                table.append_constraint(
                    c.copy(schema=schema, target_table=table))
        for index in self.indexes:
            # skip indexes that would be generated
            # by the 'index' flag on Column
            if len(index.columns) == 1 and \
                    list(index.columns)[0].index:
                continue
            Index(index.name,
                  unique=index.unique,
                  *[table.c[col] for col in index.columns.keys()],
                  **index.kwargs)
        return self._schema_item_copy(table)


class Column(SchemaItem, ColumnClause):
    """Represents a column in a database table."""

    __visit_name__ = 'column'

    def __init__(self, *args, **kwargs):
        r"""
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
          :class:`.Table`.  This is to support convenient
          usage within the :mod:`~sqlalchemy.ext.declarative` extension.

        :param type\_: The column's type, indicated using an instance which
          subclasses :class:`~sqlalchemy.types.TypeEngine`.  If no arguments
          are required for the type, the class of the type can be sent
          as well, e.g.::

            # use a type with arguments
            Column('data', String(50))

            # use no arguments
            Column('level', Integer)

          The ``type`` argument may be the second positional argument
          or specified by keyword.

          If the ``type`` is ``None`` or is omitted, it will first default to
          the special type :class:`.NullType`.  If and when this
          :class:`.Column` is made to refer to another column using
          :class:`.ForeignKey` and/or :class:`.ForeignKeyConstraint`, the type
          of the remote-referenced column will be copied to this column as
          well, at the moment that the foreign key is resolved against that
          remote :class:`.Column` object.

          .. versionchanged:: 0.9.0
            Support for propagation of type to a :class:`.Column` from its
            :class:`.ForeignKey` object has been improved and should be
            more reliable and timely.

        :param \*args: Additional positional arguments include various
          :class:`.SchemaItem` derived constructs which will be applied
          as options to the column.  These include instances of
          :class:`.Constraint`, :class:`.ForeignKey`, :class:`.ColumnDefault`,
          and :class:`.Sequence`.  In some cases an equivalent keyword
          argument is available such as ``server_default``, ``default``
          and ``unique``.

        :param autoincrement: Set up "auto increment" semantics for an integer
          primary key column.  The default value is the string ``"auto"``
          which indicates that a single-column primary key that is of
          an INTEGER type with no stated client-side or python-side defaults
          should receive auto increment semantics automatically;
          all other varieties of primary key columns will not.  This
          includes that :term:`DDL` such as PostgreSQL SERIAL or MySQL
          AUTO_INCREMENT will be emitted for this column during a table
          create, as well as that the column is assumed to generate new
          integer primary key values when an INSERT statement invokes which
          will be retrieved by the dialect.

          The flag may be set to ``True`` to indicate that a column which
          is part of a composite (e.g. multi-column) primary key should
          have autoincrement semantics, though note that only one column
          within a primary key may have this setting.    It can also
          be set to ``True`` to indicate autoincrement semantics on a
          column that has a client-side or server-side default configured,
          however note that not all dialects can accommodate all styles
          of default as an "autoincrement".  It can also be
          set to ``False`` on a single-column primary key that has a
          datatype of INTEGER in order to disable auto increment semantics
          for that column.

          .. versionchanged:: 1.1 The autoincrement flag now defaults to
             ``"auto"`` which indicates autoincrement semantics by default
             for single-column integer primary keys only; for composite
             (multi-column) primary keys, autoincrement is never implicitly
             enabled; as always, ``autoincrement=True`` will allow for
             at most one of those columns to be an "autoincrement" column.
             ``autoincrement=True`` may also be set on a :class:`.Column`
             that has an explicit client-side or server-side default,
             subject to limitations of the backend database and dialect.


          The setting *only* has an effect for columns which are:

          * Integer derived (i.e. INT, SMALLINT, BIGINT).

          * Part of the primary key

          * Not referring to another column via :class:`.ForeignKey`, unless
            the value is specified as ``'ignore_fk'``::

                # turn on autoincrement for this column despite
                # the ForeignKey()
                Column('id', ForeignKey('other.id'),
                            primary_key=True, autoincrement='ignore_fk')

            It is typically not desirable to have "autoincrement" enabled
            on a column that refers to another via foreign key, as such a column
            is required to refer to a value that originates from elsewhere.

          The setting has these two effects on columns that meet the
          above criteria:

          * DDL issued for the column will include database-specific
            keywords intended to signify this column as an
            "autoincrement" column, such as AUTO INCREMENT on MySQL,
            SERIAL on PostgreSQL, and IDENTITY on MS-SQL.  It does
            *not* issue AUTOINCREMENT for SQLite since this is a
            special SQLite flag that is not required for autoincrementing
            behavior.

            .. seealso::

                :ref:`sqlite_autoincrement`

          * The column will be considered to be available using an
            "autoincrement" method specific to the backend database, such
            as calling upon ``cursor.lastrowid``, using RETURNING in an
            INSERT statement to get at a sequence-generated value, or using
            special functions such as "SELECT scope_identity()".
            These methods are highly specific to the DBAPIs and databases in
            use and vary greatly, so care should be taken when associating
            ``autoincrement=True`` with a custom default generation function.


        :param default: A scalar, Python callable, or
            :class:`.ColumnElement` expression representing the
            *default value* for this column, which will be invoked upon insert
            if this column is otherwise not specified in the VALUES clause of
            the insert. This is a shortcut to using :class:`.ColumnDefault` as
            a positional argument; see that class for full detail on the
            structure of the argument.

            Contrast this argument to :paramref:`.Column.server_default`
            which creates a default generator on the database side.

            .. seealso::

                :ref:`metadata_defaults_toplevel`

        :param doc: optional String that can be used by the ORM or similar
            to document attributes.   This attribute does not render SQL
            comments (a future attribute 'comment' will achieve that).

        :param key: An optional string identifier which will identify this
            ``Column`` object on the :class:`.Table`. When a key is provided,
            this is the only identifier referencing the ``Column`` within the
            application, including ORM attribute mapping; the ``name`` field
            is used only when rendering SQL.

        :param index: When ``True``, indicates that the column is indexed.
            This is a shortcut for using a :class:`.Index` construct on the
            table. To specify indexes with explicit names or indexes that
            contain multiple columns, use the :class:`.Index` construct
            instead.

        :param info: Optional data dictionary which will be populated into the
            :attr:`.SchemaItem.info` attribute of this object.

        :param nullable: When set to ``False``, will cause the "NOT NULL"
            phrase to be added when generating DDL for the column.   When
            ``True``, will normally generate nothing (in SQL this defaults to
            "NULL"), except in some very specific backend-specific edge cases
            where "NULL" may render explicitly.   Defaults to ``True`` unless
            :paramref:`~.Column.primary_key` is also ``True``, in which case it
            defaults to ``False``.  This parameter is only used when issuing
            CREATE TABLE statements.

        :param onupdate: A scalar, Python callable, or
            :class:`~sqlalchemy.sql.expression.ClauseElement` representing a
            default value to be applied to the column within UPDATE
            statements, which wil be invoked upon update if this column is not
            present in the SET clause of the update. This is a shortcut to
            using :class:`.ColumnDefault` as a positional argument with
            ``for_update=True``.

            .. seealso::

                :ref:`metadata_defaults` - complete discussion of onupdate

        :param primary_key: If ``True``, marks this column as a primary key
            column. Multiple columns can have this flag set to specify
            composite primary keys. As an alternative, the primary key of a
            :class:`.Table` can be specified via an explicit
            :class:`.PrimaryKeyConstraint` object.

        :param server_default: A :class:`.FetchedValue` instance, str, Unicode
            or :func:`~sqlalchemy.sql.expression.text` construct representing
            the DDL DEFAULT value for the column.

            String types will be emitted as-is, surrounded by single quotes::

                Column('x', Text, server_default="val")

                x TEXT DEFAULT 'val'

            A :func:`~sqlalchemy.sql.expression.text` expression will be
            rendered as-is, without quotes::

                Column('y', DateTime, server_default=text('NOW()'))

                y DATETIME DEFAULT NOW()

            Strings and text() will be converted into a
            :class:`.DefaultClause` object upon initialization.

            Use :class:`.FetchedValue` to indicate that an already-existing
            column will generate a default value on the database side which
            will be available to SQLAlchemy for post-fetch after inserts. This
            construct does not specify any DDL and the implementation is left
            to the database, such as via a trigger.

            .. seealso::

                :ref:`server_defaults` - complete discussion of server side
                defaults

        :param server_onupdate:   A :class:`.FetchedValue` instance
             representing a database-side default generation function,
             such as a trigger. This
             indicates to SQLAlchemy that a newly generated value will be
             available after updates. This construct does not actually
             implement any kind of generation function within the database,
             which instead must be specified separately.

            .. seealso::

                :ref:`triggered_columns`

        :param quote: Force quoting of this column's name on or off,
             corresponding to ``True`` or ``False``. When left at its default
             of ``None``, the column identifier will be quoted according to
             whether the name is case sensitive (identifiers with at least one
             upper case character are treated as case sensitive), or if it's a
             reserved word. This flag is only needed to force quoting of a
             reserved word which is not known by the SQLAlchemy dialect.

        :param unique: When ``True``, indicates that this column contains a
             unique constraint, or if ``index`` is ``True`` as well, indicates
             that the :class:`.Index` should be created with the unique flag.
             To specify multiple columns in the constraint/index or to specify
             an explicit name, use the :class:`.UniqueConstraint` or
             :class:`.Index` constructs explicitly.

        :param system: When ``True``, indicates this is a "system" column,
             that is a column which is automatically made available by the
             database, and should not be included in the columns list for a
             ``CREATE TABLE`` statement.

             For more elaborate scenarios where columns should be
             conditionally rendered differently on different backends,
             consider custom compilation rules for :class:`.CreateColumn`.

             .. versionadded:: 0.8.3 Added the ``system=True`` parameter to
                :class:`.Column`.

        """

        name = kwargs.pop('name', None)
        type_ = kwargs.pop('type_', None)
        args = list(args)
        if args:
            if isinstance(args[0], util.string_types):
                if name is not None:
                    raise exc.ArgumentError(
                        "May not pass name positionally and as a keyword.")
                name = args.pop(0)
        if args:
            coltype = args[0]

            if hasattr(coltype, "_sqla_type"):
                if type_ is not None:
                    raise exc.ArgumentError(
                        "May not pass type_ positionally and as a keyword.")
                type_ = args.pop(0)

        if name is not None:
            name = quoted_name(name, kwargs.pop('quote', None))
        elif "quote" in kwargs:
            raise exc.ArgumentError("Explicit 'name' is required when "
                                    "sending 'quote' argument")

        super(Column, self).__init__(name, type_)
        self.key = kwargs.pop('key', name)
        self.primary_key = kwargs.pop('primary_key', False)
        self.nullable = kwargs.pop('nullable', not self.primary_key)
        self.default = kwargs.pop('default', None)
        self.server_default = kwargs.pop('server_default', None)
        self.server_onupdate = kwargs.pop('server_onupdate', None)

        # these default to None because .index and .unique is *not*
        # an informational flag about Column - there can still be an
        # Index or UniqueConstraint referring to this Column.
        self.index = kwargs.pop('index', None)
        self.unique = kwargs.pop('unique', None)

        self.system = kwargs.pop('system', False)
        self.doc = kwargs.pop('doc', None)
        self.onupdate = kwargs.pop('onupdate', None)
        self.autoincrement = kwargs.pop('autoincrement', "auto")
        self.constraints = set()
        self.foreign_keys = set()

        # check if this Column is proxying another column
        if '_proxies' in kwargs:
            self._proxies = kwargs.pop('_proxies')
        # otherwise, add DDL-related events
        elif isinstance(self.type, SchemaEventTarget):
            self.type._set_parent_with_dispatch(self)

        if self.default is not None:
            if isinstance(self.default, (ColumnDefault, Sequence)):
                args.append(self.default)
            else:
                if getattr(self.type, '_warn_on_bytestring', False):
                    if isinstance(self.default, util.binary_type):
                        util.warn(
                            "Unicode column '%s' has non-unicode "
                            "default value %r specified." % (
                                self.key,
                                self.default
                            ))
                args.append(ColumnDefault(self.default))

        if self.server_default is not None:
            if isinstance(self.server_default, FetchedValue):
                args.append(self.server_default._as_for_update(False))
            else:
                args.append(DefaultClause(self.server_default))

        if self.onupdate is not None:
            if isinstance(self.onupdate, (ColumnDefault, Sequence)):
                args.append(self.onupdate)
            else:
                args.append(ColumnDefault(self.onupdate, for_update=True))

        if self.server_onupdate is not None:
            if isinstance(self.server_onupdate, FetchedValue):
                args.append(self.server_onupdate._as_for_update(True))
            else:
                args.append(DefaultClause(self.server_onupdate,
                                          for_update=True))
        self._init_items(*args)

        util.set_creation_order(self)

        if 'info' in kwargs:
            self.info = kwargs.pop('info')

        if kwargs:
            raise exc.ArgumentError(
                "Unknown arguments passed to Column: " + repr(list(kwargs)))

#    @property
#    def quote(self):
#        return getattr(self.name, "quote", None)

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
        """Return True if this Column references the given column via foreign
        key."""

        for fk in self.foreign_keys:
            if fk.column.proxy_set.intersection(column.proxy_set):
                return True
        else:
            return False

    def append_foreign_key(self, fk):
        fk._set_parent_with_dispatch(self)

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
            [(self.table is not None and "table=<%s>" %
              self.table.description or "table=None")] +
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg])

    def _set_parent(self, table):
        if not self.name:
            raise exc.ArgumentError(
                "Column must be constructed with a non-blank name or "
                "assign a non-blank .name before adding to a Table.")
        if self.key is None:
            self.key = self.name

        existing = getattr(self, 'table', None)
        if existing is not None and existing is not table:
            raise exc.ArgumentError(
                "Column object '%s' already assigned to Table '%s'" % (
                    self.key,
                    existing.description
                ))

        if self.key in table._columns:
            col = table._columns.get(self.key)
            if col is not self:
                for fk in col.foreign_keys:
                    table.foreign_keys.remove(fk)
                    if fk.constraint in table.constraints:
                        # this might have been removed
                        # already, if it's a composite constraint
                        # and more than one col being replaced
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
            if isinstance(self.index, util.string_types):
                raise exc.ArgumentError(
                    "The 'index' keyword argument on Column is boolean only. "
                    "To create indexes with a specific name, create an "
                    "explicit Index object external to the Table.")
            Index(None, self, unique=bool(self.unique))
        elif self.unique:
            if isinstance(self.unique, util.string_types):
                raise exc.ArgumentError(
                    "The 'unique' keyword argument on Column is boolean "
                    "only. To create unique constraints or indexes with a "
                    "specific name, append an explicit UniqueConstraint to "
                    "the Table's list of elements, or create an explicit "
                    "Index object external to the Table.")
            table.append_constraint(UniqueConstraint(self.key))

        self._setup_on_memoized_fks(lambda fk: fk._set_remote_table(table))

    def _setup_on_memoized_fks(self, fn):
        fk_keys = [
            ((self.table.key, self.key), False),
            ((self.table.key, self.name), True),
        ]
        for fk_key, link_to_name in fk_keys:
            if fk_key in self.table.metadata._fk_memos:
                for fk in self.table.metadata._fk_memos[fk_key]:
                    if fk.link_to_name is link_to_name:
                        fn(fk)

    def _on_table_attach(self, fn):
        if self.table is not None:
            fn(self, self.table)
        else:
            event.listen(self, 'after_parent_attach', fn)

    def copy(self, **kw):
        """Create a copy of this ``Column``, unitialized.

        This is used in ``Table.tometadata``.

        """

        # Constraint objects plus non-constraint-bound ForeignKey objects
        args = \
            [c.copy(**kw) for c in self.constraints if not c._type_bound] + \
            [c.copy(**kw) for c in self.foreign_keys if not c.constraint]

        type_ = self.type
        if isinstance(type_, SchemaEventTarget):
            type_ = type_.copy(**kw)

        c = self._constructor(
            name=self.name,
            type_=type_,
            key=self.key,
            primary_key=self.primary_key,
            nullable=self.nullable,
            unique=self.unique,
            system=self.system,
            # quote=self.quote,
            index=self.index,
            autoincrement=self.autoincrement,
            default=self.default,
            server_default=self.server_default,
            onupdate=self.onupdate,
            server_onupdate=self.server_onupdate,
            doc=self.doc,
            *args
        )
        return self._schema_item_copy(c)

    def _make_proxy(self, selectable, name=None, key=None,
                    name_is_truncatable=False, **kw):
        """Create a *proxy* for this column.

        This is a copy of this ``Column`` referenced by a different parent
        (such as an alias or select statement).  The column should
        be used only in select scenarios, as its full DDL/default
        information is not transferred.

        """
        fk = [ForeignKey(f.column, _constraint=f.constraint)
              for f in self.foreign_keys]
        if name is None and self.name is None:
            raise exc.InvalidRequestError(
                "Cannot initialize a sub-selectable"
                " with this Column object until its 'name' has "
                "been assigned.")
        try:
            c = self._constructor(
                _as_truncated(name or self.name) if
                name_is_truncatable else (name or self.name),
                self.type,
                key=key if key else name if name else self.key,
                primary_key=self.primary_key,
                nullable=self.nullable,
                _proxies=[self], *fk)
        except TypeError:
            util.raise_from_cause(
                TypeError(
                    "Could not create a copy of this %r object.  "
                    "Ensure the class includes a _constructor() "
                    "attribute or method which accepts the "
                    "standard Column constructor arguments, or "
                    "references the Column class itself." % self.__class__)
            )

        c.table = selectable
        selectable._columns.add(c)
        if selectable._is_clone_of is not None:
            c._is_clone_of = selectable._is_clone_of.columns[c.key]
        if self.primary_key:
            selectable.primary_key.add(c)
        c.dispatch.after_parent_attach(c, selectable)
        return c

    def get_children(self, schema_visitor=False, **kwargs):
        if schema_visitor:
            return [x for x in (self.default, self.onupdate)
                    if x is not None] + \
                list(self.foreign_keys) + list(self.constraints)
        else:
            return ColumnClause.get_children(self, **kwargs)


class ForeignKey(DialectKWArgs, SchemaItem):
    """Defines a dependency between two columns.

    ``ForeignKey`` is specified as an argument to a :class:`.Column` object,
    e.g.::

        t = Table("remote_table", metadata,
            Column("remote_id", ForeignKey("main_table.id"))
        )

    Note that ``ForeignKey`` is only a marker object that defines
    a dependency between two columns.   The actual constraint
    is in all cases represented by the :class:`.ForeignKeyConstraint`
    object.   This object will be generated automatically when
    a ``ForeignKey`` is associated with a :class:`.Column` which
    in turn is associated with a :class:`.Table`.   Conversely,
    when :class:`.ForeignKeyConstraint` is applied to a :class:`.Table`,
    ``ForeignKey`` markers are automatically generated to be
    present on each associated :class:`.Column`, which are also
    associated with the constraint object.

    Note that you cannot define a "composite" foreign key constraint,
    that is a constraint between a grouping of multiple parent/child
    columns, using ``ForeignKey`` objects.   To define this grouping,
    the :class:`.ForeignKeyConstraint` object must be used, and applied
    to the :class:`.Table`.   The associated ``ForeignKey`` objects
    are created automatically.

    The ``ForeignKey`` objects associated with an individual
    :class:`.Column` object are available in the `foreign_keys` collection
    of that column.

    Further examples of foreign key configuration are in
    :ref:`metadata_foreignkeys`.

    """

    __visit_name__ = 'foreign_key'

    def __init__(self, column, _constraint=None, use_alter=False, name=None,
                 onupdate=None, ondelete=None, deferrable=None,
                 initially=None, link_to_name=False, match=None,
                 info=None,
                 **dialect_kw):
        r"""
        Construct a column-level FOREIGN KEY.

        The :class:`.ForeignKey` object when constructed generates a
        :class:`.ForeignKeyConstraint` which is associated with the parent
        :class:`.Table` object's collection of constraints.

        :param column: A single target column for the key relationship. A
            :class:`.Column` object or a column name as a string:
            ``tablename.columnkey`` or ``schema.tablename.columnkey``.
            ``columnkey`` is the ``key`` which has been assigned to the column
            (defaults to the column name itself), unless ``link_to_name`` is
            ``True`` in which case the rendered name of the column is used.

            .. versionadded:: 0.7.4
                Note that if the schema name is not included, and the
                underlying :class:`.MetaData` has a "schema", that value will
                be used.

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
            :class:`.ForeignKeyConstraint` to indicate the constraint should
            be generated/dropped externally from the CREATE TABLE/ DROP TABLE
            statement.  See :paramref:`.ForeignKeyConstraint.use_alter`
            for further description.

            .. seealso::

                :paramref:`.ForeignKeyConstraint.use_alter`

                :ref:`use_alter`

        :param match: Optional string. If set, emit MATCH <value> when issuing
            DDL for this constraint. Typical values include SIMPLE, PARTIAL
            and FULL.

        :param info: Optional data dictionary which will be populated into the
            :attr:`.SchemaItem.info` attribute of this object.

            .. versionadded:: 1.0.0

        :param \**dialect_kw:  Additional keyword arguments are dialect
            specific, and passed in the form ``<dialectname>_<argname>``.  The
            arguments are ultimately handled by a corresponding
            :class:`.ForeignKeyConstraint`.  See the documentation regarding
            an individual dialect at :ref:`dialect_toplevel` for detail on
            documented arguments.

            .. versionadded:: 0.9.2

        """

        self._colspec = column
        if isinstance(self._colspec, util.string_types):
            self._table_column = None
        else:
            if hasattr(self._colspec, '__clause_element__'):
                self._table_column = self._colspec.__clause_element__()
            else:
                self._table_column = self._colspec

            if not isinstance(self._table_column, ColumnClause):
                raise exc.ArgumentError(
                    "String, Column, or Column-bound argument "
                    "expected, got %r" % self._table_column)
            elif not isinstance(
                    self._table_column.table, (util.NoneType, TableClause)):
                raise exc.ArgumentError(
                    "ForeignKey received Column not bound "
                    "to a Table, got: %r" % self._table_column.table
                )

        # the linked ForeignKeyConstraint.
        # ForeignKey will create this when parent Column
        # is attached to a Table, *or* ForeignKeyConstraint
        # object passes itself in when creating ForeignKey
        # markers.
        self.constraint = _constraint
        self.parent = None
        self.use_alter = use_alter
        self.name = name
        self.onupdate = onupdate
        self.ondelete = ondelete
        self.deferrable = deferrable
        self.initially = initially
        self.link_to_name = link_to_name
        self.match = match
        if info:
            self.info = info
        self._unvalidated_dialect_kw = dialect_kw

    def __repr__(self):
        return "ForeignKey(%r)" % self._get_colspec()

    def copy(self, schema=None):
        """Produce a copy of this :class:`.ForeignKey` object.

        The new :class:`.ForeignKey` will not be bound
        to any :class:`.Column`.

        This method is usually used by the internal
        copy procedures of :class:`.Column`, :class:`.Table`,
        and :class:`.MetaData`.

        :param schema: The returned :class:`.ForeignKey` will
          reference the original table and column name, qualified
          by the given string schema name.

        """

        fk = ForeignKey(
            self._get_colspec(schema=schema),
            use_alter=self.use_alter,
            name=self.name,
            onupdate=self.onupdate,
            ondelete=self.ondelete,
            deferrable=self.deferrable,
            initially=self.initially,
            link_to_name=self.link_to_name,
            match=self.match,
            **self._unvalidated_dialect_kw
        )
        return self._schema_item_copy(fk)

    def _get_colspec(self, schema=None, table_name=None):
        """Return a string based 'column specification' for this
        :class:`.ForeignKey`.

        This is usually the equivalent of the string-based "tablename.colname"
        argument first passed to the object's constructor.

        """
        if schema:
            _schema, tname, colname = self._column_tokens
            if table_name is not None:
                tname = table_name
            return "%s.%s.%s" % (schema, tname, colname)
        elif table_name:
            schema, tname, colname = self._column_tokens
            if schema:
                return "%s.%s.%s" % (schema, table_name, colname)
            else:
                return "%s.%s" % (table_name, colname)
        elif self._table_column is not None:
            return "%s.%s" % (
                self._table_column.table.fullname, self._table_column.key)
        else:
            return self._colspec

    @property
    def _referred_schema(self):
        return self._column_tokens[0]

    def _table_key(self):
        if self._table_column is not None:
            if self._table_column.table is None:
                return None
            else:
                return self._table_column.table.key
        else:
            schema, tname, colname = self._column_tokens
            return _get_table_key(tname, schema)

    target_fullname = property(_get_colspec)

    def references(self, table):
        """Return True if the given :class:`.Table` is referenced by this
        :class:`.ForeignKey`."""

        return table.corresponding_column(self.column) is not None

    def get_referent(self, table):
        """Return the :class:`.Column` in the given :class:`.Table`
        referenced by this :class:`.ForeignKey`.

        Returns None if this :class:`.ForeignKey` does not reference the given
        :class:`.Table`.

        """

        return table.corresponding_column(self.column)

    @util.memoized_property
    def _column_tokens(self):
        """parse a string-based _colspec into its component parts."""

        m = self._get_colspec().split('.')
        if m is None:
            raise exc.ArgumentError(
                "Invalid foreign key column specification: %s" %
                self._colspec)
        if (len(m) == 1):
            tname = m.pop()
            colname = None
        else:
            colname = m.pop()
            tname = m.pop()

        # A FK between column 'bar' and table 'foo' can be
        # specified as 'foo', 'foo.bar', 'dbo.foo.bar',
        # 'otherdb.dbo.foo.bar'. Once we have the column name and
        # the table name, treat everything else as the schema
        # name. Some databases (e.g. Sybase) support
        # inter-database foreign keys. See tickets#1341 and --
        # indirectly related -- Ticket #594. This assumes that '.'
        # will never appear *within* any component of the FK.

        if (len(m) > 0):
            schema = '.'.join(m)
        else:
            schema = None
        return schema, tname, colname

    def _resolve_col_tokens(self):
        if self.parent is None:
            raise exc.InvalidRequestError(
                "this ForeignKey object does not yet have a "
                "parent Column associated with it.")

        elif self.parent.table is None:
            raise exc.InvalidRequestError(
                "this ForeignKey's parent column is not yet associated "
                "with a Table.")

        parenttable = self.parent.table

        # assertion, can be commented out.
        # basically Column._make_proxy() sends the actual
        # target Column to the ForeignKey object, so the
        # string resolution here is never called.
        for c in self.parent.base_columns:
            if isinstance(c, Column):
                assert c.table is parenttable
                break
        else:
            assert False
        ######################

        schema, tname, colname = self._column_tokens

        if schema is None and parenttable.metadata.schema is not None:
            schema = parenttable.metadata.schema

        tablekey = _get_table_key(tname, schema)
        return parenttable, tablekey, colname

    def _link_to_col_by_colstring(self, parenttable, table, colname):
        if not hasattr(self.constraint, '_referred_table'):
            self.constraint._referred_table = table
        else:
            assert self.constraint._referred_table is table

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
                "Could not initialize target column "
                "for ForeignKey '%s' on table '%s': "
                "table '%s' has no column named '%s'" %
                (self._colspec, parenttable.name, table.name, key),
                table.name, key)

        self._set_target_column(_column)

    def _set_target_column(self, column):
        # propagate TypeEngine to parent if it didn't have one
        if self.parent.type._isnull:
            self.parent.type = column.type

        # super-edgy case, if other FKs point to our column,
        # they'd get the type propagated out also.
        if isinstance(self.parent.table, Table):

            def set_type(fk):
                if fk.parent.type._isnull:
                    fk.parent.type = column.type
            self.parent._setup_on_memoized_fks(set_type)

        self.column = column

    @util.memoized_property
    def column(self):
        """Return the target :class:`.Column` referenced by this
        :class:`.ForeignKey`.

        If no target column has been established, an exception
        is raised.

        .. versionchanged:: 0.9.0
            Foreign key target column resolution now occurs as soon as both
            the ForeignKey object and the remote Column to which it refers
            are both associated with the same MetaData object.

        """

        if isinstance(self._colspec, util.string_types):

            parenttable, tablekey, colname = self._resolve_col_tokens()

            if tablekey not in parenttable.metadata:
                raise exc.NoReferencedTableError(
                    "Foreign key associated with column '%s' could not find "
                    "table '%s' with which to generate a "
                    "foreign key to target column '%s'" %
                    (self.parent, tablekey, colname),
                    tablekey)
            elif parenttable.key not in parenttable.metadata:
                raise exc.InvalidRequestError(
                    "Table %s is no longer associated with its "
                    "parent MetaData" % parenttable)
            else:
                raise exc.NoReferencedColumnError(
                    "Could not initialize target column for "
                    "ForeignKey '%s' on table '%s': "
                    "table '%s' has no column named '%s'" % (
                        self._colspec, parenttable.name, tablekey, colname),
                    tablekey, colname)
        elif hasattr(self._colspec, '__clause_element__'):
            _column = self._colspec.__clause_element__()
            return _column
        else:
            _column = self._colspec
            return _column

    def _set_parent(self, column):
        if self.parent is not None and self.parent is not column:
            raise exc.InvalidRequestError(
                "This ForeignKey already has a parent !")
        self.parent = column
        self.parent.foreign_keys.add(self)
        self.parent._on_table_attach(self._set_table)

    def _set_remote_table(self, table):
        parenttable, tablekey, colname = self._resolve_col_tokens()
        self._link_to_col_by_colstring(parenttable, table, colname)
        self.constraint._validate_dest_table(table)

    def _remove_from_metadata(self, metadata):
        parenttable, table_key, colname = self._resolve_col_tokens()
        fk_key = (table_key, colname)

        if self in metadata._fk_memos[fk_key]:
            # TODO: no test coverage for self not in memos
            metadata._fk_memos[fk_key].remove(self)

    def _set_table(self, column, table):
        # standalone ForeignKey - create ForeignKeyConstraint
        # on the hosting Table when attached to the Table.
        if self.constraint is None and isinstance(table, Table):
            self.constraint = ForeignKeyConstraint(
                [], [], use_alter=self.use_alter, name=self.name,
                onupdate=self.onupdate, ondelete=self.ondelete,
                deferrable=self.deferrable, initially=self.initially,
                match=self.match,
                **self._unvalidated_dialect_kw
            )
            self.constraint._append_element(column, self)
            self.constraint._set_parent_with_dispatch(table)
        table.foreign_keys.add(self)

        # set up remote ".column" attribute, or a note to pick it
        # up when the other Table/Column shows up
        if isinstance(self._colspec, util.string_types):
            parenttable, table_key, colname = self._resolve_col_tokens()
            fk_key = (table_key, colname)
            if table_key in parenttable.metadata.tables:
                table = parenttable.metadata.tables[table_key]
                try:
                    self._link_to_col_by_colstring(
                        parenttable, table, colname)
                except exc.NoReferencedColumnError:
                    # this is OK, we'll try later
                    pass
            parenttable.metadata._fk_memos[fk_key].append(self)
        elif hasattr(self._colspec, '__clause_element__'):
            _column = self._colspec.__clause_element__()
            self._set_target_column(_column)
        else:
            _column = self._colspec
            self._set_target_column(_column)


class _NotAColumnExpr(object):
    def _not_a_column_expr(self):
        raise exc.InvalidRequestError(
            "This %s cannot be used directly "
            "as a column expression." % self.__class__.__name__)

    __clause_element__ = self_group = lambda self: self._not_a_column_expr()
    _from_objects = property(lambda self: self._not_a_column_expr())


class DefaultGenerator(_NotAColumnExpr, SchemaItem):
    """Base class for column *default* values."""

    __visit_name__ = 'default_generator'

    is_sequence = False
    is_server_default = False
    column = None

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

    def _execute_on_connection(self, connection, multiparams, params):
        return connection._execute_default(self, multiparams, params)

    @property
    def bind(self):
        """Return the connectable associated with this default."""
        if getattr(self, 'column', None) is not None:
            return self.column.table.bind
        else:
            return None


class ColumnDefault(DefaultGenerator):
    """A plain default value on a column.

    This could correspond to a constant, a callable function,
    or a SQL clause.

    :class:`.ColumnDefault` is generated automatically
    whenever the ``default``, ``onupdate`` arguments of
    :class:`.Column` are used.  A :class:`.ColumnDefault`
    can be passed positionally as well.

    For example, the following::

        Column('foo', Integer, default=50)

    Is equivalent to::

        Column('foo', Integer, ColumnDefault(50))


    """

    def __init__(self, arg, **kwargs):
        """"Construct a new :class:`.ColumnDefault`.


        :param arg: argument representing the default value.
         May be one of the following:

         * a plain non-callable Python value, such as a
           string, integer, boolean, or other simple type.
           The default value will be used as is each time.
         * a SQL expression, that is one which derives from
           :class:`.ColumnElement`.  The SQL expression will
           be rendered into the INSERT or UPDATE statement,
           or in the case of a primary key column when
           RETURNING is not used may be
           pre-executed before an INSERT within a SELECT.
         * A Python callable.  The function will be invoked for each
           new row subject to an INSERT or UPDATE.
           The callable must accept exactly
           zero or one positional arguments.  The one-argument form
           will receive an instance of the :class:`.ExecutionContext`,
           which provides contextual information as to the current
           :class:`.Connection` in use as well as the current
           statement and parameters.

        """
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
        return isinstance(self.arg, ClauseElement)

    @util.memoized_property
    def is_scalar(self):
        return not self.is_callable and \
            not self.is_clause_element and \
            not self.is_sequence

    def _maybe_wrap_callable(self, fn):
        """Wrap callables that don't accept a context.

        This is to allow easy compatibility with default callables
        that aren't specific to accepting of a context.

        """
        try:
            argspec = util.get_callable_argspec(fn, no_self=True)
        except TypeError:
            return util.wrap_callable(lambda ctx: fn(), fn)

        defaulted = argspec[3] is not None and len(argspec[3]) or 0
        positionals = len(argspec[0]) - defaulted

        if positionals == 0:
            return util.wrap_callable(lambda ctx: fn(), fn)

        elif positionals == 1:
            return fn
        else:
            raise exc.ArgumentError(
                "ColumnDefault Python function takes zero or one "
                "positional arguments")

    def _visit_name(self):
        if self.for_update:
            return "column_onupdate"
        else:
            return "column_default"
    __visit_name__ = property(_visit_name)

    def __repr__(self):
        return "ColumnDefault(%r)" % (self.arg, )


class Sequence(DefaultGenerator):
    """Represents a named database sequence.

    The :class:`.Sequence` object represents the name and configurational
    parameters of a database sequence.   It also represents
    a construct that can be "executed" by a SQLAlchemy :class:`.Engine`
    or :class:`.Connection`, rendering the appropriate "next value" function
    for the target database and returning a result.

    The :class:`.Sequence` is typically associated with a primary key column::

        some_table = Table(
            'some_table', metadata,
            Column('id', Integer, Sequence('some_table_seq'),
            primary_key=True)
        )

    When CREATE TABLE is emitted for the above :class:`.Table`, if the
    target platform supports sequences, a CREATE SEQUENCE statement will
    be emitted as well.   For platforms that don't support sequences,
    the :class:`.Sequence` construct is ignored.

    .. seealso::

        :class:`.CreateSequence`

        :class:`.DropSequence`

    """

    __visit_name__ = 'sequence'

    is_sequence = True

    def __init__(self, name, start=None, increment=None, minvalue=None,
                 maxvalue=None, nominvalue=None, nomaxvalue=None, cycle=None,
                 schema=None, cache=None, order=None, optional=False,
                 quote=None, metadata=None, quote_schema=None,
                 for_update=False):
        """Construct a :class:`.Sequence` object.

        :param name: The name of the sequence.
        :param start: the starting index of the sequence.  This value is
         used when the CREATE SEQUENCE command is emitted to the database
         as the value of the "START WITH" clause.   If ``None``, the
         clause is omitted, which on most platforms indicates a starting
         value of 1.
        :param increment: the increment value of the sequence.  This
         value is used when the CREATE SEQUENCE command is emitted to
         the database as the value of the "INCREMENT BY" clause.  If ``None``,
         the clause is omitted, which on most platforms indicates an
         increment of 1.
        :param minvalue: the minimum value of the sequence.  This
         value is used when the CREATE SEQUENCE command is emitted to
         the database as the value of the "MINVALUE" clause.  If ``None``,
         the clause is omitted, which on most platforms indicates a
         minvalue of 1 and -2^63-1 for ascending and descending sequences,
         respectively.

         .. versionadded:: 1.0.7

        :param maxvalue: the maximum value of the sequence.  This
         value is used when the CREATE SEQUENCE command is emitted to
         the database as the value of the "MAXVALUE" clause.  If ``None``,
         the clause is omitted, which on most platforms indicates a
         maxvalue of 2^63-1 and -1 for ascending and descending sequences,
         respectively.

         .. versionadded:: 1.0.7

        :param nominvalue: no minimum value of the sequence.  This
         value is used when the CREATE SEQUENCE command is emitted to
         the database as the value of the "NO MINVALUE" clause.  If ``None``,
         the clause is omitted, which on most platforms indicates a
         minvalue of 1 and -2^63-1 for ascending and descending sequences,
         respectively.

         .. versionadded:: 1.0.7

        :param nomaxvalue: no maximum value of the sequence.  This
         value is used when the CREATE SEQUENCE command is emitted to
         the database as the value of the "NO MAXVALUE" clause.  If ``None``,
         the clause is omitted, which on most platforms indicates a
         maxvalue of 2^63-1 and -1 for ascending and descending sequences,
         respectively.

         .. versionadded:: 1.0.7

        :param cycle: allows the sequence to wrap around when the maxvalue
         or minvalue has been reached by an ascending or descending sequence
         respectively.  This value is used when the CREATE SEQUENCE command
         is emitted to the database as the "CYCLE" clause.  If the limit is
         reached, the next number generated will be the minvalue or maxvalue,
         respectively.  If cycle=False (the default) any calls to nextval
         after the sequence has reached its maximum value will return an
         error.

         .. versionadded:: 1.0.7

        :param schema: Optional schema name for the sequence, if located
         in a schema other than the default.  The rules for selecting the
         schema name when a :class:`.MetaData` is also present are the same
         as that of :paramref:`.Table.schema`.

        :param cache: optional integer value; number of future values in the
         sequence which are calculated in advance.  Renders the CACHE keyword
         understood by Oracle and PostgreSQL.

         .. versionadded:: 1.1.12

        :param order: optional boolean value; if true, renders the
         ORDER keyword, understood by Oracle, indicating the sequence is
         definitively ordered.   May be necessary to provide deterministic
         ordering using Oracle RAC.

         .. versionadded:: 1.1.12

        :param optional: boolean value, when ``True``, indicates that this
         :class:`.Sequence` object only needs to be explicitly generated
         on backends that don't provide another way to generate primary
         key identifiers.  Currently, it essentially means, "don't create
         this sequence on the PostgreSQL backend, where the SERIAL keyword
         creates a sequence for us automatically".
        :param quote: boolean value, when ``True`` or ``False``, explicitly
         forces quoting of the schema name on or off.  When left at its
         default of ``None``, normal quoting rules based on casing and
         reserved words take place.
        :param quote_schema: set the quoting preferences for the ``schema``
         name.

        :param metadata: optional :class:`.MetaData` object which this
         :class:`.Sequence` will be associated with.  A :class:`.Sequence`
         that is associated with a :class:`.MetaData` gains the following
         capabilities:

         * The :class:`.Sequence` will inherit the :paramref:`.MetaData.schema`
           parameter specified to the target :class:`.MetaData`, which
           affects the production of CREATE / DROP DDL, if any.

         * The :meth:`.Sequence.create` and :meth:`.Sequence.drop` methods
           automatically use the engine bound to the :class:`.MetaData`
           object, if any.

         * The :meth:`.MetaData.create_all` and :meth:`.MetaData.drop_all`
           methods will emit CREATE / DROP for this :class:`.Sequence`,
           even if the :class:`.Sequence` is not associated with any
           :class:`.Table` / :class:`.Column` that's a member of this
           :class:`.MetaData`.

         The above behaviors can only occur if the :class:`.Sequence` is
         explicitly associated with the :class:`.MetaData` via this parameter.

         .. seealso::

            :ref:`sequence_metadata` - full discussion of the
            :paramref:`.Sequence.metadata` parameter.

        :param for_update: Indicates this :class:`.Sequence`, when associated
         with a :class:`.Column`, should be invoked for UPDATE statements
         on that column's table, rather than for INSERT statements, when
         no value is otherwise present for that column in the statement.

        """
        super(Sequence, self).__init__(for_update=for_update)
        self.name = quoted_name(name, quote)
        self.start = start
        self.increment = increment
        self.minvalue = minvalue
        self.maxvalue = maxvalue
        self.nominvalue = nominvalue
        self.nomaxvalue = nomaxvalue
        self.cycle = cycle
        self.cache = cache
        self.order = order
        self.optional = optional
        if schema is BLANK_SCHEMA:
            self.schema = schema = None
        elif metadata is not None and schema is None and metadata.schema:
            self.schema = schema = metadata.schema
        else:
            self.schema = quoted_name(schema, quote_schema)
        self.metadata = metadata
        self._key = _get_table_key(name, schema)
        if metadata:
            self._set_metadata(metadata)

    @util.memoized_property
    def is_callable(self):
        return False

    @util.memoized_property
    def is_clause_element(self):
        return False

    @util.dependencies("sqlalchemy.sql.functions.func")
    def next_value(self, func):
        """Return a :class:`.next_value` function element
        which will render the appropriate increment function
        for this :class:`.Sequence` within any SQL expression.

        """
        return func.next_value(self, bind=self.bind)

    def _set_parent(self, column):
        super(Sequence, self)._set_parent(column)
        column._on_table_attach(self._set_table)

    def _set_table(self, column, table):
        self._set_metadata(table.metadata)

    def _set_metadata(self, metadata):
        self.metadata = metadata
        self.metadata._sequences[self._key] = self

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
        bind._run_visitor(ddl.SchemaGenerator,
                          self,
                          checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=True):
        """Drops this sequence from the database."""

        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper,
                          self,
                          checkfirst=checkfirst)

    def _not_a_column_expr(self):
        raise exc.InvalidRequestError(
            "This %s cannot be used directly "
            "as a column expression.  Use func.next_value(sequence) "
            "to produce a 'next value' function that's usable "
            "as a column element."
            % self.__class__.__name__)


@inspection._self_inspects
class FetchedValue(_NotAColumnExpr, SchemaEventTarget):
    """A marker for a transparent database-side default.

    Use :class:`.FetchedValue` when the database is configured
    to provide some automatic default for a column.

    E.g.::

        Column('foo', Integer, FetchedValue())

    Would indicate that some trigger or default generator
    will create a new value for the ``foo`` column during an
    INSERT.

    .. seealso::

        :ref:`triggered_columns`

    """
    is_server_default = True
    reflected = False
    has_argument = False

    def __init__(self, for_update=False):
        self.for_update = for_update

    def _as_for_update(self, for_update):
        if for_update == self.for_update:
            return self
        else:
            return self._clone(for_update)

    def _clone(self, for_update):
        n = self.__class__.__new__(self.__class__)
        n.__dict__.update(self.__dict__)
        n.__dict__.pop('column', None)
        n.for_update = for_update
        return n

    def _set_parent(self, column):
        self.column = column
        if self.for_update:
            self.column.server_onupdate = self
        else:
            self.column.server_default = self

    def __repr__(self):
        return util.generic_repr(self)


class DefaultClause(FetchedValue):
    """A DDL-specified DEFAULT column value.

    :class:`.DefaultClause` is a :class:`.FetchedValue`
    that also generates a "DEFAULT" clause when
    "CREATE TABLE" is emitted.

    :class:`.DefaultClause` is generated automatically
    whenever the ``server_default``, ``server_onupdate`` arguments of
    :class:`.Column` are used.  A :class:`.DefaultClause`
    can be passed positionally as well.

    For example, the following::

        Column('foo', Integer, server_default="50")

    Is equivalent to::

        Column('foo', Integer, DefaultClause("50"))

    """

    has_argument = True

    def __init__(self, arg, for_update=False, _reflected=False):
        util.assert_arg_type(arg, (util.string_types[0],
                                   ClauseElement,
                                   TextClause), 'arg')
        super(DefaultClause, self).__init__(for_update)
        self.arg = arg
        self.reflected = _reflected

    def __repr__(self):
        return "DefaultClause(%r, for_update=%r)" % \
            (self.arg, self.for_update)


class PassiveDefault(DefaultClause):
    """A DDL-specified DEFAULT column value.

    .. deprecated:: 0.6
        :class:`.PassiveDefault` is deprecated.
        Use :class:`.DefaultClause`.
    """
    @util.deprecated("0.6",
                     ":class:`.PassiveDefault` is deprecated.  "
                     "Use :class:`.DefaultClause`.",
                     False)
    def __init__(self, *arg, **kw):
        DefaultClause.__init__(self, *arg, **kw)


class Constraint(DialectKWArgs, SchemaItem):
    """A table-level SQL constraint."""

    __visit_name__ = 'constraint'

    def __init__(self, name=None, deferrable=None, initially=None,
                 _create_rule=None, info=None, _type_bound=False,
                 **dialect_kw):
        r"""Create a SQL constraint.

        :param name:
          Optional, the in-database name of this ``Constraint``.

        :param deferrable:
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        :param initially:
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.

        :param info: Optional data dictionary which will be populated into the
            :attr:`.SchemaItem.info` attribute of this object.

            .. versionadded:: 1.0.0

        :param _create_rule:
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

        :param \**dialect_kw:  Additional keyword arguments are dialect
            specific, and passed in the form ``<dialectname>_<argname>``.  See
            the documentation regarding an individual dialect at
            :ref:`dialect_toplevel` for detail on documented arguments.

        """

        self.name = name
        self.deferrable = deferrable
        self.initially = initially
        if info:
            self.info = info
        self._create_rule = _create_rule
        self._type_bound = _type_bound
        util.set_creation_order(self)
        self._validate_dialect_kwargs(dialect_kw)

    @property
    def table(self):
        try:
            if isinstance(self.parent, Table):
                return self.parent
        except AttributeError:
            pass
        raise exc.InvalidRequestError(
            "This constraint is not bound to a table.  Did you "
            "mean to call table.append_constraint(constraint) ?")

    def _set_parent(self, parent):
        self.parent = parent
        parent.constraints.add(self)

    def copy(self, **kw):
        raise NotImplementedError()


def _to_schema_column(element):
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    if not isinstance(element, Column):
        raise exc.ArgumentError("schema.Column object expected")
    return element


def _to_schema_column_or_string(element):
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    if not isinstance(element, util.string_types + (ColumnElement, )):
        msg = "Element %r is not a string name or column element"
        raise exc.ArgumentError(msg % element)
    return element


class ColumnCollectionMixin(object):

    columns = None
    """A :class:`.ColumnCollection` of :class:`.Column` objects.

    This collection represents the columns which are referred to by
    this object.

    """

    _allow_multiple_tables = False

    def __init__(self, *columns, **kw):
        _autoattach = kw.pop('_autoattach', True)
        self.columns = ColumnCollection()
        self._pending_colargs = [_to_schema_column_or_string(c)
                                 for c in columns]
        if _autoattach and self._pending_colargs:
            self._check_attach()

    @classmethod
    def _extract_col_expression_collection(cls, expressions):
        for expr in expressions:
            strname = None
            column = None
            if hasattr(expr, '__clause_element__'):
                expr = expr.__clause_element__()

            if not isinstance(expr, (ColumnElement, TextClause)):
                # this assumes a string
                strname = expr
            else:
                cols = []
                visitors.traverse(expr, {}, {'column': cols.append})
                if cols:
                    column = cols[0]
            add_element = column if column is not None else strname
            yield expr, column, strname, add_element

    def _check_attach(self, evt=False):
        col_objs = [
            c for c in self._pending_colargs
            if isinstance(c, Column)
        ]

        cols_w_table = [
            c for c in col_objs if isinstance(c.table, Table)
        ]

        cols_wo_table = set(col_objs).difference(cols_w_table)

        if cols_wo_table:
            # feature #3341 - place event listeners for Column objects
            # such that when all those cols are attached, we autoattach.
            assert not evt, "Should not reach here on event call"

            # issue #3411 - don't do the per-column auto-attach if some of the
            # columns are specified as strings.
            has_string_cols = set(self._pending_colargs).difference(col_objs)
            if not has_string_cols:
                def _col_attached(column, table):
                    # this isinstance() corresponds with the
                    # isinstance() above; only want to count Table-bound
                    # columns
                    if isinstance(table, Table):
                        cols_wo_table.discard(column)
                        if not cols_wo_table:
                            self._check_attach(evt=True)
                self._cols_wo_table = cols_wo_table
                for col in cols_wo_table:
                    col._on_table_attach(_col_attached)
                return

        columns = cols_w_table

        tables = set([c.table for c in columns])
        if len(tables) == 1:
            self._set_parent_with_dispatch(tables.pop())
        elif len(tables) > 1 and not self._allow_multiple_tables:
            table = columns[0].table
            others = [c for c in columns[1:] if c.table is not table]
            if others:
                raise exc.ArgumentError(
                    "Column(s) %s are not part of table '%s'." %
                    (", ".join("'%s'" % c for c in others),
                        table.description)
                )

    def _set_parent(self, table):
        for col in self._pending_colargs:
            if isinstance(col, util.string_types):
                col = table.c[col]
            self.columns.add(col)


class ColumnCollectionConstraint(ColumnCollectionMixin, Constraint):
    """A constraint that proxies a ColumnCollection."""

    def __init__(self, *columns, **kw):
        r"""
        :param \*columns:
          A sequence of column names or Column objects.

        :param name:
          Optional, the in-database name of this constraint.

        :param deferrable:
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        :param initially:
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.

        :param \**kw: other keyword arguments including dialect-specific
          arguments are propagated to the :class:`.Constraint` superclass.

        """
        _autoattach = kw.pop('_autoattach', True)
        Constraint.__init__(self, **kw)
        ColumnCollectionMixin.__init__(self, *columns, _autoattach=_autoattach)

    columns = None
    """A :class:`.ColumnCollection` representing the set of columns
    for this constraint.

    """

    def _set_parent(self, table):
        Constraint._set_parent(self, table)
        ColumnCollectionMixin._set_parent(self, table)

    def __contains__(self, x):
        return x in self.columns

    def copy(self, **kw):
        c = self.__class__(name=self.name, deferrable=self.deferrable,
                           initially=self.initially, *self.columns.keys())
        return self._schema_item_copy(c)

    def contains_column(self, col):
        """Return True if this constraint contains the given column.

        Note that this object also contains an attribute ``.columns``
        which is a :class:`.ColumnCollection` of :class:`.Column` objects.

        """

        return self.columns.contains_column(col)

    def __iter__(self):
        # inlining of
        # return iter(self.columns)
        # ColumnCollection->OrderedProperties->OrderedDict
        ordered_dict = self.columns._data
        return (ordered_dict[key] for key in ordered_dict._list)

    def __len__(self):
        return len(self.columns._data)


class CheckConstraint(ColumnCollectionConstraint):
    """A table- or column-level CHECK constraint.

    Can be included in the definition of a Table or Column.
    """

    _allow_multiple_tables = True

    def __init__(self, sqltext, name=None, deferrable=None,
                 initially=None, table=None, info=None, _create_rule=None,
                 _autoattach=True, _type_bound=False):
        r"""Construct a CHECK constraint.

        :param sqltext:
          A string containing the constraint definition, which will be used
          verbatim, or a SQL expression construct.   If given as a string,
          the object is converted to a :class:`.Text` object.   If the textual
          string includes a colon character, escape this using a backslash::

            CheckConstraint(r"foo ~ E'a(?\:b|c)d")

        :param name:
          Optional, the in-database name of the constraint.

        :param deferrable:
          Optional bool.  If set, emit DEFERRABLE or NOT DEFERRABLE when
          issuing DDL for this constraint.

        :param initially:
          Optional string.  If set, emit INITIALLY <value> when issuing DDL
          for this constraint.

        :param info: Optional data dictionary which will be populated into the
            :attr:`.SchemaItem.info` attribute of this object.

            .. versionadded:: 1.0.0

        """

        self.sqltext = _literal_as_text(sqltext, warn=False)

        columns = []
        visitors.traverse(self.sqltext, {}, {'column': columns.append})

        super(CheckConstraint, self).\
            __init__(
                name=name, deferrable=deferrable,
                initially=initially, _create_rule=_create_rule, info=info,
                _type_bound=_type_bound, _autoattach=_autoattach,
                *columns)
        if table is not None:
            self._set_parent_with_dispatch(table)

    def __visit_name__(self):
        if isinstance(self.parent, Table):
            return "check_constraint"
        else:
            return "column_check_constraint"
    __visit_name__ = property(__visit_name__)

    def copy(self, target_table=None, **kw):
        if target_table is not None:
            def replace(col):
                if self.table.c.contains_column(col):
                    return target_table.c[col.key]
                else:
                    return None
            sqltext = visitors.replacement_traverse(self.sqltext, {}, replace)
        else:
            sqltext = self.sqltext
        c = CheckConstraint(sqltext,
                            name=self.name,
                            initially=self.initially,
                            deferrable=self.deferrable,
                            _create_rule=self._create_rule,
                            table=target_table,
                            _autoattach=False,
                            _type_bound=self._type_bound)
        return self._schema_item_copy(c)


class ForeignKeyConstraint(ColumnCollectionConstraint):
    """A table-level FOREIGN KEY constraint.

    Defines a single column or composite FOREIGN KEY ... REFERENCES
    constraint. For a no-frills, single column foreign key, adding a
    :class:`.ForeignKey` to the definition of a :class:`.Column` is a
    shorthand equivalent for an unnamed, single column
    :class:`.ForeignKeyConstraint`.

    Examples of foreign key configuration are in :ref:`metadata_foreignkeys`.

    """
    __visit_name__ = 'foreign_key_constraint'

    def __init__(self, columns, refcolumns, name=None, onupdate=None,
                 ondelete=None, deferrable=None, initially=None,
                 use_alter=False, link_to_name=False, match=None,
                 table=None, info=None, **dialect_kw):
        r"""Construct a composite-capable FOREIGN KEY.

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
          the full collection of tables are dropped.

          The use of :paramref:`.ForeignKeyConstraint.use_alter` is
          particularly geared towards the case where two or more tables
          are established within a mutually-dependent foreign key constraint
          relationship; however, the :meth:`.MetaData.create_all` and
          :meth:`.MetaData.drop_all` methods will perform this resolution
          automatically, so the flag is normally not needed.

          .. versionchanged:: 1.0.0  Automatic resolution of foreign key
             cycles has been added, removing the need to use the
             :paramref:`.ForeignKeyConstraint.use_alter` in typical use
             cases.

          .. seealso::

                :ref:`use_alter`

        :param match: Optional string. If set, emit MATCH <value> when issuing
          DDL for this constraint. Typical values include SIMPLE, PARTIAL
          and FULL.

        :param info: Optional data dictionary which will be populated into the
            :attr:`.SchemaItem.info` attribute of this object.

            .. versionadded:: 1.0.0

        :param \**dialect_kw:  Additional keyword arguments are dialect
          specific, and passed in the form ``<dialectname>_<argname>``.  See
          the documentation regarding an individual dialect at
          :ref:`dialect_toplevel` for detail on documented arguments.

            .. versionadded:: 0.9.2

        """

        Constraint.__init__(
            self, name=name, deferrable=deferrable, initially=initially,
            info=info, **dialect_kw)
        self.onupdate = onupdate
        self.ondelete = ondelete
        self.link_to_name = link_to_name
        self.use_alter = use_alter
        self.match = match

        if len(set(columns)) != len(refcolumns):
            if len(set(columns)) != len(columns):
                # e.g. FOREIGN KEY (a, a) REFERENCES r (b, c)
                raise exc.ArgumentError(
                    "ForeignKeyConstraint with duplicate source column "
                    "references are not supported."
                )
            else:
                # e.g. FOREIGN KEY (a) REFERENCES r (b, c)
                # paraphrasing https://www.postgresql.org/docs/9.2/static/\
                # ddl-constraints.html
                raise exc.ArgumentError(
                    "ForeignKeyConstraint number "
                    "of constrained columns must match the number of "
                    "referenced columns.")

        # standalone ForeignKeyConstraint - create
        # associated ForeignKey objects which will be applied to hosted
        # Column objects (in col.foreign_keys), either now or when attached
        # to the Table for string-specified names
        self.elements = [
            ForeignKey(
                refcol,
                _constraint=self,
                name=self.name,
                onupdate=self.onupdate,
                ondelete=self.ondelete,
                use_alter=self.use_alter,
                link_to_name=self.link_to_name,
                match=self.match,
                deferrable=self.deferrable,
                initially=self.initially,
                **self.dialect_kwargs
            ) for refcol in refcolumns
        ]

        ColumnCollectionMixin.__init__(self, *columns)
        if table is not None:
            if hasattr(self, "parent"):
                assert table is self.parent
            self._set_parent_with_dispatch(table)

    def _append_element(self, column, fk):
        self.columns.add(column)
        self.elements.append(fk)

    columns = None
    """A :class:`.ColumnCollection` representing the set of columns
    for this constraint.

    """

    elements = None
    """A sequence of :class:`.ForeignKey` objects.

    Each :class:`.ForeignKey` represents a single referring column/referred
    column pair.

    This collection is intended to be read-only.

    """

    @property
    def _elements(self):
        # legacy - provide a dictionary view of (column_key, fk)
        return util.OrderedDict(
            zip(self.column_keys, self.elements)
        )

    @property
    def _referred_schema(self):
        for elem in self.elements:
            return elem._referred_schema
        else:
            return None

    @property
    def referred_table(self):
        """The :class:`.Table` object to which this
        :class:`.ForeignKeyConstraint` references.

        This is a dynamically calculated attribute which may not be available
        if the constraint and/or parent table is not yet associated with
        a metadata collection that contains the referred table.

        .. versionadded:: 1.0.0

        """
        return self.elements[0].column.table

    def _validate_dest_table(self, table):
        table_keys = set([elem._table_key()
                          for elem in self.elements])
        if None not in table_keys and len(table_keys) > 1:
            elem0, elem1 = sorted(table_keys)[0:2]
            raise exc.ArgumentError(
                'ForeignKeyConstraint on %s(%s) refers to '
                'multiple remote tables: %s and %s' % (
                    table.fullname,
                    self._col_description,
                    elem0,
                    elem1
                ))

    @property
    def column_keys(self):
        """Return a list of string keys representing the local
        columns in this :class:`.ForeignKeyConstraint`.

        This list is either the original string arguments sent
        to the constructor of the :class:`.ForeignKeyConstraint`,
        or if the constraint has been initialized with :class:`.Column`
        objects, is the string .key of each element.

        .. versionadded:: 1.0.0

        """
        if hasattr(self, "parent"):
            return self.columns.keys()
        else:
            return [
                col.key if isinstance(col, ColumnElement)
                else str(col) for col in self._pending_colargs
            ]

    @property
    def _col_description(self):
        return ", ".join(self.column_keys)

    def _set_parent(self, table):
        Constraint._set_parent(self, table)

        try:
            ColumnCollectionConstraint._set_parent(self, table)
        except KeyError as ke:
            raise exc.ArgumentError(
                "Can't create ForeignKeyConstraint "
                "on table '%s': no column "
                "named '%s' is present." % (table.description, ke.args[0]))

        for col, fk in zip(self.columns, self.elements):
            if not hasattr(fk, 'parent') or \
                    fk.parent is not col:
                fk._set_parent_with_dispatch(col)

        self._validate_dest_table(table)

    def copy(self, schema=None, target_table=None, **kw):
        fkc = ForeignKeyConstraint(
            [x.parent.key for x in self.elements],
            [x._get_colspec(
                schema=schema,
                table_name=target_table.name
                if target_table is not None
                and x._table_key() == x.parent.table.key
                else None)
             for x in self.elements],
            name=self.name,
            onupdate=self.onupdate,
            ondelete=self.ondelete,
            use_alter=self.use_alter,
            deferrable=self.deferrable,
            initially=self.initially,
            link_to_name=self.link_to_name,
            match=self.match
        )
        for self_fk, other_fk in zip(
                self.elements,
                fkc.elements):
            self_fk._schema_item_copy(other_fk)
        return self._schema_item_copy(fkc)


class PrimaryKeyConstraint(ColumnCollectionConstraint):
    """A table-level PRIMARY KEY constraint.

    The :class:`.PrimaryKeyConstraint` object is present automatically
    on any :class:`.Table` object; it is assigned a set of
    :class:`.Column` objects corresponding to those marked with
    the :paramref:`.Column.primary_key` flag::

        >>> my_table = Table('mytable', metadata,
        ...                 Column('id', Integer, primary_key=True),
        ...                 Column('version_id', Integer, primary_key=True),
        ...                 Column('data', String(50))
        ...     )
        >>> my_table.primary_key
        PrimaryKeyConstraint(
            Column('id', Integer(), table=<mytable>,
                   primary_key=True, nullable=False),
            Column('version_id', Integer(), table=<mytable>,
                   primary_key=True, nullable=False)
        )

    The primary key of a :class:`.Table` can also be specified by using
    a :class:`.PrimaryKeyConstraint` object explicitly; in this mode of usage,
    the "name" of the constraint can also be specified, as well as other
    options which may be recognized by dialects::

        my_table = Table('mytable', metadata,
                    Column('id', Integer),
                    Column('version_id', Integer),
                    Column('data', String(50)),
                    PrimaryKeyConstraint('id', 'version_id',
                                         name='mytable_pk')
                )

    The two styles of column-specification should generally not be mixed.
    An warning is emitted if the columns present in the
    :class:`.PrimaryKeyConstraint`
    don't match the columns that were marked as ``primary_key=True``, if both
    are present; in this case, the columns are taken strictly from the
    :class:`.PrimaryKeyConstraint` declaration, and those columns otherwise
    marked as ``primary_key=True`` are ignored.  This behavior is intended to
    be backwards compatible with previous behavior.

    .. versionchanged:: 0.9.2  Using a mixture of columns within a
       :class:`.PrimaryKeyConstraint` in addition to columns marked as
       ``primary_key=True`` now emits a warning if the lists don't match.
       The ultimate behavior of ignoring those columns marked with the flag
       only is currently maintained for backwards compatibility; this warning
       may raise an exception in a future release.

    For the use case where specific options are to be specified on the
    :class:`.PrimaryKeyConstraint`, but the usual style of using
    ``primary_key=True`` flags is still desirable, an empty
    :class:`.PrimaryKeyConstraint` may be specified, which will take on the
    primary key column collection from the :class:`.Table` based on the
    flags::

        my_table = Table('mytable', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('version_id', Integer, primary_key=True),
                    Column('data', String(50)),
                    PrimaryKeyConstraint(name='mytable_pk',
                                         mssql_clustered=True)
                )

    .. versionadded:: 0.9.2 an empty :class:`.PrimaryKeyConstraint` may now
       be specified for the purposes of establishing keyword arguments with
       the constraint, independently of the specification of "primary key"
       columns within the :class:`.Table` itself; columns marked as
       ``primary_key=True`` will be gathered into the empty constraint's
       column collection.

    """

    __visit_name__ = 'primary_key_constraint'

    def __init__(self, *columns, **kw):
        self._implicit_generated = kw.pop('_implicit_generated', False)
        super(PrimaryKeyConstraint, self).__init__(*columns, **kw)

    def _set_parent(self, table):
        super(PrimaryKeyConstraint, self)._set_parent(table)

        if table.primary_key is not self:
            table.constraints.discard(table.primary_key)
            table.primary_key = self
            table.constraints.add(self)

        table_pks = [c for c in table.c if c.primary_key]
        if self.columns and table_pks and \
                set(table_pks) != set(self.columns.values()):
            util.warn(
                "Table '%s' specifies columns %s as primary_key=True, "
                "not matching locally specified columns %s; setting the "
                "current primary key columns to %s. This warning "
                "may become an exception in a future release" %
                (
                    table.name,
                    ", ".join("'%s'" % c.name for c in table_pks),
                    ", ".join("'%s'" % c.name for c in self.columns),
                    ", ".join("'%s'" % c.name for c in self.columns)
                )
            )
            table_pks[:] = []

        for c in self.columns:
            c.primary_key = True
            c.nullable = False
        self.columns.extend(table_pks)

    def _reload(self, columns):
        """repopulate this :class:`.PrimaryKeyConstraint` given
        a set of columns.

        Existing columns in the table that are marked as primary_key=True
        are maintained.

        Also fires a new event.

        This is basically like putting a whole new
        :class:`.PrimaryKeyConstraint` object on the parent
        :class:`.Table` object without actually replacing the object.

        The ordering of the given list of columns is also maintained; these
        columns will be appended to the list of columns after any which
        are already present.

        """

        # set the primary key flag on new columns.
        # note any existing PK cols on the table also have their
        # flag still set.
        for col in columns:
            col.primary_key = True

        self.columns.extend(columns)

        PrimaryKeyConstraint._autoincrement_column._reset(self)
        self._set_parent_with_dispatch(self.table)

    def _replace(self, col):
        PrimaryKeyConstraint._autoincrement_column._reset(self)
        self.columns.replace(col)

    @property
    def columns_autoinc_first(self):
        autoinc = self._autoincrement_column

        if autoinc is not None:
            return [autoinc] + [c for c in self.columns if c is not autoinc]
        else:
            return list(self.columns)

    @util.memoized_property
    def _autoincrement_column(self):

        def _validate_autoinc(col, autoinc_true):
            if col.type._type_affinity is None or not issubclass(
                col.type._type_affinity,
                    type_api.INTEGERTYPE._type_affinity):
                if autoinc_true:
                    raise exc.ArgumentError(
                        "Column type %s on column '%s' is not "
                        "compatible with autoincrement=True" % (
                            col.type,
                            col
                        ))
                else:
                    return False
            elif not isinstance(col.default, (type(None), Sequence)) and \
                    not autoinc_true:
                    return False
            elif col.server_default is not None and not autoinc_true:
                return False
            elif (
                    col.foreign_keys and col.autoincrement
                    not in (True, 'ignore_fk')):
                return False
            return True

        if len(self.columns) == 1:
            col = list(self.columns)[0]

            if col.autoincrement is True:
                _validate_autoinc(col, True)
                return col
            elif (
                col.autoincrement in ('auto', 'ignore_fk') and
                    _validate_autoinc(col, False)
            ):
                return col

        else:
            autoinc = None
            for col in self.columns:
                if col.autoincrement is True:
                    _validate_autoinc(col, True)
                    if autoinc is not None:
                        raise exc.ArgumentError(
                            "Only one Column may be marked "
                            "autoincrement=True, found both %s and %s." %
                            (col.name, autoinc.name)
                        )
                    else:
                        autoinc = col

            return autoinc


class UniqueConstraint(ColumnCollectionConstraint):
    """A table-level UNIQUE constraint.

    Defines a single column or composite UNIQUE constraint. For a no-frills,
    single column constraint, adding ``unique=True`` to the ``Column``
    definition is a shorthand equivalent for an unnamed, single column
    UniqueConstraint.
    """

    __visit_name__ = 'unique_constraint'


class Index(DialectKWArgs, ColumnCollectionMixin, SchemaItem):
    """A table-level INDEX.

    Defines a composite (one or more column) INDEX.

    E.g.::

        sometable = Table("sometable", metadata,
                        Column("name", String(50)),
                        Column("address", String(100))
                    )

        Index("some_index", sometable.c.name)

    For a no-frills, single column index, adding
    :class:`.Column` also supports ``index=True``::

        sometable = Table("sometable", metadata,
                        Column("name", String(50), index=True)
                    )

    For a composite index, multiple columns can be specified::

        Index("some_index", sometable.c.name, sometable.c.address)

    Functional indexes are supported as well, typically by using the
    :data:`.func` construct in conjunction with table-bound
    :class:`.Column` objects::

        Index("some_index", func.lower(sometable.c.name))

    .. versionadded:: 0.8 support for functional and expression-based indexes.

    An :class:`.Index` can also be manually associated with a :class:`.Table`,
    either through inline declaration or using
    :meth:`.Table.append_constraint`.  When this approach is used, the names
    of the indexed columns can be specified as strings::

        Table("sometable", metadata,
                        Column("name", String(50)),
                        Column("address", String(100)),
                        Index("some_index", "name", "address")
                )

    To support functional or expression-based indexes in this form, the
    :func:`.text` construct may be used::

        from sqlalchemy import text

        Table("sometable", metadata,
                        Column("name", String(50)),
                        Column("address", String(100)),
                        Index("some_index", text("lower(name)"))
                )

    .. versionadded:: 0.9.5 the :func:`.text` construct may be used to
       specify :class:`.Index` expressions, provided the :class:`.Index`
       is explicitly associated with the :class:`.Table`.


    .. seealso::

        :ref:`schema_indexes` - General information on :class:`.Index`.

        :ref:`postgresql_indexes` - PostgreSQL-specific options available for
        the :class:`.Index` construct.

        :ref:`mysql_indexes` - MySQL-specific options available for the
        :class:`.Index` construct.

        :ref:`mssql_indexes` - MSSQL-specific options available for the
        :class:`.Index` construct.

    """

    __visit_name__ = 'index'

    def __init__(self, name, *expressions, **kw):
        r"""Construct an index object.

        :param name:
          The name of the index

        :param \*expressions:
          Column expressions to include in the index.   The expressions
          are normally instances of :class:`.Column`, but may also
          be arbitrary SQL expressions which ultimately refer to a
          :class:`.Column`.

        :param unique=False:
            Keyword only argument; if True, create a unique index.

        :param quote=None:
            Keyword only argument; whether to apply quoting to the name of
            the index.  Works in the same manner as that of
            :paramref:`.Column.quote`.

        :param info=None: Optional data dictionary which will be populated
            into the :attr:`.SchemaItem.info` attribute of this object.

            .. versionadded:: 1.0.0

        :param \**kw: Additional keyword arguments not mentioned above are
            dialect specific, and passed in the form
            ``<dialectname>_<argname>``. See the documentation regarding an
            individual dialect at :ref:`dialect_toplevel` for detail on
            documented arguments.

        """
        self.table = None

        columns = []
        processed_expressions = []
        for expr, column, strname, add_element in self.\
                _extract_col_expression_collection(expressions):
            if add_element is not None:
                columns.append(add_element)
            processed_expressions.append(expr)

        self.expressions = processed_expressions
        self.name = quoted_name(name, kw.pop("quote", None))
        self.unique = kw.pop('unique', False)
        if 'info' in kw:
            self.info = kw.pop('info')
        self._validate_dialect_kwargs(kw)

        # will call _set_parent() if table-bound column
        # objects are present
        ColumnCollectionMixin.__init__(self, *columns)

    def _set_parent(self, table):
        ColumnCollectionMixin._set_parent(self, table)

        if self.table is not None and table is not self.table:
            raise exc.ArgumentError(
                "Index '%s' is against table '%s', and "
                "cannot be associated with table '%s'." % (
                    self.name,
                    self.table.description,
                    table.description
                )
            )
        self.table = table
        table.indexes.add(self)

        self.expressions = [
            expr if isinstance(expr, ClauseElement)
            else colexpr
            for expr, colexpr in util.zip_longest(self.expressions,
                                                  self.columns)
        ]

    @property
    def bind(self):
        """Return the connectable associated with this Index."""

        return self.table.bind

    def create(self, bind=None):
        """Issue a ``CREATE`` statement for this
        :class:`.Index`, using the given :class:`.Connectable`
        for connectivity.

        .. seealso::

            :meth:`.MetaData.create_all`.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaGenerator, self)
        return self

    def drop(self, bind=None):
        """Issue a ``DROP`` statement for this
        :class:`.Index`, using the given :class:`.Connectable`
        for connectivity.

        .. seealso::

            :meth:`.MetaData.drop_all`.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper, self)

    def __repr__(self):
        return 'Index(%s)' % (
            ", ".join(
                [repr(self.name)] +
                [repr(e) for e in self.expressions] +
                (self.unique and ["unique=True"] or [])
            ))


DEFAULT_NAMING_CONVENTION = util.immutabledict({
    "ix": 'ix_%(column_0_label)s'
})


class MetaData(SchemaItem):
    """A collection of :class:`.Table` objects and their associated schema
    constructs.

    Holds a collection of :class:`.Table` objects as well as
    an optional binding to an :class:`.Engine` or
    :class:`.Connection`.  If bound, the :class:`.Table` objects
    in the collection and their columns may participate in implicit SQL
    execution.

    The :class:`.Table` objects themselves are stored in the
    :attr:`.MetaData.tables` dictionary.

    :class:`.MetaData` is a thread-safe object for read operations.
    Construction of new tables within a single :class:`.MetaData` object,
    either explicitly or via reflection, may not be completely thread-safe.

    .. seealso::

        :ref:`metadata_describing` - Introduction to database metadata

    """

    __visit_name__ = 'metadata'

    def __init__(self, bind=None, reflect=False, schema=None,
                 quote_schema=None,
                 naming_convention=DEFAULT_NAMING_CONVENTION,
                 info=None
                 ):
        """Create a new MetaData object.

        :param bind:
          An Engine or Connection to bind to.  May also be a string or URL
          instance, these are passed to create_engine() and this MetaData will
          be bound to the resulting engine.

        :param reflect:
          Optional, automatically load all tables from the bound database.
          Defaults to False. ``bind`` is required when this option is set.

          .. deprecated:: 0.8
                Please use the :meth:`.MetaData.reflect` method.

        :param schema:
           The default schema to use for the :class:`.Table`,
           :class:`.Sequence`, and potentially other objects associated with
           this :class:`.MetaData`. Defaults to ``None``.

           When this value is set, any :class:`.Table` or :class:`.Sequence`
           which specifies ``None`` for the schema parameter will instead
           have this schema name defined.  To build a :class:`.Table`
           or :class:`.Sequence` that still has ``None`` for the schema
           even when this parameter is present, use the :attr:`.BLANK_SCHEMA`
           symbol.

           .. note::

                As refered above, the :paramref:`.MetaData.schema` parameter
                only refers to the **default value** that will be applied to
                the :paramref:`.Table.schema` parameter of an incoming
                :class:`.Table` object.   It does not refer to how the
                :class:`.Table` is catalogued within the :class:`.MetaData`,
                which remains consistent vs. a :class:`.MetaData` collection
                that does not define this parameter.  The :class:`.Table`
                within the :class:`.MetaData` will still be keyed based on its
                schema-qualified name, e.g.
                ``my_metadata.tables["some_schema.my_table"]``.

                The current behavior of the :class:`.ForeignKey` object is to
                circumvent this restriction, where it can locate a table given
                the table name alone, where the schema will be assumed to be
                present from this value as specified on the owning
                :class:`.MetaData` collection.  However, this implies  that a
                table qualified with BLANK_SCHEMA cannot currently be referred
                to by string name from :class:`.ForeignKey`.    Other parts of
                SQLAlchemy such as Declarative may not have similar behaviors
                built in, however may do so in a future release, along with a
                consistent method of referring to a table in BLANK_SCHEMA.


           .. seealso::

                :paramref:`.Table.schema`

                :paramref:`.Sequence.schema`

        :param quote_schema:
            Sets the ``quote_schema`` flag for those :class:`.Table`,
            :class:`.Sequence`, and other objects which make usage of the
            local ``schema`` name.

        :param info: Optional data dictionary which will be populated into the
            :attr:`.SchemaItem.info` attribute of this object.

            .. versionadded:: 1.0.0

        :param naming_convention: a dictionary referring to values which
          will establish default naming conventions for :class:`.Constraint`
          and :class:`.Index` objects, for those objects which are not given
          a name explicitly.

          The keys of this dictionary may be:

          * a constraint or Index class, e.g. the :class:`.UniqueConstraint`,
            :class:`.ForeignKeyConstraint` class, the :class:`.Index` class

          * a string mnemonic for one of the known constraint classes;
            ``"fk"``, ``"pk"``, ``"ix"``, ``"ck"``, ``"uq"`` for foreign key,
            primary key, index, check, and unique constraint, respectively.

          * the string name of a user-defined "token" that can be used
            to define new naming tokens.

          The values associated with each "constraint class" or "constraint
          mnemonic" key are string naming templates, such as
          ``"uq_%(table_name)s_%(column_0_name)s"``,
          which describe how the name should be composed.  The values
          associated with user-defined "token" keys should be callables of the
          form ``fn(constraint, table)``, which accepts the constraint/index
          object and :class:`.Table` as arguments, returning a string
          result.

          The built-in names are as follows, some of which may only be
          available for certain types of constraint:

            * ``%(table_name)s`` - the name of the :class:`.Table` object
              associated with the constraint.

            * ``%(referred_table_name)s`` - the name of the :class:`.Table`
              object associated with the referencing target of a
              :class:`.ForeignKeyConstraint`.

            * ``%(column_0_name)s`` - the name of the :class:`.Column` at
              index position "0" within the constraint.

            * ``%(column_0_label)s`` - the label of the :class:`.Column` at
              index position "0", e.g. :attr:`.Column.label`

            * ``%(column_0_key)s`` - the key of the :class:`.Column` at
              index position "0", e.g. :attr:`.Column.key`

            * ``%(referred_column_0_name)s`` - the name of a :class:`.Column`
              at index position "0" referenced by a
              :class:`.ForeignKeyConstraint`.

            * ``%(constraint_name)s`` - a special key that refers to the
              existing name given to the constraint.  When this key is
              present, the :class:`.Constraint` object's existing name will be
              replaced with one that is composed from template string that
              uses this token. When this token is present, it is required that
              the :class:`.Constraint` is given an explicit name ahead of time.

            * user-defined: any additional token may be implemented by passing
              it along with a ``fn(constraint, table)`` callable to the
              naming_convention dictionary.

          .. versionadded:: 0.9.2

          .. seealso::

                :ref:`constraint_naming_conventions` - for detailed usage
                examples.

        """
        self.tables = util.immutabledict()
        self.schema = quoted_name(schema, quote_schema)
        self.naming_convention = naming_convention
        if info:
            self.info = info
        self._schemas = set()
        self._sequences = {}
        self._fk_memos = collections.defaultdict(list)

        self.bind = bind
        if reflect:
            util.warn_deprecated("reflect=True is deprecate; please "
                                 "use the reflect() method.")
            if not bind:
                raise exc.ArgumentError(
                    "A bind must be supplied in conjunction "
                    "with reflect=True")
            self.reflect()

    tables = None
    """A dictionary of :class:`.Table` objects keyed to their name or "table key".

    The exact key is that determined by the :attr:`.Table.key` attribute;
    for a table with no :attr:`.Table.schema` attribute, this is the same
    as :attr:`.Table.name`.  For a table with a schema, it is typically of the
    form ``schemaname.tablename``.

    .. seealso::

        :attr:`.MetaData.sorted_tables`

    """

    def __repr__(self):
        return 'MetaData(bind=%r)' % self.bind

    def __contains__(self, table_or_key):
        if not isinstance(table_or_key, util.string_types):
            table_or_key = table_or_key.key
        return table_or_key in self.tables

    def _add_table(self, name, schema, table):
        key = _get_table_key(name, schema)
        dict.__setitem__(self.tables, key, table)
        if schema:
            self._schemas.add(schema)

    def _remove_table(self, name, schema):
        key = _get_table_key(name, schema)
        removed = dict.pop(self.tables, key, None)
        if removed is not None:
            for fk in removed.foreign_keys:
                fk._remove_from_metadata(self)
        if self._schemas:
            self._schemas = set([t.schema
                                 for t in self.tables.values()
                                 if t.schema is not None])

    def __getstate__(self):
        return {'tables': self.tables,
                'schema': self.schema,
                'schemas': self._schemas,
                'sequences': self._sequences,
                'fk_memos': self._fk_memos,
                'naming_convention': self.naming_convention
                }

    def __setstate__(self, state):
        self.tables = state['tables']
        self.schema = state['schema']
        self.naming_convention = state['naming_convention']
        self._bind = None
        self._sequences = state['sequences']
        self._schemas = state['schemas']
        self._fk_memos = state['fk_memos']

    def is_bound(self):
        """True if this MetaData is bound to an Engine or Connection."""

        return self._bind is not None

    def bind(self):
        """An :class:`.Engine` or :class:`.Connection` to which this
        :class:`.MetaData` is bound.

        Typically, a :class:`.Engine` is assigned to this attribute
        so that "implicit execution" may be used, or alternatively
        as a means of providing engine binding information to an
        ORM :class:`.Session` object::

            engine = create_engine("someurl://")
            metadata.bind = engine

        .. seealso::

           :ref:`dbengine_implicit` - background on "bound metadata"

        """
        return self._bind

    @util.dependencies("sqlalchemy.engine.url")
    def _bind_to(self, url, bind):
        """Bind this MetaData to an Engine, Connection, string or URL."""

        if isinstance(bind, util.string_types + (url.URL, )):
            self._bind = sqlalchemy.create_engine(bind)
        else:
            self._bind = bind
    bind = property(bind, _bind_to)

    def clear(self):
        """Clear all Table objects from this MetaData."""

        dict.clear(self.tables)
        self._schemas.clear()
        self._fk_memos.clear()

    def remove(self, table):
        """Remove the given Table object from this MetaData."""

        self._remove_table(table.name, table.schema)

    @property
    def sorted_tables(self):
        """Returns a list of :class:`.Table` objects sorted in order of
        foreign key dependency.

        The sorting will place :class:`.Table` objects that have dependencies
        first, before the dependencies themselves, representing the
        order in which they can be created.   To get the order in which
        the tables would be dropped, use the ``reversed()`` Python built-in.

        .. warning::

            The :attr:`.sorted_tables` accessor cannot by itself accommodate
            automatic resolution of dependency cycles between tables, which
            are usually caused by mutually dependent foreign key constraints.
            To resolve these cycles, either the
            :paramref:`.ForeignKeyConstraint.use_alter` parameter may be appled
            to those constraints, or use the
            :func:`.schema.sort_tables_and_constraints` function which will break
            out foreign key constraints involved in cycles separately.

        .. seealso::

            :func:`.schema.sort_tables`

            :func:`.schema.sort_tables_and_constraints`

            :attr:`.MetaData.tables`

            :meth:`.Inspector.get_table_names`

            :meth:`.Inspector.get_sorted_table_and_fkc_names`


        """
        return ddl.sort_tables(sorted(self.tables.values(), key=lambda t: t.key))

    def reflect(self, bind=None, schema=None, views=False, only=None,
                extend_existing=False,
                autoload_replace=True,
                **dialect_kwargs):
        r"""Load all available table definitions from the database.

        Automatically creates ``Table`` entries in this ``MetaData`` for any
        table available in the database but not yet present in the
        ``MetaData``.  May be called multiple times to pick up tables recently
        added to the database, however no special action is taken if a table
        in this ``MetaData`` no longer exists in the database.

        :param bind:
          A :class:`.Connectable` used to access the database; if None, uses
          the existing bind on this ``MetaData``, if any.

        :param schema:
          Optional, query and reflect tables from an alterate schema.
          If None, the schema associated with this :class:`.MetaData`
          is used, if any.

        :param views:
          If True, also reflect views.

        :param only:
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

        :param extend_existing: Passed along to each :class:`.Table` as
          :paramref:`.Table.extend_existing`.

          .. versionadded:: 0.9.1

        :param autoload_replace: Passed along to each :class:`.Table` as
          :paramref:`.Table.autoload_replace`.

          .. versionadded:: 0.9.1

        :param \**dialect_kwargs: Additional keyword arguments not mentioned
         above are dialect specific, and passed in the form
         ``<dialectname>_<argname>``.  See the documentation regarding an
         individual dialect at :ref:`dialect_toplevel` for detail on
         documented arguments.

          .. versionadded:: 0.9.2 - Added
             :paramref:`.MetaData.reflect.**dialect_kwargs` to support
             dialect-level reflection options for all :class:`.Table`
             objects reflected.

        """
        if bind is None:
            bind = _bind_or_error(self)

        with bind.connect() as conn:

            reflect_opts = {
                'autoload': True,
                'autoload_with': conn,
                'extend_existing': extend_existing,
                'autoload_replace': autoload_replace,
                '_extend_on': set()
            }

            reflect_opts.update(dialect_kwargs)

            if schema is None:
                schema = self.schema

            if schema is not None:
                reflect_opts['schema'] = schema

            available = util.OrderedSet(
                bind.engine.table_names(schema, connection=conn))
            if views:
                available.update(
                    bind.dialect.get_view_names(conn, schema)
                )

            if schema is not None:
                available_w_schema = util.OrderedSet(["%s.%s" % (schema, name)
                                                      for name in available])
            else:
                available_w_schema = available

            current = set(self.tables)

            if only is None:
                load = [name for name, schname in
                        zip(available, available_w_schema)
                        if extend_existing or schname not in current]
            elif util.callable(only):
                load = [name for name, schname in
                        zip(available, available_w_schema)
                        if (extend_existing or schname not in current)
                        and only(name, self)]
            else:
                missing = [name for name in only if name not in available]
                if missing:
                    s = schema and (" schema '%s'" % schema) or ''
                    raise exc.InvalidRequestError(
                        'Could not reflect: requested table(s) not available '
                        'in %r%s: (%s)' %
                        (bind.engine, s, ', '.join(missing)))
                load = [name for name in only if extend_existing or
                        name not in current]

            for name in load:
                Table(name, self, **reflect_opts)

    def append_ddl_listener(self, event_name, listener):
        """Append a DDL event listener to this ``MetaData``.

        .. deprecated:: 0.7
            See :class:`.DDLEvents`.

        """
        def adapt_listener(target, connection, **kw):
            tables = kw['tables']
            listener(event, target, connection, tables=tables)

        event.listen(self, "" + event_name.replace('-', '_'), adapt_listener)

    def create_all(self, bind=None, tables=None, checkfirst=True):
        """Create all tables stored in this metadata.

        Conditional by default, will not attempt to recreate tables already
        present in the target database.

        :param bind:
          A :class:`.Connectable` used to access the
          database; if None, uses the existing bind on this ``MetaData``, if
          any.

        :param tables:
          Optional list of ``Table`` objects, which is a subset of the total
          tables in the ``MetaData`` (others are ignored).

        :param checkfirst:
          Defaults to True, don't issue CREATEs for tables already present
          in the target database.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaGenerator,
                          self,
                          checkfirst=checkfirst,
                          tables=tables)

    def drop_all(self, bind=None, tables=None, checkfirst=True):
        """Drop all tables stored in this metadata.

        Conditional by default, will not attempt to drop tables not present in
        the target database.

        :param bind:
          A :class:`.Connectable` used to access the
          database; if None, uses the existing bind on this ``MetaData``, if
          any.

        :param tables:
          Optional list of ``Table`` objects, which is a subset of the
          total tables in the ``MetaData`` (others are ignored).

        :param checkfirst:
          Defaults to True, only issue DROPs for tables confirmed to be
          present in the target database.

        """
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper,
                          self,
                          checkfirst=checkfirst,
                          tables=tables)


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

    @util.dependencies("sqlalchemy.engine.url")
    def _bind_to(self, url, bind):
        """Bind to a Connectable in the caller's thread."""

        if isinstance(bind, util.string_types + (url.URL, )):
            try:
                self.context._engine = self.__engines[bind]
            except KeyError:
                e = sqlalchemy.create_engine(bind)
                self.__engines[bind] = e
                self.context._engine = e
        else:
            # TODO: this is squirrely.  we shouldn't have to hold onto engines
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

        for e in self.__engines.values():
            if hasattr(e, 'dispose'):
                e.dispose()


class _SchemaTranslateMap(object):
    """Provide translation of schema names based on a mapping.

    Also provides helpers for producing cache keys and optimized
    access when no mapping is present.

    Used by the :paramref:`.Connection.execution_options.schema_translate_map`
    feature.

    .. versionadded:: 1.1


    """
    __slots__ = 'map_', '__call__', 'hash_key', 'is_default'

    _default_schema_getter = operator.attrgetter("schema")

    def __init__(self, map_):
        self.map_ = map_
        if map_ is not None:
            def schema_for_object(obj):
                effective_schema = self._default_schema_getter(obj)
                effective_schema = obj._translate_schema(
                    effective_schema, map_)
                return effective_schema
            self.__call__ = schema_for_object
            self.hash_key = ";".join(
                "%s=%s" % (k, map_[k])
                for k in sorted(map_, key=str)
            )
            self.is_default = False
        else:
            self.hash_key = 0
            self.__call__ = self._default_schema_getter
            self.is_default = True

    @classmethod
    def _schema_getter(cls, map_):
        if map_ is None:
            return _default_schema_map
        elif isinstance(map_, _SchemaTranslateMap):
            return map_
        else:
            return _SchemaTranslateMap(map_)

_default_schema_map = _SchemaTranslateMap(None)
_schema_getter = _SchemaTranslateMap._schema_getter

