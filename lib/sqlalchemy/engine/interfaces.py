# engine/interfaces.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define core interfaces used by the engine system."""

from .. import util

# backwards compat
from ..sql.compiler import Compiled, TypeCompiler


class Dialect(object):
    """Define the behavior of a specific database and DB-API combination.

    Any aspect of metadata definition, SQL query generation,
    execution, result-set handling, or anything else which varies
    between databases is defined under the general category of the
    Dialect.  The Dialect acts as a factory for other
    database-specific object implementations including
    ExecutionContext, Compiled, DefaultGenerator, and TypeEngine.

    All Dialects implement the following attributes:

    name
      identifying name for the dialect from a DBAPI-neutral point of view
      (i.e. 'sqlite')

    driver
      identifying name for the dialect's DBAPI

    positional
      True if the paramstyle for this Dialect is positional.

    paramstyle
      the paramstyle to be used (some DB-APIs support multiple
      paramstyles).

    convert_unicode
      True if Unicode conversion should be applied to all ``str``
      types.

    encoding
      type of encoding to use for unicode, usually defaults to
      'utf-8'.

    statement_compiler
      a :class:`.Compiled` class used to compile SQL statements

    ddl_compiler
      a :class:`.Compiled` class used to compile DDL statements

    server_version_info
      a tuple containing a version number for the DB backend in use.
      This value is only available for supporting dialects, and is
      typically populated during the initial connection to the database.

    default_schema_name
     the name of the default schema.  This value is only available for
     supporting dialects, and is typically populated during the
     initial connection to the database.

    execution_ctx_cls
      a :class:`.ExecutionContext` class used to handle statement execution

    execute_sequence_format
      either the 'tuple' or 'list' type, depending on what cursor.execute()
      accepts for the second argument (they vary).

    preparer
      a :class:`~sqlalchemy.sql.compiler.IdentifierPreparer` class used to
      quote identifiers.

    supports_alter
      ``True`` if the database supports ``ALTER TABLE``.

    max_identifier_length
      The maximum length of identifier names.

    supports_unicode_statements
      Indicate whether the DB-API can receive SQL statements as Python
      unicode strings

    supports_unicode_binds
      Indicate whether the DB-API can receive string bind parameters
      as Python unicode strings

    supports_sane_rowcount
      Indicate whether the dialect properly implements rowcount for
      ``UPDATE`` and ``DELETE`` statements.

    supports_sane_multi_rowcount
      Indicate whether the dialect properly implements rowcount for
      ``UPDATE`` and ``DELETE`` statements when executed via
      executemany.

    preexecute_autoincrement_sequences
      True if 'implicit' primary key functions must be executed separately
      in order to get their value.   This is currently oriented towards
      PostgreSQL.

    implicit_returning
      use RETURNING or equivalent during INSERT execution in order to load
      newly generated primary keys and other column defaults in one execution,
      which are then available via inserted_primary_key.
      If an insert statement has returning() specified explicitly,
      the "implicit" functionality is not used and inserted_primary_key
      will not be available.

    colspecs
      A dictionary of TypeEngine classes from sqlalchemy.types mapped
      to subclasses that are specific to the dialect class.  This
      dictionary is class-level only and is not accessed from the
      dialect instance itself.

    supports_default_values
      Indicates if the construct ``INSERT INTO tablename DEFAULT
      VALUES`` is supported

    supports_sequences
      Indicates if the dialect supports CREATE SEQUENCE or similar.

    sequences_optional
      If True, indicates if the "optional" flag on the Sequence() construct
      should signal to not generate a CREATE SEQUENCE. Applies only to
      dialects that support sequences. Currently used only to allow PostgreSQL
      SERIAL to be used on a column that specifies Sequence() for usage on
      other backends.

    supports_native_enum
      Indicates if the dialect supports a native ENUM construct.
      This will prevent types.Enum from generating a CHECK
      constraint when that type is used.

    supports_native_boolean
      Indicates if the dialect supports a native boolean construct.
      This will prevent types.Boolean from generating a CHECK
      constraint when that type is used.

    dbapi_exception_translation_map
       A dictionary of names that will contain as values the names of
       pep-249 exceptions ("IntegrityError", "OperationalError", etc)
       keyed to alternate class names, to support the case where a
       DBAPI has exception classes that aren't named as they are
       referred to (e.g. IntegrityError = MyException).   In the vast
       majority of cases this dictionary is empty.

       .. versionadded:: 1.0.5

    """

    _has_events = False

    def create_connect_args(self, url):
        """Build DB-API compatible connection arguments.

        Given a :class:`~sqlalchemy.engine.url.URL` object, returns a tuple
        consisting of a `*args`/`**kwargs` suitable to send directly
        to the dbapi's connect function.

        """

        raise NotImplementedError()

    @classmethod
    def type_descriptor(cls, typeobj):
        """Transform a generic type to a dialect-specific type.

        Dialect classes will usually use the
        :func:`.types.adapt_type` function in the types module to
        accomplish this.

        The returned result is cached *per dialect class* so can
        contain no dialect-instance state.

        """

        raise NotImplementedError()

    def initialize(self, connection):
        """Called during strategized creation of the dialect with a
        connection.

        Allows dialects to configure options based on server version info or
        other properties.

        The connection passed here is a SQLAlchemy Connection object,
        with full capabilities.

        The initialize() method of the base dialect should be called via
        super().

        """

        pass

    def reflecttable(
            self, connection, table, include_columns, exclude_columns):
        """Load table description from the database.

        Given a :class:`.Connection` and a
        :class:`~sqlalchemy.schema.Table` object, reflect its columns and
        properties from the database.

        The implementation of this method is provided by
        :meth:`.DefaultDialect.reflecttable`, which makes use of
        :class:`.Inspector` to retrieve column information.

        Dialects should **not** seek to implement this method, and should
        instead implement individual schema inspection operations such as
        :meth:`.Dialect.get_columns`, :meth:`.Dialect.get_pk_constraint`,
        etc.

        """

        raise NotImplementedError()

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return information about columns in `table_name`.

        Given a :class:`.Connection`, a string
        `table_name`, and an optional string `schema`, return column
        information as a list of dictionaries with these keys:

        name
          the column's name

        type
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        default
          the column's default value

        autoincrement
          boolean

        sequence
          a dictionary of the form
              {'name' : str, 'start' :int, 'increment': int, 'minvalue': int,
               'maxvalue': int, 'nominvalue': bool, 'nomaxvalue': bool,
               'cycle': bool, 'cache': int, 'order': bool}

        Additional column attributes may be present.
        """

        raise NotImplementedError()

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        """Return information about primary keys in `table_name`.


        Deprecated.  This method is only called by the default
        implementation of :meth:`.Dialect.get_pk_constraint`.  Dialects should
        instead implement the :meth:`.Dialect.get_pk_constraint` method
        directly.

        """

        raise NotImplementedError()

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Return information about the primary key constraint on
        table_name`.

        Given a :class:`.Connection`, a string
        `table_name`, and an optional string `schema`, return primary
        key information as a dictionary with these keys:

        constrained_columns
          a list of column names that make up the primary key

        name
          optional name of the primary key constraint.

        """
        raise NotImplementedError()

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Return information about foreign_keys in `table_name`.

        Given a :class:`.Connection`, a string
        `table_name`, and an optional string `schema`, return foreign
        key information as a list of dicts with these keys:

        name
          the constraint's name

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns
        """

        raise NotImplementedError()

    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names for `schema`."""

        raise NotImplementedError()

    def get_temp_table_names(self, connection, schema=None, **kw):
        """Return a list of temporary table names on the given connection,
        if supported by the underlying backend.

        """

        raise NotImplementedError()

    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of all view names available in the database.

        schema:
          Optional, retrieve names from a non-default schema.
        """

        raise NotImplementedError()

    def get_temp_view_names(self, connection, schema=None, **kw):
        """Return a list of temporary view names on the given connection,
        if supported by the underlying backend.

        """

        raise NotImplementedError()

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """Return view definition.

        Given a :class:`.Connection`, a string
        `view_name`, and an optional string `schema`, return the view
        definition.
        """

        raise NotImplementedError()

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return information about indexes in `table_name`.

        Given a :class:`.Connection`, a string
        `table_name` and an optional string `schema`, return index
        information as a list of dictionaries with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
        """

        raise NotImplementedError()

    def get_unique_constraints(
            self, connection, table_name, schema=None, **kw):
        r"""Return information about unique constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        unique constraint information as a list of dicts with these keys:

        name
          the unique constraint's name

        column_names
          list of column names in order

        \**kw
          other options passed to the dialect's get_unique_constraints()
          method.

        .. versionadded:: 0.9.0

        """

        raise NotImplementedError()

    def get_check_constraints(
            self, connection, table_name, schema=None, **kw):
        r"""Return information about check constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        check constraint information as a list of dicts with these keys:

        name
          the check constraint's name

        sqltext
          the check constraint's SQL expression

        \**kw
          other options passed to the dialect's get_check_constraints()
          method.

        .. versionadded:: 1.1.0

        """

        raise NotImplementedError()

    def get_table_comment(
            self, connection, table_name, schema=None, **kw):
        r"""Return the "comment" for the table identified by `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        table comment information as a dictionary with this key:

        text
           text of the comment

        Raises ``NotImplementedError`` for dialects that don't support
        comments.

        .. versionadded:: 1.2

        """

        raise NotImplementedError()

    def normalize_name(self, name):
        """convert the given name to lowercase if it is detected as
        case insensitive.

        this method is only used if the dialect defines
        requires_name_normalize=True.

        """
        raise NotImplementedError()

    def denormalize_name(self, name):
        """convert the given name to a case insensitive identifier
        for the backend if it is an all-lowercase name.

        this method is only used if the dialect defines
        requires_name_normalize=True.

        """
        raise NotImplementedError()

    def has_table(self, connection, table_name, schema=None):
        """Check the existence of a particular table in the database.

        Given a :class:`.Connection` object and a string
        `table_name`, return True if the given table (possibly within
        the specified `schema`) exists in the database, False
        otherwise.
        """

        raise NotImplementedError()

    def has_sequence(self, connection, sequence_name, schema=None):
        """Check the existence of a particular sequence in the database.

        Given a :class:`.Connection` object and a string
        `sequence_name`, return True if the given sequence exists in
        the database, False otherwise.
        """

        raise NotImplementedError()

    def _get_server_version_info(self, connection):
        """Retrieve the server version info from the given connection.

        This is used by the default implementation to populate the
        "server_version_info" attribute and is called exactly
        once upon first connect.

        """

        raise NotImplementedError()

    def _get_default_schema_name(self, connection):
        """Return the string name of the currently selected schema from
        the given connection.

        This is used by the default implementation to populate the
        "default_schema_name" attribute and is called exactly
        once upon first connect.

        """

        raise NotImplementedError()

    def do_begin(self, dbapi_connection):
        """Provide an implementation of ``connection.begin()``, given a
        DB-API connection.

        The DBAPI has no dedicated "begin" method and it is expected
        that transactions are implicit.  This hook is provided for those
        DBAPIs that might need additional help in this area.

        Note that :meth:`.Dialect.do_begin` is not called unless a
        :class:`.Transaction` object is in use.  The
        :meth:`.Dialect.do_autocommit`
        hook is provided for DBAPIs that need some extra commands emitted
        after a commit in order to enter the next transaction, when the
        SQLAlchemy :class:`.Connection` is used in its default "autocommit"
        mode.

        :param dbapi_connection: a DBAPI connection, typically
         proxied within a :class:`.ConnectionFairy`.

         """

        raise NotImplementedError()

    def do_rollback(self, dbapi_connection):
        """Provide an implementation of ``connection.rollback()``, given
        a DB-API connection.

        :param dbapi_connection: a DBAPI connection, typically
         proxied within a :class:`.ConnectionFairy`.

         """

        raise NotImplementedError()

    def do_commit(self, dbapi_connection):
        """Provide an implementation of ``connection.commit()``, given a
        DB-API connection.

        :param dbapi_connection: a DBAPI connection, typically
         proxied within a :class:`.ConnectionFairy`.

        """

        raise NotImplementedError()

    def do_close(self, dbapi_connection):
        """Provide an implementation of ``connection.close()``, given a DBAPI
        connection.

        This hook is called by the :class:`.Pool` when a connection has been
        detached from the pool, or is being returned beyond the normal
        capacity of the pool.

        .. versionadded:: 0.8

        """

        raise NotImplementedError()

    def create_xid(self):
        """Create a two-phase transaction ID.

        This id will be passed to do_begin_twophase(),
        do_rollback_twophase(), do_commit_twophase().  Its format is
        unspecified.
        """

        raise NotImplementedError()

    def do_savepoint(self, connection, name):
        """Create a savepoint with the given name.

        :param connection: a :class:`.Connection`.
        :param name: savepoint name.

        """

        raise NotImplementedError()

    def do_rollback_to_savepoint(self, connection, name):
        """Rollback a connection to the named savepoint.

        :param connection: a :class:`.Connection`.
        :param name: savepoint name.

        """

        raise NotImplementedError()

    def do_release_savepoint(self, connection, name):
        """Release the named savepoint on a connection.

        :param connection: a :class:`.Connection`.
        :param name: savepoint name.
        """

        raise NotImplementedError()

    def do_begin_twophase(self, connection, xid):
        """Begin a two phase transaction on the given connection.

        :param connection: a :class:`.Connection`.
        :param xid: xid

        """

        raise NotImplementedError()

    def do_prepare_twophase(self, connection, xid):
        """Prepare a two phase transaction on the given connection.

        :param connection: a :class:`.Connection`.
        :param xid: xid

        """

        raise NotImplementedError()

    def do_rollback_twophase(self, connection, xid, is_prepared=True,
                             recover=False):
        """Rollback a two phase transaction on the given connection.

        :param connection: a :class:`.Connection`.
        :param xid: xid
        :param is_prepared: whether or not
         :meth:`.TwoPhaseTransaction.prepare` was called.
        :param recover: if the recover flag was passed.

        """

        raise NotImplementedError()

    def do_commit_twophase(self, connection, xid, is_prepared=True,
                           recover=False):
        """Commit a two phase transaction on the given connection.


        :param connection: a :class:`.Connection`.
        :param xid: xid
        :param is_prepared: whether or not
         :meth:`.TwoPhaseTransaction.prepare` was called.
        :param recover: if the recover flag was passed.

        """

        raise NotImplementedError()

    def do_recover_twophase(self, connection):
        """Recover list of uncommitted prepared two phase transaction
        identifiers on the given connection.

        :param connection: a :class:`.Connection`.

        """

        raise NotImplementedError()

    def do_executemany(self, cursor, statement, parameters, context=None):
        """Provide an implementation of ``cursor.executemany(statement,
        parameters)``."""

        raise NotImplementedError()

    def do_execute(self, cursor, statement, parameters, context=None):
        """Provide an implementation of ``cursor.execute(statement,
        parameters)``."""

        raise NotImplementedError()

    def do_execute_no_params(self, cursor, statement, parameters,
                             context=None):
        """Provide an implementation of ``cursor.execute(statement)``.

        The parameter collection should not be sent.

        """

        raise NotImplementedError()

    def is_disconnect(self, e, connection, cursor):
        """Return True if the given DB-API error indicates an invalid
        connection"""

        raise NotImplementedError()

    def connect(self):
        """return a callable which sets up a newly created DBAPI connection.

        The callable accepts a single argument "conn" which is the
        DBAPI connection itself.  It has no return value.

        This is used to set dialect-wide per-connection options such as
        isolation modes, unicode modes, etc.

        If a callable is returned, it will be assembled into a pool listener
        that receives the direct DBAPI connection, with all wrappers removed.

        If None is returned, no listener will be generated.

        """
        return None

    def reset_isolation_level(self, dbapi_conn):
        """Given a DBAPI connection, revert its isolation to the default.

        Note that this is a dialect-level method which is used as part
        of the implementation of the :class:`.Connection` and
        :class:`.Engine`
        isolation level facilities; these APIs should be preferred for
        most typical use cases.

        .. seealso::

            :meth:`.Connection.get_isolation_level` - view current level

            :attr:`.Connection.default_isolation_level` - view default level

            :paramref:`.Connection.execution_options.isolation_level` -
            set per :class:`.Connection` isolation level

            :paramref:`.create_engine.isolation_level` -
            set per :class:`.Engine` isolation level

        """

        raise NotImplementedError()

    def set_isolation_level(self, dbapi_conn, level):
        """Given a DBAPI connection, set its isolation level.

        Note that this is a dialect-level method which is used as part
        of the implementation of the :class:`.Connection` and
        :class:`.Engine`
        isolation level facilities; these APIs should be preferred for
        most typical use cases.

        .. seealso::

            :meth:`.Connection.get_isolation_level` - view current level

            :attr:`.Connection.default_isolation_level` - view default level

            :paramref:`.Connection.execution_options.isolation_level` -
            set per :class:`.Connection` isolation level

            :paramref:`.create_engine.isolation_level` -
            set per :class:`.Engine` isolation level

        """

        raise NotImplementedError()

    def get_isolation_level(self, dbapi_conn):
        """Given a DBAPI connection, return its isolation level.

        When working with a :class:`.Connection` object, the corresponding
        DBAPI connection may be procured using the
        :attr:`.Connection.connection` accessor.

        Note that this is a dialect-level method which is used as part
        of the implementation of the :class:`.Connection` and
        :class:`.Engine` isolation level facilities;
        these APIs should be preferred for most typical use cases.


        .. seealso::

            :meth:`.Connection.get_isolation_level` - view current level

            :attr:`.Connection.default_isolation_level` - view default level

            :paramref:`.Connection.execution_options.isolation_level` -
            set per :class:`.Connection` isolation level

            :paramref:`.create_engine.isolation_level` -
            set per :class:`.Engine` isolation level


        """

        raise NotImplementedError()

    @classmethod
    def get_dialect_cls(cls, url):
        """Given a URL, return the :class:`.Dialect` that will be used.

        This is a hook that allows an external plugin to provide functionality
        around an existing dialect, by allowing the plugin to be loaded
        from the url based on an entrypoint, and then the plugin returns
        the actual dialect to be used.

        By default this just returns the cls.

        .. versionadded:: 1.0.3

        """
        return cls

    @classmethod
    def engine_created(cls, engine):
        """A convenience hook called before returning the final :class:`.Engine`.

        If the dialect returned a different class from the
        :meth:`.get_dialect_cls`
        method, then the hook is called on both classes, first on
        the dialect class returned by the :meth:`.get_dialect_cls` method and
        then on the class on which the method was called.

        The hook should be used by dialects and/or wrappers to apply special
        events to the engine or its components.   In particular, it allows
        a dialect-wrapping class to apply dialect-level events.

        .. versionadded:: 1.0.3

        """
        pass


class CreateEnginePlugin(object):
    """A set of hooks intended to augment the construction of an
    :class:`.Engine` object based on entrypoint names in a URL.

    The purpose of :class:`.CreateEnginePlugin` is to allow third-party
    systems to apply engine, pool and dialect level event listeners without
    the need for the target application to be modified; instead, the plugin
    names can be added to the database URL.  Target applications for
    :class:`.CreateEnginePlugin` include:

    * connection and SQL performance tools, e.g. which use events to track
      number of checkouts and/or time spent with statements

    * connectivity plugins such as proxies

    Plugins are registered using entry points in a similar way as that
    of dialects::

        entry_points={
            'sqlalchemy.plugins': [
                'myplugin = myapp.plugins:MyPlugin'
            ]

    A plugin that uses the above names would be invoked from a database
    URL as in::

        from sqlalchemy import create_engine

        engine = create_engine(
          "mysql+pymysql://scott:tiger@localhost/test?plugin=myplugin")

    The ``plugin`` argument supports multiple instances, so that a URL
    may specify multiple plugins; they are loaded in the order stated
    in the URL::

        engine = create_engine(
          "mysql+pymysql://scott:tiger@localhost/"
          "test?plugin=plugin_one&plugin=plugin_twp&plugin=plugin_three")

    A plugin can receive additional arguments from the URL string as
    well as from the keyword arguments passed to :func:`.create_engine`.
    The :class:`.URL` object and the keyword dictionary are passed to the
    constructor so that these arguments can be extracted from the url's
    :attr:`.URL.query` collection as well as from the dictionary::

        class MyPlugin(CreateEnginePlugin):
            def __init__(self, url, kwargs):
                self.my_argument_one = url.query.pop('my_argument_one')
                self.my_argument_two = url.query.pop('my_argument_two')
                self.my_argument_three = kwargs.pop('my_argument_three', None)

    Arguments like those illustrated above would be consumed from the
    following::

        from sqlalchemy import create_engine

        engine = create_engine(
          "mysql+pymysql://scott:tiger@localhost/"
          "test?plugin=myplugin&my_argument_one=foo&my_argument_two=bar",
          my_argument_three='bat')

    The URL and dictionary are used for subsequent setup of the engine
    as they are, so the plugin can modify their arguments in-place.
    Arguments that are only understood by the plugin should be popped
    or otherwise removed so that they aren't interpreted as erroneous
    arguments afterwards.

    When the engine creation process completes and produces the
    :class:`.Engine` object, it is again passed to the plugin via the
    :meth:`.CreateEnginePlugin.engine_created` hook.  In this hook, additional
    changes can be made to the engine, most typically involving setup of
    events (e.g. those defined in :ref:`core_event_toplevel`).

    .. versionadded:: 1.1

    """
    def __init__(self, url, kwargs):
        """Contruct a new :class:`.CreateEnginePlugin`.

        The plugin object is instantiated individually for each call
        to :func:`.create_engine`.  A single :class:`.Engine` will be
        passed to the :meth:`.CreateEnginePlugin.engine_created` method
        corresponding to this URL.

        :param url: the :class:`.URL` object.  The plugin should inspect
         what it needs here as well as remove its custom arguments from the
         :attr:`.URL.query` collection.  The URL can be modified in-place
         in any other way as well.
        :param kwargs: The keyword arguments passed to :func`.create_engine`.
         The plugin can read and modify this dictionary in-place, to affect
         the ultimate arguments used to create the engine.  It should
         remove its custom arguments from the dictionary as well.

        """
        self.url = url

    def handle_dialect_kwargs(self, dialect_cls, dialect_args):
        """parse and modify dialect kwargs"""

    def handle_pool_kwargs(self, pool_cls, pool_args):
        """parse and modify pool kwargs"""

    def engine_created(self, engine):
        """Receive the :class:`.Engine` object when it is fully constructed.

        The plugin may make additional changes to the engine, such as
        registering engine or connection pool events.

        """


class ExecutionContext(object):
    """A messenger object for a Dialect that corresponds to a single
    execution.

    ExecutionContext should have these data members:

    connection
      Connection object which can be freely used by default value
      generators to execute SQL.  This Connection should reference the
      same underlying connection/transactional resources of
      root_connection.

    root_connection
      Connection object which is the source of this ExecutionContext.  This
      Connection may have close_with_result=True set, in which case it can
      only be used once.

    dialect
      dialect which created this ExecutionContext.

    cursor
      DB-API cursor procured from the connection,

    compiled
      if passed to constructor, sqlalchemy.engine.base.Compiled object
      being executed,

    statement
      string version of the statement to be executed.  Is either
      passed to the constructor, or must be created from the
      sql.Compiled object by the time pre_exec() has completed.

    parameters
      bind parameters passed to the execute() method.  For compiled
      statements, this is a dictionary or list of dictionaries.  For
      textual statements, it should be in a format suitable for the
      dialect's paramstyle (i.e. dict or list of dicts for non
      positional, list or list of lists/tuples for positional).

    isinsert
      True if the statement is an INSERT.

    isupdate
      True if the statement is an UPDATE.

    should_autocommit
      True if the statement is a "committable" statement.

    prefetch_cols
      a list of Column objects for which a client-side default
      was fired off.  Applies to inserts and updates.

    postfetch_cols
      a list of Column objects for which a server-side default or
      inline SQL expression value was fired off.  Applies to inserts
      and updates.
    """

    exception = None
    """A DBAPI-level exception that was caught when this ExecutionContext
    attempted to execute a statement.

    This attribute is meaningful only within the
    :meth:`.ConnectionEvents.dbapi_error` event.

    .. versionadded:: 0.9.7

    .. seealso::

        :attr:`.ExecutionContext.is_disconnect`

        :meth:`.ConnectionEvents.dbapi_error`

    """

    is_disconnect = None
    """Boolean flag set to True or False when a DBAPI-level exception
    is caught when this ExecutionContext attempted to execute a statement.

    This attribute is meaningful only within the
    :meth:`.ConnectionEvents.dbapi_error` event.

    .. versionadded:: 0.9.7

    .. seealso::

        :attr:`.ExecutionContext.exception`

        :meth:`.ConnectionEvents.dbapi_error`

    """

    def create_cursor(self):
        """Return a new cursor generated from this ExecutionContext's
        connection.

        Some dialects may wish to change the behavior of
        connection.cursor(), such as postgresql which may return a PG
        "server side" cursor.
        """

        raise NotImplementedError()

    def pre_exec(self):
        """Called before an execution of a compiled statement.

        If a compiled statement was passed to this ExecutionContext,
        the `statement` and `parameters` datamembers must be
        initialized after this statement is complete.
        """

        raise NotImplementedError()

    def post_exec(self):
        """Called after the execution of a compiled statement.

        If a compiled statement was passed to this ExecutionContext,
        the `last_insert_ids`, `last_inserted_params`, etc.
        datamembers should be available after this method completes.
        """

        raise NotImplementedError()

    def result(self):
        """Return a result object corresponding to this ExecutionContext.

        Returns a ResultProxy.
        """

        raise NotImplementedError()

    def handle_dbapi_exception(self, e):
        """Receive a DBAPI exception which occurred upon execute, result
        fetch, etc."""

        raise NotImplementedError()

    def should_autocommit_text(self, statement):
        """Parse the given textual statement and return True if it refers to
        a "committable" statement"""

        raise NotImplementedError()

    def lastrow_has_defaults(self):
        """Return True if the last INSERT or UPDATE row contained
        inlined or database-side defaults.
        """

        raise NotImplementedError()

    def get_rowcount(self):
        """Return the DBAPI ``cursor.rowcount`` value, or in some
        cases an interpreted value.

        See :attr:`.ResultProxy.rowcount` for details on this.

        """

        raise NotImplementedError()


class Connectable(object):
    """Interface for an object which supports execution of SQL constructs.

    The two implementations of :class:`.Connectable` are
    :class:`.Connection` and :class:`.Engine`.

    Connectable must also implement the 'dialect' member which references a
    :class:`.Dialect` instance.

    """

    def connect(self, **kwargs):
        """Return a :class:`.Connection` object.

        Depending on context, this may be ``self`` if this object
        is already an instance of :class:`.Connection`, or a newly
        procured :class:`.Connection` if this object is an instance
        of :class:`.Engine`.

        """

    def contextual_connect(self):
        """Return a :class:`.Connection` object which may be part of an ongoing
        context.

        Depending on context, this may be ``self`` if this object
        is already an instance of :class:`.Connection`, or a newly
        procured :class:`.Connection` if this object is an instance
        of :class:`.Engine`.

        """

        raise NotImplementedError()

    @util.deprecated("0.7",
                     "Use the create() method on the given schema "
                     "object directly, i.e. :meth:`.Table.create`, "
                     ":meth:`.Index.create`, :meth:`.MetaData.create_all`")
    def create(self, entity, **kwargs):
        """Emit CREATE statements for the given schema entity.
        """

        raise NotImplementedError()

    @util.deprecated("0.7",
                     "Use the drop() method on the given schema "
                     "object directly, i.e. :meth:`.Table.drop`, "
                     ":meth:`.Index.drop`, :meth:`.MetaData.drop_all`")
    def drop(self, entity, **kwargs):
        """Emit DROP statements for the given schema entity.
        """

        raise NotImplementedError()

    def execute(self, object, *multiparams, **params):
        """Executes the given construct and returns a :class:`.ResultProxy`."""
        raise NotImplementedError()

    def scalar(self, object, *multiparams, **params):
        """Executes and returns the first column of the first row.

        The underlying cursor is closed after execution.
        """
        raise NotImplementedError()

    def _run_visitor(self, visitorcallable, element,
                     **kwargs):
        raise NotImplementedError()

    def _execute_clauseelement(self, elem, multiparams=None, params=None):
        raise NotImplementedError()


class ExceptionContext(object):
    """Encapsulate information about an error condition in progress.

    This object exists solely to be passed to the
    :meth:`.ConnectionEvents.handle_error` event, supporting an interface that
    can be extended without backwards-incompatibility.

    .. versionadded:: 0.9.7

    """

    connection = None
    """The :class:`.Connection` in use during the exception.

    This member is present, except in the case of a failure when
    first connecting.

    .. seealso::

        :attr:`.ExceptionContext.engine`


    """

    engine = None
    """The :class:`.Engine` in use during the exception.

    This member should always be present, even in the case of a failure
    when first connecting.

    .. versionadded:: 1.0.0

    """

    cursor = None
    """The DBAPI cursor object.

    May be None.

    """

    statement = None
    """String SQL statement that was emitted directly to the DBAPI.

    May be None.

    """

    parameters = None
    """Parameter collection that was emitted directly to the DBAPI.

    May be None.

    """

    original_exception = None
    """The exception object which was caught.

    This member is always present.

    """

    sqlalchemy_exception = None
    """The :class:`sqlalchemy.exc.StatementError` which wraps the original,
    and will be raised if exception handling is not circumvented by the event.

    May be None, as not all exception types are wrapped by SQLAlchemy.
    For DBAPI-level exceptions that subclass the dbapi's Error class, this
    field will always be present.

    """

    chained_exception = None
    """The exception that was returned by the previous handler in the
    exception chain, if any.

    If present, this exception will be the one ultimately raised by
    SQLAlchemy unless a subsequent handler replaces it.

    May be None.

    """

    execution_context = None
    """The :class:`.ExecutionContext` corresponding to the execution
    operation in progress.

    This is present for statement execution operations, but not for
    operations such as transaction begin/end.  It also is not present when
    the exception was raised before the :class:`.ExecutionContext`
    could be constructed.

    Note that the :attr:`.ExceptionContext.statement` and
    :attr:`.ExceptionContext.parameters` members may represent a
    different value than that of the :class:`.ExecutionContext`,
    potentially in the case where a
    :meth:`.ConnectionEvents.before_cursor_execute` event or similar
    modified the statement/parameters to be sent.

    May be None.

    """

    is_disconnect = None
    """Represent whether the exception as occurred represents a "disconnect"
    condition.

    This flag will always be True or False within the scope of the
    :meth:`.ConnectionEvents.handle_error` handler.

    SQLAlchemy will defer to this flag in order to determine whether or not
    the connection should be invalidated subsequently.    That is, by
    assigning to this flag, a "disconnect" event which then results in
    a connection and pool invalidation can be invoked or prevented by
    changing this flag.

    """

    invalidate_pool_on_disconnect = True
    """Represent whether all connections in the pool should be invalidated
    when a "disconnect" condition is in effect.

    Setting this flag to False within the scope of the
    :meth:`.ConnectionEvents.handle_error` event will have the effect such
    that the full collection of connections in the pool will not be
    invalidated during a disconnect; only the current connection that is the
    subject of the error will actually be invalidated.

    The purpose of this flag is for custom disconnect-handling schemes where
    the invalidation of other connections in the pool is to be performed
    based on other conditions, or even on a per-connection basis.

    .. versionadded:: 1.0.3

    """
