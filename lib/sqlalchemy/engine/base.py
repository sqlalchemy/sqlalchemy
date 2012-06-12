# engine/base.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Basic components for SQL execution and interfacing with DB-API.

Defines the basic components used to interface DB-API modules with
higher-level statement-construction, connection-management, execution
and result contexts.
"""

__all__ = [
    'BufferedColumnResultProxy', 'BufferedColumnRow',
    'BufferedRowResultProxy','Compiled', 'Connectable', 'Connection',
    'Dialect', 'Engine','ExecutionContext', 'NestedTransaction',
    'ResultProxy', 'RootTransaction','RowProxy', 'SchemaIterator',
    'StringIO', 'Transaction', 'TwoPhaseTransaction',
    'connection_memoize']

import inspect, StringIO, sys, operator
from itertools import izip
from sqlalchemy import exc, schema, util, types, log, interfaces, \
    event, events
from sqlalchemy.sql import expression, util as sql_util
from sqlalchemy import processors
import collections

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
      a :class:`~Compiled` class used to compile SQL statements

    ddl_compiler
      a :class:`~Compiled` class used to compile DDL statements

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
      Postgresql.

    implicit_returning
      use RETURNING or equivalent during INSERT execution in order to load 
      newly generated primary keys and other column defaults in one execution,
      which are then available via inserted_primary_key.
      If an insert statement has returning() specified explicitly, 
      the "implicit" functionality is not used and inserted_primary_key
      will not be available.

    dbapi_type_map
      A mapping of DB-API type objects present in this Dialect's
      DB-API implementation mapped to TypeEngine implementations used
      by the dialect.

      This is used to apply types to result sets based on the DB-API
      types present in cursor.description; it only takes effect for
      result sets against textual statements where no explicit
      typemap was present.

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
      dialects that support sequences. Currently used only to allow Postgresql
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

    """

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
        :func:`~sqlalchemy.types.adapt_type` function in the types module to
        make this job easy.

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

        The initalize() method of the base dialect should be called via
        super().

        """

        pass

    def reflecttable(self, connection, table, include_columns=None):
        """Load table description from the database.

        Given a :class:`.Connection` and a
        :class:`~sqlalchemy.schema.Table` object, reflect its columns and
        properties from the database.  If include_columns (a list or
        set) is specified, limit the autoload to the given column
        names.

        The default implementation uses the 
        :class:`~sqlalchemy.engine.reflection.Inspector` interface to 
        provide the output, building upon the granular table/column/
        constraint etc. methods of :class:`.Dialect`.

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
              {'name' : str, 'start' :int, 'increment': int}

        Additional column attributes may be present.
        """

        raise NotImplementedError()

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        """Return information about primary keys in `table_name`.

        Given a :class:`.Connection`, a string
        `table_name`, and an optional string `schema`, return primary
        key information as a list of column names.

        """
        raise NotImplementedError()

    def get_pk_constraint(self, table_name, schema=None, **kw):
        """Return information about the primary key constraint on
        table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        primary key information as a dictionary with these keys:

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

        raise NotImplementedError

    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of all view names available in the database.

        schema:
          Optional, retrieve names from a non-default schema.
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

    def do_begin(self, connection):
        """Provide an implementation of *connection.begin()*, given a 
        DB-API connection."""

        raise NotImplementedError()

    def do_rollback(self, connection):
        """Provide an implementation of *connection.rollback()*, given 
        a DB-API connection."""

        raise NotImplementedError()

    def create_xid(self):
        """Create a two-phase transaction ID.

        This id will be passed to do_begin_twophase(),
        do_rollback_twophase(), do_commit_twophase().  Its format is
        unspecified.
        """

        raise NotImplementedError()

    def do_commit(self, connection):
        """Provide an implementation of *connection.commit()*, given a 
        DB-API connection."""

        raise NotImplementedError()

    def do_savepoint(self, connection, name):
        """Create a savepoint with the given name on a SQLAlchemy
        connection."""

        raise NotImplementedError()

    def do_rollback_to_savepoint(self, connection, name):
        """Rollback a SQL Alchemy connection to the named savepoint."""

        raise NotImplementedError()

    def do_release_savepoint(self, connection, name):
        """Release the named savepoint on a SQL Alchemy connection."""

        raise NotImplementedError()

    def do_begin_twophase(self, connection, xid):
        """Begin a two phase transaction on the given connection."""

        raise NotImplementedError()

    def do_prepare_twophase(self, connection, xid):
        """Prepare a two phase transaction on the given connection."""

        raise NotImplementedError()

    def do_rollback_twophase(self, connection, xid, is_prepared=True,
                            recover=False):
        """Rollback a two phase transaction on the given connection."""

        raise NotImplementedError()

    def do_commit_twophase(self, connection, xid, is_prepared=True,
                            recover=False):
        """Commit a two phase transaction on the given connection."""

        raise NotImplementedError()

    def do_recover_twophase(self, connection):
        """Recover list of uncommited prepared two phase transaction
        identifiers on the given connection."""

        raise NotImplementedError()

    def do_executemany(self, cursor, statement, parameters, context=None):
        """Provide an implementation of ``cursor.executemany(statement,
        parameters)``."""

        raise NotImplementedError()

    def do_execute(self, cursor, statement, parameters, context=None):
        """Provide an implementation of ``cursor.execute(statement,
        parameters)``."""

        raise NotImplementedError()

    def do_execute_no_params(self, cursor, statement, parameters, context=None):
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
        """Given a DBAPI connection, revert its isolation to the default."""

        raise NotImplementedError()

    def set_isolation_level(self, dbapi_conn, level):
        """Given a DBAPI connection, set its isolation level."""

        raise NotImplementedError()

    def get_isolation_level(self, dbapi_conn):
        """Given a DBAPI connection, return its isolation level."""

        raise NotImplementedError()


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

    postfetch_cols
      a list of Column objects for which a server-side default or
      inline SQL expression value was fired off.  Applies to inserts
      and updates.
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


class Compiled(object):
    """Represent a compiled SQL or DDL expression.

    The ``__str__`` method of the ``Compiled`` object should produce
    the actual text of the statement.  ``Compiled`` objects are
    specific to their underlying database dialect, and also may
    or may not be specific to the columns referenced within a
    particular set of bind parameters.  In no case should the
    ``Compiled`` object be dependent on the actual values of those
    bind parameters, even though it may reference those values as
    defaults.
    """

    def __init__(self, dialect, statement, bind=None):
        """Construct a new ``Compiled`` object.

        :param dialect: ``Dialect`` to compile against.

        :param statement: ``ClauseElement`` to be compiled.

        :param bind: Optional Engine or Connection to compile this 
          statement against.
        """

        self.dialect = dialect
        self.bind = bind
        if statement is not None:
            self.statement = statement
            self.can_execute = statement.supports_execution
            self.string = self.process(self.statement)

    @util.deprecated("0.7", ":class:`.Compiled` objects now compile "
                        "within the constructor.")
    def compile(self):
        """Produce the internal string representation of this element."""
        pass

    @property
    def sql_compiler(self):
        """Return a Compiled that is capable of processing SQL expressions.

        If this compiler is one, it would likely just return 'self'.

        """

        raise NotImplementedError()

    def process(self, obj, **kwargs):
        return obj._compiler_dispatch(self, **kwargs)

    def __str__(self):
        """Return the string text of the generated SQL or DDL."""

        return self.string or ''

    def construct_params(self, params=None):
        """Return the bind params for this compiled object.

        :param params: a dict of string/object pairs whose values will
                       override bind values compiled in to the
                       statement.
        """

        raise NotImplementedError()

    @property
    def params(self):
        """Return the bind params for this compiled object."""
        return self.construct_params()

    def execute(self, *multiparams, **params):
        """Execute this compiled object."""

        e = self.bind
        if e is None:
            raise exc.UnboundExecutionError(
                        "This Compiled object is not bound to any Engine "
                        "or Connection.")
        return e._execute_compiled(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Execute this compiled object and return the result's 
        scalar value."""

        return self.execute(*multiparams, **params).scalar()


class TypeCompiler(object):
    """Produces DDL specification for TypeEngine objects."""

    def __init__(self, dialect):
        self.dialect = dialect

    def process(self, type_):
        return type_._compiler_dispatch(self)


class Connectable(object):
    """Interface for an object which supports execution of SQL constructs.

    The two implementations of :class:`.Connectable` are :class:`.Connection` and
    :class:`.Engine`.

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

    @util.deprecated("0.7", "Use the create() method on the given schema "
                            "object directly, i.e. :meth:`.Table.create`, "
                            ":meth:`.Index.create`, :meth:`.MetaData.create_all`")
    def create(self, entity, **kwargs):
        """Emit CREATE statements for the given schema entity."""

        raise NotImplementedError()

    @util.deprecated("0.7", "Use the drop() method on the given schema "
                            "object directly, i.e. :meth:`.Table.drop`, "
                            ":meth:`.Index.drop`, :meth:`.MetaData.drop_all`")
    def drop(self, entity, **kwargs):
        """Emit DROP statements for the given schema entity."""

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


class Connection(Connectable):
    """Provides high-level functionality for a wrapped DB-API connection.

    Provides execution support for string-based SQL statements as well as
    :class:`.ClauseElement`, :class:`.Compiled` and :class:`.DefaultGenerator`
    objects. Provides a :meth:`begin` method to return :class:`.Transaction`
    objects.

    The Connection object is **not** thread-safe.  While a Connection can be
    shared among threads using properly synchronized access, it is still
    possible that the underlying DBAPI connection may not support shared
    access between threads.  Check the DBAPI documentation for details.

    The Connection object represents a single dbapi connection checked out
    from the connection pool. In this state, the connection pool has no affect
    upon the connection, including its expiration or timeout state. For the
    connection pool to properly manage connections, connections should be
    returned to the connection pool (i.e. ``connection.close()``) whenever the
    connection is not in use.

    .. index::
      single: thread safety; Connection

    """

    def __init__(self, engine, connection=None, close_with_result=False,
                 _branch=False, _execution_options=None):
        """Construct a new Connection.

        The constructor here is not public and is only called only by an
        :class:`.Engine`. See :meth:`.Engine.connect` and
        :meth:`.Engine.contextual_connect` methods.

        """
        self.engine = engine
        self.dialect = engine.dialect
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.should_close_with_result = close_with_result
        self.__savepoint_seq = 0
        self.__branch = _branch
        self.__invalid = False
        self._has_events = engine._has_events
        self._echo = self.engine._should_log_info()
        if _execution_options:
            self._execution_options =\
                engine._execution_options.union(_execution_options)
        else:
            self._execution_options = engine._execution_options

    def _branch(self):
        """Return a new Connection which references this Connection's
        engine and connection; but does not have close_with_result enabled,
        and also whose close() method does nothing.

        This is used to execute "sub" statements within a single execution,
        usually an INSERT statement.
        """

        return self.engine._connection_cls(
                                self.engine, 
                                self.__connection, _branch=True)

    def _clone(self):
        """Create a shallow copy of this Connection.

        """
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        return c

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def execution_options(self, **opt):
        """ Set non-SQL options for the connection which take effect 
        during execution.

        The method returns a copy of this :class:`.Connection` which references
        the same underlying DBAPI connection, but also defines the given
        execution options which will take effect for a call to
        :meth:`execute`. As the new :class:`.Connection` references the same
        underlying resource, it is probably best to ensure that the copies
        would be discarded immediately, which is implicit if used as in::

            result = connection.execution_options(stream_results=True).\\
                                execute(stmt)

        :meth:`.Connection.execution_options` accepts all options as those
        accepted by :meth:`.Executable.execution_options`.  Additionally,
        it includes options that are applicable only to 
        :class:`.Connection`.

        :param autocommit: Available on: Connection, statement.
          When True, a COMMIT will be invoked after execution 
          when executed in 'autocommit' mode, i.e. when an explicit
          transaction is not begun on the connection. Note that DBAPI
          connections by default are always in a transaction - SQLAlchemy uses
          rules applied to different kinds of statements to determine if
          COMMIT will be invoked in order to provide its "autocommit" feature.
          Typically, all INSERT/UPDATE/DELETE statements as well as
          CREATE/DROP statements have autocommit behavior enabled; SELECT
          constructs do not. Use this option when invoking a SELECT or other
          specific SQL construct where COMMIT is desired (typically when
          calling stored procedures and such), and an explicit
          transaction is not in progress.

        :param compiled_cache: Available on: Connection.
          A dictionary where :class:`.Compiled` objects
          will be cached when the :class:`.Connection` compiles a clause 
          expression into a :class:`.Compiled` object.
          It is the user's responsibility to
          manage the size of this dictionary, which will have keys
          corresponding to the dialect, clause element, the column
          names within the VALUES or SET clause of an INSERT or UPDATE, 
          as well as the "batch" mode for an INSERT or UPDATE statement.
          The format of this dictionary is not guaranteed to stay the
          same in future releases.

          Note that the ORM makes use of its own "compiled" caches for 
          some operations, including flush operations.  The caching
          used by the ORM internally supersedes a cache dictionary
          specified here.

        :param isolation_level: Available on: Connection.
          Set the transaction isolation level for
          the lifespan of this connection.   Valid values include
          those string values accepted by the ``isolation_level``
          parameter passed to :func:`.create_engine`, and are
          database specific, including those for :ref:`sqlite_toplevel`, 
          :ref:`postgresql_toplevel` - see those dialect's documentation
          for further info.

          Note that this option necessarily affects the underlying 
          DBAPI connection for the lifespan of the originating 
          :class:`.Connection`, and is not per-execution. This 
          setting is not removed until the underlying DBAPI connection 
          is returned to the connection pool, i.e.
          the :meth:`.Connection.close` method is called.

        :param no_parameters: When ``True``, if the final parameter 
          list or dictionary is totally empty, will invoke the 
          statement on the cursor as ``cursor.execute(statement)``,
          not passing the parameter collection at all.
          Some DBAPIs such as psycopg2 and mysql-python consider
          percent signs as significant only when parameters are 
          present; this option allows code to generate SQL
          containing percent signs (and possibly other characters)
          that is neutral regarding whether it's executed by the DBAPI
          or piped into a script that's later invoked by 
          command line tools.

          .. versionadded:: 0.7.6

        :param stream_results: Available on: Connection, statement.
          Indicate to the dialect that results should be 
          "streamed" and not pre-buffered, if possible.  This is a limitation
          of many DBAPIs.  The flag is currently understood only by the
          psycopg2 dialect.

        """
        c = self._clone()
        c._execution_options = c._execution_options.union(opt)
        if 'isolation_level' in opt:
            c._set_isolation_level()
        return c

    def _set_isolation_level(self):
        self.dialect.set_isolation_level(self.connection, 
                                self._execution_options['isolation_level'])
        self.connection._connection_record.finalize_callback = \
                    self.dialect.reset_isolation_level

    @property
    def closed(self):
        """Return True if this connection is closed."""

        return not self.__invalid and '_Connection__connection' \
                        not in self.__dict__

    @property
    def invalidated(self):
        """Return True if this connection was invalidated."""

        return self.__invalid

    @property
    def connection(self):
        "The underlying DB-API connection managed by this Connection."

        try:
            return self.__connection
        except AttributeError:
            return self._revalidate_connection()

    def _revalidate_connection(self):
        if self.__invalid:
            if self.__transaction is not None:
                raise exc.InvalidRequestError(
                                "Can't reconnect until invalid "
                                "transaction is rolled back")
            self.__connection = self.engine.raw_connection()
            self.__invalid = False
            return self.__connection
        raise exc.ResourceClosedError("This Connection is closed")

    @property
    def _connection_is_valid(self):
        # use getattr() for is_valid to support exceptions raised in
        # dialect initializer, where the connection is not wrapped in
        # _ConnectionFairy

        return getattr(self.__connection, 'is_valid', False)

    @property
    def _still_open_and_connection_is_valid(self):
        return \
            not self.closed and \
            not self.invalidated and \
            getattr(self.__connection, 'is_valid', False)

    @property
    def info(self):
        """A collection of per-DB-API connection instance properties."""

        return self.connection.info

    def connect(self):
        """Returns self.

        This ``Connectable`` interface method returns self, allowing
        Connections to be used interchangeably with Engines in most
        situations that require a bind.
        """

        return self

    def contextual_connect(self, **kwargs):
        """Returns self.

        This ``Connectable`` interface method returns self, allowing
        Connections to be used interchangeably with Engines in most
        situations that require a bind.
        """

        return self

    def invalidate(self, exception=None):
        """Invalidate the underlying DBAPI connection associated with 
        this Connection.

        The underlying DB-API connection is literally closed (if
        possible), and is discarded.  Its source connection pool will
        typically lazily create a new connection to replace it.

        Upon the next usage, this Connection will attempt to reconnect
        to the pool with a new connection.

        Transactions in progress remain in an "opened" state (even though the
        actual transaction is gone); these must be explicitly rolled back
        before a reconnect on this Connection can proceed. This is to prevent
        applications from accidentally continuing their transactional
        operations in a non-transactional state.

        """
        if self.invalidated:
            return

        if self.closed:
            raise exc.ResourceClosedError("This Connection is closed")

        if self._connection_is_valid:
            self.__connection.invalidate(exception)
        del self.__connection
        self.__invalid = True


    def detach(self):
        """Detach the underlying DB-API connection from its connection pool.

        This Connection instance will remain usable.  When closed,
        the DB-API connection will be literally closed and not
        returned to its pool.  The pool will typically lazily create a
        new connection to replace the detached connection.

        This method can be used to insulate the rest of an application
        from a modified state on a connection (such as a transaction
        isolation level or similar).  Also see
        :class:`~sqlalchemy.interfaces.PoolListener` for a mechanism to modify
        connection state when connections leave and return to their
        connection pool.
        """

        self.__connection.detach()

    def begin(self):
        """Begin a transaction and return a transaction handle.

        The returned object is an instance of :class:`.Transaction`.
        This object represents the "scope" of the transaction,
        which completes when either the :meth:`.Transaction.rollback`
        or :meth:`.Transaction.commit` method is called.

        Nested calls to :meth:`.begin` on the same :class:`.Connection`
        will return new :class:`.Transaction` objects that represent
        an emulated transaction within the scope of the enclosing 
        transaction, that is::
        
            trans = conn.begin()   # outermost transaction
            trans2 = conn.begin()  # "nested" 
            trans2.commit()        # does nothing
            trans.commit()         # actually commits
            
        Calls to :meth:`.Transaction.commit` only have an effect 
        when invoked via the outermost :class:`.Transaction` object, though the
        :meth:`.Transaction.rollback` method of any of the
        :class:`.Transaction` objects will roll back the
        transaction.

        See also:
        
        :meth:`.Connection.begin_nested` - use a SAVEPOINT
        
        :meth:`.Connection.begin_twophase` - use a two phase /XID transaction
        
        :meth:`.Engine.begin` - context manager available from :class:`.Engine`.

        """

        if self.__transaction is None:
            self.__transaction = RootTransaction(self)
            return self.__transaction
        else:
            return Transaction(self, self.__transaction)

    def begin_nested(self):
        """Begin a nested transaction and return a transaction handle.

        The returned object is an instance of :class:`.NestedTransaction`.

        Nested transactions require SAVEPOINT support in the
        underlying database.  Any transaction in the hierarchy may
        ``commit`` and ``rollback``, however the outermost transaction
        still controls the overall ``commit`` or ``rollback`` of the
        transaction of a whole.

        See also :meth:`.Connection.begin`, 
        :meth:`.Connection.begin_twophase`.
        """

        if self.__transaction is None:
            self.__transaction = RootTransaction(self)
        else:
            self.__transaction = NestedTransaction(self, self.__transaction)
        return self.__transaction

    def begin_twophase(self, xid=None):
        """Begin a two-phase or XA transaction and return a transaction
        handle.

        The returned object is an instance of :class:`.TwoPhaseTransaction`,
        which in addition to the methods provided by
        :class:`.Transaction`, also provides a :meth:`~.TwoPhaseTransaction.prepare`
        method.

        :param xid: the two phase transaction id.  If not supplied, a 
          random id will be generated.

        See also :meth:`.Connection.begin`, 
        :meth:`.Connection.begin_twophase`.

        """

        if self.__transaction is not None:
            raise exc.InvalidRequestError(
                "Cannot start a two phase transaction when a transaction "
                "is already in progress.")
        if xid is None:
            xid = self.engine.dialect.create_xid();
        self.__transaction = TwoPhaseTransaction(self, xid)
        return self.__transaction

    def recover_twophase(self):
        return self.engine.dialect.do_recover_twophase(self)

    def rollback_prepared(self, xid, recover=False):
        self.engine.dialect.do_rollback_twophase(self, xid, recover=recover)

    def commit_prepared(self, xid, recover=False):
        self.engine.dialect.do_commit_twophase(self, xid, recover=recover)

    def in_transaction(self):
        """Return True if a transaction is in progress."""

        return self.__transaction is not None

    def _begin_impl(self):
        if self._echo:
            self.engine.logger.info("BEGIN (implicit)")

        if self._has_events:
            self.engine.dispatch.begin(self)

        try:
            self.engine.dialect.do_begin(self.connection)
        except Exception, e:
            self._handle_dbapi_exception(e, None, None, None, None)
            raise

    def _rollback_impl(self):
        if self._has_events:
            self.engine.dispatch.rollback(self)

        if self._still_open_and_connection_is_valid:
            if self._echo:
                self.engine.logger.info("ROLLBACK")
            try:
                self.engine.dialect.do_rollback(self.connection)
                self.__transaction = None
            except Exception, e:
                self._handle_dbapi_exception(e, None, None, None, None)
                raise
        else:
            self.__transaction = None

    def _commit_impl(self):
        if self._has_events:
            self.engine.dispatch.commit(self)

        if self._echo:
            self.engine.logger.info("COMMIT")
        try:
            self.engine.dialect.do_commit(self.connection)
            self.__transaction = None
        except Exception, e:
            self._handle_dbapi_exception(e, None, None, None, None)
            raise

    def _savepoint_impl(self, name=None):
        if self._has_events:
            self.engine.dispatch.savepoint(self, name)

        if name is None:
            self.__savepoint_seq += 1
            name = 'sa_savepoint_%s' % self.__savepoint_seq
        if self._still_open_and_connection_is_valid:
            self.engine.dialect.do_savepoint(self, name)
            return name

    def _rollback_to_savepoint_impl(self, name, context):
        if self._has_events:
            self.engine.dispatch.rollback_savepoint(self, name, context)

        if self._still_open_and_connection_is_valid:
            self.engine.dialect.do_rollback_to_savepoint(self, name)
        self.__transaction = context

    def _release_savepoint_impl(self, name, context):
        if self._has_events:
            self.engine.dispatch.release_savepoint(self, name, context)

        if self._still_open_and_connection_is_valid:
            self.engine.dialect.do_release_savepoint(self, name)
        self.__transaction = context

    def _begin_twophase_impl(self, xid):
        if self._has_events:
            self.engine.dispatch.begin_twophase(self, xid)

        if self._still_open_and_connection_is_valid:
            self.engine.dialect.do_begin_twophase(self, xid)

    def _prepare_twophase_impl(self, xid):
        if self._has_events:
            self.engine.dispatch.prepare_twophase(self, xid)

        if self._still_open_and_connection_is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_prepare_twophase(self, xid)

    def _rollback_twophase_impl(self, xid, is_prepared):
        if self._has_events:
            self.engine.dispatch.rollback_twophase(self, xid, is_prepared)

        if self._still_open_and_connection_is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_rollback_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _commit_twophase_impl(self, xid, is_prepared):
        if self._has_events:
            self.engine.dispatch.commit_twophase(self, xid, is_prepared)

        if self._still_open_and_connection_is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_commit_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _autorollback(self):
        if not self.in_transaction():
            self._rollback_impl()

    def close(self):
        """Close this :class:`.Connection`.

        This results in a release of the underlying database
        resources, that is, the DBAPI connection referenced
        internally. The DBAPI connection is typically restored
        back to the connection-holding :class:`.Pool` referenced
        by the :class:`.Engine` that produced this
        :class:`.Connection`. Any transactional state present on
        the DBAPI connection is also unconditionally released via
        the DBAPI connection's ``rollback()`` method, regardless
        of any :class:`.Transaction` object that may be
        outstanding with regards to this :class:`.Connection`.

        After :meth:`~.Connection.close` is called, the
        :class:`.Connection` is permanently in a closed state,
        and will allow no further operations.

        """

        try:
            conn = self.__connection
        except AttributeError:
            return
        if not self.__branch:
            conn.close()
        self.__invalid = False
        del self.__connection
        self.__transaction = None

    def scalar(self, object, *multiparams, **params):
        """Executes and returns the first column of the first row.

        The underlying result/cursor is closed after execution.
        """

        return self.execute(object, *multiparams, **params).scalar()

    def execute(self, object, *multiparams, **params):
        """Executes the a SQL statement construct and returns a :class:`.ResultProxy`.

        :param object: The statement to be executed.  May be 
         one of:

         * a plain string
         * any :class:`.ClauseElement` construct that is also
           a subclass of :class:`.Executable`, such as a 
           :func:`~.expression.select` construct
         * a :class:`.FunctionElement`, such as that generated
           by :attr:`.func`, will be automatically wrapped in
           a SELECT statement, which is then executed.
         * a :class:`.DDLElement` object
         * a :class:`.DefaultGenerator` object
         * a :class:`.Compiled` object

        :param \*multiparams/\**params: represent bound parameter
         values to be used in the execution.   Typically,
         the format is either a collection of one or more
         dictionaries passed to \*multiparams::

             conn.execute(
                 table.insert(), 
                 {"id":1, "value":"v1"},
                 {"id":2, "value":"v2"}
             )

         ...or individual key/values interpreted by \**params::

             conn.execute(
                 table.insert(), id=1, value="v1"
             )

         In the case that a plain SQL string is passed, and the underlying 
         DBAPI accepts positional bind parameters, a collection of tuples
         or individual values in \*multiparams may be passed::
 
             conn.execute(
                 "INSERT INTO table (id, value) VALUES (?, ?)",
                 (1, "v1"), (2, "v2")
             )

             conn.execute(
                 "INSERT INTO table (id, value) VALUES (?, ?)",
                 1, "v1"
             )

         Note above, the usage of a question mark "?" or other
         symbol is contingent upon the "paramstyle" accepted by the DBAPI 
         in use, which may be any of "qmark", "named", "pyformat", "format",
         "numeric".   See `pep-249 <http://www.python.org/dev/peps/pep-0249/>`_ 
         for details on paramstyle.

         To execute a textual SQL statement which uses bound parameters in a
         DBAPI-agnostic way, use the :func:`~.expression.text` construct.

        """
        for c in type(object).__mro__:
            if c in Connection.executors:
                return Connection.executors[c](
                                                self, 
                                                object,
                                                multiparams, 
                                                params)
        else:
            raise exc.InvalidRequestError(
                                "Unexecutable object type: %s" %
                                type(object))

    def __distill_params(self, multiparams, params):
        """Given arguments from the calling form *multiparams, **params,
        return a list of bind parameter structures, usually a list of
        dictionaries.

        In the case of 'raw' execution which accepts positional parameters,
        it may be a list of tuples or lists.

        """

        if not multiparams:
            if params:
                return [params]
            else:
                return []
        elif len(multiparams) == 1:
            zero = multiparams[0]
            if isinstance(zero, (list, tuple)):
                if not zero or hasattr(zero[0], '__iter__') and \
                        not hasattr(zero[0], 'strip'):
                    return zero
                else:
                    return [zero]
            elif hasattr(zero, 'keys'):
                return [zero]
            else:
                return [[zero]]
        else:
            if hasattr(multiparams[0], '__iter__') and \
                not hasattr(multiparams[0], 'strip'):
                return multiparams
            else:
                return [multiparams]

    def _execute_function(self, func, multiparams, params):
        """Execute a sql.FunctionElement object."""

        return self._execute_clauseelement(func.select(), 
                                            multiparams, params)

    def _execute_default(self, default, multiparams, params):
        """Execute a schema.ColumnDefault object."""

        if self._has_events:
            for fn in self.engine.dispatch.before_execute:
                default, multiparams, params = \
                    fn(self, default, multiparams, params)

        try:
            try:
                conn = self.__connection
            except AttributeError:
                conn = self._revalidate_connection()

            dialect = self.dialect
            ctx = dialect.execution_ctx_cls._init_default(
                                dialect, self, conn)
        except Exception, e:
            self._handle_dbapi_exception(e, None, None, None, None)
            raise

        ret = ctx._exec_default(default, None)
        if self.should_close_with_result:
            self.close()

        if self._has_events:
            self.engine.dispatch.after_execute(self, 
                default, multiparams, params, ret)

        return ret

    def _execute_ddl(self, ddl, multiparams, params):
        """Execute a schema.DDL object."""

        if self._has_events:
            for fn in self.engine.dispatch.before_execute:
                ddl, multiparams, params = \
                    fn(self, ddl, multiparams, params)

        dialect = self.dialect

        compiled = ddl.compile(dialect=dialect)
        ret = self._execute_context(
            dialect,
            dialect.execution_ctx_cls._init_ddl,
            compiled, 
            None,
            compiled
        )
        if self._has_events:
            self.engine.dispatch.after_execute(self, 
                ddl, multiparams, params, ret)
        return ret

    def _execute_clauseelement(self, elem, multiparams, params):
        """Execute a sql.ClauseElement object."""

        if self._has_events:
            for fn in self.engine.dispatch.before_execute:
                elem, multiparams, params = \
                    fn(self, elem, multiparams, params)

        distilled_params = self.__distill_params(multiparams, params)
        if distilled_params:
            keys = distilled_params[0].keys()
        else:
            keys = []

        dialect = self.dialect
        if 'compiled_cache' in self._execution_options:
            key = dialect, elem, tuple(keys), len(distilled_params) > 1
            if key in self._execution_options['compiled_cache']:
                compiled_sql = self._execution_options['compiled_cache'][key]
            else:
                compiled_sql = elem.compile(
                                dialect=dialect, column_keys=keys, 
                                inline=len(distilled_params) > 1)
                self._execution_options['compiled_cache'][key] = compiled_sql
        else:
            compiled_sql = elem.compile(
                            dialect=dialect, column_keys=keys, 
                            inline=len(distilled_params) > 1)


        ret = self._execute_context(
            dialect,
            dialect.execution_ctx_cls._init_compiled,
            compiled_sql, 
            distilled_params,
            compiled_sql, distilled_params
        )
        if self._has_events:
            self.engine.dispatch.after_execute(self, 
                elem, multiparams, params, ret)
        return ret

    def _execute_compiled(self, compiled, multiparams, params):
        """Execute a sql.Compiled object."""

        if self._has_events:
            for fn in self.engine.dispatch.before_execute:
                compiled, multiparams, params = \
                    fn(self, compiled, multiparams, params)

        dialect = self.dialect
        parameters=self.__distill_params(multiparams, params)
        ret = self._execute_context(
            dialect,
            dialect.execution_ctx_cls._init_compiled,
            compiled, 
            parameters,
            compiled, parameters
        )
        if self._has_events:
            self.engine.dispatch.after_execute(self, 
                compiled, multiparams, params, ret)
        return ret

    def _execute_text(self, statement, multiparams, params):
        """Execute a string SQL statement."""

        if self._has_events:
            for fn in self.engine.dispatch.before_execute:
                statement, multiparams, params = \
                    fn(self, statement, multiparams, params)

        dialect = self.dialect
        parameters = self.__distill_params(multiparams, params)
        ret = self._execute_context(
            dialect,
            dialect.execution_ctx_cls._init_statement,
            statement, 
            parameters,
            statement, parameters
        )
        if self._has_events:
            self.engine.dispatch.after_execute(self, 
                statement, multiparams, params, ret)
        return ret

    def _execute_context(self, dialect, constructor, 
                                    statement, parameters, 
                                    *args):
        """Create an :class:`.ExecutionContext` and execute, returning
        a :class:`.ResultProxy`."""

        try:
            try:
                conn = self.__connection
            except AttributeError:
                conn = self._revalidate_connection()

            context = constructor(dialect, self, conn, *args)
        except Exception, e:
            self._handle_dbapi_exception(e, 
                        str(statement), parameters, 
                        None, None)
            raise

        if context.compiled:
            context.pre_exec()

        cursor, statement, parameters = context.cursor, \
                                        context.statement, \
                                        context.parameters

        if not context.executemany:
            parameters = parameters[0]

        if self._has_events:
            for fn in self.engine.dispatch.before_cursor_execute:
                statement, parameters = \
                            fn(self, cursor, statement, parameters, 
                                        context, context.executemany)

        if self._echo:
            self.engine.logger.info(statement)
            self.engine.logger.info("%r", 
                    sql_util._repr_params(parameters, batches=10))
        try:
            if context.executemany:
                self.dialect.do_executemany(
                                    cursor, 
                                    statement, 
                                    parameters, 
                                    context)
            elif not parameters and context.no_parameters:
                self.dialect.do_execute_no_params(
                                    cursor, 
                                    statement, 
                                    context)
            else:
                self.dialect.do_execute(
                                    cursor, 
                                    statement, 
                                    parameters, 
                                    context)
        except Exception, e:
            self._handle_dbapi_exception(
                                e, 
                                statement, 
                                parameters, 
                                cursor, 
                                context)
            raise


        if self._has_events:
            self.engine.dispatch.after_cursor_execute(self, cursor, 
                                                statement, 
                                                parameters, 
                                                context, 
                                                context.executemany)

        if context.compiled:
            context.post_exec()

            if context.isinsert and not context.executemany:
                context.post_insert()

        # create a resultproxy, get rowcount/implicit RETURNING
        # rows, close cursor if no further results pending
        result = context.get_result_proxy()

        if context.isinsert:
            if context._is_implicit_returning:
                context._fetch_implicit_returning(result)
                result.close(_autoclose_connection=False)
            elif not context._is_explicit_returning:
                result.close(_autoclose_connection=False)
        elif result._metadata is None:
            # no results, get rowcount 
            # (which requires open cursor on some drivers
            # such as kintersbasdb, mxodbc),
            result.rowcount
            result.close(_autoclose_connection=False)

        if self.__transaction is None and context.should_autocommit:
            self._commit_impl()

        if result.closed and self.should_close_with_result:
            self.close()

        return result

    def _cursor_execute(self, cursor, statement, parameters):
        """Execute a statement + params on the given cursor.

        Adds appropriate logging and exception handling.

        This method is used by DefaultDialect for special-case
        executions, such as for sequences and column defaults.
        The path of statement execution in the majority of cases 
        terminates at _execute_context().

        """
        if self._echo:
            self.engine.logger.info(statement)
            self.engine.logger.info("%r", parameters)
        try:
            self.dialect.do_execute(
                                cursor, 
                                statement, 
                                parameters)
        except Exception, e:
            self._handle_dbapi_exception(
                                e, 
                                statement, 
                                parameters, 
                                cursor,
                                None)
            raise

    def _safe_close_cursor(self, cursor):
        """Close the given cursor, catching exceptions
        and turning into log warnings.

        """
        try:
            cursor.close()
        except Exception, e:
            try:
                ex_text = str(e)
            except TypeError:
                ex_text = repr(e)
            self.connection._logger.warn("Error closing cursor: %s", ex_text)

            if isinstance(e, (SystemExit, KeyboardInterrupt)):
                raise

    def _handle_dbapi_exception(self, 
                                    e, 
                                    statement, 
                                    parameters, 
                                    cursor, 
                                    context):
        if getattr(self, '_reentrant_error', False):
            # Py3K
            #raise exc.DBAPIError.instance(statement, parameters, e, 
            #                               self.dialect.dbapi.Error) from e
            # Py2K
            raise exc.DBAPIError.instance(statement, 
                                            parameters, 
                                            e, 
                                            self.dialect.dbapi.Error), \
                                            None, sys.exc_info()[2]
            # end Py2K
        self._reentrant_error = True
        try:
            # non-DBAPI error - if we already got a context,
            # or theres no string statement, don't wrap it
            should_wrap = isinstance(e, self.dialect.dbapi.Error) or \
                (statement is not None and context is None)

            if should_wrap and context:
                if self._has_events:
                    self.engine.dispatch.dbapi_error(self, 
                                                    cursor, 
                                                    statement, 
                                                    parameters, 
                                                    context, 
                                                    e)
                context.handle_dbapi_exception(e)

            is_disconnect = isinstance(e, self.dialect.dbapi.Error) and \
                                self.dialect.is_disconnect(e, self.__connection, cursor)


            if is_disconnect:
                self.invalidate(e)
                self.engine.dispose()
            else:
                if cursor:
                    self._safe_close_cursor(cursor)
                self._autorollback()
                if self.should_close_with_result:
                    self.close()

            if not should_wrap:
                return

            # Py3K
            #raise exc.DBAPIError.instance(
            #                        statement, 
            #                        parameters, 
            #                        e, 
            #                        self.dialect.dbapi.Error,
            #                        connection_invalidated=is_disconnect) \
            #                        from e
            # Py2K
            raise exc.DBAPIError.instance(
                                    statement, 
                                    parameters, 
                                    e, 
                                    self.dialect.dbapi.Error,
                                    connection_invalidated=is_disconnect), \
                                    None, sys.exc_info()[2]
            # end Py2K

        finally:
            del self._reentrant_error

    # poor man's multimethod/generic function thingy
    executors = {
        expression.FunctionElement: _execute_function,
        expression.ClauseElement: _execute_clauseelement,
        Compiled: _execute_compiled,
        schema.SchemaItem: _execute_default,
        schema.DDLElement: _execute_ddl,
        basestring: _execute_text
    }

    @util.deprecated("0.7", "Use the create() method on the given schema "
                            "object directly, i.e. :meth:`.Table.create`, "
                            ":meth:`.Index.create`, :meth:`.MetaData.create_all`")
    def create(self, entity, **kwargs):
        """Emit CREATE statements for the given schema entity."""

        return self.engine.create(entity, connection=self, **kwargs)

    @util.deprecated("0.7", "Use the drop() method on the given schema "
                            "object directly, i.e. :meth:`.Table.drop`, "
                            ":meth:`.Index.drop`, :meth:`.MetaData.drop_all`")
    def drop(self, entity, **kwargs):
        """Emit DROP statements for the given schema entity."""

        return self.engine.drop(entity, connection=self, **kwargs)

    @util.deprecated("0.7", "Use autoload=True with :class:`.Table`, "
                        "or use the :class:`.Inspector` object.")
    def reflecttable(self, table, include_columns=None):
        """Load table description from the database.

        Given a :class:`.Table` object, reflect its columns and
        properties from the database, populating the given :class:`.Table`
        object with attributes..  If include_columns (a list or
        set) is specified, limit the autoload to the given column
        names.

        The default implementation uses the 
        :class:`.Inspector` interface to 
        provide the output, building upon the granular table/column/
        constraint etc. methods of :class:`.Dialect`.

        """
        return self.engine.reflecttable(table, self, include_columns)

    def default_schema_name(self):
        return self.engine.dialect.get_default_schema_name(self)

    def transaction(self, callable_, *args, **kwargs):
        """Execute the given function within a transaction boundary.

        The function is passed this :class:`.Connection` 
        as the first argument, followed by the given \*args and \**kwargs,
        e.g.::
        
            def do_something(conn, x, y):
                conn.execute("some statement", {'x':x, 'y':y})

            conn.transaction(do_something, 5, 10)

        The operations inside the function are all invoked within the
        context of a single :class:`.Transaction`.   
        Upon success, the transaction is committed.  If an 
        exception is raised, the transaction is rolled back
        before propagating the exception.

        .. note::

           The :meth:`.transaction` method is superseded by
           the usage of the Python ``with:`` statement, which can
           be used with :meth:`.Connection.begin`::
        
               with conn.begin():
                   conn.execute("some statement", {'x':5, 'y':10})
            
           As well as with :meth:`.Engine.begin`::
           
               with engine.begin() as conn:
                   conn.execute("some statement", {'x':5, 'y':10})
        
        See also:
        
            :meth:`.Engine.begin` - engine-level transactional 
            context
             
            :meth:`.Engine.transaction` - engine-level version of
            :meth:`.Connection.transaction`

        """

        trans = self.begin()
        try:
            ret = self.run_callable(callable_, *args, **kwargs)
            trans.commit()
            return ret
        except:
            trans.rollback()
            raise

    def run_callable(self, callable_, *args, **kwargs):
        """Given a callable object or function, execute it, passing
        a :class:`.Connection` as the first argument.

        The given \*args and \**kwargs are passed subsequent
        to the :class:`.Connection` argument.

        This function, along with :meth:`.Engine.run_callable`, 
        allows a function to be run with a :class:`.Connection`
        or :class:`.Engine` object without the need to know
        which one is being dealt with.

        """
        return callable_(self, *args, **kwargs)

    def _run_visitor(self, visitorcallable, element, **kwargs):
        visitorcallable(self.dialect, self,
                            **kwargs).traverse_single(element)


class Transaction(object):
    """Represent a database transaction in progress.

    The :class:`.Transaction` object is procured by 
    calling the :meth:`~.Connection.begin` method of
    :class:`.Connection`::

        from sqlalchemy import create_engine
        engine = create_engine("postgresql://scott:tiger@localhost/test")
        connection = engine.connect()
        trans = connection.begin()
        connection.execute("insert into x (a, b) values (1, 2)")
        trans.commit()

    The object provides :meth:`.rollback` and :meth:`.commit`
    methods in order to control transaction boundaries.  It 
    also implements a context manager interface so that 
    the Python ``with`` statement can be used with the 
    :meth:`.Connection.begin` method::

        with connection.begin():
            connection.execute("insert into x (a, b) values (1, 2)")

    The Transaction object is **not** threadsafe.

    See also:  :meth:`.Connection.begin`, :meth:`.Connection.begin_twophase`,
    :meth:`.Connection.begin_nested`.

    .. index::
      single: thread safety; Transaction
    """

    def __init__(self, connection, parent):
        self.connection = connection
        self._parent = parent or self
        self.is_active = True

    def close(self):
        """Close this :class:`.Transaction`.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosing transaction.

        """
        if not self._parent.is_active:
            return
        if self._parent is self:
            self.rollback()

    def rollback(self):
        """Roll back this :class:`.Transaction`.

        """
        if not self._parent.is_active:
            return
        self._do_rollback()
        self.is_active = False

    def _do_rollback(self):
        self._parent.rollback()

    def commit(self):
        """Commit this :class:`.Transaction`."""

        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        self._do_commit()
        self.is_active = False

    def _do_commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None and self.is_active:
            try:
                self.commit()
            except:
                self.rollback()
                raise
        else:
            self.rollback()

class RootTransaction(Transaction):
    def __init__(self, connection):
        super(RootTransaction, self).__init__(connection, None)
        self.connection._begin_impl()

    def _do_rollback(self):
        if self.is_active:
            self.connection._rollback_impl()

    def _do_commit(self):
        if self.is_active:
            self.connection._commit_impl()


class NestedTransaction(Transaction):
    """Represent a 'nested', or SAVEPOINT transaction.

    A new :class:`.NestedTransaction` object may be procured
    using the :meth:`.Connection.begin_nested` method.

    The interface is the same as that of :class:`.Transaction`.

    """
    def __init__(self, connection, parent):
        super(NestedTransaction, self).__init__(connection, parent)
        self._savepoint = self.connection._savepoint_impl()

    def _do_rollback(self):
        if self.is_active:
            self.connection._rollback_to_savepoint_impl(
                                    self._savepoint, self._parent)

    def _do_commit(self):
        if self.is_active:
            self.connection._release_savepoint_impl(
                                    self._savepoint, self._parent)


class TwoPhaseTransaction(Transaction):
    """Represent a two-phase transaction.

    A new :class:`.TwoPhaseTransaction` object may be procured
    using the :meth:`.Connection.begin_twophase` method.

    The interface is the same as that of :class:`.Transaction`
    with the addition of the :meth:`prepare` method.

    """
    def __init__(self, connection, xid):
        super(TwoPhaseTransaction, self).__init__(connection, None)
        self._is_prepared = False
        self.xid = xid
        self.connection._begin_twophase_impl(self.xid)

    def prepare(self):
        """Prepare this :class:`.TwoPhaseTransaction`.

        After a PREPARE, the transaction can be committed.

        """
        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        self.connection._prepare_twophase_impl(self.xid)
        self._is_prepared = True

    def _do_rollback(self):
        self.connection._rollback_twophase_impl(self.xid, self._is_prepared)

    def _do_commit(self):
        self.connection._commit_twophase_impl(self.xid, self._is_prepared)


class Engine(Connectable, log.Identified):
    """
    Connects a :class:`~sqlalchemy.pool.Pool` and 
    :class:`~sqlalchemy.engine.base.Dialect` together to provide a source 
    of database connectivity and behavior.

    An :class:`.Engine` object is instantiated publicly using the 
    :func:`~sqlalchemy.create_engine` function.

    See also:

    :ref:`engines_toplevel`

    :ref:`connections_toplevel`

    """

    _execution_options = util.immutabledict()
    _has_events = False
    _connection_cls = Connection

    def __init__(self, pool, dialect, url, 
                        logging_name=None, echo=None, proxy=None,
                        execution_options=None
                        ):
        self.pool = pool
        self.url = url
        self.dialect = dialect
        if logging_name:
            self.logging_name = logging_name
        self.echo = echo
        self.engine = self
        log.instance_logger(self, echoflag=echo)
        if proxy:
            interfaces.ConnectionProxy._adapt_listener(self, proxy)
        if execution_options:
            if 'isolation_level' in execution_options:
                raise exc.ArgumentError(
                    "'isolation_level' execution option may "
                    "only be specified on Connection.execution_options(). "
                    "To set engine-wide isolation level, "
                    "use the isolation_level argument to create_engine()."
                )
            self.update_execution_options(**execution_options)

    dispatch = event.dispatcher(events.ConnectionEvents)

    def update_execution_options(self, **opt):
        """Update the default execution_options dictionary 
        of this :class:`.Engine`.

        The given keys/values in \**opt are added to the
        default execution options that will be used for 
        all connections.  The initial contents of this dictionary
        can be sent via the ``execution_options`` parameter
        to :func:`.create_engine`.

        See :meth:`.Connection.execution_options` for more
        details on execution options.

        """
        self._execution_options = \
                self._execution_options.union(opt)

    @property
    def name(self):
        """String name of the :class:`~sqlalchemy.engine.Dialect` in use by
        this ``Engine``."""

        return self.dialect.name

    @property
    def driver(self):
        """Driver name of the :class:`~sqlalchemy.engine.Dialect` in use by
        this ``Engine``."""

        return self.dialect.driver

    echo = log.echo_property()

    def __repr__(self):
        return 'Engine(%s)' % str(self.url)

    def dispose(self):
        """Dispose of the connection pool used by this :class:`.Engine`.

        A new connection pool is created immediately after the old one has
        been disposed.   This new pool, like all SQLAlchemy connection pools,
        does not make any actual connections to the database until one is 
        first requested.

        This method has two general use cases:

         * When a dropped connection is detected, it is assumed that all
           connections held by the pool are potentially dropped, and 
           the entire pool is replaced.

         * An application may want to use :meth:`dispose` within a test 
           suite that is creating multiple engines.

        It is critical to note that :meth:`dispose` does **not** guarantee
        that the application will release all open database connections - only
        those connections that are checked into the pool are closed.
        Connections which remain checked out or have been detached from
        the engine are not affected. 

        """
        self.pool.dispose()
        self.pool = self.pool.recreate()

    @util.deprecated("0.7", "Use the create() method on the given schema "
                            "object directly, i.e. :meth:`.Table.create`, "
                            ":meth:`.Index.create`, :meth:`.MetaData.create_all`")
    def create(self, entity, connection=None, **kwargs):
        """Emit CREATE statements for the given schema entity."""

        from sqlalchemy.engine import ddl

        self._run_visitor(ddl.SchemaGenerator, entity, 
                                connection=connection, **kwargs)

    @util.deprecated("0.7", "Use the drop() method on the given schema "
                            "object directly, i.e. :meth:`.Table.drop`, "
                            ":meth:`.Index.drop`, :meth:`.MetaData.drop_all`")
    def drop(self, entity, connection=None, **kwargs):
        """Emit DROP statements for the given schema entity."""

        from sqlalchemy.engine import ddl

        self._run_visitor(ddl.SchemaDropper, entity, 
                                connection=connection, **kwargs)

    def _execute_default(self, default):
        connection = self.contextual_connect()
        try:
            return connection._execute_default(default, (), {})
        finally:
            connection.close()

    @property
    @util.deprecated("0.7", 
                "Use :attr:`~sqlalchemy.sql.expression.func` to create function constructs.")
    def func(self):
        return expression._FunctionGenerator(bind=self)

    @util.deprecated("0.7", 
                "Use :func:`.expression.text` to create text constructs.")
    def text(self, text, *args, **kwargs):
        """Return a :func:`~sqlalchemy.sql.expression.text` construct, 
        bound to this engine.

        This is equivalent to::

            text("SELECT * FROM table", bind=engine)

        """

        return expression.text(text, bind=self, *args, **kwargs)

    def _run_visitor(self, visitorcallable, element, 
                                    connection=None, **kwargs):
        if connection is None:
            conn = self.contextual_connect(close_with_result=False)
        else:
            conn = connection
        try:
            conn._run_visitor(visitorcallable, element, **kwargs)
        finally:
            if connection is None:
                conn.close()

    class _trans_ctx(object):
        def __init__(self, conn, transaction, close_with_result):
            self.conn = conn
            self.transaction = transaction
            self.close_with_result = close_with_result

        def __enter__(self):
            return self.conn

        def __exit__(self, type, value, traceback):
            if type is not None:
                self.transaction.rollback()
            else:
                self.transaction.commit()
            if not self.close_with_result:
                self.conn.close()

    def begin(self, close_with_result=False):
        """Return a context manager delivering a :class:`.Connection`
        with a :class:`.Transaction` established.

        E.g.::
        
            with engine.begin() as conn:
                conn.execute("insert into table (x, y, z) values (1, 2, 3)")
                conn.execute("my_special_procedure(5)")

        Upon successful operation, the :class:`.Transaction` 
        is committed.  If an error is raised, the :class:`.Transaction`
        is rolled back.  
        
        The ``close_with_result`` flag is normally ``False``, and indicates
        that the :class:`.Connection` will be closed when the operation
        is complete.   When set to ``True``, it indicates the :class:`.Connection`
        is in "single use" mode, where the :class:`.ResultProxy`
        returned by the first call to :meth:`.Connection.execute` will
        close the :class:`.Connection` when that :class:`.ResultProxy`
        has exhausted all result rows.

        .. versionadded:: 0.7.6
        
        See also:
        
        :meth:`.Engine.connect` - procure a :class:`.Connection` from
        an :class:`.Engine`.

        :meth:`.Connection.begin` - start a :class:`.Transaction`
        for a particular :class:`.Connection`.

        """
        conn = self.contextual_connect(close_with_result=close_with_result)
        try:
            trans = conn.begin()
        except:
            conn.close()
            raise
        return Engine._trans_ctx(conn, trans, close_with_result)

    def transaction(self, callable_, *args, **kwargs):
        """Execute the given function within a transaction boundary.

        The function is passed a :class:`.Connection` newly procured
        from :meth:`.Engine.contextual_connect` as the first argument, 
        followed by the given \*args and \**kwargs.
        
        e.g.::
        
            def do_something(conn, x, y):
                conn.execute("some statement", {'x':x, 'y':y})

            engine.transaction(do_something, 5, 10)
        
        The operations inside the function are all invoked within the
        context of a single :class:`.Transaction`.   
        Upon success, the transaction is committed.  If an 
        exception is raised, the transaction is rolled back
        before propagating the exception.

        .. note::

           The :meth:`.transaction` method is superseded by
           the usage of the Python ``with:`` statement, which can
           be used with :meth:`.Engine.begin`::
           
               with engine.begin() as conn:
                   conn.execute("some statement", {'x':5, 'y':10})
        
        See also:
        
            :meth:`.Engine.begin` - engine-level transactional 
            context
             
            :meth:`.Connection.transaction` - connection-level version of
            :meth:`.Engine.transaction`

        """

        conn = self.contextual_connect()
        try:
            return conn.transaction(callable_, *args, **kwargs)
        finally:
            conn.close()

    def run_callable(self, callable_, *args, **kwargs):
        """Given a callable object or function, execute it, passing
        a :class:`.Connection` as the first argument.

        The given \*args and \**kwargs are passed subsequent
        to the :class:`.Connection` argument.

        This function, along with :meth:`.Connection.run_callable`, 
        allows a function to be run with a :class:`.Connection`
        or :class:`.Engine` object without the need to know
        which one is being dealt with.

        """
        conn = self.contextual_connect()
        try:
            return conn.run_callable(callable_, *args, **kwargs)
        finally:
            conn.close()

    def execute(self, statement, *multiparams, **params):
        """Executes the given construct and returns a :class:`.ResultProxy`.

        The arguments are the same as those used by
        :meth:`.Connection.execute`.

        Here, a :class:`.Connection` is acquired using the
        :meth:`~.Engine.contextual_connect` method, and the statement executed
        with that connection. The returned :class:`.ResultProxy` is flagged
        such that when the :class:`.ResultProxy` is exhausted and its
        underlying cursor is closed, the :class:`.Connection` created here
        will also be closed, which allows its associated DBAPI connection
        resource to be returned to the connection pool.

        """

        connection = self.contextual_connect(close_with_result=True)
        return connection.execute(statement, *multiparams, **params)

    def scalar(self, statement, *multiparams, **params):
        return self.execute(statement, *multiparams, **params).scalar()

    def _execute_clauseelement(self, elem, multiparams=None, params=None):
        connection = self.contextual_connect(close_with_result=True)
        return connection._execute_clauseelement(elem, multiparams, params)

    def _execute_compiled(self, compiled, multiparams, params):
        connection = self.contextual_connect(close_with_result=True)
        return connection._execute_compiled(compiled, multiparams, params)

    def connect(self, **kwargs):
        """Return a new :class:`.Connection` object.

        The :class:`.Connection` object is a facade that uses a DBAPI connection internally
        in order to communicate with the database.  This connection is procured
        from the connection-holding :class:`.Pool` referenced by this :class:`.Engine`.
        When the :meth:`~.Connection.close` method of the :class:`.Connection` object is called,
        the underlying DBAPI connection is then returned to the connection pool,
        where it may be used again in a subsequent call to :meth:`~.Engine.connect`.

        """

        return self._connection_cls(self, **kwargs)

    def contextual_connect(self, close_with_result=False, **kwargs):
        """Return a :class:`.Connection` object which may be part of some ongoing context.

        By default, this method does the same thing as :meth:`.Engine.connect`.
        Subclasses of :class:`.Engine` may override this method
        to provide contextual behavior.

        :param close_with_result: When True, the first :class:`.ResultProxy` created
          by the :class:`.Connection` will call the :meth:`.Connection.close` method
          of that connection as soon as any pending result rows are exhausted.
          This is used to supply the "connectionless execution" behavior provided
          by the :meth:`.Engine.execute` method.

        """

        return self._connection_cls(self, 
                                    self.pool.connect(), 
                                    close_with_result=close_with_result, 
                                    **kwargs)

    def table_names(self, schema=None, connection=None):
        """Return a list of all table names available in the database.

        :param schema: Optional, retrieve names from a non-default schema.

        :param connection: Optional, use a specified connection. Default is
          the ``contextual_connect`` for this ``Engine``.
        """

        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        if not schema:
            schema =  self.dialect.default_schema_name
        try:
            return self.dialect.get_table_names(conn, schema)
        finally:
            if connection is None:
                conn.close()

    @util.deprecated("0.7", "Use autoload=True with :class:`.Table`, "
                        "or use the :class:`.Inspector` object.")
    def reflecttable(self, table, connection=None, include_columns=None):
        """Load table description from the database.

        Uses the given :class:`.Connection`, or if None produces
        its own :class:`.Connection`, and passes the ``table``
        and ``include_columns`` arguments onto that 
        :class:`.Connection` object's :meth:`.Connection.reflecttable`
        method.  The :class:`.Table` object is then populated
        with new attributes.

        """
        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            self.dialect.reflecttable(conn, table, include_columns)
        finally:
            if connection is None:
                conn.close()

    def has_table(self, table_name, schema=None):
        return self.run_callable(self.dialect.has_table, table_name, schema)

    def raw_connection(self):
        """Return a "raw" DBAPI connection from the connection pool.

        The returned object is a proxied version of the DBAPI 
        connection object used by the underlying driver in use.
        The object will have all the same behavior as the real DBAPI
        connection, except that its ``close()`` method will result in the
        connection being returned to the pool, rather than being closed
        for real.

        This method provides direct DBAPI connection access for
        special situations.  In most situations, the :class:`.Connection`
        object should be used, which is procured using the
        :meth:`.Engine.connect` method.

        """

        return self.pool.unique_connection()


# This reconstructor is necessary so that pickles with the C extension or
# without use the same Binary format.
try:
    # We need a different reconstructor on the C extension so that we can
    # add extra checks that fields have correctly been initialized by
    # __setstate__.
    from sqlalchemy.cresultproxy import safe_rowproxy_reconstructor

    # The extra function embedding is needed so that the 
    # reconstructor function has the same signature whether or not 
    # the extension is present.
    def rowproxy_reconstructor(cls, state):
        return safe_rowproxy_reconstructor(cls, state)
except ImportError:
    def rowproxy_reconstructor(cls, state):
        obj = cls.__new__(cls)
        obj.__setstate__(state)
        return obj

try:
    from sqlalchemy.cresultproxy import BaseRowProxy
except ImportError:
    class BaseRowProxy(object):
        __slots__ = ('_parent', '_row', '_processors', '_keymap')

        def __init__(self, parent, row, processors, keymap):
            """RowProxy objects are constructed by ResultProxy objects."""

            self._parent = parent
            self._row = row
            self._processors = processors
            self._keymap = keymap

        def __reduce__(self):
            return (rowproxy_reconstructor,
                    (self.__class__, self.__getstate__()))

        def values(self):
            """Return the values represented by this RowProxy as a list."""
            return list(self)

        def __iter__(self):
            for processor, value in izip(self._processors, self._row):
                if processor is None:
                    yield value
                else:
                    yield processor(value)

        def __len__(self):
            return len(self._row)

        def __getitem__(self, key):
            try:
                processor, obj, index = self._keymap[key]
            except KeyError:
                processor, obj, index = self._parent._key_fallback(key)
            except TypeError:
                if isinstance(key, slice):
                    l = []
                    for processor, value in izip(self._processors[key],
                                                 self._row[key]):
                        if processor is None:
                            l.append(value)
                        else:
                            l.append(processor(value))
                    return tuple(l)
                else:
                    raise
            if index is None:
                raise exc.InvalidRequestError(
                        "Ambiguous column name '%s' in result set! "
                        "try 'use_labels' option on select statement." % key)
            if processor is not None:
                return processor(self._row[index])
            else:
                return self._row[index]

        def __getattr__(self, name):
            try:
                return self[name]
            except KeyError, e:
                raise AttributeError(e.args[0])


class RowProxy(BaseRowProxy):
    """Proxy values from a single cursor row.

    Mostly follows "ordered dictionary" behavior, mapping result
    values to the string-based column name, the integer position of
    the result in the row, as well as Column instances which can be
    mapped to the original Columns that produced this result set (for
    results that correspond to constructed SQL expressions).
    """
    __slots__ = ()

    def __contains__(self, key):
        return self._parent._has_key(self._row, key)

    def __getstate__(self):
        return {
            '_parent': self._parent,
            '_row': tuple(self)
        }

    def __setstate__(self, state):
        self._parent = parent = state['_parent']
        self._row = state['_row']
        self._processors = parent._processors
        self._keymap = parent._keymap

    __hash__ = None

    def __eq__(self, other):
        return other is self or other == tuple(self)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return repr(tuple(self))

    def has_key(self, key):
        """Return True if this RowProxy contains the given key."""

        return self._parent._has_key(self._row, key)

    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair."""
        # TODO: no coverage here
        return [(key, self[key]) for key in self.iterkeys()]

    def keys(self):
        """Return the list of keys as strings represented by this RowProxy."""

        return self._parent.keys

    def iterkeys(self):
        return iter(self._parent.keys)

    def itervalues(self):
        return iter(self)

try:
    # Register RowProxy with Sequence, 
    # so sequence protocol is implemented
    from collections import Sequence
    Sequence.register(RowProxy)
except ImportError:
    pass


class ResultMetaData(object):
    """Handle cursor.description, applying additional info from an execution
    context."""

    def __init__(self, parent, metadata):
        self._processors = processors = []

        # We do not strictly need to store the processor in the key mapping,
        # though it is faster in the Python version (probably because of the
        # saved attribute lookup self._processors)
        self._keymap = keymap = {}
        self.keys = []
        context = parent.context
        dialect = context.dialect
        typemap = dialect.dbapi_type_map
        translate_colname = context._translate_colname

        # high precedence key values.
        primary_keymap = {}

        for i, rec in enumerate(metadata):
            colname = rec[0]
            coltype = rec[1]

            if dialect.description_encoding:
                colname = dialect._description_decoder(colname)

            if translate_colname:
                colname, untranslated = translate_colname(colname)

            if context.result_map:
                try:
                    name, obj, type_ = context.result_map[colname.lower()]
                except KeyError:
                    name, obj, type_ = \
                        colname, None, typemap.get(coltype, types.NULLTYPE)
            else:
                name, obj, type_ = \
                        colname, None, typemap.get(coltype, types.NULLTYPE)

            processor = type_._cached_result_processor(dialect, coltype)

            processors.append(processor)
            rec = (processor, obj, i)

            # indexes as keys. This is only needed for the Python version of
            # RowProxy (the C version uses a faster path for integer indexes).
            primary_keymap[i] = rec

            # populate primary keymap, looking for conflicts.
            if primary_keymap.setdefault(name.lower(), rec) is not rec: 
                # place a record that doesn't have the "index" - this
                # is interpreted later as an AmbiguousColumnError,
                # but only when actually accessed.   Columns 
                # colliding by name is not a problem if those names
                # aren't used; integer and ColumnElement access is always
                # unambiguous.
                primary_keymap[name.lower()] = (processor, obj, None)

            if dialect.requires_name_normalize:
                colname = dialect.normalize_name(colname)

            self.keys.append(colname)
            if obj:
                for o in obj:
                    keymap[o] = rec

            if translate_colname and \
                untranslated:
                keymap[untranslated] = rec

        # overwrite keymap values with those of the
        # high precedence keymap.
        keymap.update(primary_keymap)

        if parent._echo:
            context.engine.logger.debug(
                "Col %r", tuple(x[0] for x in metadata))

    @util.pending_deprecation("0.8", "sqlite dialect uses "
                    "_translate_colname() now")
    def _set_keymap_synonym(self, name, origname):
        """Set a synonym for the given name.

        Some dialects (SQLite at the moment) may use this to 
        adjust the column names that are significant within a
        row.

        """
        rec = (processor, obj, i) = self._keymap[origname.lower()]
        if self._keymap.setdefault(name, rec) is not rec:
            self._keymap[name] = (processor, obj, None)

    def _key_fallback(self, key, raiseerr=True):
        map = self._keymap
        result = None
        if isinstance(key, basestring):
            result = map.get(key.lower())
        # fallback for targeting a ColumnElement to a textual expression
        # this is a rare use case which only occurs when matching text()
        # or colummn('name') constructs to ColumnElements, or after a 
        # pickle/unpickle roundtrip
        elif isinstance(key, expression.ColumnElement):
            if key._label and key._label.lower() in map:
                result = map[key._label.lower()]
            elif hasattr(key, 'name') and key.name.lower() in map:
                # match is only on name.
                result = map[key.name.lower()]
            # search extra hard to make sure this 
            # isn't a column/label name overlap.
            # this check isn't currently available if the row
            # was unpickled.
            if result is not None and \
                result[1] is not None:
                for obj in result[1]:
                    if key._compare_name_for_result(obj):
                        break
                else:
                    result = None
        if result is None:
            if raiseerr:
                raise exc.NoSuchColumnError(
                    "Could not locate column in row for column '%s'" % 
                        expression._string_or_unprintable(key))
            else:
                return None
        else:
            map[key] = result
        return result

    def _has_key(self, row, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key, False) is not None

    def __getstate__(self):
        return {
            '_pickled_keymap': dict(
                (key, index)
                for key, (processor, obj, index) in self._keymap.iteritems()
                if isinstance(key, (basestring, int))
            ),
            'keys': self.keys
        }

    def __setstate__(self, state):
        # the row has been processed at pickling time so we don't need any
        # processor anymore
        self._processors = [None for _ in xrange(len(state['keys']))]
        self._keymap = keymap = {}
        for key, index in state['_pickled_keymap'].iteritems():
            # not preserving "obj" here, unfortunately our
            # proxy comparison fails with the unpickle
            keymap[key] = (None, None, index)
        self.keys = state['keys']
        self._echo = False


class ResultProxy(object):
    """Wraps a DB-API cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by ``schema.Column``
    object. e.g.::

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ``ResultProxy`` also handles post-processing of result column
    data using ``TypeEngine`` objects, which are referenced from 
    the originating SQL statement that produced this result set.

    """

    _process_row = RowProxy
    out_parameters = None
    _can_close_connection = False

    def __init__(self, context):
        self.context = context
        self.dialect = context.dialect
        self.closed = False
        self.cursor = self._saved_cursor = context.cursor
        self.connection = context.root_connection
        self._echo = self.connection._echo and \
                        context.engine._should_log_debug()
        self._init_metadata()

    def _init_metadata(self):
        metadata = self._cursor_description()
        if metadata is None:
            self._metadata = None
        else:
            self._metadata = ResultMetaData(self, metadata)

    def keys(self):
        """Return the current set of string keys for rows."""
        if self._metadata:
            return self._metadata.keys
        else:
            return []

    @util.memoized_property
    def rowcount(self):
        """Return the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.  
        
        .. note::
        
           Notes regarding :attr:`.ResultProxy.rowcount`:
           
           
           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.
             On backends that feature both styles, such as MySQL, 
             rowcount is configured by default to return the match 
             count in all cases.

           * :attr:`.ResultProxy.rowcount` is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are 
             unbuffered.
        
           * :attr:`.ResultProxy.rowcount` may not be fully implemented by
             all dialects.  In particular, most DBAPIs do not support an
             aggregate rowcount result from an executemany call.
             The :meth:`.ResultProxy.supports_sane_rowcount` and 
             :meth:`.ResultProxy.supports_sane_multi_rowcount` methods
             will report from the dialect if each usage is known to be
             supported.
         
           * Statements that use RETURNING may not return a correct
             rowcount.

        """
        try:
            return self.context.rowcount
        except Exception, e:
            self.connection._handle_dbapi_exception(
                              e, None, None, self.cursor, self.context)
            raise

    @property
    def lastrowid(self):
        """return the 'lastrowid' accessor on the DBAPI cursor.

        This is a DBAPI specific method and is only functional
        for those backends which support it, for statements
        where it is appropriate.  It's behavior is not 
        consistent across backends.

        Usage of this method is normally unnecessary; the
        :attr:`~ResultProxy.inserted_primary_key` attribute provides a
        tuple of primary key values for a newly inserted row,
        regardless of database backend.

        """
        try:
            return self._saved_cursor.lastrowid
        except Exception, e:
            self.connection._handle_dbapi_exception(
                                 e, None, None, 
                                 self._saved_cursor, self.context)
            raise

    @property
    def returns_rows(self):
        """True if this :class:`.ResultProxy` returns rows.

        I.e. if it is legal to call the methods 
        :meth:`~.ResultProxy.fetchone`, 
        :meth:`~.ResultProxy.fetchmany`
        :meth:`~.ResultProxy.fetchall`.

        """
        return self._metadata is not None

    @property
    def is_insert(self):
        """True if this :class:`.ResultProxy` is the result
        of a executing an expression language compiled 
        :func:`.expression.insert` construct.

        When True, this implies that the 
        :attr:`inserted_primary_key` attribute is accessible,
        assuming the statement did not include
        a user defined "returning" construct.

        """
        return self.context.isinsert

    def _cursor_description(self):
        """May be overridden by subclasses."""

        return self._saved_cursor.description

    def close(self, _autoclose_connection=True):
        """Close this ResultProxy.

        Closes the underlying DBAPI cursor corresponding to the execution.

        Note that any data cached within this ResultProxy is still available.
        For some types of results, this may include buffered rows.

        If this ResultProxy was generated from an implicit execution,
        the underlying Connection will also be closed (returns the
        underlying DBAPI connection to the connection pool.)

        This method is called automatically when:

        * all result rows are exhausted using the fetchXXX() methods.
        * cursor.description is None.

        """

        if not self.closed:
            self.closed = True
            self.connection._safe_close_cursor(self.cursor)
            if _autoclose_connection and \
                self.connection.should_close_with_result:
                self.connection.close()
            # allow consistent errors
            self.cursor = None

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                raise StopIteration
            else:
                yield row

    @util.memoized_property
    def inserted_primary_key(self):
        """Return the primary key for the row just inserted.

        The return value is a list of scalar values 
        corresponding to the list of primary key columns
        in the target table.

        This only applies to single row :func:`.insert` 
        constructs which did not explicitly specify 
        :meth:`.Insert.returning`.

        Note that primary key columns which specify a
        server_default clause, 
        or otherwise do not qualify as "autoincrement"
        columns (see the notes at :class:`.Column`), and were
        generated using the database-side default, will
        appear in this list as ``None`` unless the backend 
        supports "returning" and the insert statement executed
        with the "implicit returning" enabled.

        """

        if not self.context.isinsert:
            raise exc.InvalidRequestError(
                        "Statement is not an insert() expression construct.")
        elif self.context._is_explicit_returning:
            raise exc.InvalidRequestError(
                        "Can't call inserted_primary_key when returning() "
                        "is used.")

        return self.context.inserted_primary_key

    @util.deprecated("0.6", "Use :attr:`.ResultProxy.inserted_primary_key`")
    def last_inserted_ids(self):
        """Return the primary key for the row just inserted."""

        return self.inserted_primary_key

    def last_updated_params(self):
        """Return the collection of updated parameters from this
        execution.

        """
        if self.context.executemany:
            return self.context.compiled_parameters
        else:
            return self.context.compiled_parameters[0]

    def last_inserted_params(self):
        """Return the collection of inserted parameters from this
        execution.

        """
        if self.context.executemany:
            return self.context.compiled_parameters
        else:
            return self.context.compiled_parameters[0]

    def lastrow_has_defaults(self):
        """Return ``lastrow_has_defaults()`` from the underlying
        ExecutionContext.

        See ExecutionContext for details.
        """

        return self.context.lastrow_has_defaults()

    def postfetch_cols(self):
        """Return ``postfetch_cols()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.context.postfetch_cols

    def prefetch_cols(self):
        return self.context.prefetch_cols

    def supports_sane_rowcount(self):
        """Return ``supports_sane_rowcount`` from the dialect.
        
        See :attr:`.ResultProxy.rowcount` for background.
        
        """

        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect.

        See :attr:`.ResultProxy.rowcount` for background.
        
        """

        return self.dialect.supports_sane_multi_rowcount

    def _fetchone_impl(self):
        try:
            return self.cursor.fetchone()
        except AttributeError:
            self._non_result()

    def _fetchmany_impl(self, size=None):
        try:
            if size is None:
                return self.cursor.fetchmany()
            else:
                return self.cursor.fetchmany(size)
        except AttributeError:
            self._non_result()

    def _fetchall_impl(self):
        try:
            return self.cursor.fetchall()
        except AttributeError:
            self._non_result()

    def _non_result(self):
        if self._metadata is None:
            raise exc.ResourceClosedError(
            "This result object does not return rows. "
            "It has been closed automatically.",
            )
        else:
            raise exc.ResourceClosedError("This result object is closed.")

    def process_rows(self, rows):
        process_row = self._process_row
        metadata = self._metadata
        keymap = metadata._keymap
        processors = metadata._processors
        if self._echo:
            log = self.context.engine.logger.debug
            l = []
            for row in rows:
                log("Row %r", row)
                l.append(process_row(metadata, row, processors, keymap))
            return l
        else:
            return [process_row(metadata, row, processors, keymap)
                    for row in rows]

    def fetchall(self):
        """Fetch all rows, just like DB-API ``cursor.fetchall()``."""

        try:
            l = self.process_rows(self._fetchall_impl())
            self.close()
            return l
        except Exception, e:
            self.connection._handle_dbapi_exception(
                                    e, None, None, 
                                    self.cursor, self.context)
            raise

    def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API
        ``cursor.fetchmany(size=cursor.arraysize)``.

        If rows are present, the cursor remains open after this is called.
        Else the cursor is automatically closed and an empty list is returned.

        """

        try:
            l = self.process_rows(self._fetchmany_impl(size))
            if len(l) == 0:
                self.close()
            return l
        except Exception, e:
            self.connection._handle_dbapi_exception(
                                    e, None, None, 
                                    self.cursor, self.context)
            raise

    def fetchone(self):
        """Fetch one row, just like DB-API ``cursor.fetchone()``.

        If a row is present, the cursor remains open after this is called.
        Else the cursor is automatically closed and None is returned.

        """
        try:
            row = self._fetchone_impl()
            if row is not None:
                return self.process_rows([row])[0]
            else:
                self.close()
                return None
        except Exception, e:
            self.connection._handle_dbapi_exception(
                                    e, None, None, 
                                    self.cursor, self.context)
            raise

    def first(self):
        """Fetch the first row and then close the result set unconditionally.

        Returns None if no row is present.

        """
        if self._metadata is None:
            self._non_result()

        try:
            row = self._fetchone_impl()
        except Exception, e:
            self.connection._handle_dbapi_exception(
                                    e, None, None, 
                                    self.cursor, self.context)
            raise

        try:
            if row is not None:
                return self.process_rows([row])[0]
            else:
                return None
        finally:
            self.close()

    def scalar(self):
        """Fetch the first column of the first row, and close the result set.

        Returns None if no row is present.

        """
        row = self.first()
        if row is not None:
            return row[0]
        else:
            return None

class BufferedRowResultProxy(ResultProxy):
    """A ResultProxy with row buffering behavior.

    ``ResultProxy`` that buffers the contents of a selection of rows
    before ``fetchone()`` is called.  This is to allow the results of
    ``cursor.description`` to be available immediately, when
    interfacing with a DB-API that requires rows to be consumed before
    this information is available (currently psycopg2, when used with
    server-side cursors).

    The pre-fetching behavior fetches only one row initially, and then
    grows its buffer size by a fixed amount with each successive need
    for additional rows up to a size of 100.
    """

    def _init_metadata(self):
        self.__buffer_rows()
        super(BufferedRowResultProxy, self)._init_metadata()

    # this is a "growth chart" for the buffering of rows.
    # each successive __buffer_rows call will use the next
    # value in the list for the buffer size until the max
    # is reached
    size_growth = {
        1 : 5,
        5 : 10,
        10 : 20,
        20 : 50,
        50 : 100,
        100 : 250,
        250 : 500,
        500 : 1000
    }

    def __buffer_rows(self):
        size = getattr(self, '_bufsize', 1)
        self.__rowbuffer = collections.deque(self.cursor.fetchmany(size))
        self._bufsize = self.size_growth.get(size, size)

    def _fetchone_impl(self):
        if self.closed:
            return None
        if not self.__rowbuffer:
            self.__buffer_rows()
            if not self.__rowbuffer:
                return None
        return self.__rowbuffer.popleft()

    def _fetchmany_impl(self, size=None):
        if size is None:
            return self._fetchall_impl()
        result = []
        for x in range(0, size):
            row = self._fetchone_impl()
            if row is None:
                break
            result.append(row)
        return result

    def _fetchall_impl(self):
        self.__rowbuffer.extend(self.cursor.fetchall())
        ret = self.__rowbuffer
        self.__rowbuffer = collections.deque()
        return ret

class FullyBufferedResultProxy(ResultProxy):
    """A result proxy that buffers rows fully upon creation.

    Used for operations where a result is to be delivered
    after the database conversation can not be continued,
    such as MSSQL INSERT...OUTPUT after an autocommit.

    """
    def _init_metadata(self):
        super(FullyBufferedResultProxy, self)._init_metadata()
        self.__rowbuffer = self._buffer_rows()

    def _buffer_rows(self):
        return collections.deque(self.cursor.fetchall())

    def _fetchone_impl(self):
        if self.__rowbuffer:
            return self.__rowbuffer.popleft()
        else:
            return None

    def _fetchmany_impl(self, size=None):
        if size is None:
            return self._fetchall_impl()
        result = []
        for x in range(0, size):
            row = self._fetchone_impl()
            if row is None:
                break
            result.append(row)
        return result

    def _fetchall_impl(self):
        ret = self.__rowbuffer
        self.__rowbuffer = collections.deque()
        return ret

class BufferedColumnRow(RowProxy):
    def __init__(self, parent, row, processors, keymap):
        # preprocess row
        row = list(row)
        # this is a tad faster than using enumerate
        index = 0
        for processor in parent._orig_processors:
            if processor is not None:
                row[index] = processor(row[index])
            index += 1
        row = tuple(row)
        super(BufferedColumnRow, self).__init__(parent, row,
                                                processors, keymap)

class BufferedColumnResultProxy(ResultProxy):
    """A ResultProxy with column buffering behavior.

    ``ResultProxy`` that loads all columns into memory each time
    fetchone() is called.  If fetchmany() or fetchall() are called,
    the full grid of results is fetched.  This is to operate with
    databases where result rows contain "live" results that fall out
    of scope unless explicitly fetched.  Currently this includes
    cx_Oracle LOB objects.

    """

    _process_row = BufferedColumnRow

    def _init_metadata(self):
        super(BufferedColumnResultProxy, self)._init_metadata()
        metadata = self._metadata
        # orig_processors will be used to preprocess each row when they are
        # constructed.
        metadata._orig_processors = metadata._processors
        # replace the all type processors by None processors.
        metadata._processors = [None for _ in xrange(len(metadata.keys))]
        keymap = {}
        for k, (func, obj, index) in metadata._keymap.iteritems():
            keymap[k] = (None, obj, index)
        self._metadata._keymap = keymap

    def fetchall(self):
        # can't call cursor.fetchall(), since rows must be
        # fully processed before requesting more from the DBAPI.
        l = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            l.append(row)
        return l

    def fetchmany(self, size=None):
        # can't call cursor.fetchmany(), since rows must be
        # fully processed before requesting more from the DBAPI.
        if size is None:
            return self.fetchall()
        l = []
        for i in xrange(size):
            row = self.fetchone()
            if row is None:
                break
            l.append(row)
        return l

def connection_memoize(key):
    """Decorator, memoize a function in a connection.info stash.

    Only applicable to functions which take no arguments other than a
    connection.  The memo will be stored in ``connection.info[key]``.
    """

    @util.decorator
    def decorated(fn, self, connection):
        connection = connection.connect()
        try:
            return connection.info[key]
        except KeyError:
            connection.info[key] = val = fn(self, connection)
            return val

    return decorated
