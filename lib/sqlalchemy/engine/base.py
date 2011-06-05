# engine/base.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
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
from sqlalchemy import exc, schema, util, types, log
from sqlalchemy.sql import expression

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
      a :class:`ExecutionContext` class used to handle statement execution

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

        Given a :class:`~sqlalchemy.engine.Connection` and a
        :class:`~sqlalchemy.schema.Table` object, reflect its columns and
        properties from the database.  If include_columns (a list or
        set) is specified, limit the autoload to the given column
        names.

        The default implementation uses the 
        :class:`~sqlalchemy.engine.reflection.Inspector` interface to 
        provide the output, building upon the granular table/column/
        constraint etc. methods of :class:`Dialect`.

        """

        raise NotImplementedError()

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return information about columns in `table_name`.

        Given a :class:`~sqlalchemy.engine.Connection`, a string
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

        Given a :class:`~sqlalchemy.engine.Connection`, a string
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

        Given a :class:`~sqlalchemy.engine.Connection`, a string
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

        Given a :class:`~sqlalchemy.engine.Connection`, a string
        `view_name`, and an optional string `schema`, return the view
        definition.
        """

        raise NotImplementedError()

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return information about indexes in `table_name`.

        Given a :class:`~sqlalchemy.engine.Connection`, a string
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

        Given a :class:`~sqlalchemy.engine.Connection` object and a string
        `table_name`, return True if the given table (possibly within
        the specified `schema`) exists in the database, False
        otherwise.
        """

        raise NotImplementedError()

    def has_sequence(self, connection, sequence_name, schema=None):
        """Check the existence of a particular sequence in the database.

        Given a :class:`~sqlalchemy.engine.Connection` object and a string
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
        """Provide an implementation of *cursor.executemany(statement,
        parameters)*."""

        raise NotImplementedError()

    def do_execute(self, cursor, statement, parameters, context=None):
        """Provide an implementation of *cursor.execute(statement,
        parameters)*."""

        raise NotImplementedError()

    def is_disconnect(self, e):
        """Return True if the given DB-API error indicates an invalid
        connection"""

        raise NotImplementedError()

    def on_connect(self):
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

    def last_inserted_params(self):
        """Return a dictionary of the full parameter dictionary for the last
        compiled INSERT statement.

        Includes any ColumnDefaults or Sequences that were pre-executed.
        """

        raise NotImplementedError()

    def last_updated_params(self):
        """Return a dictionary of the full parameter dictionary for the last
        compiled UPDATE statement.

        Includes any ColumnDefaults that were pre-executed.
        """

        raise NotImplementedError()

    def lastrow_has_defaults(self):
        """Return True if the last INSERT or UPDATE row contained
        inlined or database-side defaults.
        """

        raise NotImplementedError()

    def get_rowcount(self):
        """Return the number of rows produced (by a SELECT query)
        or affected (by an INSERT/UPDATE/DELETE statement).

        Note that this row count may not be properly implemented 
        in some dialects; this is indicated by the 
        ``supports_sane_rowcount`` and ``supports_sane_multi_rowcount``
        dialect attributes.

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
        self.statement = statement
        self.bind = bind
        self.can_execute = statement.supports_execution

    def compile(self):
        """Produce the internal string representation of this element."""

        self.string = self.process(self.statement)

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

        :param params: a dict of string/object pairs whos values will
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

    The two implementations of ``Connectable`` are :class:`Connection` and
    :class:`Engine`.

    Connectable must also implement the 'dialect' member which references a
    :class:`Dialect` instance.
    """

    def contextual_connect(self):
        """Return a Connection object which may be part of an ongoing
        context."""

        raise NotImplementedError()

    def create(self, entity, **kwargs):
        """Create a table or index given an appropriate schema object."""

        raise NotImplementedError()

    def drop(self, entity, **kwargs):
        """Drop a table or index given an appropriate schema object."""

        raise NotImplementedError()

    def execute(self, object, *multiparams, **params):
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
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.should_close_with_result = close_with_result
        self.__savepoint_seq = 0
        self.__branch = _branch
        self.__invalid = False
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

        return self.engine.Connection(
                                self.engine, 
                                self.__connection, _branch=True)

    def _clone(self):
        """Create a shallow copy of this Connection.

        """
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        return c

    def execution_options(self, **opt):
        """ Set non-SQL options for the connection which take effect 
        during execution.

        The method returns a copy of this :class:`Connection` which references
        the same underlying DBAPI connection, but also defines the given
        execution options which will take effect for a call to
        :meth:`execute`. As the new :class:`Connection` references the same
        underlying resource, it is probably best to ensure that the copies
        would be discarded immediately, which is implicit if used as in::

            result = connection.execution_options(stream_results=True).\
                                execute(stmt)

        The options are the same as those accepted by 
        :meth:`sqlalchemy.sql.expression.Executable.execution_options`.

        """
        c = self._clone()
        c._execution_options = c._execution_options.union(opt)
        return c

    @property
    def dialect(self):
        "Dialect used by this Connection."

        return self.engine.dialect

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
    def info(self):
        """A collection of per-DB-API connection instance properties."""

        return self.connection.info

    def connect(self):
        """Returns self.

        This ``Connectable`` interface method returns self, allowing
        Connections to be used interchangably with Engines in most
        situations that require a bind.
        """

        return self

    def contextual_connect(self, **kwargs):
        """Returns self.

        This ``Connectable`` interface method returns self, allowing
        Connections to be used interchangably with Engines in most
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

        This Connection instance will remain useable.  When closed,
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
        """Begin a transaction and return a Transaction handle.

        Repeated calls to ``begin`` on the same Connection will create
        a lightweight, emulated nested transaction.  Only the
        outermost transaction may ``commit``.  Calls to ``commit`` on
        inner transactions are ignored.  Any transaction in the
        hierarchy may ``rollback``, however.
        """

        if self.__transaction is None:
            self.__transaction = RootTransaction(self)
            return self.__transaction
        else:
            return Transaction(self, self.__transaction)

    def begin_nested(self):
        """Begin a nested transaction and return a Transaction handle.

        Nested transactions require SAVEPOINT support in the
        underlying database.  Any transaction in the hierarchy may
        ``commit`` and ``rollback``, however the outermost transaction
        still controls the overall ``commit`` or ``rollback`` of the
        transaction of a whole.
        """

        if self.__transaction is None:
            self.__transaction = RootTransaction(self)
        else:
            self.__transaction = NestedTransaction(self, self.__transaction)
        return self.__transaction

    def begin_twophase(self, xid=None):
        """Begin a two-phase or XA transaction and return a Transaction
        handle.

        :param xid: the two phase transaction id.  If not supplied, a 
          random id will be generated.

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
        try:
            self.engine.dialect.do_begin(self.connection)
        except Exception, e:
            self._handle_dbapi_exception(e, None, None, None, None)
            raise

    def _rollback_impl(self):
        if not self.closed and not self.invalidated and \
                        self._connection_is_valid:
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
        if self._echo:
            self.engine.logger.info("COMMIT")
        try:
            self.engine.dialect.do_commit(self.connection)
            self.__transaction = None
        except Exception, e:
            self._handle_dbapi_exception(e, None, None, None, None)
            raise

    def _savepoint_impl(self, name=None):
        if name is None:
            self.__savepoint_seq += 1
            name = 'sa_savepoint_%s' % self.__savepoint_seq
        if self._connection_is_valid:
            self.engine.dialect.do_savepoint(self, name)
            return name

    def _rollback_to_savepoint_impl(self, name, context):
        if self._connection_is_valid:
            self.engine.dialect.do_rollback_to_savepoint(self, name)
        self.__transaction = context

    def _release_savepoint_impl(self, name, context):
        if self._connection_is_valid:
            self.engine.dialect.do_release_savepoint(self, name)
        self.__transaction = context

    def _begin_twophase_impl(self, xid):
        if self._connection_is_valid:
            self.engine.dialect.do_begin_twophase(self, xid)

    def _prepare_twophase_impl(self, xid):
        if self._connection_is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_prepare_twophase(self, xid)

    def _rollback_twophase_impl(self, xid, is_prepared):
        if self._connection_is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_rollback_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _commit_twophase_impl(self, xid, is_prepared):
        if self._connection_is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_commit_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _autorollback(self):
        if not self.in_transaction():
            self._rollback_impl()

    def close(self):
        """Close this Connection."""

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
        """Executes the given construct and returns a :class:`.ResultProxy`.

        The construct can be one of:

        * a textual SQL string
        * any :class:`.ClauseElement` construct that is also
          a subclass of :class:`.Executable`, such as a 
          :func:`.select` construct
        * a :class:`.FunctionElement`, such as that generated
          by :attr:`.func`, will be automatically wrapped in
          a SELECT statement, which is then executed.
        * a :class:`.DDLElement` object
        * a :class:`.DefaultGenerator` object
        * a :class:`.Compiled` object

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
                if not zero or hasattr(zero[0], '__iter__'):
                    return zero
                else:
                    return [zero]
            elif hasattr(zero, 'keys'):
                return [zero]
            else:
                return [[zero]]
        else:
            if hasattr(multiparams[0], '__iter__'):
                return multiparams
            else:
                return [multiparams]

    def _execute_function(self, func, multiparams, params):
        return self._execute_clauseelement(func.select(), multiparams, params)

    def _execute_default(self, default, multiparams, params):
        ctx = self.__create_execution_context()
        ret = ctx._exec_default(default)
        if self.should_close_with_result:
            self.close()
        return ret

    def _execute_ddl(self, ddl, params, multiparams):
        context = self.__create_execution_context(
                        compiled_ddl=ddl.compile(dialect=self.dialect),
                        parameters=None
                    )
        return self.__execute_context(context)

    def _execute_clauseelement(self, elem, multiparams, params):
        params = self.__distill_params(multiparams, params)
        if params:
            keys = params[0].keys()
        else:
            keys = []

        if 'compiled_cache' in self._execution_options:
            key = self.dialect, elem, tuple(keys), len(params) > 1
            if key in self._execution_options['compiled_cache']:
                compiled_sql = self._execution_options['compiled_cache'][key]
            else:
                compiled_sql = elem.compile(
                                dialect=self.dialect, column_keys=keys, 
                                inline=len(params) > 1)
                self._execution_options['compiled_cache'][key] = compiled_sql
        else:
            compiled_sql = elem.compile(
                            dialect=self.dialect, column_keys=keys, 
                            inline=len(params) > 1)

        context = self.__create_execution_context(
                        compiled_sql=compiled_sql,
                        parameters=params
                    )
        return self.__execute_context(context)

    def _execute_compiled(self, compiled, multiparams, params):
        """Execute a sql.Compiled object."""

        context = self.__create_execution_context(
                    compiled_sql=compiled,
                    parameters=self.__distill_params(multiparams, params)
                )
        return self.__execute_context(context)

    def _execute_text(self, statement, multiparams, params):
        parameters = self.__distill_params(multiparams, params)
        context = self.__create_execution_context(
                                statement=statement, 
                                parameters=parameters)
        return self.__execute_context(context)

    def __execute_context(self, context):
        if context.compiled:
            context.pre_exec()

        if context.executemany:
            self._cursor_executemany(
                            context.cursor, 
                            context.statement, 
                            context.parameters, context=context)
        else:
            self._cursor_execute(
                            context.cursor, 
                            context.statement, 
                            context.parameters[0], context=context)

        if context.compiled:
            context.post_exec()

            if context.isinsert and not context.executemany:
                context.post_insert()

        # create a resultproxy, get rowcount/implicit RETURNING
        # rows, close cursor if no further results pending
        r = context.get_result_proxy()._autoclose()

        if self.__transaction is None and context.should_autocommit:
            self._commit_impl()

        if r.closed and self.should_close_with_result:
            self.close()

        return r

    def _handle_dbapi_exception(self, 
                                    e, 
                                    statement, 
                                    parameters, 
                                    cursor, 
                                    context):
        if getattr(self, '_reentrant_error', False):
            # Py3K
            #raise exc.DBAPIError.instance(statement, parameters, e) from e
            # Py2K
            raise exc.DBAPIError.instance(statement, parameters, e), \
                                            None, sys.exc_info()[2]
            # end Py2K
        self._reentrant_error = True
        try:
            if not isinstance(e, self.dialect.dbapi.Error):
                return

            if context:
                context.handle_dbapi_exception(e)

            is_disconnect = self.dialect.is_disconnect(e)
            if is_disconnect:
                self.invalidate(e)
                self.engine.dispose()
            else:
                if cursor:
                    cursor.close()
                self._autorollback()
                if self.should_close_with_result:
                    self.close()
            # Py3K
            #raise exc.DBAPIError.instance(
            #                        statement, 
            #                        parameters, 
            #                        e, 
            #                        connection_invalidated=is_disconnect) \
            #                        from e
            # Py2K
            raise exc.DBAPIError.instance(
                                    statement, 
                                    parameters, 
                                    e, 
                                    connection_invalidated=is_disconnect), \
                                    None, sys.exc_info()[2]
            # end Py2K

        finally:
            del self._reentrant_error

    def __create_execution_context(self, **kwargs):
        try:
            dialect = self.engine.dialect
            return dialect.execution_ctx_cls(
                                dialect, 
                                connection=self, **kwargs)
        except Exception, e:
            self._handle_dbapi_exception(e, 
                                            kwargs.get('statement', None), 
                                            kwargs.get('parameters', None), 
                                            None, None)
            raise

    def _cursor_execute(self, cursor, statement, parameters, context=None):
        if self._echo:
            self.engine.logger.info(statement)
            self.engine.logger.info("%r", parameters)
        try:
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

    def _cursor_executemany(self, cursor, statement, 
                                        parameters, context=None):
        if self._echo:
            self.engine.logger.info(statement)
            self.engine.logger.info("%r", parameters)
        try:
            self.dialect.do_executemany(
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

    # poor man's multimethod/generic function thingy
    executors = {
        expression.FunctionElement: _execute_function,
        expression.ClauseElement: _execute_clauseelement,
        Compiled: _execute_compiled,
        schema.SchemaItem: _execute_default,
        schema.DDLElement: _execute_ddl,
        basestring: _execute_text
    }

    def create(self, entity, **kwargs):
        """Create a Table or Index given an appropriate Schema object."""

        return self.engine.create(entity, connection=self, **kwargs)

    def drop(self, entity, **kwargs):
        """Drop a Table or Index given an appropriate Schema object."""

        return self.engine.drop(entity, connection=self, **kwargs)

    def reflecttable(self, table, include_columns=None):
        """Reflect the columns in the given string table name from the
        database."""

        return self.engine.reflecttable(table, self, include_columns)

    def default_schema_name(self):
        return self.engine.dialect.get_default_schema_name(self)

    def transaction(self, callable_, *args, **kwargs):
        """Execute the given function within a transaction boundary.

        This is a shortcut for explicitly calling `begin()` and `commit()`
        and optionally `rollback()` when exceptions are raised.  The
        given `*args` and `**kwargs` will be passed to the function.

        See also transaction() on engine.

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
        return callable_(self, *args, **kwargs)


class Transaction(object):
    """Represent a Transaction in progress.

    The object provides :meth:`.rollback` and :meth:`.commit`
    methods in order to control transaction boundaries.  It 
    also implements a context manager interface so that 
    the Python ``with`` statement can be used with the 
    :meth:`.Connection.begin` method.

    The Transaction object is **not** threadsafe.

    .. index::
      single: thread safety; Transaction
    """

    def __init__(self, connection, parent):
        """The constructor for :class:`.Transaction` is private
        and is called from within the :class:`.Connection.begin` 
        implementation.

        """
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
            self.commit()
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
    def __init__(self, connection, xid):
        super(TwoPhaseTransaction, self).__init__(connection, None)
        self._is_prepared = False
        self.xid = xid
        self.connection._begin_twophase_impl(self.xid)

    def prepare(self):
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

    An :class:`Engine` object is instantiated publically using the 
    :func:`~sqlalchemy.create_engine` function.

    """

    _execution_options = util.frozendict()

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
        self.logger = log.instance_logger(self, echoflag=echo)
        if proxy:
            self.Connection = _proxy_connection_cls(Connection, proxy)
        else:
            self.Connection = Connection
        if execution_options:
            self.update_execution_options(**execution_options)

    def update_execution_options(self, **opt):
        """update the execution_options dictionary of this :class:`Engine`.

        For details on execution_options, see
        :meth:`Connection.execution_options` as well as
        :meth:`sqlalchemy.sql.expression.Executable.execution_options`.


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
        """Dispose of the connection pool used by this :class:`Engine`.

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

    def create(self, entity, connection=None, **kwargs):
        """Create a table or index within this engine's database connection
        given a schema object."""

        from sqlalchemy.engine import ddl

        self._run_visitor(ddl.SchemaGenerator, entity, 
                                connection=connection, **kwargs)

    def drop(self, entity, connection=None, **kwargs):
        """Drop a table or index within this engine's database connection
        given a schema object."""

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
    def func(self):
        return expression._FunctionGenerator(bind=self)

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
            visitorcallable(self.dialect, conn,
                            **kwargs).traverse_single(element)
        finally:
            if connection is None:
                conn.close()

    def transaction(self, callable_, *args, **kwargs):
        """Execute the given function within a transaction boundary.

        This is a shortcut for explicitly calling `begin()` and `commit()`
        and optionally `rollback()` when exceptions are raised.  The
        given `*args` and `**kwargs` will be passed to the function.

        The connection used is that of contextual_connect().

        See also the similar method on Connection itself.

        """

        conn = self.contextual_connect()
        try:
            return conn.transaction(callable_, *args, **kwargs)
        finally:
            conn.close()

    def run_callable(self, callable_, *args, **kwargs):
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

        The :class:`.Connection`, upon construction, will procure a DBAPI connection
        from the :class:`.Pool` referenced by this :class:`.Engine`,
        returning it back to the :class:`.Pool` after the :meth:`.Connection.close`
        method is called.

        """

        return self.Connection(self, **kwargs)

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

        return self.Connection(self, 
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

    def reflecttable(self, table, connection=None, include_columns=None):
        """Given a Table object, reflects its columns and properties from the
        database."""

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
        """Return a DB-API connection."""

        return self.pool.unique_connection()


def _proxy_connection_cls(cls, proxy):
    class ProxyConnection(cls):
        def execute(self, object, *multiparams, **params):
            return proxy.execute(self, super(ProxyConnection, self).execute, 
                                            object, *multiparams, **params)

        def _execute_clauseelement(self, elem, multiparams=None, params=None):
            return proxy.execute(self, super(ProxyConnection, self).execute, 
                                            elem, 
                                            *(multiparams or []),
                                            **(params or {}))

        def _cursor_execute(self, cursor, statement, 
                                    parameters, context=None):
            return proxy.cursor_execute(
                            super(ProxyConnection, self)._cursor_execute, 
                            cursor, statement, parameters, context, False)

        def _cursor_executemany(self, cursor, statement, 
                                    parameters, context=None):
            return proxy.cursor_execute(
                            super(ProxyConnection, self)._cursor_executemany, 
                            cursor, statement, parameters, context, True)

        def _begin_impl(self):
            return proxy.begin(self, super(ProxyConnection, self)._begin_impl)

        def _rollback_impl(self):
            return proxy.rollback(self, 
                                super(ProxyConnection, self)._rollback_impl)

        def _commit_impl(self):
            return proxy.commit(self, 
                                super(ProxyConnection, self)._commit_impl)

        def _savepoint_impl(self, name=None):
            return proxy.savepoint(self, 
                                super(ProxyConnection, self)._savepoint_impl,
                                name=name)

        def _rollback_to_savepoint_impl(self, name, context):
            return proxy.rollback_savepoint(self, 
                        super(ProxyConnection,
                                self)._rollback_to_savepoint_impl, 
                                name, context)

        def _release_savepoint_impl(self, name, context):
            return proxy.release_savepoint(self, 
                        super(ProxyConnection, self)._release_savepoint_impl, 
                        name, context)

        def _begin_twophase_impl(self, xid):
            return proxy.begin_twophase(self, 
                        super(ProxyConnection, self)._begin_twophase_impl,
                        xid)

        def _prepare_twophase_impl(self, xid):
            return proxy.prepare_twophase(self, 
                        super(ProxyConnection, self)._prepare_twophase_impl, 
                        xid)

        def _rollback_twophase_impl(self, xid, is_prepared):
            return proxy.rollback_twophase(self, 
                        super(ProxyConnection, self)._rollback_twophase_impl, 
                        xid, is_prepared)

        def _commit_twophase_impl(self, xid, is_prepared):
            return proxy.commit_twophase(self, 
                        super(ProxyConnection, self)._commit_twophase_impl, 
                        xid, is_prepared)

    return ProxyConnection

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
                processor, index = self._keymap[key]
            except KeyError:
                processor, index = self._parent._key_fallback(key)
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
                # TODO: no test coverage here
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
        self._echo = parent._echo
        context = parent.context
        dialect = context.dialect
        typemap = dialect.dbapi_type_map

        for i, (colname, coltype) in enumerate(m[0:2] for m in metadata):
            if dialect.description_encoding:
                colname = colname.decode(dialect.description_encoding)

            if '.' in colname:
                # sqlite will in some circumstances prepend table name to
                # colnames, so strip
                origname = colname
                colname = colname.split('.')[-1]
            else:
                origname = None

            if context.result_map:
                try:
                    name, obj, type_ = context.result_map[colname.lower()]
                except KeyError:
                    name, obj, type_ = \
                        colname, None, typemap.get(coltype, types.NULLTYPE)
            else:
                name, obj, type_ = \
                        colname, None, typemap.get(coltype, types.NULLTYPE)

            processor = type_.dialect_impl(dialect).\
                            result_processor(dialect, coltype)

            processors.append(processor)
            rec = (processor, i)

            # indexes as keys. This is only needed for the Python version of
            # RowProxy (the C version uses a faster path for integer indexes).
            keymap[i] = rec

            # Column names as keys 
            if keymap.setdefault(name.lower(), rec) is not rec: 
                # We do not raise an exception directly because several
                # columns colliding by name is not a problem as long as the
                # user does not try to access them (ie use an index directly,
                # or the more precise ColumnElement)
                keymap[name.lower()] = (processor, None)

            # store the "origname" if we truncated (sqlite only)
            if origname and \
                    keymap.setdefault(origname.lower(), rec) is not rec:
                keymap[origname.lower()] = (processor, None)

            if dialect.requires_name_normalize:
                colname = dialect.normalize_name(colname)

            self.keys.append(colname)
            if obj:
                for o in obj:
                    keymap[o] = rec

        if self._echo:
            self.logger = context.engine.logger
            self.logger.debug(
                "Col %r", tuple(x[0] for x in metadata))

    def _key_fallback(self, key, raiseerr=True):
        map = self._keymap
        result = None
        if isinstance(key, basestring):
            result = map.get(key.lower())
        # fallback for targeting a ColumnElement to a textual expression
        # this is a rare use case which only occurs when matching text()
        # constructs to ColumnElements, and after a pickle/unpickle roundtrip
        elif isinstance(key, expression.ColumnElement):
            if key._label and key._label.lower() in map:
                result = map[key._label.lower()]
            elif hasattr(key, 'name') and key.name.lower() in map:
                result = map[key.name.lower()]
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

    def __len__(self):
        return len(self.keys)

    def __getstate__(self):
        return {
            '_pickled_keymap': dict(
                (key, index)
                for key, (processor, index) in self._keymap.iteritems()
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
            keymap[key] = (None, index)
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

        The 'rowcount' reports the number of rows affected
        by an UPDATE or DELETE statement.  It has *no* other
        uses and is not intended to provide the number of rows
        present from a SELECT.

        Note that this row count may not be properly implemented in some
        dialects; this is indicated by
        :meth:`~sqlalchemy.engine.base.ResultProxy.supports_sane_rowcount()`
        and
        :meth:`~sqlalchemy.engine.base.ResultProxy.supports_sane_multi_rowcount()`.
        ``rowcount()`` also may not work at this time for a statement that
        uses ``returning()``.

        """
        return self.context.rowcount

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
        return self._saved_cursor.lastrowid

    @property
    def returns_rows(self):
        """True if this :class:`.ResultProxy` returns rows.
        
        I.e. if it is legal to call the methods 
        :meth:`~.ResultProxy.fetchone`, 
        :meth:`~.ResultProxy.fetchmany`
        :meth:`~.ResultProxy.fetchall`.
        
        New in 0.6.7.

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

        New in 0.6.7.
        
        """
        return self.context.isinsert

    def _cursor_description(self):
        """May be overridden by subclasses."""

        return self._saved_cursor.description

    def _autoclose(self):
        """called by the Connection to autoclose cursors that have no pending
        results beyond those used by an INSERT/UPDATE/DELETE with no explicit
        RETURNING clause.

        """
        if self.context.isinsert:
            if self.context._is_implicit_returning:
                self.context._fetch_implicit_returning(self)
                self.close(_autoclose_connection=False)
            elif not self.context._is_explicit_returning:
                self.close(_autoclose_connection=False)
        elif self._metadata is None:
            # no results, get rowcount 
            # (which requires open cursor on some drivers
            # such as kintersbasdb, mxodbc),
            self.rowcount
            self.close(_autoclose_connection=False)

        return self

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
            self.cursor.close()
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

        This only applies to single row insert() constructs which
        did not explicitly specify returning().

        """
        if not self.context.isinsert:
            raise exc.InvalidRequestError(
                        "Statement is not an insert() expression construct.")
        elif self.context._is_explicit_returning:
            raise exc.InvalidRequestError(
                        "Can't call inserted_primary_key when returning() "
                        "is used.")

        return self.context._inserted_primary_key

    @util.deprecated("0.6", "Use :attr:`.ResultProxy.inserted_primary_key`")
    def last_inserted_ids(self):
        """Return the primary key for the row just inserted."""

        return self.inserted_primary_key

    def last_updated_params(self):
        """Return ``last_updated_params()`` from the underlying
        ExecutionContext.

        See ExecutionContext for details.
        """

        return self.context.last_updated_params()

    def last_inserted_params(self):
        """Return ``last_inserted_params()`` from the underlying
        ExecutionContext.

        See ExecutionContext for details.
        """

        return self.context.last_inserted_params()

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
        """Return ``supports_sane_rowcount`` from the dialect."""

        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect."""

        return self.dialect.supports_sane_multi_rowcount

    def _fetchone_impl(self):
        try:
            return self.cursor.fetchone()
        except AttributeError:
            self._non_result()

    def _fetchmany_impl(self, size=None):
        try:
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
        50 : 100
    }

    def __buffer_rows(self):
        size = getattr(self, '_bufsize', 1)
        self.__rowbuffer = self.cursor.fetchmany(size)
        self._bufsize = self.size_growth.get(size, size)

    def _fetchone_impl(self):
        if self.closed:
            return None
        if len(self.__rowbuffer) == 0:
            self.__buffer_rows()
            if len(self.__rowbuffer) == 0:
                return None
        return self.__rowbuffer.pop(0)

    def _fetchmany_impl(self, size=None):
        result = []
        for x in range(0, size):
            row = self._fetchone_impl()
            if row is None:
                break
            result.append(row)
        return result

    def _fetchall_impl(self):
        ret = self.__rowbuffer + list(self.cursor.fetchall())
        self.__rowbuffer[:] = []
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
        return self.cursor.fetchall()

    def _fetchone_impl(self):
        if self.__rowbuffer:
            return self.__rowbuffer.pop(0)
        else:
            return None

    def _fetchmany_impl(self, size=None):
        result = []
        for x in range(0, size):
            row = self._fetchone_impl()
            if row is None:
                break
            result.append(row)
        return result

    def _fetchall_impl(self):
        ret = self.__rowbuffer
        self.__rowbuffer = []
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
        for k, (func, index) in metadata._keymap.iteritems():
            keymap[k] = (None, index)
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
