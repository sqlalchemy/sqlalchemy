# engine/base.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Basic components for SQL execution and interfacing with DB-API.

Defines the basic components used to interface DB-API modules with
higher-level statement-construction, connection-management, execution
and result contexts.

"""

__all__ = ['BufferedColumnResultProxy', 'BufferedColumnRow', 'BufferedRowResultProxy', 'Compiled', 'Connectable', 
        'Connection', 'DefaultRunner', 'Dialect', 'Engine', 'ExecutionContext', 'NestedTransaction', 'ResultProxy', 
        'RootTransaction', 'RowProxy', 'SchemaIterator', 'StringIO', 'Transaction', 'TwoPhaseTransaction', 'connection_memoize']

import inspect, StringIO
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
      identifying name for the dialect (i.e. 'sqlite')
      
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

    schemagenerator
      a :class:`~sqlalchemy.schema.SchemaVisitor` class which generates
      schemas.

    schemadropper
      a :class:`~sqlalchemy.schema.SchemaVisitor` class which drops schemas.

    defaultrunner
      a :class:`~sqlalchemy.schema.SchemaVisitor` class which executes
      defaults.

    statement_compiler
      a :class:`~sqlalchemy.engine.base.Compiled` class used to compile SQL
      statements

    preparer
      a :class:`~sqlalchemy.sql.compiler.IdentifierPreparer` class used to
      quote identifiers.

    supports_alter
      ``True`` if the database supports ``ALTER TABLE``.

    max_identifier_length
      The maximum length of identifier names.

    supports_unicode_statements
      Indicate whether the DB-API can receive SQL statements as Python unicode strings

    supports_sane_rowcount
      Indicate whether the dialect properly implements rowcount for ``UPDATE`` and ``DELETE`` statements.

    supports_sane_multi_rowcount
      Indicate whether the dialect properly implements rowcount for ``UPDATE`` and ``DELETE`` statements
      when executed via executemany.

    preexecute_pk_sequences
      Indicate if the dialect should pre-execute sequences on primary key
      columns during an INSERT, if it's desired that the new row's primary key
      be available after execution.

    supports_pk_autoincrement
      Indicates if the dialect should allow the database to passively assign
      a primary key column value.

    dbapi_type_map
      A mapping of DB-API type objects present in this Dialect's
      DB-API implmentation mapped to TypeEngine implementations used
      by the dialect.

      This is used to apply types to result sets based on the DB-API
      types present in cursor.description; it only takes effect for
      result sets against textual statements where no explicit
      typemap was present.

    supports_default_values
      Indicates if the construct ``INSERT INTO tablename DEFAULT VALUES`` is supported

    description_encoding
      type of encoding to use for unicode when working with metadata
      descriptions. If set to ``None`` no encoding will be done.
      This usually defaults to 'utf-8'.
    """

    def create_connect_args(self, url):
        """Build DB-API compatible connection arguments.

        Given a :class:`~sqlalchemy.engine.url.URL` object, returns a tuple
        consisting of a `*args`/`**kwargs` suitable to send directly
        to the dbapi's connect function.
        """

        raise NotImplementedError()


    def type_descriptor(self, typeobj):
        """Transform a generic type to a database-specific type.

        Transforms the given :class:`~sqlalchemy.types.TypeEngine` instance
        from generic to database-specific.

        Subclasses will usually use the
        :func:`~sqlalchemy.types.adapt_type` method in the types module to
        make this job easy.
        """

        raise NotImplementedError()


    def server_version_info(self, connection):
        """Return a tuple of the database's version number."""

        raise NotImplementedError()

    def reflecttable(self, connection, table, include_columns=None):
        """Load table description from the database.

        Given a :class:`~sqlalchemy.engine.Connection` and a
        :class:`~sqlalchemy.schema.Table` object, reflect its columns and
        properties from the database.  If include_columns (a list or
        set) is specified, limit the autoload to the given column
        names.
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

    def get_default_schema_name(self, connection):
        """Return the string name of the currently selected schema given a :class:`~sqlalchemy.engine.Connection`."""

        raise NotImplementedError()

    def do_begin(self, connection):
        """Provide an implementation of *connection.begin()*, given a DB-API connection."""

        raise NotImplementedError()

    def do_rollback(self, connection):
        """Provide an implementation of *connection.rollback()*, given a DB-API connection."""

        raise NotImplementedError()

    def create_xid(self):
        """Create a two-phase transaction ID.

        This id will be passed to do_begin_twophase(),
        do_rollback_twophase(), do_commit_twophase().  Its format is
        unspecified.
        """

        raise NotImplementedError()

    def do_commit(self, connection):
        """Provide an implementation of *connection.commit()*, given a DB-API connection."""

        raise NotImplementedError()

    def do_savepoint(self, connection, name):
        """Create a savepoint with the given name on a SQLAlchemy connection."""

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

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        """Rollback a two phase transaction on the given connection."""

        raise NotImplementedError()

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        """Commit a two phase transaction on the given connection."""

        raise NotImplementedError()

    def do_recover_twophase(self, connection):
        """Recover list of uncommited prepared two phase transaction identifiers on the given connection."""

        raise NotImplementedError()

    def do_executemany(self, cursor, statement, parameters, context=None):
        """Provide an implementation of *cursor.executemany(statement, parameters)*."""

        raise NotImplementedError()

    def do_execute(self, cursor, statement, parameters, context=None):
        """Provide an implementation of *cursor.execute(statement, parameters)*."""

        raise NotImplementedError()

    def is_disconnect(self, e):
        """Return True if the given DB-API error indicates an invalid connection"""

        raise NotImplementedError()


class ExecutionContext(object):
    """A messenger object for a Dialect that corresponds to a single execution.

    ExecutionContext should have these datamembers:

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
      True if the statement is a "committable" statement

    postfetch_cols
     a list of Column objects for which a server-side default
     or inline SQL expression value was fired off.  applies to inserts and updates.


    """

    def create_cursor(self):
        """Return a new cursor generated from this ExecutionContext's connection.

        Some dialects may wish to change the behavior of
        connection.cursor(), such as postgres which may return a PG
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
        """Receive a DBAPI exception which occured upon execute, result fetch, etc."""
        
        raise NotImplementedError()
        
    def should_autocommit_text(self, statement):
        """Parse the given textual statement and return True if it refers to a "committable" statement"""

        raise NotImplementedError()

    def last_inserted_ids(self):
        """Return the list of the primary key values for the last insert statement executed.

        This does not apply to straight textual clauses; only to
        ``sql.Insert`` objects compiled against a ``schema.Table``
        object.  The order of items in the list is the same as that of
        the Table's 'primary_key' attribute.
        """

        raise NotImplementedError()

    def last_inserted_params(self):
        """Return a dictionary of the full parameter dictionary for the last compiled INSERT statement.

        Includes any ColumnDefaults or Sequences that were pre-executed.
        """

        raise NotImplementedError()

    def last_updated_params(self):
        """Return a dictionary of the full parameter dictionary for the last compiled UPDATE statement.

        Includes any ColumnDefaults that were pre-executed.
        """

        raise NotImplementedError()

    def lastrow_has_defaults(self):
        """Return True if the last INSERT or UPDATE row contained
        inlined or database-side defaults.
        """

        raise NotImplementedError()


class Compiled(object):
    """Represent a compiled SQL expression.

    The ``__str__`` method of the ``Compiled`` object should produce
    the actual text of the statement.  ``Compiled`` objects are
    specific to their underlying database dialect, and also may
    or may not be specific to the columns referenced within a
    particular set of bind parameters.  In no case should the
    ``Compiled`` object be dependent on the actual values of those
    bind parameters, even though it may reference those values as
    defaults.
    """

    def __init__(self, dialect, statement, column_keys=None, bind=None):
        """Construct a new ``Compiled`` object.

        dialect
          ``Dialect`` to compile against.

        statement
          ``ClauseElement`` to be compiled.

        column_keys
          a list of column names to be compiled into an INSERT or UPDATE
          statement.

        bind
          Optional Engine or Connection to compile this statement against.
          
        """
        self.dialect = dialect
        self.statement = statement
        self.column_keys = column_keys
        self.bind = bind
        self.can_execute = statement.supports_execution

    def compile(self):
        """Produce the internal string representation of this element."""

        raise NotImplementedError()

    def __str__(self):
        """Return the string text of the generated SQL statement."""

        raise NotImplementedError()

    @util.deprecated('Deprecated. Use construct_params(). '
                     '(supports Unicode key names.)')
    def get_params(self, **params):
        return self.construct_params(params)

    def construct_params(self, params):
        """Return the bind params for this compiled object.

        `params` is a dict of string/object pairs whos
        values will override bind values compiled in
        to the statement.
        """
        raise NotImplementedError()

    def execute(self, *multiparams, **params):
        """Execute this compiled object."""

        e = self.bind
        if e is None:
            raise exc.UnboundExecutionError("This Compiled object is not bound to any Engine or Connection.")
        return e._execute_compiled(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Execute this compiled object and return the result's scalar value."""

        return self.execute(*multiparams, **params).scalar()


class Connectable(object):
    """Interface for an object which supports execution of SQL constructs.
    
    The two implementations of ``Connectable`` are :class:`Connection` and
    :class:`Engine`.
    
    """

    def contextual_connect(self):
        """Return a Connection object which may be part of an ongoing context."""

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

    Provides execution support for string-based SQL statements as well
    as ClauseElement, Compiled and DefaultGenerator objects.  Provides
    a begin method to return Transaction objects.

    The Connection object is **not** thread-safe.

    .. index::
      single: thread safety; Connection

    """

    def __init__(self, engine, connection=None, close_with_result=False,
                 _branch=False):
        """Construct a new Connection.

        Connection objects are typically constructed by an
        :class:`~sqlalchemy.engine.Engine`, see the ``connect()`` and
        ``contextual_connect()`` methods of Engine.
        
        """

        self.engine = engine
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.__close_with_result = close_with_result
        self.__savepoint_seq = 0
        self.__branch = _branch
        self.__invalid = False
        
    def _branch(self):
        """Return a new Connection which references this Connection's
        engine and connection; but does not have close_with_result enabled,
        and also whose close() method does nothing.

        This is used to execute "sub" statements within a single execution,
        usually an INSERT statement.
        
        """
        return self.engine.Connection(self.engine, self.__connection, _branch=True)

    @property
    def dialect(self):
        "Dialect used by this Connection."

        return self.engine.dialect

    @property
    def closed(self):
        """return True if this connection is closed."""

        return not self.__invalid and '_Connection__connection' not in self.__dict__

    @property
    def invalidated(self):
        """return True if this connection was invalidated."""

        return self.__invalid

    @property
    def connection(self):
        "The underlying DB-API connection managed by this Connection."

        try:
            return self.__connection
        except AttributeError:
            if self.__invalid:
                if self.__transaction is not None:
                    raise exc.InvalidRequestError("Can't reconnect until invalid transaction is rolled back")
                self.__connection = self.engine.raw_connection()
                self.__invalid = False
                return self.__connection
            raise exc.InvalidRequestError("This Connection is closed")

    @property
    def should_close_with_result(self):
        """Indicates if this Connection should be closed when a corresponding
        ResultProxy is closed; this is essentially an auto-release mode.
        
        """
        return self.__close_with_result

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
        """Invalidate the underlying DBAPI connection associated with this Connection.

        The underlying DB-API connection is literally closed (if
        possible), and is discarded.  Its source connection pool will
        typically lazily create a new connection to replace it.

        Upon the next usage, this Connection will attempt to reconnect
        to the pool with a new connection.

        Transactions in progress remain in an "opened" state (even though
        the actual transaction is gone); these must be explicitly
        rolled back before a reconnect on this Connection can proceed.  This
        is to prevent applications from accidentally continuing their transactional
        operations in a non-transactional state.

        """
        if self.closed:
            raise exc.InvalidRequestError("This Connection is closed")

        if self.__connection.is_valid:
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
        else:
            return Transaction(self, self.__transaction)
        return self.__transaction

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
        """Begin a two-phase or XA transaction and return a Transaction handle.

        xid
          the two phase transaction id.  If not supplied, a random id
          will be generated.
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
        if self.engine._should_log_info:
            self.engine.logger.info("BEGIN")
        try:
            self.engine.dialect.do_begin(self.connection)
        except Exception, e:
            self._handle_dbapi_exception(e, None, None, None, None)
            raise

    def _rollback_impl(self):
        if not self.closed and not self.invalidated and self.__connection.is_valid:
            if self.engine._should_log_info:
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
        if self.engine._should_log_info:
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
        if self.__connection.is_valid:
            self.engine.dialect.do_savepoint(self, name)
            return name

    def _rollback_to_savepoint_impl(self, name, context):
        if self.__connection.is_valid:
            self.engine.dialect.do_rollback_to_savepoint(self, name)
        self.__transaction = context

    def _release_savepoint_impl(self, name, context):
        if self.__connection.is_valid:
            self.engine.dialect.do_release_savepoint(self, name)
        self.__transaction = context

    def _begin_twophase_impl(self, xid):
        if self.__connection.is_valid:
            self.engine.dialect.do_begin_twophase(self, xid)

    def _prepare_twophase_impl(self, xid):
        if self.__connection.is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_prepare_twophase(self, xid)

    def _rollback_twophase_impl(self, xid, is_prepared):
        if self.__connection.is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.engine.dialect.do_rollback_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _commit_twophase_impl(self, xid, is_prepared):
        if self.__connection.is_valid:
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

    def scalar(self, object, *multiparams, **params):
        """Executes and returns the first column of the first row.

        The underlying result/cursor is closed after execution.
        """

        return self.execute(object, *multiparams, **params).scalar()

    def statement_compiler(self, statement, **kwargs):
        return self.dialect.statement_compiler(self.dialect, statement, bind=self, **kwargs)

    def execute(self, object, *multiparams, **params):
        """Executes and returns a ResultProxy."""

        for c in type(object).__mro__:
            if c in Connection.executors:
                return Connection.executors[c](self, object, multiparams, params)
        else:
            raise exc.InvalidRequestError("Unexecutable object type: " + str(type(object)))

    def __distill_params(self, multiparams, params):
        """given arguments from the calling form *multiparams, **params, return a list
        of bind parameter structures, usually a list of dictionaries.

        in the case of 'raw' execution which accepts positional parameters,
        it may be a list of tuples or lists."""

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
        return self.engine.dialect.defaultrunner(self.__create_execution_context()).traverse_single(default)

    def _execute_clauseelement(self, elem, multiparams, params):
        params = self.__distill_params(multiparams, params)
        if params:
            keys = params[0].keys()
        else:
            keys = []

        context = self.__create_execution_context(
                        compiled=elem.compile(dialect=self.dialect, column_keys=keys, inline=len(params) > 1), 
                        parameters=params
                    )
        return self.__execute_context(context)

    def _execute_compiled(self, compiled, multiparams, params):
        """Execute a sql.Compiled object."""

        context = self.__create_execution_context(
                    compiled=compiled, 
                    parameters=self.__distill_params(multiparams, params)
                )
        return self.__execute_context(context)

    def _execute_text(self, statement, multiparams, params):
        parameters = self.__distill_params(multiparams, params)
        context = self.__create_execution_context(statement=statement, parameters=parameters)
        return self.__execute_context(context)
    
    def __execute_context(self, context):
        if context.compiled:
            context.pre_exec()
        if context.executemany:
            self._cursor_executemany(context.cursor, context.statement, context.parameters, context=context)
        else:
            self._cursor_execute(context.cursor, context.statement, context.parameters[0], context=context)
        if context.compiled:
            context.post_exec()
        if context.should_autocommit and not self.in_transaction():
            self._commit_impl()
        return context.get_result_proxy()
        
    def _execute_ddl(self, ddl, params, multiparams):
        if params:
            schema_item, params = params[0], params[1:]
        else:
            schema_item = None
        return ddl(None, schema_item, self, *params, **multiparams)

    def _handle_dbapi_exception(self, e, statement, parameters, cursor, context):
        if getattr(self, '_reentrant_error', False):
            raise exc.DBAPIError.instance(None, None, e)
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
                if self.__close_with_result:
                    self.close()
            raise exc.DBAPIError.instance(statement, parameters, e, connection_invalidated=is_disconnect)
        finally:
            del self._reentrant_error

    def __create_execution_context(self, **kwargs):
        try:
            dialect = self.engine.dialect
            return dialect.execution_ctx_cls(dialect, connection=self, **kwargs)
        except Exception, e:
            self._handle_dbapi_exception(e, kwargs.get('statement', None), kwargs.get('parameters', None), None, None)
            raise

    def _cursor_execute(self, cursor, statement, parameters, context=None):
        if self.engine._should_log_info:
            self.engine.logger.info(statement)
            self.engine.logger.info(repr(parameters))
        try:
            self.dialect.do_execute(cursor, statement, parameters, context=context)
        except Exception, e:
            self._handle_dbapi_exception(e, statement, parameters, cursor, context)
            raise

    def _cursor_executemany(self, cursor, statement, parameters, context=None):
        if self.engine._should_log_info:
            self.engine.logger.info(statement)
            self.engine.logger.info(repr(parameters))
        try:
            self.dialect.do_executemany(cursor, statement, parameters, context=context)
        except Exception, e:
            self._handle_dbapi_exception(e, statement, parameters, cursor, context)
            raise

    # poor man's multimethod/generic function thingy
    executors = {
        expression.Function: _execute_function,
        expression.ClauseElement: _execute_clauseelement,
        Compiled: _execute_compiled,
        schema.SchemaItem: _execute_default,
        schema.DDL: _execute_ddl,
        basestring: _execute_text
    }

    def create(self, entity, **kwargs):
        """Create a Table or Index given an appropriate Schema object."""

        return self.engine.create(entity, connection=self, **kwargs)

    def drop(self, entity, **kwargs):
        """Drop a Table or Index given an appropriate Schema object."""

        return self.engine.drop(entity, connection=self, **kwargs)

    def reflecttable(self, table, include_columns=None):
        """Reflect the columns in the given string table name from the database."""

        return self.engine.reflecttable(table, self, include_columns)

    def default_schema_name(self):
        return self.engine.dialect.get_default_schema_name(self)

    def run_callable(self, callable_):
        return callable_(self)

class Transaction(object):
    """Represent a Transaction in progress.

    The Transaction object is **not** threadsafe.

    .. index::
      single: thread safety; Transaction

    """

    def __init__(self, connection, parent):
        self.connection = connection
        self._parent = parent or self
        self.is_active = True
    
    def close(self):
        """Close this transaction.

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
        if not self._parent.is_active:
            return
        self.is_active = False
        self._do_rollback()

    def _do_rollback(self):
        self._parent.rollback()

    def commit(self):
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
        self.connection._rollback_impl()

    def _do_commit(self):
        self.connection._commit_impl()

class NestedTransaction(Transaction):
    def __init__(self, connection, parent):
        super(NestedTransaction, self).__init__(connection, parent)
        self._savepoint = self.connection._savepoint_impl()

    def _do_rollback(self):
        self.connection._rollback_to_savepoint_impl(self._savepoint, self._parent)

    def _do_commit(self):
        self.connection._release_savepoint_impl(self._savepoint, self._parent)

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

class Engine(Connectable):
    """
    Connects a :class:`~sqlalchemy.pool.Pool` and :class:`~sqlalchemy.engine.base.Dialect` 
    together to provide a source of database connectivity and behavior.

    """

    def __init__(self, pool, dialect, url, echo=None, proxy=None):
        self.pool = pool
        self.url = url
        self.dialect = dialect
        self.echo = echo
        self.engine = self
        self.logger = log.instance_logger(self, echoflag=echo)
        if proxy:
            self.Connection = _proxy_connection_cls(Connection, proxy)
        else:
            self.Connection = Connection

    @property
    def name(self):
        "String name of the :class:`~sqlalchemy.engine.Dialect` in use by this ``Engine``."
        
        return self.dialect.name

    echo = log.echo_property()

    def __repr__(self):
        return 'Engine(%s)' % str(self.url)

    def dispose(self):
        self.pool.dispose()
        self.pool = self.pool.recreate()

    def create(self, entity, connection=None, **kwargs):
        """Create a table or index within this engine's database connection given a schema.Table object."""

        self._run_visitor(self.dialect.schemagenerator, entity, connection=connection, **kwargs)

    def drop(self, entity, connection=None, **kwargs):
        """Drop a table or index within this engine's database connection given a schema.Table object."""

        self._run_visitor(self.dialect.schemadropper, entity, connection=connection, **kwargs)

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
        """Return a sql.text() object for performing literal queries."""

        return expression.text(text, bind=self, *args, **kwargs)

    def _run_visitor(self, visitorcallable, element, connection=None, **kwargs):
        if connection is None:
            conn = self.contextual_connect(close_with_result=False)
        else:
            conn = connection
        try:
            visitorcallable(self.dialect, conn, **kwargs).traverse(element)
        finally:
            if connection is None:
                conn.close()

    def transaction(self, callable_, connection=None, *args, **kwargs):
        """Execute the given function within a transaction boundary.

        This is a shortcut for explicitly calling `begin()` and `commit()`
        and optionally `rollback()` when exceptions are raised.  The
        given `*args` and `**kwargs` will be passed to the function, as
        well as the Connection used in the transaction.
        """

        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            trans = conn.begin()
            try:
                ret = callable_(conn, *args, **kwargs)
                trans.commit()
                return ret
            except:
                trans.rollback()
                raise
        finally:
            if connection is None:
                conn.close()

    def run_callable(self, callable_, connection=None, *args, **kwargs):
        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            return callable_(conn, *args, **kwargs)
        finally:
            if connection is None:
                conn.close()

    def execute(self, statement, *multiparams, **params):
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

    def statement_compiler(self, statement, **kwargs):
        return self.dialect.statement_compiler(self.dialect, statement, bind=self, **kwargs)

    def connect(self, **kwargs):
        """Return a newly allocated Connection object."""

        return self.Connection(self, **kwargs)

    def contextual_connect(self, close_with_result=False, **kwargs):
        """Return a Connection object which may be newly allocated, or may be part of some ongoing context.

        This Connection is meant to be used by the various "auto-connecting" operations.
        """

        return self.Connection(self, self.pool.connect(), close_with_result=close_with_result, **kwargs)

    def table_names(self, schema=None, connection=None):
        """Return a list of all table names available in the database.

        schema:
          Optional, retrieve names from a non-default schema.

        connection:
          Optional, use a specified connection.  Default is the
          ``contextual_connect`` for this ``Engine``.
        """

        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        if not schema:
            try:
                schema =  self.dialect.get_default_schema_name(conn)
            except NotImplementedError:
                pass
        try:
            return self.dialect.table_names(conn, schema)
        finally:
            if connection is None:
                conn.close()

    def reflecttable(self, table, connection=None, include_columns=None):
        """Given a Table object, reflects its columns and properties from the database."""

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
        return self.run_callable(lambda c: self.dialect.has_table(c, table_name, schema=schema))

    def raw_connection(self):
        """Return a DB-API connection."""

        return self.pool.unique_connection()

def _proxy_connection_cls(cls, proxy):
    class ProxyConnection(cls):
        def execute(self, object, *multiparams, **params):
            return proxy.execute(self, super(ProxyConnection, self).execute, object, *multiparams, **params)
 
        def _execute_clauseelement(self, elem, multiparams=None, params=None):
            return proxy.execute(self, super(ProxyConnection, self).execute, elem, *(multiparams or []), **(params or {}))
            
        def _cursor_execute(self, cursor, statement, parameters, context=None):
            return proxy.cursor_execute(super(ProxyConnection, self)._cursor_execute, cursor, statement, parameters, context, False)
 
        def _cursor_executemany(self, cursor, statement, parameters, context=None):
            return proxy.cursor_execute(super(ProxyConnection, self)._cursor_executemany, cursor, statement, parameters, context, True)

    return ProxyConnection

class RowProxy(object):
    """Proxy a single cursor row for a parent ResultProxy.

    Mostly follows "ordered dictionary" behavior, mapping result
    values to the string-based column name, the integer position of
    the result in the row, as well as Column instances which can be
    mapped to the original Columns that produced this result set (for
    results that correspond to constructed SQL expressions).
    """

    __slots__ = ['__parent', '__row']
    
    def __init__(self, parent, row):
        """RowProxy objects are constructed by ResultProxy objects."""

        self.__parent = parent
        self.__row = row
        if self.__parent._echo:
            self.__parent.context.engine.logger.debug("Row " + repr(row))

    def close(self):
        """Close the parent ResultProxy."""

        self.__parent.close()

    def __contains__(self, key):
        return self.__parent._has_key(self.__row, key)

    def __len__(self):
        return len(self.__row)

    def __iter__(self):
        for i in xrange(len(self.__row)):
            yield self.__parent._get_col(self.__row, i)

    __hash__ = None
    
    def __eq__(self, other):
        return ((other is self) or
                (other == tuple(self.__parent._get_col(self.__row, key)
                                for key in xrange(len(self.__row)))))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return repr(tuple(self))

    def has_key(self, key):
        """Return True if this RowProxy contains the given key."""

        return self.__parent._has_key(self.__row, key)

    def __getitem__(self, key):
        return self.__parent._get_col(self.__row, key)

    def __getattr__(self, name):
        try:
            return self.__parent._get_col(self.__row, name)
        except KeyError, e:
            raise AttributeError(e.args[0])

    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair."""

        return [(key, getattr(self, key)) for key in self.iterkeys()]

    def keys(self):
        """Return the list of keys as strings represented by this RowProxy."""

        return self.__parent.keys
    
    def iterkeys(self):
        return iter(self.__parent.keys)
        
    def values(self):
        """Return the values represented by this RowProxy as a list."""

        return list(self)
    
    def itervalues(self):
        return iter(self)

class BufferedColumnRow(RowProxy):
    def __init__(self, parent, row):
        row = [ResultProxy._get_col(parent, row, i) for i in xrange(len(row))]
        super(BufferedColumnRow, self).__init__(parent, row)


class ResultProxy(object):
    """Wraps a DB-API cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by ``schema.Column``
    object. e.g.::

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ResultProxy also contains a map of TypeEngine objects and will
    invoke the appropriate ``result_processor()`` method before
    returning columns, as well as the ExecutionContext corresponding
    to the statement execution.  It provides several methods for which
    to obtain information from the underlying ExecutionContext.
    """

    _process_row = RowProxy

    def __init__(self, context):
        """ResultProxy objects are constructed via the execute() method on SQLEngine."""
        self.context = context
        self.dialect = context.dialect
        self.closed = False
        self.cursor = context.cursor
        self.connection = context.root_connection
        self._echo = context.engine._should_log_info
        self._init_metadata()
    
    @property
    def rowcount(self):
        if self._rowcount is None:
            return self.context.get_rowcount()
        else:
            return self._rowcount

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    @property
    def out_parameters(self):
        return self.context.out_parameters

    def _init_metadata(self):
        metadata = self.cursor.description
        if metadata is None:
            # no results, get rowcount (which requires open cursor on some DB's such as firebird),
            # then close
            self._rowcount = self.context.get_rowcount()
            self.close()
            return
            
        self._rowcount = None
        self._props = util.populate_column_dict(None)
        self._props.creator = self.__key_fallback()
        self.keys = []

        typemap = self.dialect.dbapi_type_map

        for i, item in enumerate(metadata):
            colname = item[0]
            if self.dialect.description_encoding:
                colname = colname.decode(self.dialect.description_encoding)

            if '.' in colname:
                # sqlite will in some circumstances prepend table name to colnames, so strip
                origname = colname
                colname = colname.split('.')[-1]
            else:
                origname = None

            if self.context.result_map:
                try:
                    (name, obj, type_) = self.context.result_map[colname.lower()]
                except KeyError:
                    (name, obj, type_) = (colname, None, typemap.get(item[1], types.NULLTYPE))
            else:
                (name, obj, type_) = (colname, None, typemap.get(item[1], types.NULLTYPE))

            rec = (type_, type_.dialect_impl(self.dialect).result_processor(self.dialect), i)

            if self._props.setdefault(name.lower(), rec) is not rec:
                self._props[name.lower()] = (type_, self.__ambiguous_processor(name), 0)

            # store the "origname" if we truncated (sqlite only)
            if origname:
                if self._props.setdefault(origname.lower(), rec) is not rec:
                    self._props[origname.lower()] = (type_, self.__ambiguous_processor(origname), 0)

            self.keys.append(colname)
            self._props[i] = rec
            if obj:
                for o in obj:
                    self._props[o] = rec

        if self._echo:
            self.context.engine.logger.debug(
                "Col " + repr(tuple(x[0] for x in metadata)))
    
    def __key_fallback(self):
        # create a closure without 'self' to avoid circular references
        props = self._props
        
        def fallback(key):
            if isinstance(key, basestring):
                key = key.lower()
                if key in props:
                    return props[key]

            # fallback for targeting a ColumnElement to a textual expression
            # this is a rare use case which only occurs when matching text()
            # constructs to ColumnElements
            if isinstance(key, expression.ColumnElement):
                if key._label and key._label.lower() in props:
                    return props[key._label.lower()]
                elif hasattr(key, 'name') and key.name.lower() in props:
                    return props[key.name.lower()]

            raise exc.NoSuchColumnError("Could not locate column in row for column '%s'" % (str(key)))
        return fallback

    def __ambiguous_processor(self, colname):
        def process(value):
            raise exc.InvalidRequestError("Ambiguous column name '%s' in result set! "
                        "try 'use_labels' option on select statement." % colname)
        return process

    def close(self):
        """Close this ResultProxy.
        
        Closes the underlying DBAPI cursor corresponding to the execution.

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
            if self.connection.should_close_with_result:
                self.connection.close()

    def _has_key(self, row, key):
        try:
            # _key_cache uses __missing__ in 2.5, so not much alternative
            # to catching KeyError
            self._props[key]
            return True
        except KeyError:
            return False

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                raise StopIteration
            else:
                yield row

    def last_inserted_ids(self):
        """Return ``last_inserted_ids()`` from the underlying ExecutionContext.

        See ExecutionContext for details.

        """
        return self.context.last_inserted_ids()

    def last_updated_params(self):
        """Return ``last_updated_params()`` from the underlying ExecutionContext.

        See ExecutionContext for details.

        """
        return self.context.last_updated_params()

    def last_inserted_params(self):
        """Return ``last_inserted_params()`` from the underlying ExecutionContext.

        See ExecutionContext for details.

        """
        return self.context.last_inserted_params()

    def lastrow_has_defaults(self):
        """Return ``lastrow_has_defaults()`` from the underlying ExecutionContext.

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

    def _get_col(self, row, key):
        try:
            type_, processor, index = self._props[key]
        except TypeError:
            # the 'slice' use case is very infrequent,
            # so we use an exception catch to reduce conditionals in _get_col
            if isinstance(key, slice):
                indices = key.indices(len(row))
                return tuple(self._get_col(row, i) for i in xrange(*indices))
            else:
                raise

        if processor:
            return processor(row[index])
        else:
            return row[index]

    def _fetchone_impl(self):
        return self.cursor.fetchone()

    def _fetchmany_impl(self, size=None):
        return self.cursor.fetchmany(size)

    def _fetchall_impl(self):
        return self.cursor.fetchall()

    def fetchall(self):
        """Fetch all rows, just like DB-API ``cursor.fetchall()``."""

        try:
            process_row = self._process_row
            l = [process_row(self, row) for row in self._fetchall_impl()]
            self.close()
            return l
        except Exception, e:
            self.connection._handle_dbapi_exception(e, None, None, self.cursor, self.context)
            raise

    def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API ``cursor.fetchmany(size=cursor.arraysize)``."""

        try:
            process_row = self._process_row
            l = [process_row(self, row) for row in self._fetchmany_impl(size)]
            if len(l) == 0:
                self.close()
            return l
        except Exception, e:
            self.connection._handle_dbapi_exception(e, None, None, self.cursor, self.context)
            raise

    def fetchone(self):
        """Fetch one row, just like DB-API ``cursor.fetchone()``."""
        try:
            row = self._fetchone_impl()
            if row is not None:
                return self._process_row(self, row)
            else:
                self.close()
                return None
        except Exception, e:
            self.connection._handle_dbapi_exception(e, None, None, self.cursor, self.context)
            raise

    def scalar(self):
        """Fetch the first column of the first row, and close the result set."""
        try:
            row = self._fetchone_impl()
        except Exception, e:
            self.connection._handle_dbapi_exception(e, None, None, self.cursor, self.context)
            raise
            
        try:
            if row is not None:
                return self._process_row(self, row)[0]
            else:
                return None
        finally:
            self.close()

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
        return self.__rowbuffer + list(self.cursor.fetchall())

class BufferedColumnResultProxy(ResultProxy):
    """A ResultProxy with column buffering behavior.

    ``ResultProxy`` that loads all columns into memory each time
    fetchone() is called.  If fetchmany() or fetchall() are called,
    the full grid of results is fetched.  This is to operate with
    databases where result rows contain "live" results that fall out
    of scope unless explicitly fetched.  Currently this includes just
    cx_Oracle LOB objects, but this behavior is known to exist in
    other DB-APIs as well (Pygresql, currently unsupported).
    
    """

    _process_row = BufferedColumnRow

    def _get_col(self, row, key):
        try:
            rec = self._props[key]
            return row[rec[2]]
        except TypeError:
            # the 'slice' use case is very infrequent,
            # so we use an exception catch to reduce conditionals in _get_col
            if isinstance(key, slice):
                indices = key.indices(len(row))
                return tuple(self._get_col(row, i) for i in xrange(*indices))
            else:
                raise

    def fetchall(self):
        l = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            l.append(row)
        return l

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        l = []
        for i in xrange(size):
            row = self.fetchone()
            if row is None:
                break
            l.append(row)
        return l


class SchemaIterator(schema.SchemaVisitor):
    """A visitor that can gather text into a buffer and execute the contents of the buffer."""

    def __init__(self, connection):
        """Construct a new SchemaIterator."""
        
        self.connection = connection
        self.buffer = StringIO.StringIO()

    def append(self, s):
        """Append content to the SchemaIterator's query buffer."""

        self.buffer.write(s)

    def execute(self):
        """Execute the contents of the SchemaIterator's buffer."""

        try:
            return self.connection.execute(self.buffer.getvalue())
        finally:
            self.buffer.truncate(0)

class DefaultRunner(schema.SchemaVisitor):
    """A visitor which accepts ColumnDefault objects, produces the
    dialect-specific SQL corresponding to their execution, and
    executes the SQL, returning the result value.

    DefaultRunners are used internally by Engines and Dialects.
    Specific database modules should provide their own subclasses of
    DefaultRunner to allow database-specific behavior.

    """

    def __init__(self, context):
        self.context = context
        self.dialect = context.dialect
        self.cursor = context.cursor

    def get_column_default(self, column):
        if column.default is not None:
            return self.traverse_single(column.default)
        else:
            return None

    def get_column_onupdate(self, column):
        if column.onupdate is not None:
            return self.traverse_single(column.onupdate)
        else:
            return None

    def visit_passive_default(self, default):
        return None

    def visit_sequence(self, seq):
        return None

    def exec_default_sql(self, default):
        conn = self.context.connection
        c = expression.select([default.arg]).compile(bind=conn)
        return conn._execute_compiled(c, (), {}).scalar()

    def execute_string(self, stmt, params=None):
        """execute a string statement, using the raw cursor, and return a scalar result."""
        
        conn = self.context._connection
        if isinstance(stmt, unicode) and not self.dialect.supports_unicode_statements:
            stmt = stmt.encode(self.dialect.encoding)
        conn._cursor_execute(self.cursor, stmt, params)
        return self.cursor.fetchone()[0]

    def visit_column_onupdate(self, onupdate):
        if isinstance(onupdate.arg, expression.ClauseElement):
            return self.exec_default_sql(onupdate)
        elif util.callable(onupdate.arg):
            return onupdate.arg(self.context)
        else:
            return onupdate.arg

    def visit_column_default(self, default):
        if isinstance(default.arg, expression.ClauseElement):
            return self.exec_default_sql(default)
        elif util.callable(default.arg):
            return default.arg(self.context)
        else:
            return default.arg


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
