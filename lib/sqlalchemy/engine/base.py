# engine/base.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Basic components for SQL execution and interfacing with DB-API..

Defines the basic components used to interface DB-API modules with
higher-level statement-construction, connection-management, execution
and result contexts.
"""

from sqlalchemy import exceptions, schema, util, types, logging
from sqlalchemy.sql import expression, visitors
import StringIO, sys


class Dialect(object):
    """Define the behavior of a specific database and DB-API combination.

    Any aspect of metadata definition, SQL query generation,
    execution, result-set handling, or anything else which varies
    between databases is defined under the general category of the
    Dialect.  The Dialect acts as a factory for other
    database-specific object implementations including
    ExecutionContext, Compiled, DefaultGenerator, and TypeEngine.

    All Dialects implement the following attributes:

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
      a [sqlalchemy.schema#SchemaVisitor] class which generates
      schemas.

    schemadropper
      a [sqlalchemy.schema#SchemaVisitor] class which drops schemas.

    defaultrunner
      a [sqlalchemy.schema#SchemaVisitor] class which executes
      defaults.

    statement_compiler
      a [sqlalchemy.engine.base#Compiled] class used to compile SQL
      statements

    preparer
      a [sqlalchemy.sql.compiler#IdentifierPreparer] class used to
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

    preexecute_sequences
      Indicate if the dialect should pre-execute sequences on primary key columns during an INSERT,
      if it's desired that the new row's primary key be available after execution.
    """

    def create_connect_args(self, url):
        """Build DB-API compatible connection arguments.

        Given a [sqlalchemy.engine.url#URL] object, returns a tuple
        consisting of a `*args`/`**kwargs` suitable to send directly
        to the dbapi's connect function.
        """

        raise NotImplementedError()

    def dbapi_type_map(self):
        """Returns a DB-API to sqlalchemy.types mapping.

        A mapping of DB-API type objects present in this Dialect's
        DB-API implmentation mapped to TypeEngine implementations used
        by the dialect.

        This is used to apply types to result sets based on the DB-API
        types present in cursor.description; it only takes effect for
        result sets against textual statements where no explicit
        typemap was present.  Constructed SQL statements always have
        type information explicitly embedded.
        """

        raise NotImplementedError()

    def type_descriptor(self, typeobj):
        """Transform a generic type to a database-specific type.

        Transforms the given [sqlalchemy.types#TypeEngine] instance
        from generic to database-specific.

        Subclasses will usually use the
        [sqlalchemy.types#adapt_type()] method in the types module to
        make this job easy.
        """

        raise NotImplementedError()

    def oid_column_name(self, column):
        """Return the oid column name for this Dialect

        May return ``None`` if the dialect can't o won't support
        OID/ROWID features.

        The [sqlalchemy.schema#Column] instance which represents OID
        for the query being compiled is passed, so that the dialect
        can inspect the column and its parent selectable to determine
        if OID/ROWID is not selected for a particular selectable
        (i.e. Oracle doesnt support ROWID for UNION, GROUP BY,
        DISTINCT, etc.)
        """

        raise NotImplementedError()



    def server_version_info(self, connection):
        """Return a tuple of the database's version number."""

        raise NotImplementedError()

    def reflecttable(self, connection, table, include_columns=None):
        """Load table description from the database.

        Given a [sqlalchemy.engine#Connection] and a
        [sqlalchemy.schema#Table] object, reflect its columns and
        properties from the database.  If include_columns (a list or
        set) is specified, limit the autoload to the given column
        names.
        """

        raise NotImplementedError()

    def has_table(self, connection, table_name, schema=None):
        """Check the existence of a particular table in the database.

        Given a [sqlalchemy.engine#Connection] object and a string
        `table_name`, return True if the given table (possibly within
        the specified `schema`) exists in the database, False
        otherwise.
        """

        raise NotImplementedError()

    def has_sequence(self, connection, sequence_name):
        """Check the existence of a particular sequence in the database.

        Given a [sqlalchemy.engine#Connection] object and a string
        `sequence_name`, return True if the given sequence exists in
        the database, False otherwise.
        """

        raise NotImplementedError()

    def get_default_schema_name(self, connection):
        """Return the string name of the currently selected schema given a [sqlalchemy.engine#Connection]."""

        raise NotImplementedError()

    def create_execution_context(self, connection, compiled=None, compiled_parameters=None, statement=None, parameters=None):
        """Return a new [sqlalchemy.engine#ExecutionContext] object."""

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

    The Dialect should provide an ExecutionContext via the
    create_execution_context() method.  The `pre_exec` and `post_exec`
    methods will be called for compiled statements.
    """

    def create_cursor(self):
        """Return a new cursor generated from this ExecutionContext's connection.

        Some dialects may wish to change the behavior of
        connection.cursor(), such as postgres which may return a PG
        "server side" cursor.
        """

        raise NotImplementedError()

    def pre_execution(self):
        """Called before an execution of a compiled statement.

        If a compiled statement was passed to this ExecutionContext,
        the `statement` and `parameters` datamembers must be
        initialized after this statement is complete.
        """

        raise NotImplementedError()

    def post_execution(self):
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

    def get_rowcount(self):
        """Return the count of rows updated/deleted for an UPDATE/DELETE statement."""

        raise NotImplementedError()

    def should_autocommit(self):
        """Return True if this context's statement should be 'committed' automatically in a non-transactional context"""

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

    def postfetch_cols(self):
        """return a list of Column objects for which a 'passive' server-side default
        value was fired off.  applies to inserts and updates."""

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
        self.can_execute = statement.supports_execution()
    
    def compile(self):
        """Produce the internal string representation of this element."""
        
        raise NotImplementedError()
        
    def __str__(self):
        """Return the string text of the generated SQL statement."""

        raise NotImplementedError()

    def get_params(self, **params):
        """Use construct_params().  (supports unicode names)
        """

        return self.construct_params(params)
    get_params = util.deprecated(get_params)

    def construct_params(self, params):
        """Return the bind params for this compiled object.

        params is a dict of string/object pairs whos 
        values will override bind values compiled in
        to the statement.
        """
        raise NotImplementedError()

    def execute(self, *multiparams, **params):
        """Execute this compiled object."""

        e = self.bind
        if e is None:
            raise exceptions.InvalidRequestError("This Compiled object is not bound to any Engine or Connection.")
        return e._execute_compiled(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Execute this compiled object and return the result's scalar value."""

        return self.execute(*multiparams, **params).scalar()


class Connectable(object):
    """Interface for an object that can provide an Engine and a Connection object which correponds to that Engine."""

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

class Connection(Connectable):
    """Provides high-level functionality for a wrapped DB-API connection.

    Provides execution support for string-based SQL statements as well
    as ClauseElement, Compiled and DefaultGenerator objects.  Provides
    a begin method to return Transaction objects.

    The Connection object is **not** threadsafe.
    """

    def __init__(self, engine, connection=None, close_with_result=False,
                 _branch=False):
        """Construct a new Connection.

        Connection objects are typically constructed by an
        [sqlalchemy.engine#Engine], see the ``connect()`` and
        ``contextual_connect()`` methods of Engine.
        """

        self.__engine = engine
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.__close_with_result = close_with_result
        self.__savepoint_seq = 0
        self.__branch = _branch

    def _get_connection(self):
        try:
            return self.__connection
        except AttributeError:
            raise exceptions.InvalidRequestError("This Connection is closed")

    def _branch(self):
        """return a new Connection which references this Connection's 
        engine and connection; but does not have close_with_result enabled,
        and also whose close() method does nothing.

        This is used to execute "sub" statements within a single execution,
        usually an INSERT statement.
        """
        return Connection(self.__engine, self.__connection, _branch=True)

    engine = property(lambda s:s.__engine, doc="The Engine with which this Connection is associated.")
    dialect = property(lambda s:s.__engine.dialect, doc="Dialect used by this Connection.")
    connection = property(_get_connection, doc="The underlying DB-API connection managed by this Connection.")
    should_close_with_result = property(lambda s:s.__close_with_result, doc="Indicates if this Connection should be closed when a corresponding ResultProxy is closed; this is essentially an auto-release mode.")
    properties = property(lambda s: s._get_connection().properties,
                          doc="A collection of per-DB-API connection instance properties.")

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

    def invalidate(self):
        """Invalidate and close the Connection.

        The underlying DB-API connection is literally closed (if
        possible), and is discarded.  Its source connection pool will
        typically lazilly create a new connection to replace it.
        """

        self.__connection.invalidate()
        self.__connection = None

    def detach(self):
        """Detach the underlying DB-API connection from its connection pool.

        This Connection instance will remain useable.  When closed,
        the DB-API connection will be literally closed and not
        returned to its pool.  The pool will typically lazily create a
        new connection to replace the detached connection.

        This method can be used to insulate the rest of an application
        from a modified state on a connection (such as a transaction
        isolation level or similar).  Also see
        [sqlalchemy.interfaces#PoolListener] for a mechanism to modify
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
            raise exceptions.InvalidRequestError(
                "Cannot start a two phase transaction when a transaction "
                "is already in progress.")
        if xid is None:
            xid = self.__engine.dialect.create_xid();
        self.__transaction = TwoPhaseTransaction(self, xid)
        return self.__transaction

    def recover_twophase(self):
        return self.__engine.dialect.do_recover_twophase(self)

    def rollback_prepared(self, xid, recover=False):
        self.__engine.dialect.do_rollback_twophase(self, xid, recover=recover)

    def commit_prepared(self, xid, recover=False):
        self.__engine.dialect.do_commit_twophase(self, xid, recover=recover)

    def in_transaction(self):
        """Return True if a transaction is in progress."""

        return self.__transaction is not None

    def _begin_impl(self):
        if self.__connection.is_valid:
            if self.__engine._should_log_info:
                self.__engine.logger.info("BEGIN")
            try:
                self.__engine.dialect.do_begin(self.connection)
            except Exception, e:
                raise exceptions.DBAPIError.instance(None, None, e)

    def _rollback_impl(self):
        if self.__connection.is_valid:
            if self.__engine._should_log_info:
                self.__engine.logger.info("ROLLBACK")
            try:
                self.__engine.dialect.do_rollback(self.connection)
            except Exception, e:
                raise exceptions.DBAPIError.instance(None, None, e)
        self.__transaction = None

    def _commit_impl(self):
        if self.__connection.is_valid:
            if self.__engine._should_log_info:
                self.__engine.logger.info("COMMIT")
            try:
                self.__engine.dialect.do_commit(self.connection)
            except Exception, e:
                raise exceptions.DBAPIError.instance(None, None, e)
        self.__transaction = None

    def _savepoint_impl(self, name=None):
        if name is None:
            self.__savepoint_seq += 1
            name = '__sa_savepoint_%s' % self.__savepoint_seq
        if self.__connection.is_valid:
            self.__engine.dialect.do_savepoint(self, name)
            return name

    def _rollback_to_savepoint_impl(self, name, context):
        if self.__connection.is_valid:
            self.__engine.dialect.do_rollback_to_savepoint(self, name)
        self.__transaction = context

    def _release_savepoint_impl(self, name, context):
        if self.__connection.is_valid:
            self.__engine.dialect.do_release_savepoint(self, name)
        self.__transaction = context

    def _begin_twophase_impl(self, xid):
        if self.__connection.is_valid:
            self.__engine.dialect.do_begin_twophase(self, xid)

    def _prepare_twophase_impl(self, xid):
        if self.__connection.is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.__engine.dialect.do_prepare_twophase(self, xid)

    def _rollback_twophase_impl(self, xid, is_prepared):
        if self.__connection.is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.__engine.dialect.do_rollback_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _commit_twophase_impl(self, xid, is_prepared):
        if self.__connection.is_valid:
            assert isinstance(self.__transaction, TwoPhaseTransaction)
            self.__engine.dialect.do_commit_twophase(self, xid, is_prepared)
        self.__transaction = None

    def _autocommit(self, context):
        """Possibly issue a commit.

        When no Transaction is present, this is called after statement
        execution to provide "autocommit" behavior.  Dialects may
        inspect the statement to determine if a commit is actually
        required.
        """

        # TODO: have the dialect determine if autocommit can be set on
        # the connection directly without this extra step
        if not self.in_transaction() and context.should_autocommit():
            self._commit_impl()

    def _autorollback(self):
        if not self.in_transaction():
            self._rollback_impl()

    def close(self):
        """Close this Connection."""

        try:
            c = self.__connection
        except AttributeError:
            return
        if not self.__branch:
            self.__connection.close()
        self.__connection = None
        del self.__connection

    def scalar(self, object, *multiparams, **params):
        """Executes and returns the first column of the first row."""

        return self.execute(object, *multiparams, **params).scalar()

    def statement_compiler(self, statement, **kwargs):
        return self.dialect.statement_compiler(self.dialect, statement, bind=self, **kwargs)

    def execute(self, object, *multiparams, **params):
        """Executes and returns a ResultProxy."""

        for c in type(object).__mro__:
            if c in Connection.executors:
                return Connection.executors[c](self, object, multiparams, params)
        else:
            raise exceptions.InvalidRequestError("Unexecuteable object type: " + str(type(object)))

    def _execute_default(self, default, multiparams=None, params=None):
        return self.__engine.dialect.defaultrunner(self.__create_execution_context()).traverse_single(default)

    def _execute_text(self, statement, multiparams, params):
        parameters = self.__distill_params(multiparams, params)
        context = self.__create_execution_context(statement=statement, parameters=parameters)
        self.__execute_raw(context)
        return context.result()

    def __distill_params(self, multiparams, params):
        """given arguments from the calling form *multiparams, **params, return a list
        of bind parameter structures, usually a list of dictionaries.  
        
        in the case of 'raw' execution which accepts positional parameters, 
        it may be a list of tuples or lists."""
        
        if multiparams is None or len(multiparams) == 0:
            if params:
                return [params]
            else:
                return [{}]
        elif len(multiparams) == 1:
            if isinstance(multiparams[0], (list, tuple)):
                if isinstance(multiparams[0][0], (list, tuple, dict)):
                    return multiparams[0]
                else:
                    return [multiparams[0]]
            elif isinstance(multiparams[0], dict):
                return [multiparams[0]]
            else:
                return [[multiparams[0]]]
        else:
            if isinstance(multiparams[0], (list, tuple, dict)):
                return multiparams
            else:
                return [multiparams]

    def _execute_function(self, func, multiparams, params):
        return self._execute_clauseelement(func.select(), multiparams, params)

    def _execute_clauseelement(self, elem, multiparams=None, params=None):
        params = self.__distill_params(multiparams, params)
        if params:
            keys = params[0].keys()
        else:
            keys = None
        return self._execute_compiled(elem.compile(dialect=self.dialect, column_keys=keys, inline=len(params) > 1), distilled_params=params)

    def _execute_compiled(self, compiled, multiparams=None, params=None, distilled_params=None):
        """Execute a sql.Compiled object."""
        if not compiled.can_execute:
            raise exceptions.ArgumentError("Not an executeable clause: %s" % (str(compiled)))

        if distilled_params is None:
            distilled_params = self.__distill_params(multiparams, params)
        context = self.__create_execution_context(compiled=compiled, parameters=distilled_params)

        context.pre_execution()
        self.__execute_raw(context)
        context.post_execution()
        return context.result()

    def __create_execution_context(self, **kwargs):
        return self.__engine.dialect.create_execution_context(connection=self, **kwargs)

    def __execute_raw(self, context):
        if context.executemany:
            self._cursor_executemany(context.cursor, context.statement, context.parameters, context=context)
        else:
            self._cursor_execute(context.cursor, context.statement, context.parameters[0], context=context)
        self._autocommit(context)

    def _cursor_execute(self, cursor, statement, parameters, context=None):
        if self.__engine._should_log_info:
            self.__engine.logger.info(statement)
            self.__engine.logger.info(repr(parameters))
        try:
            self.dialect.do_execute(cursor, statement, parameters, context=context)
        except Exception, e:
            if self.dialect.is_disconnect(e):
                self.__connection.invalidate(e=e)
                self.engine.dispose()
            cursor.close()
            self._autorollback()
            if self.__close_with_result:
                self.close()
            raise exceptions.DBAPIError.instance(statement, parameters, e)

    def _cursor_executemany(self, cursor, statement, parameters, context=None):
        if self.__engine._should_log_info:
            self.__engine.logger.info(statement)
            self.__engine.logger.info(repr(parameters))
        try:
            self.dialect.do_executemany(cursor, statement, parameters, context=context)
        except Exception, e:
            if self.dialect.is_disconnect(e):
                self.__connection.invalidate(e=e)
                self.engine.dispose()
            cursor.close()
            self._autorollback()
            if self.__close_with_result:
                self.close()
            raise exceptions.DBAPIError.instance(statement, parameters, e)

    # poor man's multimethod/generic function thingy
    executors = {
        expression._Function : _execute_function,
        expression.ClauseElement : _execute_clauseelement,
        visitors.ClauseVisitor : _execute_compiled,
        schema.SchemaItem:_execute_default,
        str.__mro__[-2] : _execute_text
    }

    def create(self, entity, **kwargs):
        """Create a Table or Index given an appropriate Schema object."""

        return self.__engine.create(entity, connection=self, **kwargs)

    def drop(self, entity, **kwargs):
        """Drop a Table or Index given an appropriate Schema object."""

        return self.__engine.drop(entity, connection=self, **kwargs)

    def reflecttable(self, table, include_columns=None):
        """Reflect the columns in the given string table name from the database."""

        return self.__engine.reflecttable(table, self, include_columns)

    def default_schema_name(self):
        return self.__engine.dialect.get_default_schema_name(self)

    def run_callable(self, callable_):
        return callable_(self)

class Transaction(object):
    """Represent a Transaction in progress.

    The Transaction object is **not** threadsafe.
    """

    def __init__(self, connection, parent):
        self._connection = connection
        self._parent = parent or self
        self._is_active = True

    connection = property(lambda s:s._connection, doc="The Connection object referenced by this Transaction")
    is_active = property(lambda s:s._is_active)

    def close(self):
        """close this transaction.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosign transaction.
        """
        if not self._parent._is_active:
            return
        if self._parent is self:
            self.rollback()

    def rollback(self):
        if not self._parent._is_active:
            return
        self._is_active = False
        self._do_rollback()

    def _do_rollback(self):
        self._parent.rollback()

    def commit(self):
        if not self._parent._is_active:
            raise exceptions.InvalidRequestError("This transaction is inactive")
        self._is_active = False
        self._do_commit()

    def _do_commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None and self._is_active:
            self.commit()
        else:
            self.rollback()

class RootTransaction(Transaction):
    def __init__(self, connection):
        super(RootTransaction, self).__init__(connection, None)
        self._connection._begin_impl()
    
    def _do_rollback(self):
        self._connection._rollback_impl()

    def _do_commit(self):
        self._connection._commit_impl()

class NestedTransaction(Transaction):
    def __init__(self, connection, parent):
        super(NestedTransaction, self).__init__(connection, parent)
        self._savepoint = self._connection._savepoint_impl()
    
    def _do_rollback(self):
        self._connection._rollback_to_savepoint_impl(self._savepoint, self._parent)

    def _do_commit(self):
        self._connection._release_savepoint_impl(self._savepoint, self._parent)

class TwoPhaseTransaction(Transaction):
    def __init__(self, connection, xid):
        super(TwoPhaseTransaction, self).__init__(connection, None)
        self._is_prepared = False
        self.xid = xid
        self._connection._begin_twophase_impl(self.xid)
    
    def prepare(self):
        if not self._parent._is_active:
            raise exceptions.InvalidRequestError("This transaction is inactive")
        self._connection._prepare_twophase_impl(self.xid)
        self._is_prepared = True
    
    def _do_rollback(self):
        self._connection._rollback_twophase_impl(self.xid, self._is_prepared)
    
    def commit(self):
        self._connection._commit_twophase_impl(self.xid, self._is_prepared)

class Engine(Connectable):
    """
    Connects a Pool, a Dialect and a CompilerFactory together to
    provide a default implementation of SchemaEngine.
    """

    def __init__(self, pool, dialect, url, echo=None):
        self.pool = pool
        self.url = url
        self.dialect=dialect
        self.echo = echo
        self.engine = self
        self.logger = logging.instance_logger(self, echoflag=echo)

    name = property(lambda s:sys.modules[s.dialect.__module__].descriptor()['name'], doc="String name of the [sqlalchemy.engine#Dialect] in use by this ``Engine``.")
    echo = logging.echo_property()
    
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
            return connection._execute_default(default)
        finally:
            connection.close()

    def _func(self):
        return expression._FunctionGenerator(bind=self)

    func = property(_func)

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

        return Connection(self, **kwargs)

    def contextual_connect(self, close_with_result=False, **kwargs):
        """Return a Connection object which may be newly allocated, or may be part of some ongoing context.

        This Connection is meant to be used by the various "auto-connecting" operations.
        """

        return Connection(self, self.pool.connect(), close_with_result=close_with_result, **kwargs)
    
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

    def __ambiguous_processor(self, colname):
        def process(value):
            raise exceptions.InvalidRequestError("Ambiguous column name '%s' in result set! try 'use_labels' option on select statement." % colname)
        return process

    def __init__(self, context):
        """ResultProxy objects are constructed via the execute() method on SQLEngine."""
        self.context = context
        self.dialect = context.dialect
        self.closed = False
        self.cursor = context.cursor
        self.__echo = context.engine._should_log_info
        self._process_row = self._row_processor()
        if context.is_select():
            self._init_metadata()
            self._rowcount = None
        else:
            self._rowcount = context.get_rowcount()
            self.close()

    connection = property(lambda self:self.context.root_connection)

    def _get_rowcount(self):
        if self._rowcount is not None:
            return self._rowcount
        else:
            return self.context.get_rowcount()
    rowcount = property(_get_rowcount)
    lastrowid = property(lambda s:s.cursor.lastrowid)
    out_parameters = property(lambda s:s.context.out_parameters)

    def _init_metadata(self):
        if hasattr(self, '_ResultProxy__props'):
            return
        self.__props = {}
        self._key_cache = self._create_key_cache()
        self.__keys = []
        metadata = self.cursor.description

        if metadata is not None:
            typemap = self.dialect.dbapi_type_map()

            for i, item in enumerate(metadata):
                # sqlite possibly prepending table name to colnames so strip
                colname = (item[0].split('.')[-1]).decode(self.dialect.encoding)
                if self.context.typemap is not None:
                    type = self.context.typemap.get(colname.lower(), typemap.get(item[1], types.NULLTYPE))
                else:
                    type = typemap.get(item[1], types.NULLTYPE)

                rec = (type, type.dialect_impl(self.dialect).result_processor(self.dialect), i)

                if rec[0] is None:
                    raise exceptions.InvalidRequestError(
                        "None for metadata " + colname)
                if self.__props.setdefault(colname.lower(), rec) is not rec:
                    self.__props[colname.lower()] = (type, self.__ambiguous_processor(colname), 0)
                self.__keys.append(colname)
                self.__props[i] = rec

            if self.__echo:
                self.context.engine.logger.debug("Col " + repr(tuple([x[0] for x in metadata])))

    def _create_key_cache(self):
        # local copies to avoid circular ref against 'self'
        props = self.__props
        context = self.context
        def lookup_key(key):
            """Given a key, which could be a ColumnElement, string, etc.,
            matches it to the appropriate key we got from the result set's
            metadata; then cache it locally for quick re-access."""

            if isinstance(key, int) and key in props:
                rec = props[key]
            elif isinstance(key, basestring) and key.lower() in props:
                rec = props[key.lower()]
            elif isinstance(key, expression.ColumnElement):
                label = context.column_labels.get(key._label, key.name).lower()
                if label in props:
                    rec = props[label]
            if not "rec" in locals():
                raise exceptions.NoSuchColumnError("Could not locate column in row for column '%s'" % (str(key)))

            return rec
        return util.PopulateDict(lookup_key)

    def close(self):
        """Close this ResultProxy, and the underlying DB-API cursor corresponding to the execution.

        If this ResultProxy was generated from an implicit execution,
        the underlying Connection will also be closed (returns the
        underlying DB-API connection to the connection pool.)

        This method is also called automatically when all result rows
        are exhausted.
        """
        if not self.closed:
            self.closed = True
            self.cursor.close()
            if self.connection.should_close_with_result:
                self.connection.close()

    keys = property(lambda s:s.__keys)

    def _has_key(self, row, key):
        try:
            self._key_cache[key]
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
        return self.context.postfetch_cols()
        
    def supports_sane_rowcount(self):
        """Return ``supports_sane_rowcount`` from the dialect.

        """
        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect.
        """

        return self.dialect.supports_sane_multi_rowcount

    def _get_col(self, row, key):
        rec = self._key_cache[key]
        if rec[1]:
            return rec[1](row[rec[2]])
        else:
            return row[rec[2]]

    def _fetchone_impl(self):
        return self.cursor.fetchone()
    def _fetchmany_impl(self, size=None):
        return self.cursor.fetchmany(size)
    def _fetchall_impl(self):
        return self.cursor.fetchall()

    def _row_processor(self):
        return RowProxy

    def fetchall(self):
        """Fetch all rows, just like DB-API ``cursor.fetchall()``."""

        l = [self._process_row(self, row) for row in self._fetchall_impl()]
        self.close()
        return l

    def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API ``cursor.fetchmany(size=cursor.arraysize)``."""

        l = [self._process_row(self, row) for row in self._fetchmany_impl(size)]
        if len(l) == 0:
            self.close()
        return l

    def fetchone(self):
        """Fetch one row, just like DB-API ``cursor.fetchone()``."""
        row = self._fetchone_impl()
        if row is not None:
            return self._process_row(self, row)
        else:
            self.close()
            return None

    def scalar(self):
        """Fetch the first column of the first row, and close the result set."""
        row = self._fetchone_impl()
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
        #self.context.engine.logger.debug("Buffered %d rows" % size)
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

    def _get_col(self, row, key):
        rec = self._key_cache[key]
        return row[rec[2]]

    def _row_processor(self):
        return BufferedColumnRow

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

class RowProxy(object):
    """Proxy a single cursor row for a parent ResultProxy.

    Mostly follows "ordered dictionary" behavior, mapping result
    values to the string-based column name, the integer position of
    the result in the row, as well as Column instances which can be
    mapped to the original Columns that produced this result set (for
    results that correspond to constructed SQL expressions).
    """

    def __init__(self, parent, row):
        """RowProxy objects are constructed by ResultProxy objects."""

        self.__parent = parent
        self.__row = row
        if self.__parent._ResultProxy__echo:
            self.__parent.context.engine.logger.debug("Row " + repr(row))

    def close(self):
        """Close the parent ResultProxy."""

        self.__parent.close()

    def __contains__(self, key):
        return self.__parent._has_key(self.__row, key)

    def __len__(self):
        return len(self.__row)
        
    def __iter__(self):
        for i in range(0, len(self.__row)):
            yield self.__parent._get_col(self.__row, i)

    def __eq__(self, other):
        return (other is self) or (other == tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))

    def __repr__(self):
        return repr(tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))

    def has_key(self, key):
        """Return True if this RowProxy contains the given key."""

        return self.__parent._has_key(self.__row, key)

    def __getitem__(self, key):
        if isinstance(key, slice):
            indices = key.indices(len(self))
            return tuple([self.__parent._get_col(self.__row, i) for i in range(*indices)])
        else:
            return self.__parent._get_col(self.__row, key)

    def __getattr__(self, name):
        try:
            return self.__parent._get_col(self.__row, name)
        except KeyError, e:
            raise AttributeError(e.args[0])

    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair."""

        return [(key, getattr(self, key)) for key in self.keys()]

    def keys(self):
        """Return the list of keys as strings represented by this RowProxy."""

        return self.__parent.keys

    def values(self):
        """Return the values represented by this RowProxy as a list."""

        return list(self)

    def __len__(self):
        return len(self.__row)

class BufferedColumnRow(RowProxy):
    def __init__(self, parent, row):
        row = [ResultProxy._get_col(parent, row, i) for i in xrange(len(row))]
        super(BufferedColumnRow, self).__init__(parent, row)

class SchemaIterator(schema.SchemaVisitor):
    """A visitor that can gather text into a buffer and execute the contents of the buffer."""

    def __init__(self, connection):
        """Construct a new SchemaIterator.
        """
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
        """Do nothing.

        Passive defaults by definition return None on the app side,
        and are post-fetched to get the DB-side value.
        """

        return None

    def visit_sequence(self, seq):
        """Do nothing.

        """

        return None

    def exec_default_sql(self, default):
        conn = self.context.connection
        c = expression.select([default.arg]).compile(bind=conn)
        return conn._execute_compiled(c).scalar()
    
    def execute_string(self, stmt, params=None):
        """execute a string statement, using the raw cursor,
        and return a scalar result."""
        conn = self.context._connection
        conn._cursor_execute(self.context.cursor, stmt, params)
        return self.context.cursor.fetchone()[0]
        
    def visit_column_onupdate(self, onupdate):
        if isinstance(onupdate.arg, expression.ClauseElement):
            return self.exec_default_sql(onupdate)
        elif callable(onupdate.arg):
            return onupdate.arg(self.context)
        else:
            return onupdate.arg

    def visit_column_default(self, default):
        if isinstance(default.arg, expression.ClauseElement):
            return self.exec_default_sql(default)
        elif callable(default.arg):
            return default.arg(self.context)
        else:
            return default.arg
