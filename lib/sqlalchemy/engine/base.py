# engine/base.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines the basic components used to interface DBAPI modules with
higher-level statement-construction, connection-management, 
execution and result contexts."""

from sqlalchemy import exceptions, sql, schema, util, types, logging
import StringIO, sys, re


class ConnectionProvider(object):
    """Define an interface that returns raw Connection objects (or compatible)."""

    def get_connection(self):
        """Return a Connection or compatible object from a DBAPI which also contains a close() method.

        It is not defined what context this connection belongs to.  It
        may be newly connected, returned from a pool, part of some
        other kind of context such as thread-local, or can be a fixed
        member of this object.
        """

        raise NotImplementedError()

    def dispose(self):
        """Release all resources corresponding to this ConnectionProvider.

        This includes any underlying connection pools.
        """

        raise NotImplementedError()


class Dialect(sql.AbstractDialect):
    """Define the behavior of a specific database/DBAPI.

    Any aspect of metadata definition, SQL query generation, execution,
    result-set handling, or anything else which varies between
    databases is defined under the general category of the Dialect.
    The Dialect acts as a factory for other database-specific object
    implementations including ExecutionContext, Compiled,
    DefaultGenerator, and TypeEngine.

    All Dialects implement the following attributes:

    positional
      True if the paramstyle for this Dialect is positional

    paramstyle
      The paramstyle to be used (some DBAPIs support multiple paramstyles)

    supports_autoclose_results
      Usually True; if False, indicates that rows returned by
      fetchone() might not be just plain tuples, and may be
      "live" proxy objects which still require the cursor to be open
      in order to be read (such as pyPgSQL which has active
      filehandles for BLOBs).  In that case, an auto-closing
      ResultProxy cannot automatically close itself after results are
      consumed.

    convert_unicode
      True if unicode conversion should be applied to all str types

    encoding
      type of encoding to use for unicode, usually defaults to 'utf-8'
    """

    def create_connect_args(self, opts):
        """Build DBAPI compatible connection arguments.

        Given a dictionary of key-valued connect parameters, returns a
        tuple consisting of a `*args`/`**kwargs` suitable to send directly
        to the dbapi's connect function.  The connect args will have
        any number of the following keynames: host, hostname,
        database, dbname, user, username, password, pw, passwd,
        filename.
        """

        raise NotImplementedError()

    def convert_compiled_params(self, parameters):
        """Build DBAPI execute arguments from a ClauseParameters.

        Given a sql.ClauseParameters object, returns an array or
        dictionary suitable to pass directly to this Dialect's DBAPI's
        execute method.
        """

        raise NotImplementedError()

    def type_descriptor(self, typeobj):
        """Transform the type from generic to database-specific.

        Provides a database-specific TypeEngine object, given the
        generic object which comes from the types module.  Subclasses
        will usually use the adapt_type() method in the types module
        to make this job easy.
        """

        raise NotImplementedError()

    def oid_column_name(self, column):
        """Return the oid column name for this dialect, or None if the dialect can't/won't support OID/ROWID.

        The Column instance which represents OID for the query being
        compiled is passed, so that the dialect can inspect the column
        and its parent selectable to determine if OID/ROWID is not
        selected for a particular selectable (i.e. oracle doesnt
        support ROWID for UNION, GROUP BY, DISTINCT, etc.)
        """

        raise NotImplementedError()

    def supports_alter(self):
        """return True if the database supports ALTER TABLE."""
        raise NotImplementedError()

    def max_identifier_length(self):
        """Return the maximum length of identifier names.
        
        Return None if no limit."""
        return None

    def supports_unicode_statements(self):
        """indicate whether the DBAPI can receive SQL statements as Python unicode strings"""
        raise NotImplementedError()
        
    def supports_sane_rowcount(self):
        """Indicate whether the dialect properly implements statements rowcount.

        This was needed for MySQL which had non-standard behavior of rowcount,
        but this issue has since been resolved.
        """

        raise NotImplementedError()

    def schemagenerator(self, connection, **kwargs):
        """Return a ``schema.SchemaVisitor`` instance that can generate schemas.

            connection
                a Connection to use for statement execution
                
        `schemagenerator()` is called via the `create()` method on Table,
        Index, and others.
        """

        raise NotImplementedError()

    def schemadropper(self, connection, **kwargs):
        """Return a ``schema.SchemaVisitor`` instance that can drop schemas.

            connection
                a Connection to use for statement execution

        `schemadropper()` is called via the `drop()` method on Table,
        Index, and others.
        """

        raise NotImplementedError()

    def defaultrunner(self, connection, **kwargs):
        """Return a ``schema.SchemaVisitor`` instance that can execute defaults.
        
            connection
                a Connection to use for statement execution
        
        """

        raise NotImplementedError()

    def compiler(self, statement, parameters):
        """Return a ``sql.ClauseVisitor`` able to transform a ``ClauseElement`` into a string.

        The returned object is usually a subclass of
        ansisql.ANSICompiler, and will produce a string representation
        of the given ClauseElement and `parameters` dictionary.

        """

        raise NotImplementedError()

    def reflecttable(self, connection, table):
        """Load table description from the database.

        Given a ``Connection`` and a ``Table`` object, reflect its
        columns and properties from the database.
        """

        raise NotImplementedError()

    def has_table(self, connection, table_name, schema=None):
        """Check the existence of a particular table in the database.

        Given a ``Connection`` object and a `table_name`, return True
        if the given table (possibly within the specified `schema`)
        exists in the database, False otherwise.
        """

        raise NotImplementedError()

    def has_sequence(self, connection, sequence_name):
        """Check the existence of a particular sequence in the database.

        Given a ``Connection`` object and a `sequence_name`, return
        True if the given sequence exists in the database, False
        otherwise.
        """

        raise NotImplementedError()

    def get_default_schema_name(self, connection):
        """Return the currently selected schema given a connection"""

        raise NotImplementedError()

    def create_execution_context(self, connection, compiled=None, compiled_parameters=None, statement=None, parameters=None):
        """Return a new ExecutionContext object."""
        raise NotImplementedError()

    def do_begin(self, connection):
        """Provide an implementation of connection.begin()."""

        raise NotImplementedError()

    def do_rollback(self, connection):
        """Provide an implementation of connection.rollback()."""

        raise NotImplementedError()

    def do_commit(self, connection):
        """Provide an implementation of connection.commit()"""

        raise NotImplementedError()

    def do_executemany(self, cursor, statement, parameters):
        """Execute a single SQL statement looping over a sequence of parameters."""

        raise NotImplementedError()

    def do_execute(self, cursor, statement, parameters):
        """Execute a single SQL statement with given parameters."""

        raise NotImplementedError()


    def compile(self, clauseelement, parameters=None):
        """Compile the given ClauseElement using this Dialect.

        A convenience method which simply flips around the compile()
        call on ClauseElement.
        """

        return clauseelement.compile(dialect=self, parameters=parameters)

    def is_disconnect(self, e):
        """Return True if the given DBAPI error indicates an invalid connection"""
        raise NotImplementedError()


class ExecutionContext(object):
    """A messenger object for a Dialect that corresponds to a single execution.

    ExecutionContext should have these datamembers:
    
        connection
            Connection object which initiated the call to the
            dialect to create this ExecutionContext.

        dialect
            dialect which created this ExecutionContext.
            
        cursor
            DBAPI cursor procured from the connection
            
        compiled
            if passed to constructor, sql.Compiled object being executed
        
        compiled_parameters
            if passed to constructor, sql.ClauseParameters object
             
        statement
            string version of the statement to be executed.  Is either
            passed to the constructor, or must be created from the 
            sql.Compiled object by the time pre_exec() has completed.
            
        parameters
            "raw" parameters suitable for direct execution by the
            dialect.  Either passed to the constructor, or must be
            created from the sql.ClauseParameters object by the time 
            pre_exec() has completed.
            
    
    The Dialect should provide an ExecutionContext via the
    create_execution_context() method.  The `pre_exec` and `post_exec`
    methods will be called for compiled statements.
    
    """

    def create_cursor(self):
        """Return a new cursor generated this ExecutionContext's connection."""

        raise NotImplementedError()

    def pre_exec(self):
        """Called before an execution of a compiled statement.
        
        If compiled and compiled_parameters were passed to this
        ExecutionContext, the `statement` and `parameters` datamembers
        must be initialized after this statement is complete.
        """

        raise NotImplementedError()

    def post_exec(self):
        """Called after the execution of a compiled statement.
        
        If compiled was passed to this ExecutionContext,
        the `last_insert_ids`, `last_inserted_params`, etc. 
        datamembers should be available after this method
        completes.
        """

        raise NotImplementedError()
    
    def get_result_proxy(self):
        """return a ResultProxy corresponding to this ExecutionContext."""
        raise NotImplementedError()
        
    def get_rowcount(self):
        """Return the count of rows updated/deleted for an UPDATE/DELETE statement."""

        raise NotImplementedError()

    def last_inserted_ids(self):
        """Return the list of the primary key values for the last insert statement executed.

        This does not apply to straight textual clauses; only to
        ``sql.Insert`` objects compiled against a ``schema.Table`` object,
        which are executed via `execute()`.  The order of
        items in the list is the same as that of the Table's
        'primary_key' attribute.

        In some cases, this method may invoke a query back to the
        database to retrieve the data, based on the "lastrowid" value
        in the cursor.
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
        """Return True if the last row INSERTED via a compiled insert statement contained PassiveDefaults.

        The presence of PassiveDefaults indicates that the database
        inserted data beyond that which we passed to the query
        programmatically.
        """

        raise NotImplementedError()


class Connectable(sql.Executor):
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

    engine = util.NotImplProperty("The Engine which this Connectable is associated with.")
    dialect = util.NotImplProperty("Dialect which this Connectable is associated with.")

class Connection(Connectable):
    """Represent a single DBAPI connection returned from the underlying connection pool.

    Provides execution support for string-based SQL statements as well
    as ClauseElement, Compiled and DefaultGenerator objects.  Provides
    a begin method to return Transaction objects.

    The Connection object is **not** threadsafe.
    """

    def __init__(self, engine, connection=None, close_with_result=False):
        self.__engine = engine
        self.__connection = connection or engine.raw_connection()
        self.__transaction = None
        self.__close_with_result = close_with_result

    def _get_connection(self):
        try:
            return self.__connection
        except AttributeError:
            raise exceptions.InvalidRequestError("This Connection is closed")

    engine = property(lambda s:s.__engine, doc="The Engine with which this Connection is associated.")
    dialect = property(lambda s:s.__engine.dialect, doc="Dialect used by this Connection.")
    connection = property(_get_connection, doc="The underlying DBAPI connection managed by this Connection.")
    should_close_with_result = property(lambda s:s.__close_with_result, doc="Indicates if this Connection should be closed when a corresponding ResultProxy is closed; this is essentially an auto-release mode.")

    def _create_transaction(self, parent):
        return Transaction(self, parent)

    def connect(self):
        """connect() is implemented to return self so that an incoming Engine or Connection object can be treated similarly."""
        return self

    def contextual_connect(self, **kwargs):
        """contextual_connect() is implemented to return self so that an incoming Engine or Connection object can be treated similarly."""
        return self

    def begin(self):
        if self.__transaction is None:
            self.__transaction = self._create_transaction(None)
            return self.__transaction
        else:
            return self._create_transaction(self.__transaction)

    def in_transaction(self):
        return self.__transaction is not None

    def _begin_impl(self):
        if self.__connection.is_valid:
            self.__engine.logger.info("BEGIN")
            try:
                self.__engine.dialect.do_begin(self.connection)
            except Exception, e:
                raise exceptions.SQLError(None, None, e)

    def _rollback_impl(self):
        if self.__connection.is_valid:
            self.__engine.logger.info("ROLLBACK")
            try:
                self.__engine.dialect.do_rollback(self.connection)
            except Exception, e:
                raise exceptions.SQLError(None, None, e)
            self.__connection.close_open_cursors()
        self.__transaction = None

    def _commit_impl(self):
        if self.__connection.is_valid:
            self.__engine.logger.info("COMMIT")
            try:
                self.__engine.dialect.do_commit(self.connection)
            except Exception, e:
                raise exceptions.SQLError(None, None, e)
        self.__transaction = None

    def _autocommit(self, statement):
        """When no Transaction is present, this is called after executions to provide "autocommit" behavior."""
        # TODO: have the dialect determine if autocommit can be set on the connection directly without this
        # extra step
        if not self.in_transaction() and re.match(r'UPDATE|INSERT|CREATE|DELETE|DROP|ALTER', statement.lstrip(), re.I):
            self._commit_impl()

    def _autorollback(self):
        if not self.in_transaction():
            self._rollback_impl()

    def close(self):
        try:
            c = self.__connection
        except AttributeError:
            return
        self.__connection.close()
        self.__connection = None
        del self.__connection

    def scalar(self, object, *multiparams, **params):
        return self.execute(object, *multiparams, **params).scalar()

    def compiler(self, statement, parameters, **kwargs):
        return self.dialect.compiler(statement, parameters, engine=self.engine, **kwargs)

    def execute(self, object, *multiparams, **params):
        for c in type(object).__mro__:
            if c in Connection.executors:
                return Connection.executors[c](self, object, *multiparams, **params)
        else:
            raise exceptions.InvalidRequestError("Unexecuteable object type: " + str(type(object)))

    def execute_default(self, default, **kwargs):
        return default.accept_visitor(self.__engine.dialect.defaultrunner(self))

    def execute_text(self, statement, *multiparams, **params):
        if len(multiparams) == 0:
            parameters = params or None
        elif len(multiparams) == 1 and (isinstance(multiparams[0], list) or isinstance(multiparams[0], tuple) or isinstance(multiparams[0], dict)):
            parameters = multiparams[0]
        else:
            parameters = list(multiparams)
        context = self._create_execution_context(statement=statement, parameters=parameters)
        self._execute_raw(context)
        return context.get_result_proxy()

    def _params_to_listofdicts(self, *multiparams, **params):
        if len(multiparams) == 0:
            return [params]
        elif len(multiparams) == 1:
            if multiparams[0] == None:
                return [{}]
            elif isinstance (multiparams[0], list) or isinstance (multiparams[0], tuple):
                return multiparams[0]
            else:
                return [multiparams[0]]
        else:
            return multiparams
    
    def execute_function(self, func, *multiparams, **params):
        return self.execute_clauseelement(func.select(), *multiparams, **params)
        
    def execute_clauseelement(self, elem, *multiparams, **params):
        executemany = len(multiparams) > 0
        if executemany:
            param = multiparams[0]
        else:
            param = params
        return self.execute_compiled(elem.compile(dialect=self.dialect, parameters=param), *multiparams, **params)

    def execute_compiled(self, compiled, *multiparams, **params):
        """Execute a sql.Compiled object."""
        if not compiled.can_execute:
            raise exceptions.ArgumentError("Not an executeable clause: %s" % (str(compiled)))
        parameters = [compiled.construct_params(m) for m in self._params_to_listofdicts(*multiparams, **params)]
        if len(parameters) == 1:
            parameters = parameters[0]
        context = self._create_execution_context(compiled=compiled, compiled_parameters=parameters)
        context.pre_exec()
        self._execute_raw(context)
        context.post_exec()
        return context.get_result_proxy()
    
    def _create_execution_context(self, **kwargs):
        return self.__engine.dialect.create_execution_context(connection=self, **kwargs)
        
    def _execute_raw(self, context):
        self.__engine.logger.info(context.statement)
        self.__engine.logger.info(repr(context.parameters))
        if context.parameters is not None and isinstance(context.parameters, list) and len(context.parameters) > 0 and (isinstance(context.parameters[0], list) or isinstance(context.parameters[0], tuple) or isinstance(context.parameters[0], dict)):
            self._executemany(context)
        else:
            self._execute(context)
        self._autocommit(context.statement)

    def _execute(self, context):
        if context.parameters is None:
            if context.dialect.positional:
                context.parameters = ()
            else:
                context.parameters = {}
        try:
            context.dialect.do_execute(context.cursor, context.statement, context.parameters, context=context)
        except Exception, e:
            if self.dialect.is_disconnect(e):
                self.__connection.invalidate(e=e)
                self.engine.connection_provider.dispose()
            self._autorollback()
            if self.__close_with_result:
                self.close()
            raise exceptions.SQLError(context.statement, context.parameters, e)

    def _executemany(self, context):
        try:
            context.dialect.do_executemany(context.cursor, context.statement, context.parameters, context=context)
        except Exception, e:
            if self.dialect.is_disconnect(e):
                self.__connection.invalidate(e=e)
                self.engine.connection_provider.dispose()
            self._autorollback()
            if self.__close_with_result:
                self.close()
            raise exceptions.SQLError(context.statement, context.parameters, e)

    # poor man's multimethod/generic function thingy
    executors = {
        sql._Function : execute_function,
        sql.ClauseElement : execute_clauseelement,
        sql.ClauseVisitor : execute_compiled,
        schema.SchemaItem:execute_default,
        str.__mro__[-2] : execute_text
    }

    def create(self, entity, **kwargs):
        """Create a Table or Index given an appropriate Schema object."""

        return self.__engine.create(entity, connection=self, **kwargs)

    def drop(self, entity, **kwargs):
        """Drop a Table or Index given an appropriate Schema object."""

        return self.__engine.drop(entity, connection=self, **kwargs)

    def reflecttable(self, table, **kwargs):
        """Reflect the columns in the given string table name from the database."""

        return self.__engine.reflecttable(table, connection=self, **kwargs)

    def default_schema_name(self):
        return self.__engine.dialect.get_default_schema_name(self)

    def run_callable(self, callable_):
        return callable_(self)

class Transaction(object):
    """Represent a Transaction in progress.

    The Transaction object is **not** threadsafe.
    """

    def __init__(self, connection, parent):
        self.__connection = connection
        self.__parent = parent or self
        self.__is_active = True
        if self.__parent is self:
            self.__connection._begin_impl()

    connection = property(lambda s:s.__connection, doc="The Connection object referenced by this Transaction")
    is_active = property(lambda s:s.__is_active)

    def rollback(self):
        if not self.__parent.__is_active:
            return
        if self.__parent is self:
            self.__connection._rollback_impl()
            self.__is_active = False
        else:
            self.__parent.rollback()

    def commit(self):
        if not self.__parent.__is_active:
            raise exceptions.InvalidRequestError("This transaction is inactive")
        if self.__parent is self:
            self.__connection._commit_impl()
            self.__is_active = False

class Engine(Connectable):
    """
    Connects a ConnectionProvider, a Dialect and a CompilerFactory together to
    provide a default implementation of SchemaEngine.
    """

    def __init__(self, connection_provider, dialect, echo=None):
        self.connection_provider = connection_provider
        self._dialect=dialect
        self.echo = echo
        self.logger = logging.instance_logger(self)

    name = property(lambda s:sys.modules[s.dialect.__module__].descriptor()['name'], doc="String name of the [sqlalchemy.engine#Dialect] in use by this ``Engine``.")
    engine = property(lambda s:s)
    dialect = property(lambda s:s._dialect, doc="the [sqlalchemy.engine#Dialect] in use by this engine.")
    echo = logging.echo_property()
    url = property(lambda s:s.connection_provider.url, doc="The [sqlalchemy.engine.url#URL] object representing this ``Engine`` object's datasource.")
    
    def dispose(self):
        self.connection_provider.dispose()

    def create(self, entity, connection=None, **kwargs):
        """Create a table or index within this engine's database connection given a schema.Table object."""

        self._run_visitor(self.dialect.schemagenerator, entity, connection=connection, **kwargs)

    def drop(self, entity, connection=None, **kwargs):
        """Drop a table or index within this engine's database connection given a schema.Table object."""

        self._run_visitor(self.dialect.schemadropper, entity, connection=connection, **kwargs)

    def execute_default(self, default, **kwargs):
        connection = self.contextual_connect()
        try:
            return connection.execute_default(default, **kwargs)
        finally:
            connection.close()

    def _func(self):
        return sql._FunctionGenerator(self)

    func = property(_func)

    def text(self, text, *args, **kwargs):
        """Return a sql.text() object for performing literal queries."""

        return sql.text(text, engine=self, *args, **kwargs)

    def _run_visitor(self, visitorcallable, element, connection=None, **kwargs):
        if connection is None:
            conn = self.contextual_connect(close_with_result=False)
        else:
            conn = connection
        try:
            element.accept_visitor(visitorcallable(conn, **kwargs))
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

    def execute_compiled(self, compiled, *multiparams, **params):
        connection = self.contextual_connect(close_with_result=True)
        return connection.execute_compiled(compiled, *multiparams, **params)

    def compiler(self, statement, parameters, **kwargs):
        return self.dialect.compiler(statement, parameters, engine=self, **kwargs)

    def connect(self, **kwargs):
        """Return a newly allocated Connection object."""

        return Connection(self, **kwargs)

    def contextual_connect(self, close_with_result=False, **kwargs):
        """Return a Connection object which may be newly allocated, or may be part of some ongoing context.

        This Connection is meant to be used by the various "auto-connecting" operations.
        """

        return Connection(self, close_with_result=close_with_result, **kwargs)

    def reflecttable(self, table, connection=None):
        """Given a Table object, reflects its columns and properties from the database."""

        if connection is None:
            conn = self.contextual_connect()
        else:
            conn = connection
        try:
            self.dialect.reflecttable(conn, table)
        finally:
            if connection is None:
                conn.close()

    def has_table(self, table_name, schema=None):
        return self.run_callable(lambda c: self.dialect.has_table(c, table_name, schema=schema))

    def raw_connection(self):
        """Return a DBAPI connection."""

        return self.connection_provider.get_connection()

    def log(self, msg):
        """Log a message using this SQLEngine's logger stream."""

        self.logger.info(msg)

class ResultProxy(object):
    """Wraps a DBAPI cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by ``schema.Column``
    object. e.g.::

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ResultProxy also contains a map of TypeEngine objects and will
    invoke the appropriate ``convert_result_value()`` method before
    returning columns, as well as the ExecutionContext corresponding
    to the statement execution.  It provides several methods for which
    to obtain information from the underlying ExecutionContext.
    """

    class AmbiguousColumn(object):
        def __init__(self, key):
            self.key = key
        def dialect_impl(self, dialect):
            return self
        def convert_result_value(self, arg, engine):
            raise exceptions.InvalidRequestError("Ambiguous column name '%s' in result set! try 'use_labels' option on select statement." % (self.key))

    def __init__(self, context):
        """ResultProxy objects are constructed via the execute() method on SQLEngine."""
        self.context = context
        self.closed = False
        self.cursor = context.cursor
        self.__echo = logging.is_debug_enabled(context.engine.logger)
        self._init_metadata()
        
    dialect = property(lambda s:s.context.dialect)
    rowcount = property(lambda s:s.context.get_rowcount())
    connection = property(lambda s:s.context.connection)
    
    def _init_metadata(self):
        if hasattr(self, '_ResultProxy__props'):
            return
        self.__key_cache = {}
        self.__props = {}
        self.__keys = []
        metadata = self.cursor.description
        if metadata is not None:
            for i, item in enumerate(metadata):
                # sqlite possibly prepending table name to colnames so strip
                colname = item[0].split('.')[-1]
                if self.context.typemap is not None:
                    rec = (self.context.typemap.get(colname.lower(), types.NULLTYPE), i)
                else:
                    rec = (types.NULLTYPE, i)
                if rec[0] is None:
                    raise DBAPIError("None for metadata " + colname)
                if self.__props.setdefault(colname.lower(), rec) is not rec:
                    self.__props[colname.lower()] = (ResultProxy.AmbiguousColumn(colname), 0)
                self.__keys.append(colname)
                self.__props[i] = rec

    def close(self):
        """Close this ResultProxy, and the underlying DBAPI cursor corresponding to the execution.

        If this ResultProxy was generated from an implicit execution,
        the underlying Connection will also be closed (returns the
        underlying DBAPI connection to the connection pool.)

        This method is also called automatically when all result rows
        are exhausted.
        """
        if not self.closed:
            self.closed = True
            self.cursor.close()
            if self.connection.should_close_with_result and self.dialect.supports_autoclose_results:
                self.connection.close()
            
    def _convert_key(self, key):
        """Convert and cache a key.

        Given a key, which could be a ColumnElement, string, etc.,
        matches it to the appropriate key we got from the result set's
        metadata; then cache it locally for quick re-access.
        """

        if key in self.__key_cache:
            return self.__key_cache[key]
        else:
            if isinstance(key, int) and key in self.__props:
                rec = self.__props[key]
            elif isinstance(key, basestring) and key.lower() in self.__props:
                rec = self.__props[key.lower()]
            elif isinstance(key, sql.ColumnElement):
                label = self.context.column_labels.get(key._label, key.name).lower()
                if label in self.__props:
                    rec = self.__props[label]
                        
            if not "rec" in locals():
                raise exceptions.NoSuchColumnError("Could not locate column in row for column '%s'" % (repr(key)))

            self.__key_cache[key] = rec
            return rec
    
    keys = property(lambda s:s.__keys)
    
    def _has_key(self, row, key):
        try:
            self._convert_key(key)
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

    def supports_sane_rowcount(self):
        """Return ``supports_sane_rowcount()`` from the underlying ExecutionContext.

        See ExecutionContext for details.
        """

        return self.context.supports_sane_rowcount()

    def _get_col(self, row, key):
        rec = self._convert_key(key)
        return rec[0].dialect_impl(self.dialect).convert_result_value(row[rec[1]], self.dialect)
    
    def _fetchone_impl(self):
        return self.cursor.fetchone()
    def _fetchmany_impl(self, size=None):
        return self.cursor.fetchmany(size)
    def _fetchall_impl(self):
        return self.cursor.fetchall()
        
    def _process_row(self, row):
        return RowProxy(self, row)
            
    def fetchall(self):
        """Fetch all rows, just like DBAPI ``cursor.fetchall()``."""

        l = [self._process_row(row) for row in self._fetchall_impl()]
        self.close()
        return l

    def fetchmany(self, size=None):
        """Fetch many rows, just like DBAPI ``cursor.fetchmany(size=cursor.arraysize)``."""

        l = [self._process_row(row) for row in self._fetchmany_impl(size)]
        if len(l) == 0:
            self.close()
        return l

    def fetchone(self):
        """Fetch one row, just like DBAPI ``cursor.fetchone()``."""
        row = self._fetchone_impl()
        if row is not None:
            return self._process_row(row)
        else:
            self.close()
            return None

    def scalar(self):
        """Fetch the first column of the first row, and close the result set."""
        row = self._fetchone_impl()
        try:
            if row is not None:
                return self._process_row(row)[0]
            else:
                return None
        finally:
            self.close()

class BufferedRowResultProxy(ResultProxy):
    """``ResultProxy`` that buffers the contents of a selection of rows before 
    ``fetchone()`` is called.  This is to allow the results of 
    ``cursor.description`` to be available immediately, when interfacing
    with a DBAPI that requires rows to be consumed before this information is
    available (currently psycopg2, when used with server-side cursors).
    
    The pre-fetching behavior fetches only one row initially, and then grows
    its buffer size by a fixed amount with each successive need for additional 
    rows up to a size of 100.
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
    """``ResultProxy`` that loads all columns into memory each time fetchone() is
    called.  If fetchmany() or fetchall() are called, the full grid of results
    is fetched.  This is to operate with databases where result rows contain "live"
    results that fall out of scope unless explicitly fetched.  Currently this includes
    just cx_Oracle LOB objects, but this behavior is known to exist in other DBAPIs as 
    well (Pygresql, currently unsupported).

    """
    def _get_col(self, row, key):
        rec = self._convert_key(key)
        return row[rec[1]]
    
    def _process_row(self, row):
        sup = super(BufferedColumnResultProxy, self)
        row = [sup._get_col(row, i) for i in xrange(len(row))]
        return RowProxy(self, row)

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

    def __init__(self, connection):
        self.connection = connection
        self.dialect = connection.dialect
        
    def get_column_default(self, column):
        if column.default is not None:
            return column.default.accept_visitor(self)
        else:
            return None

    def get_column_onupdate(self, column):
        if column.onupdate is not None:
            return column.onupdate.accept_visitor(self)
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

        Sequences are not supported by default.
        """

        return None

    def exec_default_sql(self, default):
        c = sql.select([default.arg]).compile(engine=self.connection)
        return self.connection.execute_compiled(c).scalar()

    def visit_column_onupdate(self, onupdate):
        if isinstance(onupdate.arg, sql.ClauseElement):
            return self.exec_default_sql(onupdate)
        elif callable(onupdate.arg):
            return onupdate.arg()
        else:
            return onupdate.arg

    def visit_column_default(self, default):
        if isinstance(default.arg, sql.ClauseElement):
            return self.exec_default_sql(default)
        elif callable(default.arg):
            return default.arg()
        else:
            return default.arg
