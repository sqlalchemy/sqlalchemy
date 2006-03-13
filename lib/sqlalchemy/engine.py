# engine.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the SQLEngine class, which serves as the primary "database" object
used throughout the sql construction and object-relational mapper packages.
A SQLEngine is a facade around a single connection pool corresponding to a 
particular set of connection parameters, and provides thread-local transactional 
methods and statement execution methods for Connection objects.  It also provides 
a facade around a Cursor object to allow richer column selection for result rows 
as well as type conversion operations, known as a ResultProxy.

A SQLEngine is provided to an application as a subclass that is specific to a particular type 
of DBAPI, and is the central switching point for abstracting different kinds of database
behavior into a consistent set of behaviors.  It provides a variety of factory methods 
to produce everything specific to a certain kind of database, including a Compiler, 
schema creation/dropping objects.

The term "database-specific" will be used to describe any object or function that has behavior
corresponding to a particular vendor, such as mysql-specific, sqlite-specific, etc.
"""

import sqlalchemy.pool
import schema
import exceptions
import util
import sql
import sqlalchemy.databases
import sqlalchemy.types as types
import StringIO, sys, re

__all__ = ['create_engine', 'engine_descriptors']

def create_engine(name, opts=None,**kwargs):
    """creates a new SQLEngine instance.  There are two forms of calling this method.
    
    In the first, the "name" argument is the type of engine to load, i.e. 'sqlite', 'postgres', 
    'oracle', 'mysql'.  "opts" is a dictionary of options to be sent to the underlying DBAPI module 
    to create a connection, usually including a hostname, username, password, etc.
    
    In the second, the "name" argument is a URL in the form <enginename>://opt1=val1&opt2=val2.  
    Where <enginename> is the name as above, and the contents of the option dictionary are 
    spelled out as a URL encoded string.  The "opts" argument is not used.
    
    In both cases, **kwargs represents options to be sent to the SQLEngine itself.  A possibly
    partial listing of those options is as follows:
    
    pool=None : an instance of sqlalchemy.pool.DBProxy to be used as the underlying source 
    for connections (DBProxy is described in the previous section).  If None, a default DBProxy 
    will be created using the engine's own database module with the given arguments.
    
    echo=False : if True, the SQLEngine will log all statements as well as a repr() of their 
    parameter lists to the engines logger, which defaults to sys.stdout.  A SQLEngine instances' 
    "echo" data member can be modified at any time to turn logging on and off.  If set to the string 
    'debug', result rows will be printed to the standard output as well.
    
    logger=None : a file-like object where logging output can be sent, if echo is set to True.  
    This defaults to sys.stdout.
    
    module=None : used by Oracle and Postgres, this is a reference to a DBAPI2 module to be used 
    instead of the engine's default module.  For Postgres, the default is psycopg2, or psycopg1 if 
    2 cannot be found.  For Oracle, its cx_Oracle.  For mysql, MySQLdb.
    
    use_ansi=True : used only by Oracle;  when False, the Oracle driver attempts to support a 
    particular "quirk" of some Oracle databases, that the LEFT OUTER JOIN SQL syntax is not 
    supported, and the "Oracle join" syntax of using <column1>(+)=<column2> must be used 
    in order to achieve a LEFT OUTER JOIN.  Its advised that the Oracle database be configured to 
    have full ANSI support instead of using this feature.

    """
    m = re.match(r'(\w+)://(.*)', name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = {}
        def assign(m):
            opts[m.group(1)] = m.group(2)
        re.sub(r'([^&]+)=([^&]*)', assign, args)
    module = getattr(__import__('sqlalchemy.databases.%s' % name).databases, name)
    return module.engine(opts, **kwargs)

def engine_descriptors():
    """provides a listing of all the database implementations supported.  this data
    is provided as a list of dictionaries, where each dictionary contains the following
    key/value pairs:
    
    name :       the name of the engine, suitable for use in the create_engine function

    description: a plain description of the engine.

    arguments :  a dictionary describing the name and description of each parameter
                 used to connect to this engine's underlying DBAPI.
    
    This function is meant for usage in automated configuration tools that wish to 
    query the user for database and connection information.
    """
    result = []
    for module in sqlalchemy.databases.__all__:
        module = getattr(__import__('sqlalchemy.databases.%s' % module).databases, module)
        result.append(module.descriptor())
    return result
    
class SchemaIterator(schema.SchemaVisitor):
    """a visitor that can gather text into a buffer and execute the contents of the buffer."""
    def __init__(self, engine, **params):
        """initializes this SchemaIterator and initializes its buffer.
        
        sqlproxy - a callable function returned by SQLEngine.proxy(), which executes a
        statement plus optional parameters.
        """
        self.engine = engine
        self.buffer = StringIO.StringIO()

    def append(self, s):
        """appends content to the SchemaIterator's query buffer."""
        self.buffer.write(s)
        
    def execute(self):
        """executes the contents of the SchemaIterator's buffer using its sql proxy and
        clears out the buffer."""
        try:
            return self.engine.execute(self.buffer.getvalue(), None)
        finally:
            self.buffer.truncate(0)

class DefaultRunner(schema.SchemaVisitor):
    def __init__(self, engine, proxy):
        self.proxy = proxy
        self.engine = engine

    def get_column_default(self, column):
        if column.default is not None:
            return column.default.accept_schema_visitor(self)
        else:
            return None

    def get_column_onupdate(self, column):
        if column.onupdate is not None:
            return column.onupdate.accept_schema_visitor(self)
        else:
            return None
        
    def visit_passive_default(self, default):
        """passive defaults by definition return None on the app side,
        and are post-fetched to get the DB-side value"""
        return None
        
    def visit_sequence(self, seq):
        """sequences are not supported by default"""
        return None

    def exec_default_sql(self, default):
        c = sql.select([default.arg], engine=self.engine).compile()
        return self.proxy(str(c), c.get_params()).fetchone()[0]
    
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
            

class SQLEngine(schema.SchemaEngine):
    """
    The central "database" object used by an application.  Subclasses of this object is used
    by the schema and SQL construction packages to provide database-specific behaviors,
    as well as an execution and thread-local transaction context.
    
    SQLEngines are constructed via the create_engine() function inside this package.
    """
    
    def __init__(self, pool=None, echo=False, logger=None, default_ordering=False, echo_pool=False, echo_uow=False, convert_unicode=False, encoding='utf-8', **params):
        """constructs a new SQLEngine.   SQLEngines should be constructed via the create_engine()
        function which will construct the appropriate subclass of SQLEngine."""
        # get a handle on the connection pool via the connect arguments
        # this insures the SQLEngine instance integrates with the pool referenced
        # by direct usage of pool.manager(<module>).connect(*args, **params)
        schema.SchemaEngine.__init__(self)
        (cargs, cparams) = self.connect_args()
        if pool is None:
            params['echo'] = echo_pool
            self._pool = sqlalchemy.pool.manage(self.dbapi(), **params).get_pool(*cargs, **cparams)
        else:
            self._pool = pool
        self.default_ordering=default_ordering
        self.echo = echo
        self.echo_uow = echo_uow
        self.convert_unicode = convert_unicode
        self.encoding = encoding
        self.context = util.ThreadLocal(raiseerror=False)
        self._ischema = None
        self._figure_paramstyle()
        self.logger = logger or util.Logger(origin='engine')

    def _translate_connect_args(self, names, args):
        """translates a dictionary of connection arguments to those used by a specific dbapi.
        the names parameter is a tuple of argument names in the form ('host', 'database', 'user', 'password')
        where the given strings match the corresponding argument names for the dbapi.  Will return a dictionary
        with the dbapi-specific parameters, the generic ones removed, and any additional parameters still remaining,
        from the dictionary represented by args.  Will return a blank dictionary if args is null."""
        if args is None:
            return {}
        a = args.copy()
        standard_names = [('host','hostname'), ('database', 'dbname'), ('user', 'username'), ('password', 'passwd', 'pw')]
        for n in names:
            sname = standard_names.pop(0)
            if n is None:
                continue
            for sn in sname:
                if sn != n and a.has_key(sn):
                    a[n] = a[sn]
                    del a[sn]
        return a
    def _get_ischema(self):
        # We use a property for ischema so that the accessor
        # creation only happens as needed, since otherwise we
        # have a circularity problem with the generic
        # ansisql.engine()
        if self._ischema is None:
            import sqlalchemy.databases.information_schema as ischema
            self._ischema = ischema.ISchema(self)
        return self._ischema
    ischema = property(_get_ischema, doc="""returns an ISchema object for this engine, which allows access to information_schema tables (if supported)""")
    
    def hash_key(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.connect_args()))
    
    def _get_name(self):
        return sys.modules[self.__module__].descriptor()['name']
    name = property(_get_name)
        
    def dispose(self):
        """disposes of the underlying pool manager for this SQLEngine."""
        (cargs, cparams) = self.connect_args()
        sqlalchemy.pool.manage(self.dbapi()).dispose(*cargs, **cparams)
        self._pool = None
        
    def _set_paramstyle(self, style):
        self._paramstyle = style
        self._figure_paramstyle(style)
    paramstyle = property(lambda s:s._paramstyle, _set_paramstyle)
    
    def _figure_paramstyle(self, paramstyle=None):
        db = self.dbapi()
        if paramstyle is not None:
            self._paramstyle = paramstyle
        elif db is not None:
            self._paramstyle = db.paramstyle
        else:
            self._paramstyle = 'named'

        if self._paramstyle == 'named':
            self.positional=False
        elif self._paramstyle == 'pyformat':
            self.positional=False
        elif self._paramstyle == 'qmark' or self._paramstyle == 'format' or self._paramstyle == 'numeric':
            # for positional, use pyformat internally, ANSICompiler will convert
            # to appropriate character upon compilation
            self.positional = True
        else:
            raise DBAPIError("Unsupported paramstyle '%s'" % self._paramstyle)
        
    def type_descriptor(self, typeobj):
        """provides a database-specific TypeEngine object, given the generic object
        which comes from the types module.  Subclasses will usually use the adapt_type()
        method in the types module to make this job easy."""
        if type(typeobj) is type:
            typeobj = typeobj()
        return typeobj

    def _func(self):
        class FunctionGateway(object):
            def __getattr__(s, name):
                return lambda *c, **kwargs: sql.Function(name, engine=self, *c, **kwargs)
        return FunctionGateway()
    func = property(_func)
    
    def text(self, text, *args, **kwargs):
        """returns a sql.text() object for performing literal queries."""
        return sql.text(text, engine=self, *args, **kwargs)
        
    def schemagenerator(self, **params):
        """returns a schema.SchemaVisitor instance that can generate schemas, when it is
        invoked to traverse a set of schema objects. 
        
        schemagenerator is called via the create() method.
        """
        raise NotImplementedError()

    def schemadropper(self, **params):
        """returns a schema.SchemaVisitor instance that can drop schemas, when it is
        invoked to traverse a set of schema objects. 
        
        schemagenerator is called via the drop() method.
        """
        raise NotImplementedError()

    def defaultrunner(self, proxy=None):
        """Returns a schema.SchemaVisitor instance that can execute the default values on a column.
        The base class for this visitor is the DefaultRunner class inside this module.
        This visitor will typically only receive schema.DefaultGenerator schema objects.  The given 
        proxy is a callable that takes a string statement and a dictionary of bind parameters
        to be executed.  For engines that require positional arguments, the dictionary should 
        be an instance of OrderedDict which returns its bind parameters in the proper order.
        
        defaultrunner is called within the context of the execute_compiled() method."""
        return DefaultRunner(self, proxy)
    
    def compiler(self, statement, parameters):
        """returns a sql.ClauseVisitor which will produce a string representation of the given
        ClauseElement and parameter dictionary.  This object is usually a subclass of 
        ansisql.ANSICompiler.  
        
        compiler is called within the context of the compile() method."""
        raise NotImplementedError()

    def oid_column_name(self):
        """returns the oid column name for this engine, or None if the engine cant/wont support OID/ROWID."""
        return None

    def supports_sane_rowcount(self):
        """Provided to indicate when MySQL is being used, which does not have standard behavior
        for the "rowcount" function on a statement handle.  """
        return True
        
    def create(self, entity, **params):
        """creates a table or index within this engine's database connection given a schema.Table object."""
        entity.accept_schema_visitor(self.schemagenerator(**params))
        return entity

    def drop(self, entity, **params):
        """drops a table or index within this engine's database connection given a schema.Table object."""
        entity.accept_schema_visitor(self.schemadropper(**params))

    def compile(self, statement, parameters, **kwargs):
        """given a sql.ClauseElement statement plus optional bind parameters, creates a new
        instance of this engine's SQLCompiler, compiles the ClauseElement, and returns the
        newly compiled object."""
        compiler = self.compiler(statement, parameters, **kwargs)
        compiler.compile()
        return compiler

    def reflecttable(self, table):
        """given a Table object, reflects its columns and properties from the database."""
        raise NotImplementedError()

    def get_default_schema_name(self):
        """returns the currently selected schema in the current connection."""
        return None
        
    def last_inserted_ids(self):
        """returns a thread-local list of the primary key values for the last insert statement executed.
        This does not apply to straight textual clauses; only to sql.Insert objects compiled against 
        a schema.Table object, which are executed via statement.execute().  The order of items in the 
        list is the same as that of the Table's 'primary_key' attribute.
        
        In some cases, this method may invoke a query back to the database to retrieve the data, based on
        the "lastrowid" value in the cursor."""
        raise NotImplementedError()

    def connect_args(self):
        """subclasses override this method to provide a two-item tuple containing the *args
        and **kwargs used to establish a connection."""
        raise NotImplementedError()

    def dbapi(self):
        """subclasses override this method to provide the DBAPI module used to establish
        connections."""
        raise NotImplementedError()

    def do_begin(self, connection):
        """implementations might want to put logic here for turning autocommit on/off,
        etc."""
        pass
    def do_rollback(self, connection):
        """implementations might want to put logic here for turning autocommit on/off,
        etc."""
        connection.rollback()
    def do_commit(self, connection):
        """implementations might want to put logic here for turning autocommit on/off, etc."""
        connection.commit()

    def connection(self):
        """returns a managed DBAPI connection from this SQLEngine's connection pool."""
        return self._pool.connect()

    def multi_transaction(self, tables, func):
        """provides a transaction boundary across tables which may be in multiple databases.
        If you have three tables, and a function that operates upon them, providing the tables as a 
        list and the function will result in a begin()/commit() pair invoked for each distinct engine
        represented within those tables, and the function executed within the context of that transaction.
        any exceptions will result in a rollback().
        
        clearly, this approach only goes so far, such as if database A commits, then database B commits
        and fails, A is already committed.  Any failure conditions have to be raised before anyone
        commits for this to be useful."""
        engines = util.HashSet()
        for table in tables:
            engines.append(table.engine)
        for engine in engines:
            engine.begin()
        try:
            func()
        except:
            for engine in engines:
                engine.rollback()
            raise
        for engine in engines:
            engine.commit()
            
    def transaction(self, func):
        """executes the given function within a transaction boundary.  this is a shortcut for
        explicitly calling begin() and commit() and optionally rollback() when execptions are raised."""
        self.begin()
        try:
            func()
        except:
            self.rollback()
            raise
        self.commit()
        
    def begin(self):
        """"begins" a transaction on a pooled connection, and stores the connection in a thread-local
        context.  repeated calls to begin() within the same thread will increment a counter that must be
        decreased by corresponding commit() statements before an actual commit occurs.  this is to provide
        "nested" behavior of transactions so that different functions can all call begin()/commit() and still
        call each other."""
        if getattr(self.context, 'transaction', None) is None:
            conn = self.connection()
            self.do_begin(conn)
            self.context.transaction = conn
            self.context.tcount = 1
        else:
            self.context.tcount += 1
            
    def rollback(self):
        """rolls back the current thread-local transaction started by begin().  the "begin" counter 
        is cleared and the transaction ended."""
        if self.context.transaction is not None:
            self.do_rollback(self.context.transaction)
            self.context.transaction = None
            self.context.tcount = None
            
    def commit(self):
        """commits the current thread-local transaction started by begin().  If begin() was called multiple
        times, a counter will be decreased for each call to commit(), with the actual commit operation occuring
        when the counter reaches zero.  this is to provide
        "nested" behavior of transactions so that different functions can all call begin()/commit() and still
        call each other."""
        if self.context.transaction is not None:
            count = self.context.tcount - 1
            self.context.tcount = count
            if count == 0:
                self.do_commit(self.context.transaction)
                self.context.transaction = None
                self.context.tcount = None

    def _process_defaults(self, proxy, compiled, parameters, **kwargs):
        """INSERT and UPDATE statements, when compiled, may have additional columns added to their
        VALUES and SET lists corresponding to column defaults/onupdates that are present on the 
        Table object (i.e. ColumnDefault, Sequence, PassiveDefault).  This method pre-execs those
        DefaultGenerator objects that require pre-execution and sets their values within the 
        parameter list, and flags the thread-local state about
        PassiveDefault objects that may require post-fetching the row after it is inserted/updated.  
        This method relies upon logic within the ANSISQLCompiler in its visit_insert and 
        visit_update methods that add the appropriate column clauses to the statement when its 
        being compiled, so that these parameters can be bound to the statement."""
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            drunner = self.defaultrunner(proxy)
            self.context.lastrow_has_defaults = False
            for param in plist:
                last_inserted_ids = []
                need_lastrowid=False
                for c in compiled.statement.table.c:
                    if not param.has_key(c.name) or param[c.name] is None:
                        if isinstance(c.default, schema.PassiveDefault):
                            self.context.lastrow_has_defaults = True
                        newid = drunner.get_column_default(c)
                        if newid is not None:
                            param[c.name] = newid
                            if c.primary_key:
                                last_inserted_ids.append(param[c.name])
                        elif c.primary_key:
                            need_lastrowid = True
                    elif c.primary_key:
                        last_inserted_ids.append(param[c.name])
                if need_lastrowid:
                    self.context.last_inserted_ids = None
                else:
                    self.context.last_inserted_ids = last_inserted_ids
                self.context.last_inserted_params = param
        elif getattr(compiled, 'isupdate', False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            drunner = self.defaultrunner(proxy)
            self.context.lastrow_has_defaults = False
            for param in plist:
                for c in compiled.statement.table.c:
                    if c.onupdate is not None and (not param.has_key(c.name) or param[c.name] is None):
                        value = drunner.get_column_onupdate(c)
                        if value is not None:
                            param[c.name] = value
                self.context.last_updated_params = param
    
    def last_inserted_params(self):
        """returns a dictionary of the full parameter dictionary for the last compiled INSERT statement,
        including any ColumnDefaults or Sequences that were pre-executed.  this value is thread-local."""
        return self.context.last_inserted_params
    def last_updated_params(self):
        """returns a dictionary of the full parameter dictionary for the last compiled UPDATE statement,
        including any ColumnDefaults that were pre-executed. this value is thread-local."""
        return self.context.last_updated_params                
    def lastrow_has_defaults(self):
        """returns True if the last row INSERTED via a compiled insert statement contained PassiveDefaults,
        indicating that the database inserted data beyond that which we gave it. this value is thread-local."""
        return self.context.lastrow_has_defaults
        
    def pre_exec(self, proxy, compiled, parameters, **kwargs):
        """called by execute_compiled before the compiled statement is executed."""
        pass

    def post_exec(self, proxy, compiled, parameters, **kwargs):
        """called by execute_compiled after the compiled statement is executed."""
        pass

    def execute_compiled(self, compiled, parameters, connection=None, cursor=None, echo=None, **kwargs):
        """executes the given compiled statement object with the given parameters.  

        The parameters can be a dictionary of key/value pairs, or a list of dictionaries for an
        executemany() style of execution.  Engines that use positional parameters will convert
        the parameters to a list before execution.

        If the current thread has specified a transaction begin() for this engine, the
        statement will be executed in the context of the current transactional connection.
        Otherwise, a commit() will be performed immediately after execution, since the local
        pooled connection is returned to the pool after execution without a transaction set
        up.

        In all error cases, a rollback() is immediately performed on the connection before
        propigating the exception outwards.

        Other options include:

        connection  -  a DBAPI connection to use for the execute.  If None, a connection is
                       pulled from this engine's connection pool.

        echo        -  enables echo for this execution, which causes all SQL and parameters
                       to be dumped to the engine's logging output before execution.

        typemap     -  a map of column names mapped to sqlalchemy.types.TypeEngine objects.
                       These will be passed to the created ResultProxy to perform
                       post-processing on result-set values.

        commit      -  if True, will automatically commit the statement after completion. """
        
        if connection is None:
            connection = self.connection()

        if cursor is None:
            cursor = connection.cursor()

        executemany = parameters is not None and (isinstance(parameters, list) or isinstance(parameters, tuple))
        if executemany:
            parameters = [compiled.get_params(**m) for m in parameters]
        else:
            parameters = compiled.get_params(**parameters)
        def proxy(statement=None, parameters=None):
            if statement is None:
                return cursor
            
            parameters = self._convert_compiled_params(parameters)
            self.execute(statement, parameters, connection=connection, cursor=cursor, return_raw=True)        
            return cursor

        self.pre_exec(proxy, compiled, parameters, **kwargs)
        self._process_defaults(proxy, compiled, parameters, **kwargs)
        proxy(str(compiled), parameters)
        self.post_exec(proxy, compiled, parameters, **kwargs)
        return ResultProxy(cursor, self, typemap=compiled.typemap)

    def execute(self, statement, parameters=None, connection=None, cursor=None, echo=None, typemap=None, commit=False, return_raw=False, **kwargs):
        """executes the given string-based SQL statement with the given parameters.  

        The parameters can be a dictionary or a list, or a list of dictionaries or lists, depending
        on the paramstyle of the DBAPI.
        
        If the current thread has specified a transaction begin() for this engine, the
        statement will be executed in the context of the current transactional connection.
        Otherwise, a commit() will be performed immediately after execution, since the local
        pooled connection is returned to the pool after execution without a transaction set
        up.

        In all error cases, a rollback() is immediately performed on the connection before
        propigating the exception outwards.

        Other options include:

        connection  -  a DBAPI connection to use for the execute.  If None, a connection is
                       pulled from this engine's connection pool.

        echo        -  enables echo for this execution, which causes all SQL and parameters
                       to be dumped to the engine's logging output before execution.

        typemap     -  a map of column names mapped to sqlalchemy.types.TypeEngine objects.
                       These will be passed to the created ResultProxy to perform
                       post-processing on result-set values.

        commit      -  if True, will automatically commit the statement after completion. """
        
        if connection is None:
            connection = self.connection()

        if cursor is None:
            cursor = connection.cursor()

        try:
            if echo is True or self.echo is not False:
                self.log(statement)
                self.log(repr(parameters))
            if parameters is not None and isinstance(parameters, list) and len(parameters) > 0 and (isinstance(parameters[0], list) or isinstance(parameters[0], dict)):
                self._executemany(cursor, statement, parameters)
            else:
                self._execute(cursor, statement, parameters)
            if self.context.transaction is None:
                self.do_commit(connection)
        except:
            self.do_rollback(connection)
            raise
        if return_raw:
            return cursor
        else:
            return ResultProxy(cursor, self, typemap=typemap)

    def _execute(self, c, statement, parameters):
        if parameters is None:
            if self.positional:
                parameters = ()
            else:
                parameters = {}
        try:
            c.execute(statement, parameters)
        except Exception, e:
            raise exceptions.SQLError(statement, parameters, e)
        self.context.rowcount = c.rowcount
    def _executemany(self, c, statement, parameters):
        c.executemany(statement, parameters)
        self.context.rowcount = c.rowcount

    def _convert_compiled_params(self, parameters):
        executemany = parameters is not None and isinstance(parameters, list)
        # the bind params are a CompiledParams object.  but all the DBAPI's hate
        # that object (or similar).  so convert it to a clean 
        # dictionary/list/tuple of dictionary/tuple of list
        if parameters is not None:
           if self.positional:
                if executemany:
                    parameters = [p.values() for p in parameters]
                else:
                    parameters = parameters.values()
           else:
                if executemany:
                    parameters = [p.get_raw_dict() for p in parameters]
                else:
                    parameters = parameters.get_raw_dict()
        return parameters

    def proxy(self, statement=None, parameters=None):
        parameters = self._convert_compiled_params(parameters)
        return self.execute(statement, parameters)
    
    def log(self, msg):
        """logs a message using this SQLEngine's logger stream."""
        self.logger.write(msg)


class ResultProxy:
    """wraps a DBAPI cursor object to provide access to row columns based on integer
    position, case-insensitive column name, or by schema.Column object. e.g.:
    
    row = fetchone()

    col1 = row[0]    # access via integer position

    col2 = row['col2']   # access via name

    col3 = row[mytable.c.mycol] # access via Column object.  
    
    ResultProxy also contains a map of TypeEngine objects and will invoke the appropriate
    convert_result_value() method before returning columns.
    """
    class AmbiguousColumn(object):
        def __init__(self, key):
            self.key = key
        def convert_result_value(self, arg, engine):
            raise InvalidRequestError("Ambiguous column name '%s' in result set! try 'use_labels' option on select statement." % (self.key))
    
    def __init__(self, cursor, engine, typemap = None):
        """ResultProxy objects are constructed via the execute() method on SQLEngine."""
        self.cursor = cursor
        self.engine = engine
        self.echo = engine.echo=="debug"
        self.rowcount = engine.context.rowcount
        metadata = cursor.description
        self.props = {}
        self.keys = []
        i = 0
        if metadata is not None:
            for item in metadata:
                # sqlite possibly prepending table name to colnames so strip
                colname = item[0].split('.')[-1].lower()
                if typemap is not None:
                    rec = (typemap.get(colname, types.NULLTYPE), i)
                else:
                    rec = (types.NULLTYPE, i)
                if rec[0] is None:
                    raise DBAPIError("None for metadata " + colname)
                if self.props.setdefault(colname, rec) is not rec:
                    self.props[colname] = (ResultProxy.AmbiguousColumn(colname), 0)
                self.keys.append(colname)
                #print "COLNAME", colname
                self.props[i] = rec
                i+=1

    def _get_col(self, row, key):
        if isinstance(key, schema.Column) or isinstance(key, sql.ColumnElement):
            try:
                rec = self.props[key._label.lower()]
                #print "GOT IT FROM LABEL FOR ", key._label
            except KeyError:
                try:
                    rec = self.props[key.key.lower()]
                except KeyError:
                    rec = self.props[key.name.lower()]
        elif isinstance(key, str):
            rec = self.props[key.lower()]
        else:
            rec = self.props[key]
        return rec[0].convert_result_value(row[rec[1]], self.engine)
        
    def fetchall(self):
        """fetches all rows, just like DBAPI cursor.fetchall()."""
        l = []
        while True:
            v = self.fetchone()
            if v is None:
                return l
            l.append(v)
            
    def fetchone(self):
        """fetches one row, just like DBAPI cursor.fetchone()."""
        row = self.cursor.fetchone()
        if row is not None:
            if self.echo: self.engine.log(repr(row))
            return RowProxy(self, row)
        else:
            return None

class RowProxy:
    """proxies a single cursor row for a parent ResultProxy."""
    def __init__(self, parent, row):
        """RowProxy objects are constructed by ResultProxy objects."""
        self.__parent = parent
        self.__row = row
    def __iter__(self):
        for i in range(0, len(self.__row)):
            yield self.__parent._get_col(self.__row, i)
    def __eq__(self, other):
        return (other is self) or (other == tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))
    def __repr__(self):
        return repr(tuple([self.__parent._get_col(self.__row, key) for key in range(0, len(self.__row))]))
    def __getitem__(self, key):
        return self.__parent._get_col(self.__row, key)
    def __getattr__(self, name):
        try:
            return self.__parent._get_col(self.__row, name)
        except:
            raise AttributeError
    def items(self):
        return [(key, getattr(self, key)) for key in self.keys()]
    def keys(self):
        return self.__parent.keys
    def values(self): 
        return list(self)
    def __len__(self): 
        return len(self.__row)
