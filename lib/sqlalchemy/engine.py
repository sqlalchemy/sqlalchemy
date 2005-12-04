# engine.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

"""builds upon the schema and sql packages to provide a central object for tying schema
objects and sql constructs to database-specific query compilation and execution"""

import sqlalchemy.schema as schema
import sqlalchemy.pool
import sqlalchemy.util as util
import sqlalchemy.sql as sql
import StringIO, sys, re
import sqlalchemy.types as types
import sqlalchemy.databases

__all__ = ['create_engine', 'engine_descriptors']

def create_engine(name, *args ,**kwargs):
    """creates a new SQLEngine instance.
    
    name - the type of engine to load, i.e. 'sqlite', 'postgres', 'oracle'
    
    *args, **kwargs - sent directly to the specific engine instance as connect arguments,
    options.
    """
    m = re.match(r'(\w+)://(.*)', name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = {}
        def assign(m):
            opts[m.group(1)] = m.group(2)
        re.sub(r'([^&]+)=([^&]*)', assign, args)
        args = [opts]
    module = getattr(__import__('sqlalchemy.databases.%s' % name).databases, name)
    return module.engine(*args, **kwargs)

def engine_descriptors():
    result = []
    for module in sqlalchemy.databases.__all__:
        module = getattr(__import__('sqlalchemy.databases.%s' % module).databases, module)
        result.append(module.descriptor())
    return result
    
class SchemaIterator(schema.SchemaVisitor):
    """a visitor that can gather text into a buffer and execute the contents of the buffer."""
    def __init__(self, sqlproxy, **params):
        """initializes this SchemaIterator and initializes its buffer.
        
        sqlproxy - a callable function returned by SQLEngine.proxy(), which executes a
        statement plus optional parameters.
        """
        self.sqlproxy = sqlproxy
        self.buffer = StringIO.StringIO()

    def append(self, s):
        """appends content to the SchemaIterator's query buffer."""
        self.buffer.write(s)
        
    def execute(self):
        """executes the contents of the SchemaIterator's buffer using its sql proxy and
        clears out the buffer."""
        try:
            return self.sqlproxy(self.buffer.getvalue())
        finally:
            self.buffer.truncate(0)

class SQLEngine(schema.SchemaEngine):
    """base class for a series of database-specific engines.  serves as an abstract factory
    for implementation objects as well as database connections, transactions, SQL generators,
    etc."""
    
    def __init__(self, pool = None, echo = False, logger = None, **params):
        # get a handle on the connection pool via the connect arguments
        # this insures the SQLEngine instance integrates with the pool referenced
        # by direct usage of pool.manager(<module>).connect(*args, **params)
        (cargs, cparams) = self.connect_args()
        if pool is None:
            self._pool = sqlalchemy.pool.manage(self.dbapi(), **params).get_pool(*cargs, **cparams)
        else:
            self._pool = pool
        self.echo = echo
        self.context = util.ThreadLocal(raiseerror=False)
        self.tables = {}
        self.notes = {}
        if logger is None:
            self.logger = sys.stdout
        else:
            self.logger = logger

        
    def type_descriptor(self, typeobj):
        if type(typeobj) is type:
            typeobj = typeobj()
        return typeobj
        
    def schemagenerator(self, proxy, **params):
        raise NotImplementedError()

    def schemadropper(self, proxy, **params):
        raise NotImplementedError()

    def compiler(self, statement, bindparams):
        raise NotImplementedError()

    def rowid_column_name(self):
        """returns the ROWID column name for this engine."""
        return "oid"

    def supports_sane_rowcount(self):
        """ill give everyone one guess which database warrants this method."""
        return True
        
    def create(self, table, **params):
        """creates a table given a schema.Table object."""
        table.accept_visitor(self.schemagenerator(self.proxy(), **params))

    def drop(self, table, **params):
        """drops a table given a schema.Table object."""
        table.accept_visitor(self.schemadropper(self.proxy(), **params))

    def compile(self, statement, bindparams, **kwargs):
        """given a sql.ClauseElement statement plus optional bind parameters, creates a new
        instance of this engine's SQLCompiler, compiles the ClauseElement, and returns the
        newly compiled object."""
        compiler = self.compiler(statement, bindparams, **kwargs)
        statement.accept_visitor(compiler)
        compiler.after_compile()
        return compiler

    def reflecttable(self, table):
        """given a Table object, reflects its columns and properties from the database."""
        raise NotImplementedError()

    def tableimpl(self, table):
        """returns a new sql.TableImpl object to correspond to the given Table object."""
        return sql.TableImpl(table)

    def columnimpl(self, column):
        """returns a new sql.ColumnImpl object to correspond to the given Column object."""
        return sql.ColumnImpl(column)

    def get_default_schema_name(self):
        return None
        
    def last_inserted_ids(self):
        """returns a thread-local list of the primary keys for the last insert statement executed.
        This does not apply to straight textual clauses; only to sql.Insert objects compiled against a schema.Table object, which are executed via statement.execute().  The order of items in the list is the same as that of the Table's 'primary_key' attribute."""
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

    def proxy(self, **kwargs):
        return lambda s, p = None: self.execute(s, p, **kwargs)

    def connection(self):
        """returns a managed DBAPI connection from this SQLEngine's connection pool."""
        return self._pool.connect()

    def multi_transaction(self, tables, func):
        """provides a transaction boundary across tables which may be in multiple databases.
        
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
        self.begin()
        try:
            func()
        except:
            self.rollback()
            raise
        self.commit()
        
    def begin(self):
        if getattr(self.context, 'transaction', None) is None:
            conn = self.connection()
            self.do_begin(conn)
            self.context.transaction = conn
            self.context.tcount = 1
        else:
            self.context.tcount += 1
            
    def rollback(self):
        if self.context.transaction is not None:
            self.do_rollback(self.context.transaction)
            self.context.transaction = None
            self.context.tcount = None
            
    def commit(self):
        if self.context.transaction is not None:
            count = self.context.tcount - 1
            self.context.tcount = count
            if count == 0:
                self.do_commit(self.context.transaction)
                self.context.transaction = None
                self.context.tcount = None
            
    def pre_exec(self, connection, cursor, statement, parameters, many = False, echo = None, **kwargs):
        pass

    def post_exec(self, connection, cursor, statement, parameters, many = False, echo = None, **kwargs):
        pass

    def execute(self, statement, parameters, connection = None, echo = None, typemap = None, commit=False, **kwargs):
        """executes the given string-based SQL statement with the given parameters.  This is
        a direct interface to a DBAPI connection object.  The parameters may be a dictionary,
        or an array of dictionaries.  If an array of dictionaries is sent, executemany will
        be performed on the cursor instead of execute.

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
        if parameters is None:
            parameters = {}

        if connection is None:
            connection = self.connection()
            c = connection.cursor()
        else:
            c = connection.cursor()

        try:
            self.pre_exec(connection, c, statement, parameters, echo = echo, **kwargs)

            if echo is True or self.echo:
                self.log(statement)
                self.log(repr(parameters))
            if isinstance(parameters, list) and len(parameters) > 0 and (isinstance(parameters[0], list) or isinstance(parameters[0], dict)):
                self._executemany(c, statement, parameters)
            else:
                self._execute(c, statement, parameters)
            self.post_exec(connection, c, statement, parameters, echo = echo, **kwargs)
            if commit or self.context.transaction is None:
                self.do_commit(connection)
        except:
            self.do_rollback(connection)
            # TODO: wrap DB exceptions ?
            raise
        return ResultProxy(c, self, typemap = typemap)

    def _execute(self, c, statement, parameters):
        c.execute(statement, parameters)
        self.context.rowcount = c.rowcount
    def _executemany(self, c, statement, parameters):
        c.executemany(statement, parameters)
        self.context.rowcount = c.rowcount
    
    def log(self, msg):
        """logs a message using this SQLEngine's logger stream."""
        self.logger.write(msg + "\n")


class ResultProxy:
    """wraps a DBAPI cursor object to provide access to row columns based on integer
    position, case-insensitive column name, or by schema.Column object. e.g.:
    
    row = fetchone()
    col1 = row[0]    # access via integer position
    col2 = row['col2']   # access via name
    col3 = row[mytable.c.mycol]   # access via Column object.  
                                  #the Column's 'label', 'key', and 'name' properties are
                                  # searched in that order.
    
    """
    class AmbiguousColumn(object):
        def __init__(self, key):
            self.key = key
        def convert_result_value(self, arg):
            raise "Ambiguous column name '%s' in result set! try 'use_labels' option on select statement." % (self.key)
    
    def __init__(self, cursor, engine, typemap = None):
        self.cursor = cursor
        self.echo = engine.echo
        self.rowcount = engine.context.rowcount
        metadata = cursor.description
        self.props = {}
        i = 0
        if metadata is not None:
            for item in metadata:
                # sqlite possibly prepending table name to colnames so strip
                colname = item[0].split('.')[-1].lower()
                if typemap is not None:
                    rec = (typemap.get(colname, types.NULLTYPE), i)
                else:
                    rec = (types.NULLTYPE, i)
                if self.props.setdefault(colname, rec) is not rec:
                    self.props[colname] = (ResultProxy.AmbiguousColumn(colname), 0)
                self.props[i] = rec
                i+=1

    def _get_col(self, row, key):
        if isinstance(key, schema.Column):
            try:
                rec = self.props[key.label.lower()]
            except KeyError:
                try:
                    rec = self.props[key.key.lower()]
                except KeyError:
                    rec = self.props[key.name.lower()]
        elif isinstance(key, str):
            rec = self.props[key.lower()]
        else:
            rec = self.props[key]
        return rec[0].convert_result_value(row[rec[1]])
        
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
            if self.echo: print repr(row)
            return RowProxy(self, row)
        else:
            return None

class RowProxy:
    """proxies a single cursor row for a parent ResultProxy."""
    def __init__(self, parent, row):
        self.parent = parent
        self.row = row
    def __iter__(self):
        return iter(self.row)
    def __eq__(self, other):
        return (other is self) or (other == tuple([self.parent._get_col(self.row, key) for key in range(0, len(self.row))]))
    def __repr__(self):
        return repr(tuple([self.parent._get_col(self.row, key) for key in range(0, len(self.row))]))
    def __getitem__(self, key):
        return self.parent._get_col(self.row, key)


