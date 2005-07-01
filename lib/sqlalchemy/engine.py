"""builds upon the schema and sql packages to provide a central object for tying schema objects
and sql constructs to database-specific query compilation and execution"""

import sqlalchemy.schema as schema
import sqlalchemy.pool
import sqlalchemy.util as util
import sqlalchemy.sql as sql
import StringIO

class SchemaIterator(schema.SchemaVisitor):
    """a visitor that can gather text into a buffer and execute the contents of the buffer."""
    
    def __init__(self, sqlproxy, **params):
        self.sqlproxy = sqlproxy
        self.buffer = StringIO.StringIO()

    def run(self):
        raise NotImplementedError()

    def append(self, s):
        self.buffer.write(s)
        
    def execute(self):
        try:
            return self.sqlproxy(self.buffer.getvalue())
        finally:
            self.buffer.truncate(0)

class SQLEngine(schema.SchemaEngine):
    """base class for a series of database-specific engines.  serves as an abstract factory for
    implementation objects as well as database connections, transactions, SQL generators, etc."""
    
    def __init__(self, pool = None, echo = False, **params):
        # get a handle on the connection pool via the connect arguments
        # this insures the SQLEngine instance integrates with the pool referenced
        # by direct usage of pool.manager(<module>).connect(*args, **params)
        (cargs, cparams) = self.connect_args()
        self._pool = sqlalchemy.pool.manage(self.dbapi()).get_pool(*cargs, **cparams)
        self._echo = echo
        self.context = util.ThreadLocal()
        
    def schemagenerator(self, proxy, **params):
        raise NotImplementedError()

    def schemadropper(self, proxy, **params):
        raise NotImplementedError()
        
    def columnimpl(self, column):
        return sql.ColumnSelectable(column)

    def connect_args(self):
        raise NotImplementedError()
        
    def dbapi(self):
        raise NotImplementedError()

    def compile(self, statement):
        raise NotImplementedError()

    def proxy(self):
        return lambda s, p = None: self.execute(s, p)
        
    def connection(self):
        return self._pool.connect()

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
            self.context.transaction = conn
            self.context.tcount = 1
        else:
            self.context.tcount += 1
            
    def rollback(self):
        if self.context.transaction is not None:
            self.context.transaction.rollback()
            self.context.transaction = None
            self.context.tcount = None
            
    def commit(self):
        if self.context.transaction is not None:
            count = self.context.tcount - 1
            self.context.tcount = count
            if count == 0:
                self.context.transaction.commit()
                self.context.transaction = None
                self.context.tcount = None
                
    def execute(self, statement, parameters, connection = None, **params):
        if parameters is None:
            parameters = {}
        
        if self._echo:
            self.log(statement)
            self.log(repr(parameters))
            
        if connection is None:
            poolconn = self.connection()
            c = poolconn.cursor()
            c.execute(statement, parameters)
            return c
        else:
            c = connection.cursor()
            c.execute(statement, parameters)
            return c

    def log(self, msg):
        print msg
