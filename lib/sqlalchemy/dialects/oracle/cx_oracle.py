"""Support for the Oracle database via the cx_oracle driver.

Driver
------

The Oracle dialect uses the cx_oracle driver, available at 
http://cx-oracle.sourceforge.net/ .   The dialect has several behaviors 
which are specifically tailored towards compatibility with this module.

Connecting
----------

Connecting with create_engine() uses the standard URL approach of 
``oracle://user:pass@host:port/dbname[?key=value&key=value...]``.  If dbname is present, the 
host, port, and dbname tokens are converted to a TNS name using the cx_oracle 
:func:`makedsn()` function.  Otherwise, the host token is taken directly as a TNS name.

Additional arguments which may be specified either as query string arguments on the
URL, or as keyword arguments to :func:`~sqlalchemy.create_engine()` are:

* *allow_twophase* - enable two-phase transactions.  Defaults to ``True``.

* *arraysize* - set the cx_oracle.arraysize value on cursors, in SQLAlchemy
  it defaults to 50.  See the section on "LOB Objects" below.
  
* *auto_convert_lobs* - defaults to True, see the section on LOB objects.

* *auto_setinputsizes* - the cx_oracle.setinputsizes() call is issued for all bind parameters.
  This is required for LOB datatypes but can be disabled to reduce overhead.  Defaults
  to ``True``.

* *mode* - This is given the string value of SYSDBA or SYSOPER, or alternatively an
  integer value.  This value is only available as a URL query string argument.

* *threaded* - enable multithreaded access to cx_oracle connections.  Defaults
  to ``True``.  Note that this is the opposite default of cx_oracle itself.

Unicode
-------

As of cx_oracle 5, Python unicode objects can be bound directly to statements, 
and it appears that cx_oracle can handle these even without NLS_LANG being set.
SQLAlchemy tests for version 5 and will pass unicode objects straight to cx_oracle
if this is the case.  For older versions of cx_oracle, SQLAlchemy will encode bind
parameters normally using dialect.encoding as the encoding.

LOB Objects
-----------

cx_oracle presents some challenges when fetching LOB objects.  A LOB object in a result set
is presented by cx_oracle as a cx_oracle.LOB object which has a read() method.  By default, 
SQLAlchemy converts these LOB objects into Python strings.  This is for two reasons.  First,
the LOB object requires an active cursor association, meaning if you were to fetch many rows
at once such that cx_oracle had to go back to the database and fetch a new batch of rows,
the LOB objects in the already-fetched rows are now unreadable and will raise an error. 
SQLA "pre-reads" all LOBs so that their data is fetched before further rows are read.  
The size of a "batch of rows" is controlled by the cursor.arraysize value, which SQLAlchemy
defaults to 50 (cx_oracle normally defaults this to one).  

Secondly, the LOB object is not a standard DBAPI return value so SQLAlchemy seeks to 
"normalize" the results to look more like that of other DBAPIs.

The conversion of LOB objects by this dialect is unique in SQLAlchemy in that it takes place
for all statement executions, even plain string-based statements for which SQLA has no awareness
of result typing.  This is so that calls like fetchmany() and fetchall() can work in all cases
without raising cursor errors.  The conversion of LOB in all cases, as well as the "prefetch"
of LOB objects, can be disabled using auto_convert_lobs=False.  

Two Phase Transaction Support
-----------------------------

Two Phase transactions are implemented using XA transactions.  Success has been reported 
with this feature but it should be regarded as experimental.

"""

from sqlalchemy.dialects.oracle.base import OracleCompiler, OracleDialect, RESERVED_WORDS, OracleExecutionContext
from sqlalchemy.dialects.oracle import base as oracle
from sqlalchemy.engine import base
from sqlalchemy import types as sqltypes, util
from datetime import datetime

class _OracleDate(sqltypes.Date):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return value.date()
            else:
                return value
        return process

class _LOBMixin(object):
    def result_processor(self, dialect, coltype):
        if not dialect.auto_convert_lobs:
            # return the cx_oracle.LOB directly.
            return None
            
        super_process = super(_LOBMixin, self).result_processor(dialect, coltype)
        if super_process:
            def process(value):
                if value is not None:
                    return super_process(value.read())
                else:
                    return super_process(value)
        else:
            def process(value):
                if value is not None:
                    return value.read()
                else:
                    return value
        return process

class _OracleChar(sqltypes.CHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.FIXED_CHAR

class _OracleNVarChar(sqltypes.NVARCHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.UNICODE
    def result_processor(self, dialect, coltype):
        if dialect._cx_oracle_native_nvarchar:
            return None
        else:
            return sqltypes.NVARCHAR.result_processor(self, dialect, coltype)
        
class _OracleText(_LOBMixin, sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB

class _OracleUnicodeText(sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCLOB

    def result_processor(self, dialect, coltype):
        if not dialect.auto_convert_lobs:
            # return the cx_oracle.LOB directly.
            return None

        if dialect._cx_oracle_native_nvarchar:
            def process(value):
                if value is not None:
                    return value.read()
                else:
                    return value
            return process
        else:
            # TODO: this is wrong - we are getting a LOB here
            # no matter what version of oracle, so process() 
            # is still needed
            return super(_OracleUnicodeText, self).result_processor(dialect, coltype)

class _OracleInteger(sqltypes.Integer):
    def result_processor(self, dialect, coltype):
        def to_int(val):
            if val is not None:
                val = int(val)
            return val
        return to_int
        
class _OracleBinary(_LOBMixin, sqltypes.LargeBinary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def bind_processor(self, dialect):
        return None

class _OracleInterval(oracle.INTERVAL):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTERVAL
    
class _OracleRaw(oracle.RAW):
    pass

colspecs = {
    sqltypes.Date : _OracleDate,
    sqltypes.LargeBinary : _OracleBinary,
    sqltypes.Boolean : oracle._OracleBoolean,
    sqltypes.Interval : _OracleInterval,
    oracle.INTERVAL : _OracleInterval,
    sqltypes.Text : _OracleText,
    sqltypes.UnicodeText : _OracleUnicodeText,
    sqltypes.CHAR : _OracleChar,
    sqltypes.Integer : _OracleInteger,  # this is only needed for OUT parameters.
                                        # it would be nice if we could not use it otherwise.
    oracle.NUMBER : oracle.NUMBER, # don't let this get converted
    oracle.RAW: _OracleRaw,
    sqltypes.Unicode: _OracleNVarChar,
    sqltypes.NVARCHAR : _OracleNVarChar,
}

class Oracle_cx_oracleCompiler(OracleCompiler):
    def bindparam_string(self, name):
        if self.preparer._bindparam_requires_quotes(name):
            quoted_name = '"%s"' % name
            self._quoted_bind_names[name] = quoted_name
            return OracleCompiler.bindparam_string(self, quoted_name)
        else:
            return OracleCompiler.bindparam_string(self, name)

class Oracle_cx_oracleExecutionContext(OracleExecutionContext):
    def pre_exec(self):
        quoted_bind_names = getattr(self.compiled, '_quoted_bind_names', {})
        if quoted_bind_names:
            for param in self.parameters:
                for fromname, toname in self.compiled._quoted_bind_names.iteritems():
                    param[toname.encode(self.dialect.encoding)] = param[fromname]
                    del param[fromname]

        if self.dialect.auto_setinputsizes:
            self.set_input_sizes(quoted_bind_names)
            
        if len(self.compiled_parameters) == 1:
            for key in self.compiled.binds:
                bindparam = self.compiled.binds[key]
                name = self.compiled.bind_names[bindparam]
                value = self.compiled_parameters[0][name]
                if bindparam.isoutparam:
                    dbtype = bindparam.type.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                    if not hasattr(self, 'out_parameters'):
                        self.out_parameters = {}
                    self.out_parameters[name] = self.cursor.var(dbtype)
                    self.parameters[0][quoted_bind_names.get(name, name)] = self.out_parameters[name]
        
    def create_cursor(self):
        c = self._connection.connection.cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize
        return c

    def get_result_proxy(self):
        if hasattr(self, 'out_parameters') and self.compiled.returning:
            returning_params = dict((k, v.getvalue()) for k, v in self.out_parameters.items())
            return ReturningResultProxy(self, returning_params)

        result = None
        if self.cursor.description is not None:
            for column in self.cursor.description:
                type_code = column[1]
                if type_code in self.dialect.ORACLE_BINARY_TYPES:
                    result = base.BufferedColumnResultProxy(self)
        
        if result is None:
            result = base.ResultProxy(self)
            
        if hasattr(self, 'out_parameters'):
            if self.compiled_parameters is not None and len(self.compiled_parameters) == 1:
                result.out_parameters = out_parameters = {}
                
                for bind, name in self.compiled.bind_names.iteritems():
                    if name in self.out_parameters:
                        type = bind.type
                        impl_type = type.dialect_impl(self.dialect)
                        dbapi_type = impl_type.get_dbapi_type(self.dialect.dbapi)
                        result_processor = impl_type.\
                                                    result_processor(self.dialect, 
                                                    dbapi_type)
                        if result_processor is not None:
                            out_parameters[name] = \
                                    result_processor(self.out_parameters[name].getvalue())
                        else:
                            out_parameters[name] = self.out_parameters[name].getvalue()
            else:
                result.out_parameters = dict(
                                            (k, v.getvalue()) 
                                            for k, v in self.out_parameters.items()
                                        )

        return result

class ReturningResultProxy(base.FullyBufferedResultProxy):
    """Result proxy which stuffs the _returning clause + outparams into the fetch."""
    
    def __init__(self, context, returning_params):
        self._returning_params = returning_params
        super(ReturningResultProxy, self).__init__(context)
        
    def _cursor_description(self):
        returning = self.context.compiled.returning
        
        ret = []
        for c in returning:
            if hasattr(c, 'name'):
                ret.append((c.name, c.type))
            else:
                ret.append((c.anon_label, c.type))
        return ret
    
    def _buffer_rows(self):
        return [tuple(self._returning_params["ret_%d" % i] for i, c in enumerate(self._returning_params))]

class Oracle_cx_oracle(OracleDialect):
    execution_ctx_cls = Oracle_cx_oracleExecutionContext
    statement_compiler = Oracle_cx_oracleCompiler
    driver = "cx_oracle"
    colspecs = colspecs
    
    def __init__(self, 
                auto_setinputsizes=True, 
                auto_convert_lobs=True, 
                threaded=True, 
                allow_twophase=True, 
                arraysize=50, **kwargs):
        OracleDialect.__init__(self, **kwargs)
        self.threaded = threaded
        self.arraysize = arraysize
        self.allow_twophase = allow_twophase
        self.supports_timestamp = self.dbapi is None or hasattr(self.dbapi, 'TIMESTAMP' )
        self.auto_setinputsizes = auto_setinputsizes
        self.auto_convert_lobs = auto_convert_lobs
        
        def vers(num):
            return tuple([int(x) for x in num.split('.')])

        if hasattr(self.dbapi, 'version'):
            cx_oracle_ver = vers(self.dbapi.version)
            self.supports_unicode_binds = cx_oracle_ver >= (5, 0)
            self._cx_oracle_native_nvarchar = cx_oracle_ver >= (5, 0)
            
        if self.dbapi is None or not self.auto_convert_lobs or not 'CLOB' in self.dbapi.__dict__:
            self.dbapi_type_map = {}
            self.ORACLE_BINARY_TYPES = []
        else:
            # only use this for LOB objects.  using it for strings, dates
            # etc. leads to a little too much magic, reflection doesn't know if it should
            # expect encoded strings or unicodes, etc.
            self.dbapi_type_map = {
                self.dbapi.CLOB: oracle.CLOB(),
                self.dbapi.NCLOB:oracle.NCLOB(),
                self.dbapi.BLOB: oracle.BLOB(),
                self.dbapi.BINARY: oracle.RAW(),
            }
            self.ORACLE_BINARY_TYPES = [getattr(self.dbapi, k) for k in ["BFILE", "CLOB", "NCLOB", "BLOB"] if hasattr(self.dbapi, k)]
    
    @classmethod
    def dbapi(cls):
        import cx_Oracle
        return cx_Oracle

    def create_connect_args(self, url):
        dialect_opts = dict(url.query)
        for opt in ('use_ansi', 'auto_setinputsizes', 'auto_convert_lobs',
                    'threaded', 'allow_twophase'):
            if opt in dialect_opts:
                util.coerce_kw_type(dialect_opts, opt, bool)
                setattr(self, opt, dialect_opts[opt])

        if url.database:
            # if we have a database, then we have a remote host
            port = url.port
            if port:
                port = int(port)
            else:
                port = 1521
            dsn = self.dbapi.makedsn(url.host, port, url.database)
        else:
            # we have a local tnsname
            dsn = url.host

        opts = dict(
            user=url.username,
            password=url.password,
            dsn=dsn,
            threaded=self.threaded,
            twophase=self.allow_twophase,
            )
        if 'mode' in url.query:
            opts['mode'] = url.query['mode']
            if isinstance(opts['mode'], basestring):
                mode = opts['mode'].upper()
                if mode == 'SYSDBA':
                    opts['mode'] = self.dbapi.SYSDBA
                elif mode == 'SYSOPER':
                    opts['mode'] = self.dbapi.SYSOPER
                else:
                    util.coerce_kw_type(opts, 'mode', int)
        # Can't set 'handle' or 'pool' via URL query args, use connect_args

        return ([], opts)

    def _get_server_version_info(self, connection):
        return tuple(int(x) for x in connection.connection.version.split('.'))

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.InterfaceError):
            return "not connected" in str(e)
        else:
            return "ORA-03114" in str(e) or "ORA-03113" in str(e)

    def create_xid(self):
        """create a two-phase transaction ID.

        this id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  its format is unspecified."""

        id = random.randint(0, 2 ** 128)
        return (0x1234, "%032x" % id, "%032x" % 9)

    def do_begin_twophase(self, connection, xid):
        connection.connection.begin(*xid)

    def do_prepare_twophase(self, connection, xid):
        connection.connection.prepare()

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        pass

dialect = Oracle_cx_oracle
