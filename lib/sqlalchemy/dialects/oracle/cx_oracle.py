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

* *auto_convert_lobs* - defaults to True, see the section on LOB objects.

* *auto_setinputsizes* - the cx_oracle.setinputsizes() call is issued for all bind parameters.
  This is required for LOB datatypes but can be disabled to reduce overhead.  Defaults
  to ``True``.

* *mode* - This is given the string value of SYSDBA or SYSOPER, or alternatively an
  integer value.  This value is only available as a URL query string argument.

* *threaded* - enable multithreaded access to cx_oracle connections.  Defaults
  to ``True``.  Note that this is the opposite default of cx_oracle itself.


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
"normalize" the results to look more like other DBAPIs.

The conversion of LOB objects by this dialect is unique in SQLAlchemy in that it takes place
for all statement executions, even plain string-based statements for which SQLA has no awareness
of result typing.  This is so that calls like fetchmany() and fetchall() can work in all cases
without raising cursor errors.  The conversion of LOB in all cases, as well as the "prefetch"
of LOB objects, can be disabled using auto_convert_lobs=False.  

Two Phase Transaction Support
-----------------------------

Two Phase transactions are implemented using XA transactions.  Success has been reported of them
working successfully but this should be regarded as an experimental feature.

"""

from sqlalchemy.dialects.oracle.base import OracleDialect, OracleText, OracleBinary, OracleRaw, RESERVED_WORDS
from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.engine import base
from sqlalchemy import types as sqltypes, util

class OracleNVarchar(sqltypes.NVARCHAR):
    """The SQL NVARCHAR type."""

    def __init__(self, **kw):
        kw['convert_unicode'] = False  # cx_oracle does this for us, for NVARCHAR2
        sqltypes.NVARCHAR.__init__(self, **kw)

class Oracle_cx_oracleExecutionContext(DefaultExecutionContext):
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
            c.cursor.arraysize = self.dialect.arraysize
        return c

    def get_result_proxy(self):
        if hasattr(self, 'out_parameters'):
            if self.compiled_parameters is not None and len(self.compiled_parameters) == 1:
                for bind, name in self.compiled.bind_names.iteritems():
                    if name in self.out_parameters:
                        type = bind.type
                        result_processor = type.dialect_impl(self.dialect).result_processor(self.dialect)
                        if result_processor is not None:
                            self.out_parameters[name] = result_processor(self.out_parameters[name].getvalue())
                        else:
                            self.out_parameters[name] = self.out_parameters[name].getvalue()
            else:
                for k in self.out_parameters:
                    self.out_parameters[k] = self.out_parameters[k].getvalue()

        if self.cursor.description is not None:
            for column in self.cursor.description:
                type_code = column[1]
                if type_code in self.dialect.ORACLE_BINARY_TYPES:
                    return base.BufferedColumnResultProxy(self)

        return base.ResultProxy(self)


class Oracle_cx_oracle(OracleDialect):
    execution_ctx_cls = Oracle_cx_oracleExecutionContext
    driver = "cx_oracle"
    
    colspecs = util.update_copy(
        OracleDialect.colspecs,
        {
            sqltypes.NVARCHAR:OracleNVarchar
        }
    )
    
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
        if self.dbapi is None or not self.auto_convert_lobs or not 'CLOB' in self.dbapi.__dict__:
            self.dbapi_type_map = {}
            self.ORACLE_BINARY_TYPES = []
        else:
            # only use this for LOB objects.  using it for strings, dates
            # etc. leads to a little too much magic, reflection doesn't know if it should
            # expect encoded strings or unicodes, etc.
            self.dbapi_type_map = {
                self.dbapi.CLOB: OracleText(),
                self.dbapi.BLOB: OracleBinary(),
                self.dbapi.BINARY: OracleRaw(),
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

    def do_release_savepoint(self, connection, name):
        # Oracle does not support RELEASE SAVEPOINT
        pass

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
