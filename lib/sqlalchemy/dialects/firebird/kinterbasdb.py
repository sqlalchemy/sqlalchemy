# kinterbasdb.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
The most common way to connect to a Firebird engine is implemented by
kinterbasdb__, currently maintained__ directly by the Firebird people.

The connection URL is of the form
``firebird[+kinterbasdb]://user:password@host:port/path/to/db[?key=value&key=value...]``.

Kinterbasedb backend specific keyword arguments are:

* type_conv - select the kind of mapping done on the types: by default  
  SQLAlchemy uses 200 with Unicode, datetime and decimal support (see
  details__).

* concurrency_level - set the backend policy with regards to threading 
  issues: by default SQLAlchemy uses policy 1 (see details__).

* enable_rowcount - True by default, setting this to False disables 
  the usage of "cursor.rowcount" with the 
  Kinterbasdb dialect, which SQLAlchemy ordinarily calls upon automatically
  after any UPDATE or DELETE statement.   When disabled, SQLAlchemy's 
  ResultProxy will return -1 for result.rowcount.   The rationale here is 
  that Kinterbasdb requires a second round trip to the database when 
  .rowcount is called -  since SQLA's resultproxy automatically closes 
  the cursor after a non-result-returning statement, rowcount must be 
  called, if at all, before the result object is returned.   Additionally,
  cursor.rowcount may not return correct results with older versions
  of Firebird, and setting this flag to False will also cause the 
  SQLAlchemy ORM to ignore its usage. The behavior can also be controlled on a
  per-execution basis using the `enable_rowcount` option with
  :meth:`execution_options()`::
  
      conn = engine.connect().execution_options(enable_rowcount=True)
      r = conn.execute(stmt)
      print r.rowcount
  
__ http://sourceforge.net/projects/kinterbasdb
__ http://firebirdsql.org/index.php?op=devel&sub=python
__ http://kinterbasdb.sourceforge.net/dist_docs/usage.html#adv_param_conv_dynamic_type_translation
__ http://kinterbasdb.sourceforge.net/dist_docs/usage.html#special_issue_concurrency
"""

from sqlalchemy.dialects.firebird.base import FBDialect, \
                                    FBCompiler, FBExecutionContext
from sqlalchemy import util, types as sqltypes

class _FBNumeric_kinterbasdb(sqltypes.Numeric):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return str(value)
            else:
                return value
        return process

class FBExecutionContext_kinterbasdb(FBExecutionContext):
    @property
    def rowcount(self):
        if self.execution_options.get('enable_rowcount', 
                                        self.dialect.enable_rowcount):
            return self.cursor.rowcount
        else:
            return -1
            
class FBDialect_kinterbasdb(FBDialect):
    driver = 'kinterbasdb'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    execution_ctx_cls = FBExecutionContext_kinterbasdb
    
    supports_native_decimal = True
    
    colspecs = util.update_copy(
        FBDialect.colspecs,
        {
            sqltypes.Numeric:_FBNumeric_kinterbasdb
        }
        
    )
    
    def __init__(self, type_conv=200, concurrency_level=1,
                            enable_rowcount=True, **kwargs):
        super(FBDialect_kinterbasdb, self).__init__(**kwargs)
        self.enable_rowcount = enable_rowcount
        self.type_conv = type_conv
        self.concurrency_level = concurrency_level
        if enable_rowcount:
            self.supports_sane_rowcount = True
        
    @classmethod
    def dbapi(cls):
        k = __import__('kinterbasdb')
        return k

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if opts.get('port'):
            opts['host'] = "%s/%s" % (opts['host'], opts['port'])
            del opts['port']
        opts.update(url.query)
        
        util.coerce_kw_type(opts, 'type_conv', int)
        
        type_conv = opts.pop('type_conv', self.type_conv)
        concurrency_level = opts.pop('concurrency_level',
                                    self.concurrency_level)
        
        if self.dbapi is not None:
            initialized = getattr(self.dbapi, 'initialized', None)
            if initialized is None:
                # CVS rev 1.96 changed the name of the attribute:
                # http://kinterbasdb.cvs.sourceforge.net/viewvc/kinterbasdb/Kinterbasdb-3.0/__init__.py?r1=1.95&r2=1.96
                initialized = getattr(self.dbapi, '_initialized', False)
            if not initialized:
                self.dbapi.init(type_conv=type_conv,
                                    concurrency_level=concurrency_level)
        return ([], opts)

    def _get_server_version_info(self, connection):
        """Get the version of the Firebird server used by a connection.

        Returns a tuple of (`major`, `minor`, `build`), three integers
        representing the version of the attached server.
        """

        # This is the simpler approach (the other uses the services api),
        # that for backward compatibility reasons returns a string like
        #   LI-V6.3.3.12981 Firebird 2.0
        # where the first version is a fake one resembling the old
        # Interbase signature. This is more than enough for our purposes,
        # as this is mainly (only?) used by the testsuite.

        from re import match

        fbconn = connection.connection
        version = fbconn.server_version
        m = match('\w+-V(\d+)\.(\d+)\.(\d+)\.(\d+) \w+ (\d+)\.(\d+)', version)
        if not m:
            raise AssertionError(
                    "Could not determine version from string '%s'" % version)
        return tuple([int(x) for x in m.group(5, 6, 4)])

    def is_disconnect(self, e):
        if isinstance(e, (self.dbapi.OperationalError,
                            self.dbapi.ProgrammingError)):
            msg = str(e)
            return ('Unable to complete network request to host' in msg or
                    'Invalid connection state' in msg or
                    'Invalid cursor state' in msg or 
                    'connection shutdown' in msg)
        else:
            return False

dialect = FBDialect_kinterbasdb
