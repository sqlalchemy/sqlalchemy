# oracle/cx_oracle.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the Oracle database via the cx_oracle driver.

Driver
------

The Oracle dialect uses the cx_oracle driver, available at 
http://cx-oracle.sourceforge.net/ .   The dialect has several behaviors 
which are specifically tailored towards compatibility with this module.
Version 5.0 or greater is **strongly** recommended, as SQLAlchemy makes
extensive use of the cx_oracle output converters for numeric and 
string conversions.

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

cx_oracle 5 fully supports Python unicode objects.   SQLAlchemy will pass
all unicode strings directly to cx_oracle, and additionally uses an output
handler so that all string based result values are returned as unicode as well.
Generally, the ``NLS_LANG`` environment variable determines the nature
of the encoding to be used.

Note that this behavior is disabled when Oracle 8 is detected, as it has been 
observed that issues remain when passing Python unicodes to cx_oracle with Oracle 8.

LOB Objects
-----------

cx_oracle returns oracle LOBs using the cx_oracle.LOB object.  SQLAlchemy converts
these to strings so that the interface of the Binary type is consistent with that of
other backends, and so that the linkage to a live cursor is not needed in scenarios
like result.fetchmany() and result.fetchall().   This means that by default, LOB
objects are fully fetched unconditionally by SQLAlchemy, and the linkage to a live
cursor is broken.

To disable this processing, pass ``auto_convert_lobs=False`` to :func:`create_engine()`.

Two Phase Transaction Support
-----------------------------

Two Phase transactions are implemented using XA transactions.  Success has been reported 
with this feature but it should be regarded as experimental.

Precision Numerics
------------------

The SQLAlchemy dialect goes through a lot of steps to ensure
that decimal numbers are sent and received with full accuracy.
An "outputtypehandler" callable is associated with each
cx_oracle connection object which detects numeric types and
receives them as string values, instead of receiving a Python
``float`` directly, which is then passed to the Python
``Decimal`` constructor.  The :class:`.Numeric` and
:class:`.Float` types under the cx_oracle dialect are aware of
this behavior, and will coerce the ``Decimal`` to ``float`` if
the ``asdecimal`` flag is ``False`` (default on :class:`.Float`,
optional on :class:`.Numeric`).

Because the handler coerces to ``Decimal`` in all cases first,
the feature can detract significantly from performance.
If precision numerics aren't required, the decimal handling
can be disabled by passing the flag ``coerce_to_decimal=False``
to :func:`.create_engine`::

    engine = create_engine("oracle+cx_oracle://dsn", 
                        coerce_to_decimal=False)

.. versionadded:: 0.7.6
    Add the ``coerce_to_decimal`` flag.

Another alternative to performance is to use the 
`cdecimal <http://pypi.python.org/pypi/cdecimal/>`_ library; 
see :class:`.Numeric` for additional notes.

The handler attempts to use the "precision" and "scale"
attributes of the result set column to best determine if
subsequent incoming values should be received as ``Decimal`` as
opposed to int (in which case no processing is added). There are
several scenarios where OCI_ does not provide unambiguous data
as to the numeric type, including some situations where
individual rows may return a combination of floating point and
integer values. Certain values for "precision" and "scale" have
been observed to determine this scenario.  When it occurs, the
outputtypehandler receives as string and then passes off to a
processing function which detects, for each returned value, if a
decimal point is present, and if so converts to ``Decimal``,
otherwise to int.  The intention is that simple int-based
statements like "SELECT my_seq.nextval() FROM DUAL" continue to
return ints and not ``Decimal`` objects, and that any kind of
floating point value is received as a string so that there is no
floating point loss of precision.

The "decimal point is present" logic itself is also sensitive to
locale.  Under OCI_, this is controlled by the NLS_LANG
environment variable. Upon first connection, the dialect runs a
test to determine the current "decimal" character, which can be
a comma "," for european locales. From that point forward the
outputtypehandler uses that character to represent a decimal
point. Note that cx_oracle 5.0.3 or greater is required
when dealing with numerics with locale settings that don't use
a period "." as the decimal character.

.. versionchanged:: 0.6.6
    The outputtypehandler uses a comma "," character to represent
    a decimal point.

.. _OCI: http://www.oracle.com/technetwork/database/features/oci/index.html

"""

from sqlalchemy.dialects.oracle.base import OracleCompiler, OracleDialect, \
                                        RESERVED_WORDS, OracleExecutionContext
from sqlalchemy.dialects.oracle import base as oracle
from sqlalchemy.engine import base
from sqlalchemy import types as sqltypes, util, exc, processors
from datetime import datetime
import random
import collections
from sqlalchemy.util.compat import decimal
import re

class _OracleNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        # cx_oracle accepts Decimal objects and floats
        return None

    def result_processor(self, dialect, coltype):
        # we apply a cx_oracle type handler to all connections
        # that converts floating point strings to Decimal().
        # However, in some subquery situations, Oracle doesn't 
        # give us enough information to determine int or Decimal.
        # It could even be int/Decimal differently on each row,
        # regardless of the scale given for the originating type.
        # So we still need an old school isinstance() handler
        # here for decimals.

        if dialect.supports_native_decimal:
            if self.asdecimal:
                if self.scale is None:
                    fstring = "%.10f"
                else:
                    fstring = "%%.%df" % self.scale
                def to_decimal(value):
                    if value is None:
                        return None
                    elif isinstance(value, decimal.Decimal):
                        return value
                    else:
                        return decimal.Decimal(fstring % value)
                return to_decimal
            else:
                if self.precision is None and self.scale is None:
                    return processors.to_float
                elif not getattr(self, '_is_oracle_number', False) \
                    and self.scale is not None:
                    return processors.to_float
                else:
                    return None
        else:
            # cx_oracle 4 behavior, will assume 
            # floats
            return super(_OracleNumeric, self).\
                            result_processor(dialect, coltype)

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

        def process(value):
            if value is not None:
                return value.read()
            else:
                return value
        return process

class _NativeUnicodeMixin(object):
    # Py3K
    #pass
    # Py2K
    def bind_processor(self, dialect):
        if dialect._cx_oracle_with_unicode:
            def process(value):
                if value is None:
                    return value
                else:
                    return unicode(value)
            return process
        else:
            return super(_NativeUnicodeMixin, self).bind_processor(dialect)
    # end Py2K

    # we apply a connection output handler that returns
    # unicode in all cases, so the "native_unicode" flag 
    # will be set for the default String.result_processor.

class _OracleChar(_NativeUnicodeMixin, sqltypes.CHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.FIXED_CHAR

class _OracleNVarChar(_NativeUnicodeMixin, sqltypes.NVARCHAR):
    def get_dbapi_type(self, dbapi):
        return getattr(dbapi, 'UNICODE', dbapi.STRING)

class _OracleText(_LOBMixin, sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB

class _OracleString(_NativeUnicodeMixin, sqltypes.String):
    pass

class _OracleUnicodeText(_LOBMixin, _NativeUnicodeMixin, sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCLOB

    def result_processor(self, dialect, coltype):
        lob_processor = _LOBMixin.result_processor(self, dialect, coltype)
        if lob_processor is None:
            return None

        string_processor = sqltypes.UnicodeText.result_processor(self, dialect, coltype)

        if string_processor is None:
            return lob_processor
        else:
            def process(value):
                return string_processor(lob_processor(value))
            return process

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

class _OracleRowid(oracle.ROWID):
    def get_dbapi_type(self, dbapi):
        return dbapi.ROWID

class OracleCompiler_cx_oracle(OracleCompiler):
    def bindparam_string(self, name):
        if self.preparer._bindparam_requires_quotes(name):
            quoted_name = '"%s"' % name
            self._quoted_bind_names[name] = quoted_name
            return OracleCompiler.bindparam_string(self, quoted_name)
        else:
            return OracleCompiler.bindparam_string(self, name)


class OracleExecutionContext_cx_oracle(OracleExecutionContext):

    def pre_exec(self):
        quoted_bind_names = \
            getattr(self.compiled, '_quoted_bind_names', None)
        if quoted_bind_names:
            if not self.dialect.supports_unicode_statements:
                # if DBAPI doesn't accept unicode statements, 
                # keys in self.parameters would have been encoded
                # here.  so convert names in quoted_bind_names
                # to encoded as well.
                quoted_bind_names = \
                                dict(
                                    (fromname.encode(self.dialect.encoding), 
                                    toname.encode(self.dialect.encoding)) 
                                    for fromname, toname in 
                                    quoted_bind_names.items()
                                )
            for param in self.parameters:
                for fromname, toname in quoted_bind_names.items():
                    param[toname] = param[fromname]
                    del param[fromname]

        if self.dialect.auto_setinputsizes:
            # cx_oracle really has issues when you setinputsizes 
            # on String, including that outparams/RETURNING
            # breaks for varchars
            self.set_input_sizes(quoted_bind_names, 
                                 exclude_types=self.dialect._cx_oracle_string_types
                                )

        # if a single execute, check for outparams
        if len(self.compiled_parameters) == 1:
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    dbtype = bindparam.type.dialect_impl(self.dialect).\
                                    get_dbapi_type(self.dialect.dbapi)
                    if not hasattr(self, 'out_parameters'):
                        self.out_parameters = {}
                    if dbtype is None:
                        raise exc.InvalidRequestError("Cannot create out parameter for parameter "
                                                        "%r - it's type %r is not supported by"
                                                        " cx_oracle" %
                                                        (name, bindparam.type)
                                                        )
                    name = self.compiled.bind_names[bindparam]
                    self.out_parameters[name] = self.cursor.var(dbtype)
                    self.parameters[0][quoted_bind_names.get(name, name)] = \
                                                        self.out_parameters[name]

    def create_cursor(self):
        c = self._dbapi_connection.cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize

        return c

    def get_result_proxy(self):
        if hasattr(self, 'out_parameters') and self.compiled.returning:
            returning_params = dict(
                                    (k, v.getvalue()) 
                                    for k, v in self.out_parameters.items()
                                )
            return ReturningResultProxy(self, returning_params)

        result = None
        if self.cursor.description is not None:
            for column in self.cursor.description:
                type_code = column[1]
                if type_code in self.dialect._cx_oracle_binary_types:
                    result = base.BufferedColumnResultProxy(self)

        if result is None:
            result = base.ResultProxy(self)

        if hasattr(self, 'out_parameters'):
            if self.compiled_parameters is not None and \
                    len(self.compiled_parameters) == 1:
                result.out_parameters = out_parameters = {}

                for bind, name in self.compiled.bind_names.items():
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

class OracleExecutionContext_cx_oracle_with_unicode(OracleExecutionContext_cx_oracle):
    """Support WITH_UNICODE in Python 2.xx.

    WITH_UNICODE allows cx_Oracle's Python 3 unicode handling 
    behavior under Python 2.x. This mode in some cases disallows 
    and in other cases silently passes corrupted data when 
    non-Python-unicode strings (a.k.a. plain old Python strings) 
    are passed as arguments to connect(), the statement sent to execute(), 
    or any of the bind parameter keys or values sent to execute().
    This optional context therefore ensures that all statements are 
    passed as Python unicode objects.

    """
    def __init__(self, *arg, **kw):
        OracleExecutionContext_cx_oracle.__init__(self, *arg, **kw)
        self.statement = unicode(self.statement)

    def _execute_scalar(self, stmt):
        return super(OracleExecutionContext_cx_oracle_with_unicode, self).\
                            _execute_scalar(unicode(stmt))

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
        return collections.deque([tuple(self._returning_params["ret_%d" % i] 
                    for i, c in enumerate(self._returning_params))])

class OracleDialect_cx_oracle(OracleDialect):
    execution_ctx_cls = OracleExecutionContext_cx_oracle
    statement_compiler = OracleCompiler_cx_oracle

    driver = "cx_oracle"

    colspecs = colspecs = {
        sqltypes.Numeric: _OracleNumeric,
        sqltypes.Date : _OracleDate, # generic type, assume datetime.date is desired
        oracle.DATE: oracle.DATE,  # non generic type - passthru
        sqltypes.LargeBinary : _OracleBinary,
        sqltypes.Boolean : oracle._OracleBoolean,
        sqltypes.Interval : _OracleInterval,
        oracle.INTERVAL : _OracleInterval,
        sqltypes.Text : _OracleText,
        sqltypes.String : _OracleString,
        sqltypes.UnicodeText : _OracleUnicodeText,
        sqltypes.CHAR : _OracleChar,
        sqltypes.Integer : _OracleInteger,  # this is only needed for OUT parameters.
                                            # it would be nice if we could not use it otherwise.
        oracle.RAW: _OracleRaw,
        sqltypes.Unicode: _OracleNVarChar,
        sqltypes.NVARCHAR : _OracleNVarChar,
        oracle.ROWID: _OracleRowid,
    }


    execute_sequence_format = list

    def __init__(self, 
                auto_setinputsizes=True, 
                auto_convert_lobs=True, 
                threaded=True, 
                allow_twophase=True, 
                coerce_to_decimal=True,
                arraysize=50, **kwargs):
        OracleDialect.__init__(self, **kwargs)
        self.threaded = threaded
        self.arraysize = arraysize
        self.allow_twophase = allow_twophase
        self.supports_timestamp = self.dbapi is None or hasattr(self.dbapi, 'TIMESTAMP' )
        self.auto_setinputsizes = auto_setinputsizes
        self.auto_convert_lobs = auto_convert_lobs

        if hasattr(self.dbapi, 'version'):
            self.cx_oracle_ver = tuple([int(x) for x in self.dbapi.version.split('.')])
        else:
            self.cx_oracle_ver = (0, 0, 0)

        def types(*names):
            return set([
                        getattr(self.dbapi, name, None) for name in names
                    ]).difference([None])

        self._cx_oracle_string_types = types("STRING", "UNICODE", "NCLOB", "CLOB")
        self._cx_oracle_unicode_types = types("UNICODE", "NCLOB")
        self._cx_oracle_binary_types = types("BFILE", "CLOB", "NCLOB", "BLOB") 
        self.supports_unicode_binds = self.cx_oracle_ver >= (5, 0)

        self.supports_native_decimal = (
                                        self.cx_oracle_ver >= (5, 0) and 
                                        coerce_to_decimal
                                    )

        self._cx_oracle_native_nvarchar = self.cx_oracle_ver >= (5, 0)

        if self.cx_oracle_ver is None:
            # this occurs in tests with mock DBAPIs
            self._cx_oracle_string_types = set()
            self._cx_oracle_with_unicode = False
        elif self.cx_oracle_ver >= (5,) and not hasattr(self.dbapi, 'UNICODE'):
            # cx_Oracle WITH_UNICODE mode.  *only* python
            # unicode objects accepted for anything
            self.supports_unicode_statements = True
            self.supports_unicode_binds = True
            self._cx_oracle_with_unicode = True
            # Py2K
            # There's really no reason to run with WITH_UNICODE under Python 2.x.
            # Give the user a hint.
            util.warn("cx_Oracle is compiled under Python 2.xx using the "
                        "WITH_UNICODE flag.  Consider recompiling cx_Oracle without "
                        "this flag, which is in no way necessary for full support of Unicode. "
                        "Otherwise, all string-holding bind parameters must "
                        "be explicitly typed using SQLAlchemy's String type or one of its subtypes,"
                        "or otherwise be passed as Python unicode.  Plain Python strings "
                        "passed as bind parameters will be silently corrupted by cx_Oracle."
                        )
            self.execution_ctx_cls = OracleExecutionContext_cx_oracle_with_unicode
            # end Py2K
        else:
            self._cx_oracle_with_unicode = False

        if self.cx_oracle_ver is None or \
                    not self.auto_convert_lobs or \
                    not hasattr(self.dbapi, 'CLOB'):
            self.dbapi_type_map = {}
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
    @classmethod
    def dbapi(cls):
        import cx_Oracle
        return cx_Oracle

    def initialize(self, connection):
        super(OracleDialect_cx_oracle, self).initialize(connection)
        if self._is_oracle_8:
            self.supports_unicode_binds = False
        self._detect_decimal_char(connection)

    def _detect_decimal_char(self, connection):
        """detect if the decimal separator character is not '.', as 
        is the case with european locale settings for NLS_LANG.

        cx_oracle itself uses similar logic when it formats Python
        Decimal objects to strings on the bind side (as of 5.0.3), 
        as Oracle sends/receives string numerics only in the 
        current locale.

        """
        if self.cx_oracle_ver < (5,):
            # no output type handlers before version 5
            return

        cx_Oracle = self.dbapi
        conn = connection.connection

        # override the output_type_handler that's 
        # on the cx_oracle connection with a plain 
        # one on the cursor

        def output_type_handler(cursor, name, defaultType, 
                                size, precision, scale):
            return cursor.var(
                        cx_Oracle.STRING, 
                        255, arraysize=cursor.arraysize)

        cursor = conn.cursor()
        cursor.outputtypehandler = output_type_handler
        cursor.execute("SELECT 0.1 FROM DUAL")
        val = cursor.fetchone()[0]
        cursor.close()
        char = re.match(r"([\.,])", val).group(1)
        if char != '.':
            _detect_decimal = self._detect_decimal
            self._detect_decimal = \
                lambda value: _detect_decimal(value.replace(char, '.'))
            self._to_decimal = \
                lambda value: decimal.Decimal(value.replace(char, '.'))

    def _detect_decimal(self, value):
        if "." in value:
            return decimal.Decimal(value)
        else:
            return int(value)

    _to_decimal = decimal.Decimal

    def on_connect(self):
        if self.cx_oracle_ver < (5,):
            # no output type handlers before version 5
            return

        cx_Oracle = self.dbapi
        def output_type_handler(cursor, name, defaultType, 
                                    size, precision, scale):
            # convert all NUMBER with precision + positive scale to Decimal
            # this almost allows "native decimal" mode.
            if self.supports_native_decimal and \
                    defaultType == cx_Oracle.NUMBER and \
                    precision and scale > 0:
                return cursor.var(
                            cx_Oracle.STRING, 
                            255, 
                            outconverter=self._to_decimal, 
                            arraysize=cursor.arraysize)
            # if NUMBER with zero precision and 0 or neg scale, this appears
            # to indicate "ambiguous".  Use a slower converter that will 
            # make a decision based on each value received - the type 
            # may change from row to row (!).   This kills
            # off "native decimal" mode, handlers still needed.
            elif self.supports_native_decimal and \
                    defaultType == cx_Oracle.NUMBER \
                    and not precision and scale <= 0:
                return cursor.var(
                            cx_Oracle.STRING, 
                            255, 
                            outconverter=self._detect_decimal, 
                            arraysize=cursor.arraysize)
            # allow all strings to come back natively as Unicode
            elif defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
                return cursor.var(unicode, size, cursor.arraysize)

        def on_connect(conn):
            conn.outputtypehandler = output_type_handler

        return on_connect

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

        # Py2K
        if self._cx_oracle_with_unicode:
            for k, v in opts.items():
                if isinstance(v, str):
                    opts[k] = unicode(v)
        else:
            for k, v in opts.items():
                if isinstance(v, unicode):
                    opts[k] = str(v)
        # end Py2K

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
        return ([], opts)

    def _get_server_version_info(self, connection):
        return tuple(
                        int(x) 
                        for x in connection.connection.version.split('.')
                    )

    def is_disconnect(self, e, connection, cursor):
        error, = e.args
        if isinstance(e, self.dbapi.InterfaceError):
            return "not connected" in str(e)
        elif hasattr(error, 'code'):
            # ORA-00028: your session has been killed
            # ORA-03114: not connected to ORACLE
            # ORA-03113: end-of-file on communication channel
            # ORA-03135: connection lost contact
            # ORA-01033: ORACLE initialization or shutdown in progress
            # TODO: Others ?
            return error.code in (28, 3114, 3113, 3135, 1033)
        else:
            return False

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

dialect = OracleDialect_cx_oracle
