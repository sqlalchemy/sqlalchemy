# oracle/cx_oracle.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: oracle+cx_oracle
    :name: cx-Oracle
    :dbapi: cx_oracle
    :connectstring: oracle+cx_oracle://user:pass@host:port/dbname\
[?key=value&key=value...]
    :url: http://cx-oracle.sourceforge.net/

Additional Connect Arguments
----------------------------

When connecting with ``dbname`` present, the host, port, and dbname tokens are
converted to a TNS name using
the cx_oracle ``makedsn()`` function.  Otherwise, the host token is taken
directly as a TNS name.

Additional arguments which may be specified either as query string arguments
on the URL, or as keyword arguments to :func:`.create_engine()` are:

* ``allow_twophase`` - enable two-phase transactions.  This argument is
  **deprecated** as of the cx_Oracle 5.x series, two phase transactions are
  not supported under cx_Oracle and as of cx_Oracle 6.0b1 this feature is
  removed entirely.

* ``arraysize`` - set the cx_oracle.arraysize value on cursors, defaulted
  to 50.  This setting is significant with cx_Oracle as the contents of LOB
  objects are only readable within a "live" row (e.g. within a batch of
  50 rows).

* ``auto_convert_lobs`` - defaults to True; See :ref:`cx_oracle_lob`.

* ``auto_setinputsizes`` - the cx_oracle.setinputsizes() call is issued for
  all bind parameters.  This is required for LOB datatypes but can be
  disabled to reduce overhead.  Defaults to ``True``.  Specific types
  can be excluded from this process using the ``exclude_setinputsizes``
  parameter.

* ``coerce_to_unicode`` - see :ref:`cx_oracle_unicode` for detail.

* ``coerce_to_decimal`` - see :ref:`cx_oracle_numeric` for detail.

* ``exclude_setinputsizes`` - a tuple or list of string DBAPI type names to
  be excluded from the "auto setinputsizes" feature.  The type names here
  must match DBAPI types that are found in the "cx_Oracle" module namespace,
  such as cx_Oracle.UNICODE, cx_Oracle.NCLOB, etc.   Defaults to
  ``(STRING, UNICODE)``.

  .. versionadded:: 0.8 specific DBAPI types can be excluded from the
     auto_setinputsizes feature via the exclude_setinputsizes attribute.

* ``mode`` - This is given the string value of SYSDBA or SYSOPER, or
  alternatively an integer value.  This value is only available as a URL query
  string argument.

* ``threaded`` - enable multithreaded access to cx_oracle connections.
  Defaults to ``True``.  Note that this is the opposite default of the
  cx_Oracle DBAPI itself.

* ``service_name`` - An option to use connection string (DSN) with
  ``SERVICE_NAME`` instead of ``SID``. It can't be passed when a ``database``
  part is given.
  E.g. ``oracle+cx_oracle://scott:tiger@host:1521/?service_name=hr``
  is a valid url. This value is only available as a URL query string argument.

  .. versionadded:: 1.0.0

.. _cx_oracle_unicode:

Unicode
-------

The cx_Oracle DBAPI as of version 5 fully supports unicode, and has the
ability to return string results as Python unicode objects natively.

When used in Python 3, cx_Oracle returns all strings as Python unicode objects
(that is, plain ``str`` in Python 3).  In Python 2, it will return as Python
unicode those column values that are of type ``NVARCHAR`` or ``NCLOB``.  For
column values that are of type ``VARCHAR`` or other non-unicode string types,
it will return values as Python strings (e.g. bytestrings).

The cx_Oracle SQLAlchemy dialect presents two different options for the use
case of returning ``VARCHAR`` column values as Python unicode objects under
Python 2:

* the cx_Oracle DBAPI has the ability to coerce all string results to Python
  unicode objects unconditionally using output type handlers.  This has
  the advantage that the unicode conversion is global to all statements
  at the cx_Oracle driver level, meaning it works with raw textual SQL
  statements that have no typing information associated.  However, this system
  has been observed to incur signfiicant performance overhead, not only
  because it takes effect for all string values unconditionally, but also
  because cx_Oracle under Python 2 seems to use a pure-Python function call in
  order to do the decode operation, which under cPython can orders of
  magnitude slower than doing it using C functions alone.

* SQLAlchemy has unicode-decoding services built in, and when using
  SQLAlchemy's C extensions, these functions do not use any Python function
  calls and are very fast.  The disadvantage to this approach is that the
  unicode conversion only takes effect for statements where the
  :class:`.Unicode` type or :class:`.String` type with
  ``convert_unicode=True`` is explicitly associated with the result column.
  This is the case for any ORM or Core query or SQL expression as well as for
  a :func:`.text` construct that specifies output column types, so in the vast
  majority of cases this is not an issue. However, when sending a completely
  raw string to :meth:`.Connection.execute`, this typing information isn't
  present, unless the string is handled within a :func:`.text` construct that
  adds typing information.

As of version 0.9.2 of SQLAlchemy, the default approach is to use SQLAlchemy's
typing system.  This keeps cx_Oracle's expensive Python 2 approach
disabled unless the user explicitly wants it.  Under Python 3, SQLAlchemy
detects that cx_Oracle is returning unicode objects natively and cx_Oracle's
system is used.

To re-enable cx_Oracle's output type handler under Python 2, the
``coerce_to_unicode=True`` flag (new in 0.9.4) can be passed to
:func:`.create_engine`::

    engine = create_engine("oracle+cx_oracle://dsn", coerce_to_unicode=True)

Alternatively, to run a pure string SQL statement and get ``VARCHAR`` results
as Python unicode under Python 2 without using cx_Oracle's native handlers,
the :func:`.text` feature can be used::

    from sqlalchemy import text, Unicode
    result = conn.execute(
        text("select username from user").columns(username=Unicode))

.. versionchanged:: 0.9.2 cx_Oracle's outputtypehandlers are no longer used
   for unicode results of non-unicode datatypes in Python 2, after they were
   identified as a major performance bottleneck.  SQLAlchemy's own unicode
   facilities are used instead.

.. versionadded:: 0.9.4 Added the ``coerce_to_unicode`` flag, to re-enable
   cx_Oracle's outputtypehandler and revert to pre-0.9.2 behavior.

.. _cx_oracle_returning:

RETURNING Support
-----------------

The cx_oracle DBAPI supports a limited subset of Oracle's already limited
RETURNING support.  Typically, results can only be guaranteed for at most one
column being returned; this is the typical case when SQLAlchemy uses RETURNING
to get just the value of a primary-key-associated sequence value.
Additional column expressions will cause problems in a non-determinative way,
due to cx_oracle's lack of support for the OCI_DATA_AT_EXEC API which is
required for more complex RETURNING scenarios.

For this reason, stability may be enhanced by disabling RETURNING support
completely; SQLAlchemy otherwise will use RETURNING to fetch newly
sequence-generated primary keys.  As illustrated in :ref:`oracle_returning`::

    engine = create_engine("oracle://scott:tiger@dsn",
                           implicit_returning=False)

.. seealso::

    http://docs.oracle.com/cd/B10501_01/appdev.920/a96584/oci05bnd.htm#420693
    - OCI documentation for RETURNING

    http://sourceforge.net/mailarchive/message.php?msg_id=31338136
    - cx_oracle developer commentary

.. _cx_oracle_lob:

LOB Objects
-----------

cx_oracle returns oracle LOBs using the cx_oracle.LOB object.  SQLAlchemy
converts these to strings so that the interface of the Binary type is
consistent with that of other backends, and so that the linkage to a live
cursor is not needed in scenarios like result.fetchmany() and
result.fetchall().   This means that by default, LOB objects are fully fetched
unconditionally by SQLAlchemy, and the linkage to a live cursor is broken.

To disable this processing, pass ``auto_convert_lobs=False`` to
:func:`.create_engine()`.

Two Phase Transaction Support
-----------------------------

Two phase transactions are **not supported** under cx_Oracle due to poor
driver support.   As of cx_Oracle 6.0b1, the interface for
two phase transactions has been changed to be more of a direct pass-through
to the underlying OCI layer with less automation.  The additional logic
to support this system is not implemented in SQLAlchemy.


.. _cx_oracle_numeric:

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

    engine = create_engine("oracle+cx_oracle://dsn", coerce_to_decimal=False)

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
a comma "," for European locales. From that point forward the
outputtypehandler uses that character to represent a decimal
point. Note that cx_oracle 5.0.3 or greater is required
when dealing with numerics with locale settings that don't use
a period "." as the decimal character.

.. versionchanged:: 0.6.6
    The outputtypehandler supports the case where the locale uses a
    comma "," character to represent a decimal point.

.. _OCI: http://www.oracle.com/technetwork/database/features/oci/index.html

"""

from __future__ import absolute_import

from .base import OracleCompiler, OracleDialect, OracleExecutionContext
from . import base as oracle
from ...engine import result as _result
from sqlalchemy import types as sqltypes, util, exc, processors
from sqlalchemy import util
import random
import collections
import decimal
import re
import time


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
                fstring = "%%.%df" % self._effective_decimal_return_scale

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
    if util.py2k:
        def bind_processor(self, dialect):
            if dialect._cx_oracle_with_unicode:
                def process(value):
                    if value is None:
                        return value
                    else:
                        return unicode(value)
                return process
            else:
                return super(
                    _NativeUnicodeMixin, self).bind_processor(dialect)

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


class _OracleLong(oracle.LONG):
    # a raw LONG is a text type, but does *not*
    # get the LobMixin with cx_oracle.

    def get_dbapi_type(self, dbapi):
        return dbapi.LONG_STRING


class _OracleString(_NativeUnicodeMixin, sqltypes.String):
    pass

class _OracleEnum(_NativeUnicodeMixin, sqltypes.Enum):
    def bind_processor(self, dialect):
        enum_proc = sqltypes.Enum.bind_processor(self, dialect)
        if util.py2k:
            unicode_proc = _NativeUnicodeMixin.bind_processor(self, dialect)
        else:
            unicode_proc = None

        def process(value):
            raw_str = enum_proc(value)
            if unicode_proc:
                raw_str = unicode_proc(raw_str)
            return raw_str
        return process


class _OracleUnicodeText(
        _LOBMixin, _NativeUnicodeMixin, sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCLOB

    def result_processor(self, dialect, coltype):
        lob_processor = _LOBMixin.result_processor(self, dialect, coltype)
        if lob_processor is None:
            return None

        string_processor = sqltypes.UnicodeText.result_processor(
            self, dialect, coltype)

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
    def bindparam_string(self, name, **kw):
        quote = getattr(name, 'quote', None)
        if quote is True or quote is not False and \
                self.preparer._bindparam_requires_quotes(name):
            quoted_name = '"%s"' % name
            self._quoted_bind_names[name] = quoted_name
            return OracleCompiler.bindparam_string(self, quoted_name, **kw)
        else:
            return OracleCompiler.bindparam_string(self, name, **kw)


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
            self.set_input_sizes(
                quoted_bind_names,
                exclude_types=self.dialect.exclude_setinputsizes
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
                        raise exc.InvalidRequestError(
                            "Cannot create out parameter for parameter "
                            "%r - its type %r is not supported by"
                            " cx_oracle" %
                            (bindparam.key, bindparam.type)
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
                    result = _result.BufferedColumnResultProxy(self)

        if result is None:
            result = _result.ResultProxy(self)

        if hasattr(self, 'out_parameters'):
            if self.compiled_parameters is not None and \
                    len(self.compiled_parameters) == 1:
                result.out_parameters = out_parameters = {}

                for bind, name in self.compiled.bind_names.items():
                    if name in self.out_parameters:
                        type = bind.type
                        impl_type = type.dialect_impl(self.dialect)
                        dbapi_type = impl_type.get_dbapi_type(
                            self.dialect.dbapi)
                        result_processor = impl_type.\
                            result_processor(self.dialect,
                                             dbapi_type)
                        if result_processor is not None:
                            out_parameters[name] = \
                                result_processor(
                                    self.out_parameters[name].getvalue())
                        else:
                            out_parameters[name] = self.out_parameters[
                                name].getvalue()
            else:
                result.out_parameters = dict(
                                            (k, v.getvalue())
                    for k, v in self.out_parameters.items()
                )

        return result


class OracleExecutionContext_cx_oracle_with_unicode(
        OracleExecutionContext_cx_oracle):
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
        self.statement = util.text_type(self.statement)

    def _execute_scalar(self, stmt, type_):
        return super(OracleExecutionContext_cx_oracle_with_unicode, self).\
            _execute_scalar(util.text_type(stmt), type_)


class ReturningResultProxy(_result.FullyBufferedResultProxy):
    """Result proxy which stuffs the _returning clause + outparams
    into the fetch."""

    def __init__(self, context, returning_params):
        self._returning_params = returning_params
        super(ReturningResultProxy, self).__init__(context)

    def _cursor_description(self):
        returning = self.context.compiled.returning
        return [
            ("ret_%d" % i, None)
            for i, col in enumerate(returning)
        ]

    def _buffer_rows(self):
        return collections.deque(
            [tuple(self._returning_params["ret_%d" % i]
                   for i, c in enumerate(self._returning_params))]
        )


class OracleDialect_cx_oracle(OracleDialect):
    execution_ctx_cls = OracleExecutionContext_cx_oracle
    statement_compiler = OracleCompiler_cx_oracle

    driver = "cx_oracle"

    colspecs = colspecs = {
        sqltypes.Numeric: _OracleNumeric,
        # generic type, assume datetime.date is desired
        sqltypes.Date: _OracleDate,
        sqltypes.LargeBinary: _OracleBinary,
        sqltypes.Boolean: oracle._OracleBoolean,
        sqltypes.Interval: _OracleInterval,
        oracle.INTERVAL: _OracleInterval,
        sqltypes.Text: _OracleText,
        sqltypes.String: _OracleString,
        sqltypes.UnicodeText: _OracleUnicodeText,
        sqltypes.CHAR: _OracleChar,
        sqltypes.Enum: _OracleEnum,

        # a raw LONG is a text type, but does *not*
        # get the LobMixin with cx_oracle.
        oracle.LONG: _OracleLong,

        # this is only needed for OUT parameters.
        # it would be nice if we could not use it otherwise.
        sqltypes.Integer: _OracleInteger,

        oracle.RAW: _OracleRaw,
        sqltypes.Unicode: _OracleNVarChar,
        sqltypes.NVARCHAR: _OracleNVarChar,
        oracle.ROWID: _OracleRowid,
    }

    execute_sequence_format = list

    def __init__(self,
                 auto_setinputsizes=True,
                 exclude_setinputsizes=("STRING", "UNICODE"),
                 auto_convert_lobs=True,
                 threaded=True,
                 allow_twophase=True,
                 coerce_to_decimal=True,
                 coerce_to_unicode=False,
                 arraysize=50, _retry_on_12516=False,
                 **kwargs):
        OracleDialect.__init__(self, **kwargs)
        self.threaded = threaded
        self.arraysize = arraysize
        self.allow_twophase = allow_twophase
        self.supports_timestamp = self.dbapi is None or \
            hasattr(self.dbapi, 'TIMESTAMP')
        self.auto_setinputsizes = auto_setinputsizes
        self.auto_convert_lobs = auto_convert_lobs
        self._retry_on_12516 = _retry_on_12516

        if hasattr(self.dbapi, 'version'):
            self.cx_oracle_ver = self._parse_cx_oracle_ver(self.dbapi.version)

        else:
            self.cx_oracle_ver = (0, 0, 0)

        def types(*names):
            return set(
                getattr(self.dbapi, name, None) for name in names
            ).difference([None])

        self.exclude_setinputsizes = types(*(exclude_setinputsizes or ()))
        self._cx_oracle_string_types = types("STRING", "UNICODE",
                                             "NCLOB", "CLOB")
        self._cx_oracle_unicode_types = types("UNICODE", "NCLOB")
        self._cx_oracle_binary_types = types("BFILE", "CLOB", "NCLOB", "BLOB")
        self.supports_unicode_binds = self.cx_oracle_ver >= (5, 0)

        self._enable_twophase = self.cx_oracle_ver < (6, 0)

        self.coerce_to_unicode = (
            self.cx_oracle_ver >= (5, 0) and
            coerce_to_unicode
        )

        self.supports_native_decimal = (
            self.cx_oracle_ver >= (5, 0) and
            coerce_to_decimal
        )

        self._cx_oracle_native_nvarchar = self.cx_oracle_ver >= (5, 0)

        if self.cx_oracle_ver is None:
            # this occurs in tests with mock DBAPIs
            self._cx_oracle_string_types = set()
            self._cx_oracle_with_unicode = False
        elif util.py3k or (
                self.cx_oracle_ver >= (5,) and
                self.cx_oracle_ver < (5, 1) and not
                hasattr(self.dbapi, 'UNICODE')
        ):
            # cx_Oracle WITH_UNICODE mode.  *only* python
            # unicode objects accepted for anything.  This
            # mode of operation is implicit for Python 3,
            # however under Python 2 it existed as a never-used build-time
            # option for cx_Oracle 5.0 only and was removed in 5.1.
            self.supports_unicode_statements = True
            self.supports_unicode_binds = True
            self._cx_oracle_with_unicode = True

            if util.py2k:
                # There's really no reason to run with WITH_UNICODE under
                # Python 2.x.  Give the user a hint.
                util.warn(
                    "cx_Oracle is compiled under Python 2.xx using the "
                    "WITH_UNICODE flag.  Consider recompiling cx_Oracle "
                    "without this flag, which is in no way necessary for "
                    "full support of Unicode and causes significant "
                    "performance issues.")
                self.execution_ctx_cls = \
                    OracleExecutionContext_cx_oracle_with_unicode
        else:
            self._cx_oracle_with_unicode = False

        if self.cx_oracle_ver is None or \
                not self.auto_convert_lobs or \
                not hasattr(self.dbapi, 'CLOB'):
            self.dbapi_type_map = {}
        else:
            # only use this for LOB objects.  using it for strings, dates
            # etc. leads to a little too much magic, reflection doesn't know
            # if it should expect encoded strings or unicodes, etc.
            self.dbapi_type_map = {
                self.dbapi.CLOB: oracle.CLOB(),
                self.dbapi.NCLOB: oracle.NCLOB(),
                self.dbapi.BLOB: oracle.BLOB(),
                self.dbapi.BINARY: oracle.RAW(),
            }

    def _parse_cx_oracle_ver(self, version):
        m = re.match(r'(\d+)\.(\d+)(?:\.(\d+))?', version)
        if m:
            return tuple(
                int(x)
                for x in m.group(1, 2, 3)
                if x is not None)
        else:
            return (0, 0, 0)

    @classmethod
    def dbapi(cls):
        import cx_Oracle
        return cx_Oracle

    def connect(self, *cargs, **cparams):
        if self._retry_on_12516:
            # emergency flag for the SQLAlchemy test suite, which has
            # decreased in stability since cx_oracle 5.3; generalized
            # "retry on connect" functionality is part of an upcoming
            # SQLAlchemy feature
            try:
                return self.dbapi.connect(*cargs, **cparams)
            except self.dbapi.DatabaseError as err:
                if "ORA-12516" in str(err):
                    time.sleep(2)
                    return self.dbapi.connect(*cargs, **cparams)
                else:
                    raise
        else:
            return super(OracleDialect_cx_oracle, self).connect(
                *cargs, **cparams)

    def initialize(self, connection):
        super(OracleDialect_cx_oracle, self).initialize(connection)
        if self._is_oracle_8:
            self.supports_unicode_binds = False
        self._detect_decimal_char(connection)

    def _detect_decimal_char(self, connection):
        """detect if the decimal separator character is not '.', as
        is the case with European locale settings for NLS_LANG.

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
            elif self.coerce_to_unicode and \
                    defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
                return cursor.var(util.text_type, size, cursor.arraysize)

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

        database = url.database
        service_name = dialect_opts.get('service_name', None)
        if database or service_name:
            # if we have a database, then we have a remote host
            port = url.port
            if port:
                port = int(port)
            else:
                port = 1521

            if database and service_name:
                raise exc.InvalidRequestError(
                    '"service_name" option shouldn\'t '
                    'be used with a "database" part of the url')
            if database:
                makedsn_kwargs = {'sid': database}
            if service_name:
                makedsn_kwargs = {'service_name': service_name}

            dsn = self.dbapi.makedsn(url.host, port, **makedsn_kwargs)
        else:
            # we have a local tnsname
            dsn = url.host

        opts = dict(
            threaded=self.threaded,
        )

        if self._enable_twophase:
            opts['twophase'] = self.allow_twophase

        if dsn is not None:
            opts['dsn'] = dsn
        if url.password is not None:
            opts['password'] = url.password
        if url.username is not None:
            opts['user'] = url.username

        if util.py2k:
            if self._cx_oracle_with_unicode:
                for k, v in opts.items():
                    if isinstance(v, str):
                        opts[k] = unicode(v)
            else:
                for k, v in opts.items():
                    if isinstance(v, unicode):
                        opts[k] = str(v)

        if 'mode' in url.query:
            opts['mode'] = url.query['mode']
            if isinstance(opts['mode'], util.string_types):
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
        if isinstance(e, (
                self.dbapi.InterfaceError, self.dbapi.DatabaseError)):
            return "not connected" in str(e)
        elif hasattr(error, 'code'):
            # ORA-00028: your session has been killed
            # ORA-03114: not connected to ORACLE
            # ORA-03113: end-of-file on communication channel
            # ORA-03135: connection lost contact
            # ORA-01033: ORACLE initialization or shutdown in progress
            # ORA-02396: exceeded maximum idle time, please connect again
            # TODO: Others ?
            return error.code in (28, 3114, 3113, 3135, 1033, 2396)
        else:
            return False

    def create_xid(self):
        """create a two-phase transaction ID.

        this id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  its format is unspecified.

        .. deprecated:: two-phase transaction support is no longer functional
           in SQLAlchemy's cx_Oracle dialect as of cx_Oracle 6.0b1

        """

        id = random.randint(0, 2 ** 128)
        return (0x1234, "%032x" % id, "%032x" % 9)

    def do_executemany(self, cursor, statement, parameters, context=None):
        if isinstance(parameters, tuple):
            parameters = list(parameters)
        cursor.executemany(statement, parameters)

    def do_begin_twophase(self, connection, xid):
        connection.connection.begin(*xid)

    def do_prepare_twophase(self, connection, xid):
        result = connection.connection.prepare()
        connection.info['cx_oracle_prepared'] = result

    def do_rollback_twophase(self, connection, xid, is_prepared=True,
                             recover=False):
        self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid, is_prepared=True,
                           recover=False):
        if not is_prepared:
            self.do_commit(connection.connection)
        else:
            oci_prepared = connection.info['cx_oracle_prepared']
            if oci_prepared:
                self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        connection.info.pop('cx_oracle_prepared', None)

dialect = OracleDialect_cx_oracle
