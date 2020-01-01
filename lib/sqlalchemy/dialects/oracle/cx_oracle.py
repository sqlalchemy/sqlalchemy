# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

r"""
.. dialect:: oracle+cx_oracle
    :name: cx-Oracle
    :dbapi: cx_oracle
    :connectstring: oracle+cx_oracle://user:pass@host:port/dbname[?key=value&key=value...]
    :url: https://oracle.github.io/python-cx_Oracle/

Additional Connect Arguments
----------------------------

When connecting with the ``dbname`` URL token present, the ``hostname``,
``port``, and ``dbname`` tokens are converted to a TNS name using the
``cx_Oracle.makedsn()`` function. The URL below::

    e = create_engine("oracle+cx_oracle://user:pass@hostname/dbname")

Will be used to create the DSN as follows::

    >>> import cx_Oracle
    >>> cx_Oracle.makedsn("hostname", 1521, sid="dbname")
    '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=hostname)(PORT=1521))(CONNECT_DATA=(SID=dbname)))'

The ``service_name`` parameter, also consumed by ``cx_Oracle.makedsn()``, may
be specified in the URL query string, e.g. ``?service_name=my_service``.

If ``dbname`` is not present, then the value of ``hostname`` in the
URL is used directly as the DSN passed to ``cx_Oracle.connect()``.

Additional connection arguments may be sent to the ``cx_Oracle.connect()``
function using the :paramref:`.create_engine.connect_args` dictionary.
Any cx_Oracle parameter value and/or constant may be passed, such as::

    import cx_Oracle
    e = create_engine(
        "oracle+cx_oracle://user:pass@dsn",
        connect_args={
            "mode": cx_Oracle.SYSDBA,
            "events": True
        }
    )

Alternatively, most cx_Oracle DBAPI arguments can also be encoded as strings
within the URL, which includes parameters such as ``mode``, ``purity``,
``events``, ``threaded``, and others::

    e = create_engine(
        "oracle+cx_oracle://user:pass@dsn?mode=SYSDBA&events=true")

.. versionchanged:: 1.3 the cx_oracle dialect now accepts all argument names
   within the URL string itself, to be passed to the cx_Oracle DBAPI.   As
   was the case earlier but not correctly documented, the
   :paramref:`.create_engine.connect_args` parameter also accepts all
   cx_Oracle DBAPI connect arguments.

There are also options that are consumed by the SQLAlchemy cx_oracle dialect
itself.  These options are always passed directly to :func:`.create_engine`,
such as::

    e = create_engine(
        "oracle+cx_oracle://user:pass@dsn", coerce_to_unicode=False)

The parameters accepted by the cx_oracle dialect are as follows:

* ``arraysize`` - set the cx_oracle.arraysize value on cursors, defaulted
  to 50.  This setting is significant with cx_Oracle as the contents of LOB
  objects are only readable within a "live" row (e.g. within a batch of
  50 rows).

* ``auto_convert_lobs`` - defaults to True; See :ref:`cx_oracle_lob`.

* ``coerce_to_unicode`` - see :ref:`cx_oracle_unicode` for detail.

* ``coerce_to_decimal`` - see :ref:`cx_oracle_numeric` for detail.

* ``encoding_errors`` - see :ref:`cx_oracle_unicode_encoding_errors` for detail.

.. _cx_oracle_unicode:

Unicode
-------

The cx_Oracle DBAPI as of version 5 fully supports Unicode, and has the
ability to return string results as Python Unicode objects natively.

Explicit Unicode support is available by using the :class:`.Unicode` datatype
with SQLAlchemy Core expression language, as well as the :class:`.UnicodeText`
datatype.  These types correspond to the  VARCHAR2 and CLOB Oracle datatypes by
default.   When using these datatypes with Unicode data, it is expected that
the Oracle database is configured with a Unicode-aware character set, as well
as that the ``NLS_LANG`` environment variable is set appropriately, so that
the VARCHAR2 and CLOB datatypes can accommodate the data.

In the case that the Oracle database is not configured with a Unicode character
set, the two options are to use the :class:`.oracle.NCHAR` and
:class:`.oracle.NCLOB` datatypes explicitly, or to pass the flag
``use_nchar_for_unicode=True`` to :func:`.create_engine`, which will cause the
SQLAlchemy dialect to use NCHAR/NCLOB for the :class:`.Unicode` /
:class:`.UnicodeText` datatypes instead of VARCHAR/CLOB.

.. versionchanged:: 1.3  The :class:`.Unicode` and :class:`.UnicodeText`
   datatypes now correspond to the ``VARCHAR2`` and ``CLOB`` Oracle datatypes
   unless the ``use_nchar_for_unicode=True`` is passed to the dialect
   when :func:`.create_engine` is called.

When result sets are fetched that include strings, under Python 3 the cx_Oracle
DBAPI returns all strings as Python Unicode objects, since Python 3 only has a
Unicode string type.  This occurs for data fetched from datatypes such as
VARCHAR2, CHAR, CLOB, NCHAR, NCLOB, etc.  In order to provide cross-
compatibility under Python 2, the SQLAlchemy cx_Oracle dialect will add
Unicode-conversion to string data under Python 2 as well.  Historically, this
made use of converters that were supplied by cx_Oracle but were found to be
non-performant; SQLAlchemy's own converters are used for the string to Unicode
conversion under Python 2.  To disable the Python 2 Unicode conversion for
VARCHAR2, CHAR, and CLOB, the flag ``coerce_to_unicode=False`` can be passed to
:func:`.create_engine`.

.. versionchanged:: 1.3 Unicode conversion is applied to all string values
   by default under python 2.  The ``coerce_to_unicode`` now defaults to True
   and can be set to False to disable the Unicode coercion of strings that are
   delivered as VARCHAR2/CHAR/CLOB data.


.. _cx_oracle_unicode_encoding_errors:

Encoding Errors
^^^^^^^^^^^^^^^

For the unusual case that data in the Oracle database is present with a broken
encoding, the dialect accepts a parameter ``encoding_errors`` which will be
passed to Unicode decoding functions in order to affect how decoding errors are
handled.  The value is ultimately consumed by the Python `decode
<https://docs.python.org/3/library/stdtypes.html#bytes.decode>`_ function, and
is passed both via cx_Oracle's ``encodingErrors`` parameter consumed by
``Cursor.var()``, as well as SQLAlchemy's own decoding function, as the
cx_Oracle dialect makes use of both under different circumstances.

.. versionadded:: 1.3.11


.. _cx_oracle_setinputsizes:

Fine grained control over cx_Oracle data binding performance with setinputsizes
-------------------------------------------------------------------------------

The cx_Oracle DBAPI has a deep and fundamental reliance upon the usage of the
DBAPI ``setinputsizes()`` call.   The purpose of this call is to establish the
datatypes that are bound to a SQL statement for Python values being passed as
parameters.  While virtually no other DBAPI assigns any use to the
``setinputsizes()`` call, the cx_Oracle DBAPI relies upon it heavily in its
interactions with the Oracle client interface, and in some scenarios it is  not
possible for SQLAlchemy to know exactly how data should be bound, as some
settings can cause profoundly different performance characteristics, while
altering the type coercion behavior at the same time.

Users of the cx_Oracle dialect are **strongly encouraged** to read through
cx_Oracle's list of built-in datatype symbols at
http://cx-oracle.readthedocs.io/en/latest/module.html#types.
Note that in some cases, significant performance degradation can occur when
using these types vs. not, in particular when specifying ``cx_Oracle.CLOB``.

On the SQLAlchemy side, the :meth:`.DialectEvents.do_setinputsizes` event can
be used both for runtime visibility (e.g. logging) of the setinputsizes step as
well as to fully control how ``setinputsizes()`` is used on a per-statement
basis.

.. versionadded:: 1.2.9 Added :meth:`.DialectEvents.setinputsizes`


Example 1 - logging all setinputsizes calls
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example illustrates how to log the intermediary values from a
SQLAlchemy perspective before they are converted to the raw ``setinputsizes()``
parameter dictionary.  The keys of the dictionary are :class:`.BindParameter`
objects which have a ``.key`` and a ``.type`` attribute::

    from sqlalchemy import create_engine, event

    engine = create_engine("oracle+cx_oracle://scott:tiger@host/xe")

    @event.listens_for(engine, "do_setinputsizes")
    def _log_setinputsizes(inputsizes, cursor, statement, parameters, context):
        for bindparam, dbapitype in inputsizes.items():
                log.info(
                    "Bound parameter name: %s  SQLAlchemy type: %r  "
                    "DBAPI object: %s",
                    bindparam.key, bindparam.type, dbapitype)

Example 2 - remove all bindings to CLOB
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``CLOB`` datatype in cx_Oracle incurs a significant performance overhead,
however is set by default for the ``Text`` type within the SQLAlchemy 1.2
series.   This setting can be modified as follows::

    from sqlalchemy import create_engine, event
    from cx_Oracle import CLOB

    engine = create_engine("oracle+cx_oracle://scott:tiger@host/xe")

    @event.listens_for(engine, "do_setinputsizes")
    def _remove_clob(inputsizes, cursor, statement, parameters, context):
        for bindparam, dbapitype in list(inputsizes.items()):
            if dbapitype is CLOB:
                del inputsizes[bindparam]

.. _cx_oracle_returning:

RETURNING Support
-----------------

The cx_Oracle dialect implements RETURNING using OUT parameters.
The dialect supports RETURNING fully, however cx_Oracle 6 is recommended
for complete support.

.. _cx_oracle_lob:

LOB Objects
-----------

cx_oracle returns oracle LOBs using the cx_oracle.LOB object.  SQLAlchemy
converts these to strings so that the interface of the Binary type is
consistent with that of other backends, which takes place within a cx_Oracle
outputtypehandler.

cx_Oracle prior to version 6 would require that LOB objects be read before
a new batch of rows would be read, as determined by the ``cursor.arraysize``.
As of the 6 series, this limitation has been lifted.  Nevertheless, because
SQLAlchemy pre-reads these LOBs up front, this issue is avoided in any case.

To disable the auto "read()" feature of the dialect, the flag
``auto_convert_lobs=False`` may be passed to :func:`.create_engine`.  Under
the cx_Oracle 5 series, having this flag turned off means there is the chance
of reading from a stale LOB object if not read as it is fetched.   With
cx_Oracle 6, this issue is resolved.

.. versionchanged:: 1.2  the LOB handling system has been greatly simplified
   internally to make use of outputtypehandlers, and no longer makes use
   of alternate "buffered" result set objects.

Two Phase Transactions Not Supported
-------------------------------------

Two phase transactions are **not supported** under cx_Oracle due to poor
driver support.   As of cx_Oracle 6.0b1, the interface for
two phase transactions has been changed to be more of a direct pass-through
to the underlying OCI layer with less automation.  The additional logic
to support this system is not implemented in SQLAlchemy.

.. _cx_oracle_numeric:

Precision Numerics
------------------

SQLAlchemy's numeric types can handle receiving and returning values as Python
``Decimal`` objects or float objects.  When a :class:`.Numeric` object, or a
subclass such as :class:`.Float`, :class:`.oracle.DOUBLE_PRECISION` etc. is in
use, the :paramref:`.Numeric.asdecimal` flag determines if values should be
coerced to ``Decimal`` upon return, or returned as float objects.   To make
matters more complicated under Oracle, Oracle's ``NUMBER`` type can also
represent integer values if the "scale" is zero, so the Oracle-specific
:class:`.oracle.NUMBER` type takes this into account as well.

The cx_Oracle dialect makes extensive use of connection- and cursor-level
"outputtypehandler" callables in order to coerce numeric values as requested.
These callables are specific to the specific flavor of :class:`.Numeric` in
use, as well as if no SQLAlchemy typing objects are present.   There are
observed scenarios where Oracle may sends incomplete or ambiguous information
about the numeric types being returned, such as a query where the numeric types
are buried under multiple levels of subquery.  The type handlers do their best
to make the right decision in all cases, deferring to the underlying cx_Oracle
DBAPI for all those cases where the driver can make the best decision.

When no typing objects are present, as when executing plain SQL strings, a
default "outputtypehandler" is present which will generally return numeric
values which specify precision and scale as Python ``Decimal`` objects.  To
disable this coercion to decimal for performance reasons, pass the flag
``coerce_to_decimal=False`` to :func:`.create_engine`::

    engine = create_engine("oracle+cx_oracle://dsn", coerce_to_decimal=False)

The ``coerce_to_decimal`` flag only impacts the results of plain string
SQL staements that are not otherwise associated with a :class:`.Numeric`
SQLAlchemy type (or a subclass of such).

.. versionchanged:: 1.2  The numeric handling system for cx_Oracle has been
   reworked to take advantage of newer cx_Oracle features as well
   as better integration of outputtypehandlers.

"""  # noqa

from __future__ import absolute_import

import collections
import decimal
import random
import re

from . import base as oracle
from .base import OracleCompiler
from .base import OracleDialect
from .base import OracleExecutionContext
from ... import exc
from ... import processors
from ... import types as sqltypes
from ... import util
from ...engine import result as _result
from ...util import compat


class _OracleInteger(sqltypes.Integer):
    def get_dbapi_type(self, dbapi):
        # see https://github.com/oracle/python-cx_Oracle/issues/
        # 208#issuecomment-409715955
        return int

    def _cx_oracle_var(self, dialect, cursor):
        cx_Oracle = dialect.dbapi
        return cursor.var(
            cx_Oracle.STRING, 255, arraysize=cursor.arraysize, outconverter=int
        )

    def _cx_oracle_outputtypehandler(self, dialect):
        def handler(cursor, name, default_type, size, precision, scale):
            return self._cx_oracle_var(dialect, cursor)

        return handler


class _OracleNumeric(sqltypes.Numeric):
    is_number = False

    def bind_processor(self, dialect):
        if self.scale == 0:
            return None
        elif self.asdecimal:
            processor = processors.to_decimal_processor_factory(
                decimal.Decimal, self._effective_decimal_return_scale
            )

            def process(value):
                if isinstance(value, (int, float)):
                    return processor(value)
                elif value is not None and value.is_infinite():
                    return float(value)
                else:
                    return value

            return process
        else:
            return processors.to_float

    def result_processor(self, dialect, coltype):
        return None

    def _cx_oracle_outputtypehandler(self, dialect):
        cx_Oracle = dialect.dbapi

        is_cx_oracle_6 = dialect._is_cx_oracle_6

        def handler(cursor, name, default_type, size, precision, scale):
            outconverter = None

            if precision:
                if self.asdecimal:
                    if default_type == cx_Oracle.NATIVE_FLOAT:
                        # receiving float and doing Decimal after the fact
                        # allows for float("inf") to be handled
                        type_ = default_type
                        outconverter = decimal.Decimal
                    elif is_cx_oracle_6:
                        type_ = decimal.Decimal
                    else:
                        type_ = cx_Oracle.STRING
                        outconverter = dialect._to_decimal
                else:
                    if self.is_number and scale == 0:
                        # integer. cx_Oracle is observed to handle the widest
                        # variety of ints when no directives are passed,
                        # from 5.2 to 7.0.  See [ticket:4457]
                        return None
                    else:
                        type_ = cx_Oracle.NATIVE_FLOAT

            else:
                if self.asdecimal:
                    if default_type == cx_Oracle.NATIVE_FLOAT:
                        type_ = default_type
                        outconverter = decimal.Decimal
                    elif is_cx_oracle_6:
                        type_ = decimal.Decimal
                    else:
                        type_ = cx_Oracle.STRING
                        outconverter = dialect._to_decimal
                else:
                    if self.is_number and scale == 0:
                        # integer. cx_Oracle is observed to handle the widest
                        # variety of ints when no directives are passed,
                        # from 5.2 to 7.0.  See [ticket:4457]
                        return None
                    else:
                        type_ = cx_Oracle.NATIVE_FLOAT

            return cursor.var(
                type_,
                255,
                arraysize=cursor.arraysize,
                outconverter=outconverter,
            )

        return handler


class _OracleBinaryFloat(_OracleNumeric):
    def get_dbapi_type(self, dbapi):
        return dbapi.NATIVE_FLOAT


class _OracleBINARY_FLOAT(_OracleBinaryFloat, oracle.BINARY_FLOAT):
    pass


class _OracleBINARY_DOUBLE(_OracleBinaryFloat, oracle.BINARY_DOUBLE):
    pass


class _OracleNUMBER(_OracleNumeric):
    is_number = True


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


# TODO: the names used across CHAR / VARCHAR / NCHAR / NVARCHAR
# here are inconsistent and not very good
class _OracleChar(sqltypes.CHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.FIXED_CHAR


class _OracleNChar(sqltypes.NCHAR):
    def get_dbapi_type(self, dbapi):
        return dbapi.FIXED_NCHAR


class _OracleUnicodeStringNCHAR(oracle.NVARCHAR2):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCHAR


class _OracleUnicodeStringCHAR(sqltypes.Unicode):
    def get_dbapi_type(self, dbapi):
        return None


class _OracleUnicodeTextNCLOB(oracle.NCLOB):
    def get_dbapi_type(self, dbapi):
        return dbapi.NCLOB


class _OracleUnicodeTextCLOB(sqltypes.UnicodeText):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB


class _OracleText(sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB


class _OracleLong(oracle.LONG):
    def get_dbapi_type(self, dbapi):
        return dbapi.LONG_STRING


class _OracleString(sqltypes.String):
    pass


class _OracleEnum(sqltypes.Enum):
    def bind_processor(self, dialect):
        enum_proc = sqltypes.Enum.bind_processor(self, dialect)

        def process(value):
            raw_str = enum_proc(value)
            return raw_str

        return process


class _OracleBinary(sqltypes.LargeBinary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if not dialect.auto_convert_lobs:
            return None
        else:
            return super(_OracleBinary, self).result_processor(
                dialect, coltype
            )


class _OracleInterval(oracle.INTERVAL):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTERVAL


class _OracleRaw(oracle.RAW):
    pass


class _OracleRowid(oracle.ROWID):
    def get_dbapi_type(self, dbapi):
        return dbapi.ROWID


class OracleCompiler_cx_oracle(OracleCompiler):
    _oracle_cx_sql_compiler = True

    def bindparam_string(self, name, **kw):
        quote = getattr(name, "quote", None)
        if (
            quote is True
            or quote is not False
            and self.preparer._bindparam_requires_quotes(name)
        ):
            if kw.get("expanding", False):
                raise exc.CompileError(
                    "Can't use expanding feature with parameter name "
                    "%r on Oracle; it requires quoting which is not supported "
                    "in this context." % name
                )
            quoted_name = '"%s"' % name
            self._quoted_bind_names[name] = quoted_name
            return OracleCompiler.bindparam_string(self, quoted_name, **kw)
        else:
            return OracleCompiler.bindparam_string(self, name, **kw)


class OracleExecutionContext_cx_oracle(OracleExecutionContext):
    out_parameters = None

    def _setup_quoted_bind_names(self):
        quoted_bind_names = self.compiled._quoted_bind_names
        if quoted_bind_names:
            for param in self.parameters:
                for fromname, toname in quoted_bind_names.items():
                    param[toname] = param[fromname]
                    del param[fromname]

    def _handle_out_parameters(self):
        # if a single execute, check for outparams
        if len(self.compiled_parameters) == 1:
            quoted_bind_names = self.compiled._quoted_bind_names
            for bindparam in self.compiled.binds.values():
                if bindparam.isoutparam:
                    name = self.compiled.bind_names[bindparam]
                    type_impl = bindparam.type.dialect_impl(self.dialect)
                    if hasattr(type_impl, "_cx_oracle_var"):
                        self.out_parameters[name] = type_impl._cx_oracle_var(
                            self.dialect, self.cursor
                        )
                    else:
                        dbtype = type_impl.get_dbapi_type(self.dialect.dbapi)
                        if dbtype is None:
                            raise exc.InvalidRequestError(
                                "Cannot create out parameter for parameter "
                                "%r - its type %r is not supported by"
                                " cx_oracle" % (bindparam.key, bindparam.type)
                            )
                        self.out_parameters[name] = self.cursor.var(dbtype)
                    self.parameters[0][
                        quoted_bind_names.get(name, name)
                    ] = self.out_parameters[name]

    def _generate_cursor_outputtype_handler(self):
        output_handlers = {}

        for (keyname, name, objects, type_) in self.compiled._result_columns:
            handler = type_._cached_custom_processor(
                self.dialect,
                "cx_oracle_outputtypehandler",
                self._get_cx_oracle_type_handler,
            )

            if handler:
                denormalized_name = self.dialect.denormalize_name(keyname)
                output_handlers[denormalized_name] = handler

        if output_handlers:
            default_handler = self._dbapi_connection.outputtypehandler

            def output_type_handler(
                cursor, name, default_type, size, precision, scale
            ):
                if name in output_handlers:
                    return output_handlers[name](
                        cursor, name, default_type, size, precision, scale
                    )
                else:
                    return default_handler(
                        cursor, name, default_type, size, precision, scale
                    )

            self.cursor.outputtypehandler = output_type_handler

    def _get_cx_oracle_type_handler(self, impl):
        if hasattr(impl, "_cx_oracle_outputtypehandler"):
            return impl._cx_oracle_outputtypehandler(self.dialect)
        else:
            return None

    def pre_exec(self):
        if not getattr(self.compiled, "_oracle_cx_sql_compiler", False):
            return

        self.out_parameters = {}

        if self.compiled._quoted_bind_names:
            self._setup_quoted_bind_names()

        self.set_input_sizes(
            self.compiled._quoted_bind_names,
            include_types=self.dialect._include_setinputsizes,
        )

        self._handle_out_parameters()

        self._generate_cursor_outputtype_handler()

    def create_cursor(self):
        c = self._dbapi_connection.cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize

        return c

    def get_result_proxy(self):
        if self.out_parameters and self.compiled.returning:
            returning_params = [
                self.dialect._returningval(self.out_parameters["ret_%d" % i])
                for i in range(len(self.out_parameters))
            ]
            return ReturningResultProxy(self, returning_params)

        result = _result.ResultProxy(self)

        if self.out_parameters:
            if (
                self.compiled_parameters is not None
                and len(self.compiled_parameters) == 1
            ):
                result.out_parameters = out_parameters = {}

                for bind, name in self.compiled.bind_names.items():
                    if name in self.out_parameters:
                        type_ = bind.type
                        impl_type = type_.dialect_impl(self.dialect)
                        dbapi_type = impl_type.get_dbapi_type(
                            self.dialect.dbapi
                        )
                        result_processor = impl_type.result_processor(
                            self.dialect, dbapi_type
                        )
                        if result_processor is not None:
                            out_parameters[name] = result_processor(
                                self.dialect._paramval(
                                    self.out_parameters[name]
                                )
                            )
                        else:
                            out_parameters[name] = self.dialect._paramval(
                                self.out_parameters[name]
                            )
            else:
                result.out_parameters = dict(
                    (k, self._dialect._paramval(v))
                    for k, v in self.out_parameters.items()
                )

        return result


class ReturningResultProxy(_result.FullyBufferedResultProxy):
    """Result proxy which stuffs the _returning clause + outparams
    into the fetch."""

    def __init__(self, context, returning_params):
        self._returning_params = returning_params
        super(ReturningResultProxy, self).__init__(context)

    def _cursor_description(self):
        returning = self.context.compiled.returning
        return [
            (getattr(col, "name", col.anon_label), None) for col in returning
        ]

    def _buffer_rows(self):
        return collections.deque([tuple(self._returning_params)])


class OracleDialect_cx_oracle(OracleDialect):
    execution_ctx_cls = OracleExecutionContext_cx_oracle
    statement_compiler = OracleCompiler_cx_oracle

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_unicode_statements = True
    supports_unicode_binds = True

    driver = "cx_oracle"

    colspecs = {
        sqltypes.Numeric: _OracleNumeric,
        sqltypes.Float: _OracleNumeric,
        oracle.BINARY_FLOAT: _OracleBINARY_FLOAT,
        oracle.BINARY_DOUBLE: _OracleBINARY_DOUBLE,
        sqltypes.Integer: _OracleInteger,
        oracle.NUMBER: _OracleNUMBER,
        sqltypes.Date: _OracleDate,
        sqltypes.LargeBinary: _OracleBinary,
        sqltypes.Boolean: oracle._OracleBoolean,
        sqltypes.Interval: _OracleInterval,
        oracle.INTERVAL: _OracleInterval,
        sqltypes.Text: _OracleText,
        sqltypes.String: _OracleString,
        sqltypes.UnicodeText: _OracleUnicodeTextCLOB,
        sqltypes.CHAR: _OracleChar,
        sqltypes.NCHAR: _OracleNChar,
        sqltypes.Enum: _OracleEnum,
        oracle.LONG: _OracleLong,
        oracle.RAW: _OracleRaw,
        sqltypes.Unicode: _OracleUnicodeStringCHAR,
        sqltypes.NVARCHAR: _OracleUnicodeStringNCHAR,
        oracle.NCLOB: _OracleUnicodeTextNCLOB,
        oracle.ROWID: _OracleRowid,
    }

    execute_sequence_format = list

    _cx_oracle_threaded = None

    @util.deprecated_params(
        threaded=(
            "1.3",
            "The 'threaded' parameter to the cx_oracle dialect "
            "is deprecated as a dialect-level argument, and will be removed "
            "in a future release.  As of version 1.3, it defaults to False "
            "rather than True.  The 'threaded' option can be passed to "
            "cx_Oracle directly in the URL query string passed to "
            ":func:`.create_engine`.",
        )
    )
    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_unicode=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        threaded=None,
        **kwargs
    ):

        OracleDialect.__init__(self, **kwargs)
        self.arraysize = arraysize
        self.encoding_errors = encoding_errors
        if threaded is not None:
            self._cx_oracle_threaded = threaded
        self.auto_convert_lobs = auto_convert_lobs
        self.coerce_to_unicode = coerce_to_unicode
        self.coerce_to_decimal = coerce_to_decimal
        if self._use_nchar_for_unicode:
            self.colspecs = self.colspecs.copy()
            self.colspecs[sqltypes.Unicode] = _OracleUnicodeStringNCHAR
            self.colspecs[sqltypes.UnicodeText] = _OracleUnicodeTextNCLOB

        cx_Oracle = self.dbapi

        if cx_Oracle is None:
            self._include_setinputsizes = {}
            self.cx_oracle_ver = (0, 0, 0)
        else:
            self.cx_oracle_ver = self._parse_cx_oracle_ver(cx_Oracle.version)
            if self.cx_oracle_ver < (5, 2) and self.cx_oracle_ver > (0, 0, 0):
                raise exc.InvalidRequestError(
                    "cx_Oracle version 5.2 and above are supported"
                )

            self._include_setinputsizes = {
                cx_Oracle.DATETIME,
                cx_Oracle.NCLOB,
                cx_Oracle.CLOB,
                cx_Oracle.LOB,
                cx_Oracle.NCHAR,
                cx_Oracle.FIXED_NCHAR,
                cx_Oracle.BLOB,
                cx_Oracle.FIXED_CHAR,
                cx_Oracle.TIMESTAMP,
                _OracleInteger,
                _OracleBINARY_FLOAT,
                _OracleBINARY_DOUBLE,
            }

            self._paramval = lambda value: value.getvalue()

            # https://github.com/oracle/python-cx_Oracle/issues/176#issuecomment-386821291
            # https://github.com/oracle/python-cx_Oracle/issues/224
            self._values_are_lists = self.cx_oracle_ver >= (6, 3)
            if self._values_are_lists:
                cx_Oracle.__future__.dml_ret_array_val = True

                def _returningval(value):
                    try:
                        return value.values[0][0]
                    except IndexError:
                        return None

                self._returningval = _returningval
            else:
                self._returningval = self._paramval

        self._is_cx_oracle_6 = self.cx_oracle_ver >= (6,)

    @property
    def _cursor_var_unicode_kwargs(self):
        if self.encoding_errors:
            if self.cx_oracle_ver >= (6, 4):
                return {"encodingErrors": self.encoding_errors}
            else:
                util.warn(
                    "cx_oracle version %r does not support encodingErrors"
                    % (self.cx_oracle_ver,)
                )

        return {}

    def _parse_cx_oracle_ver(self, version):
        m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", version)
        if m:
            return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)
        else:
            return (0, 0, 0)

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
        # we have the option to change this setting upon connect,
        # or just look at what it is upon connect and convert.
        # to minimize the chance of interference with changes to
        # NLS_TERRITORY or formatting behavior of the DB, we opt
        # to just look at it

        self._decimal_char = connection.scalar(
            "select value from nls_session_parameters "
            "where parameter = 'NLS_NUMERIC_CHARACTERS'"
        )[0]
        if self._decimal_char != ".":
            _detect_decimal = self._detect_decimal
            _to_decimal = self._to_decimal

            self._detect_decimal = lambda value: _detect_decimal(
                value.replace(self._decimal_char, ".")
            )
            self._to_decimal = lambda value: _to_decimal(
                value.replace(self._decimal_char, ".")
            )

    def _detect_decimal(self, value):
        if "." in value:
            return self._to_decimal(value)
        else:
            return int(value)

    _to_decimal = decimal.Decimal

    def _generate_connection_outputtype_handler(self):
        """establish the default outputtypehandler established at the
        connection level.

        """

        dialect = self
        cx_Oracle = dialect.dbapi

        number_handler = _OracleNUMBER(
            asdecimal=True
        )._cx_oracle_outputtypehandler(dialect)
        float_handler = _OracleNUMBER(
            asdecimal=False
        )._cx_oracle_outputtypehandler(dialect)

        def output_type_handler(
            cursor, name, default_type, size, precision, scale
        ):
            if default_type == cx_Oracle.NUMBER:
                if not dialect.coerce_to_decimal:
                    return None
                elif precision == 0 and scale in (0, -127):
                    # ambiguous type, this occurs when selecting
                    # numbers from deep subqueries
                    return cursor.var(
                        cx_Oracle.STRING,
                        255,
                        outconverter=dialect._detect_decimal,
                        arraysize=cursor.arraysize,
                    )
                elif precision and scale > 0:
                    return number_handler(
                        cursor, name, default_type, size, precision, scale
                    )
                else:
                    return float_handler(
                        cursor, name, default_type, size, precision, scale
                    )

            # allow all strings to come back natively as Unicode
            elif dialect.coerce_to_unicode and default_type in (
                cx_Oracle.STRING,
                cx_Oracle.FIXED_CHAR,
            ):
                if compat.py2k:
                    outconverter = processors.to_unicode_processor_factory(
                        dialect.encoding, errors=dialect.encoding_errors
                    )
                    return cursor.var(
                        cx_Oracle.STRING,
                        size,
                        cursor.arraysize,
                        outconverter=outconverter,
                    )
                else:
                    return cursor.var(
                        util.text_type,
                        size,
                        cursor.arraysize,
                        **dialect._cursor_var_unicode_kwargs
                    )

            elif dialect.auto_convert_lobs and default_type in (
                cx_Oracle.CLOB,
                cx_Oracle.NCLOB,
            ):
                if compat.py2k:
                    outconverter = processors.to_unicode_processor_factory(
                        dialect.encoding, errors=dialect.encoding_errors
                    )
                    return cursor.var(
                        default_type,
                        size,
                        cursor.arraysize,
                        outconverter=lambda value: outconverter(value.read()),
                    )
                else:
                    return cursor.var(
                        default_type,
                        size,
                        cursor.arraysize,
                        outconverter=lambda value: value.read(),
                        **dialect._cursor_var_unicode_kwargs
                    )

            elif dialect.auto_convert_lobs and default_type in (
                cx_Oracle.BLOB,
            ):
                return cursor.var(
                    default_type,
                    size,
                    cursor.arraysize,
                    outconverter=lambda value: value.read(),
                )

        return output_type_handler

    def on_connect(self):

        output_type_handler = self._generate_connection_outputtype_handler()

        def on_connect(conn):
            conn.outputtypehandler = output_type_handler

        return on_connect

    def create_connect_args(self, url):
        opts = dict(url.query)

        # deprecated in 1.3
        for opt in ("use_ansi", "auto_convert_lobs"):
            if opt in opts:
                util.warn_deprecated(
                    "cx_oracle dialect option %r should only be passed to "
                    "create_engine directly, not within the URL string" % opt
                )
                util.coerce_kw_type(opts, opt, bool)
                setattr(self, opt, opts.pop(opt))

        database = url.database
        service_name = opts.pop("service_name", None)
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
                    'be used with a "database" part of the url'
                )
            if database:
                makedsn_kwargs = {"sid": database}
            if service_name:
                makedsn_kwargs = {"service_name": service_name}

            dsn = self.dbapi.makedsn(url.host, port, **makedsn_kwargs)
        else:
            # we have a local tnsname
            dsn = url.host

        if dsn is not None:
            opts["dsn"] = dsn
        if url.password is not None:
            opts["password"] = url.password
        if url.username is not None:
            opts["user"] = url.username

        if self._cx_oracle_threaded is not None:
            opts.setdefault("threaded", self._cx_oracle_threaded)

        def convert_cx_oracle_constant(value):
            if isinstance(value, util.string_types):
                try:
                    int_val = int(value)
                except ValueError:
                    value = value.upper()
                    return getattr(self.dbapi, value)
                else:
                    return int_val
            else:
                return value

        util.coerce_kw_type(opts, "mode", convert_cx_oracle_constant)
        util.coerce_kw_type(opts, "threaded", bool)
        util.coerce_kw_type(opts, "events", bool)
        util.coerce_kw_type(opts, "purity", convert_cx_oracle_constant)

        return ([], opts)

    def _get_server_version_info(self, connection):
        return tuple(int(x) for x in connection.connection.version.split("."))

    def is_disconnect(self, e, connection, cursor):
        error, = e.args
        if isinstance(
            e, (self.dbapi.InterfaceError, self.dbapi.DatabaseError)
        ) and "not connected" in str(e):
            return True

        if hasattr(error, "code"):
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

    @util.deprecated(
        "1.2",
        "The create_xid() method of the cx_Oracle dialect is deprecated and "
        "will be removed in a future release.  "
        "Two-phase transaction support is no longer functional "
        "in SQLAlchemy's cx_Oracle dialect as of cx_Oracle 6.0b1, which no "
        "longer supports the API that SQLAlchemy relied upon.",
    )
    def create_xid(self):
        """create a two-phase transaction ID.

        this id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  its format is unspecified.

        """

        id_ = random.randint(0, 2 ** 128)
        return (0x1234, "%032x" % id_, "%032x" % 9)

    def do_executemany(self, cursor, statement, parameters, context=None):
        if isinstance(parameters, tuple):
            parameters = list(parameters)
        cursor.executemany(statement, parameters)

    def do_begin_twophase(self, connection, xid):
        connection.connection.begin(*xid)

    def do_prepare_twophase(self, connection, xid):
        result = connection.connection.prepare()
        connection.info["cx_oracle_prepared"] = result

    def do_rollback_twophase(
        self, connection, xid, is_prepared=True, recover=False
    ):
        self.do_rollback(connection.connection)

    def do_commit_twophase(
        self, connection, xid, is_prepared=True, recover=False
    ):
        if not is_prepared:
            self.do_commit(connection.connection)
        else:
            oci_prepared = connection.info["cx_oracle_prepared"]
            if oci_prepared:
                self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        connection.info.pop("cx_oracle_prepared", None)


dialect = OracleDialect_cx_oracle
